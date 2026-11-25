package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// LSNSource says how far the mirror has applied. The router compares that with
// the position of the server it is talking to and refuses the mirror when the
// gap is over the configured bound.
type LSNSource interface {
	AppliedLSN(ctx context.Context) (uint64, error)
}

// ParseLSN reads PostgreSQL's X/Y form.
func ParseLSN(s string) (uint64, error) {
	hi, lo, ok := strings.Cut(strings.TrimSpace(s), "/")
	if !ok {
		return 0, fmt.Errorf("not an LSN: %q", s)
	}
	var h, l uint64
	if _, err := fmt.Sscanf(hi, "%X", &h); err != nil {
		return 0, fmt.Errorf("not an LSN: %q", s)
	}
	if _, err := fmt.Sscanf(lo, "%X", &l); err != nil {
		return 0, fmt.Errorf("not an LSN: %q", s)
	}
	return h<<32 | l, nil
}

func lsnFromJSON(b []byte) (uint64, error) {
	var doc struct {
		AppliedLSN string `json:"applied_lsn"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		return 0, err
	}
	if doc.AppliedLSN == "" {
		return 0, fmt.Errorf("no applied_lsn")
	}
	return ParseLSN(doc.AppliedLSN)
}

// fileLSN reads applied_lsn from a JSON file: a table's state.json in the
// mirror directory has exactly that field.
type fileLSN struct{ path string }

func (f fileLSN) AppliedLSN(context.Context) (uint64, error) {
	b, err := os.ReadFile(f.path)
	if err != nil {
		return 0, err
	}
	return lsnFromJSON(b)
}

// httpLSN reads applied_lsn from the sidecar's /readyz body. The status code
// is ignored on purpose: /readyz answers 503 with the same body when the mirror
// is behind, and "how far behind" is this caller's decision, not the probe's.
type httpLSN struct {
	url    string
	client *http.Client
}

func (h httpLSN) AppliedLSN(ctx context.Context) (uint64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, err
	}
	return lsnFromJSON(b)
}

func newLSNSource(spec string) (LSNSource, error) {
	switch {
	case spec == "":
		return nil, nil
	case strings.HasPrefix(spec, "http://"), strings.HasPrefix(spec, "https://"):
		return httpLSN{url: spec, client: &http.Client{Timeout: 2 * time.Second}}, nil
	case strings.HasPrefix(spec, "file:"):
		return fileLSN{path: strings.TrimPrefix(spec, "file:")}, nil
	}
	return nil, fmt.Errorf("-mirror-lsn must be http(s)://... or file:/path, not %q", spec)
}

// freshness is the gate. Anything it cannot establish is "not fresh".
type freshness struct {
	src      LSNSource
	maxBytes uint64
	hold     time.Duration // how long one reading of the mirror's position is reused

	mu      sync.Mutex
	applied uint64
	err     error
	readAt  time.Time
}

func (f *freshness) appliedLSN(ctx context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readAt.IsZero() || time.Since(f.readAt) >= f.hold {
		f.applied, f.err = f.src.AppliedLSN(ctx)
		f.readAt = time.Now()
	}
	return f.applied, f.err
}

// check compares the mirror with the server's own position.
func (f *freshness) check(ctx context.Context, serverLSN uint64) (bool, string) {
	applied, err := f.appliedLSN(ctx)
	if err != nil {
		return false, "mirror position unknown: " + err.Error()
	}
	if serverLSN > applied && serverLSN-applied > f.maxBytes {
		return false, fmt.Sprintf("mirror is %d WAL bytes behind, over %d", serverLSN-applied, f.maxBytes)
	}
	return true, ""
}
