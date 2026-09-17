// Package health provides freshness tracking, readiness gating and metrics
// (goals G4 and G7).
//
// docs/08 makes readiness gating non-negotiable in phase 1: retrofitting a
// safety property is how you get an incident first. The mechanism is
// deliberately dumb — when staleness exceeds freshnessSLO the pod fails its
// readiness probe, Kubernetes drops it from the Service endpoints, and queries
// go elsewhere. That is what turns bounded staleness from a claim into an
// enforced property.
//
// Staleness is measured from when the mirror was last CAUGHT UP, not from the
// last apply. Those differ on an idle source: no writes means no applies, and
// keying off last-apply would pull a perfectly fresh mirror out of service for
// no reason. (Found by the Python reference's phase 1 harness.)
package health

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

type Snapshot struct {
	AppliedLSN        string  `json:"applied_lsn"`
	HeadLSN           string  `json:"head_lsn"`
	LagBytes          uint64  `json:"lag_bytes"`
	LagSeconds        float64 `json:"lag_seconds"`
	CompactionBacklog int     `json:"compaction_backlog"`
	Diverged          bool    `json:"diverged"`
	Bootstrapped      bool    `json:"bootstrapped"`
	VerifiedAgo       float64 `json:"verified_seconds_ago"`

	// SlotRetainedBytes is how much WAL this mirror's replication slot is
	// pinning on the PRIMARY. It is the one number here that describes damage
	// the mirror is doing to something else, which is why it is exported even
	// though nothing gates on it: an operator watching a mirror fall behind
	// wants to see this climbing long before the guard fires (slotguard.go).
	SlotRetainedBytes int64 `json:"slot_retained_bytes"`
}

type Health struct {
	slo time.Duration

	mu           sync.Mutex
	appliedLSN   string
	headLSN      string
	lagBytes     uint64
	caughtUpAt   time.Time
	backlog      int
	diverged     bool
	verifiedAt   time.Time
	slotRetained int64

	// bootstrapped is false until the mirror has a complete copy of the source
	// to serve from. Without it a freshly-started node is READY: lag is
	// measured from process start, so it reads as zero, and the node joins the
	// read endpoint with an empty or half-written mirror. That is the same
	// mistake as the idle-vs-stale one, inverted — there, no data arriving was
	// read as staleness; here, no data having ARRIVED YET is read as freshness.
	bootstrapped bool
}

func New(slo time.Duration) *Health {
	return &Health{slo: slo, caughtUpAt: time.Now()}
}

// RecordSlotRetention notes how much WAL the slot is holding on the primary.
func (h *Health) RecordSlotRetention(b int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.slotRetained = b
}

func (h *Health) RecordApply(appliedLSN, headLSN string, backlog int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.appliedLSN, h.headLSN, h.backlog = appliedLSN, headLSN, backlog
	if a, b := changestream.ParseLSN(appliedLSN), changestream.ParseLSN(headLSN); b > a {
		h.lagBytes = b - a
	} else {
		h.lagBytes = 0
	}
}

// RecordCaughtUp marks the mirror current. Call on every empty poll — it is
// what keeps an idle database from being reported as stale.
func (h *Health) RecordCaughtUp() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.caughtUpAt = time.Now()
}

// RecordBootstrapped marks the mirror as having a complete copy to serve.
// Called once the initial snapshot finishes, or immediately on resume when the
// mirror already carries a durable applied_lsn from a previous run.
func (h *Health) RecordBootstrapped() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.bootstrapped = true
	h.caughtUpAt = time.Now()
}

func (h *Health) RecordVerification(diverged bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.diverged, h.verifiedAt = diverged, time.Now()
}

func (h *Health) Snapshot() Snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := Snapshot{
		AppliedLSN: h.appliedLSN, HeadLSN: h.headLSN, LagBytes: h.lagBytes,
		LagSeconds:        time.Since(h.caughtUpAt).Seconds(),
		CompactionBacklog: h.backlog, Diverged: h.diverged,
		Bootstrapped:      h.bootstrapped,
		SlotRetainedBytes: h.slotRetained,
	}
	if !h.verifiedAt.IsZero() {
		s.VerifiedAgo = time.Since(h.verifiedAt).Seconds()
	}
	return s
}

func (h *Health) Ready() (bool, string) {
	s := h.Snapshot()
	if !s.Bootstrapped {
		return false, "still bootstrapping — no complete copy of the source yet"
	}
	if s.Diverged {
		return false, "mirror diverged from source — verification failed"
	}
	if s.LagSeconds > h.slo.Seconds() {
		return false, fmt.Sprintf("lag %.1fs exceeds freshnessSLO %.1fs",
			s.LagSeconds, h.slo.Seconds())
	}
	return true, fmt.Sprintf("lag %.1fs within SLO %.1fs", s.LagSeconds, h.slo.Seconds())
}

func (h *Health) Metrics() string {
	s := h.Snapshot()
	ok, _ := h.Ready()
	ready := 0
	if ok {
		ready = 1
	}
	diverged := 0
	if s.Diverged {
		diverged = 1
	}
	boot := 0
	if s.Bootstrapped {
		boot = 1
	}
	return fmt.Sprintf(`# HELP quicksilver_lag_seconds Seconds since the mirror was last caught up.
# TYPE quicksilver_lag_seconds gauge
quicksilver_lag_seconds %.3f
# HELP quicksilver_lag_bytes WAL bytes between source head and applied_lsn.
# TYPE quicksilver_lag_bytes gauge
quicksilver_lag_bytes %d
# HELP quicksilver_ready 1 if the mirror is inside its freshness SLO.
# TYPE quicksilver_ready gauge
quicksilver_ready %d
# HELP quicksilver_compaction_backlog Delta files awaiting compaction.
# TYPE quicksilver_compaction_backlog gauge
quicksilver_compaction_backlog %d
# HELP quicksilver_diverged 1 if verification found a mismatch.
# TYPE quicksilver_diverged gauge
quicksilver_diverged %d
# HELP quicksilver_bootstrapped 1 once the mirror holds a complete copy to serve.
# TYPE quicksilver_bootstrapped gauge
quicksilver_bootstrapped %d
# HELP quicksilver_slot_retained_bytes WAL this mirror's slot pins on the PRIMARY
# TYPE quicksilver_slot_retained_bytes gauge
quicksilver_slot_retained_bytes %d
`, s.LagSeconds, s.LagBytes, ready, s.CompactionBacklog, diverged, boot, s.SlotRetainedBytes)
}

// Serve exposes /readyz (the Kubernetes readiness probe), /healthz (liveness —
// never gates on lag) and /metrics.
func (h *Health) Serve(addr string) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ok, why := h.Ready()
		code := http.StatusOK
		if !ok {
			code = http.StatusServiceUnavailable
		}
		body := struct {
			Ready  bool   `json:"ready"`
			Reason string `json:"reason"`
			Snapshot
		}{ok, why, h.Snapshot()}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(body)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"alive":true}`))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(h.Metrics()))
	})

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	return srv, nil
}
