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
	"sort"
	"strings"
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

	// HaltedTables maps each table that is out of service to the reason its
	// manifest records. The other tables on this Pod are still mirrored.
	HaltedTables map[string]string `json:"halted_tables,omitempty"`
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
	extra        []func() string

	// bootstrapped is false until the mirror has a complete copy of the source
	// to serve from. Without it a freshly-started node is READY: lag is
	// measured from process start, so it reads as zero, and the node joins the
	// read endpoint with an empty or half-written mirror. That is the same
	// mistake as the idle-vs-stale one, inverted — there, no data arriving was
	// read as staleness; here, no data having ARRIVED YET is read as freshness.
	bootstrapped bool

	// halted is table -> reason for every table this Pod has stopped mirroring.
	// It is filled from the manifests before anything connects to the source,
	// so a restarted Pod says "halted" and not "still bootstrapping".
	halted map[string]string
	// serveWithHalted keeps the Pod READY while some table is halted. Off by
	// default: a reader behind the Service cannot tell a Pod that mirrors three
	// tables from one that mirrors two, so the default is that it never meets
	// the second kind. See SetServeWithHalted.
	serveWithHalted bool
	// tables is how many tables this Pod is configured to mirror, so that
	// "some are halted" can be told from "all are". 0 means not told.
	tables int
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

// SetHalted records that a table is out of service and why. It reports whether
// that was news, so a caller polling the tables can log a halt exactly once.
func (h *Health) SetHalted(table, reason string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.halted == nil {
		h.halted = map[string]string{}
	}
	old, known := h.halted[table]
	h.halted[table] = reason
	return !known || old != reason
}

// ClearHalted records that a table is back in service (it was rebuilt).
func (h *Health) ClearHalted(table string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.halted, table)
}

// SetServeWithHalted chooses what one halted table does to the Pod. false (the
// default): the Pod is unready while any table is halted. true: the Pod stays
// ready on the strength of the healthy tables, and the halted one is refused
// table by table instead (Table.ViewSQL will not produce a view for it).
func (h *Health) SetServeWithHalted(on bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.serveWithHalted = on
}

// SetTables records how many tables this Pod mirrors. With every one of them
// halted there is nothing to serve, and SetServeWithHalted does not apply.
func (h *Health) SetTables(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tables = n
}

// maxHaltReason bounds one table's reason in the probe body and in a metric
// label. A halt reason quotes the offending value, and that can be a jsonb
// document; the kubelet keeps only the start of a probe body in the Pod's
// events, and a label of unbounded size is a cardinality problem besides.
const maxHaltReason = 160

// maxHaltedListed bounds how many tables the reason line names.
const maxHaltedListed = 5

func truncateReason(s string) string {
	s = strings.Join(strings.Fields(s), " ") // one line
	r := []rune(s)
	if len(r) <= maxHaltReason {
		return s
	}
	return string(r[:maxHaltReason]) + "…"
}

// HaltedReason renders the halted tables as one line: sorted, each with its
// reason, so two probes of the same state read identically. "" when none.
func HaltedReason(halted map[string]string) string {
	if len(halted) == 0 {
		return ""
	}
	names := make([]string, 0, len(halted))
	for t := range halted {
		names = append(names, t)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for i, t := range names {
		if i == maxHaltedListed {
			parts = append(parts, fmt.Sprintf("and %d more", len(names)-i))
			break
		}
		parts = append(parts, fmt.Sprintf("%s (%s)", t, truncateReason(halted[t])))
	}
	noun := "table is"
	if len(names) > 1 {
		noun = "tables are"
	}
	return fmt.Sprintf("%d %s halted and not mirrored: %s", len(names), noun, strings.Join(parts, "; "))
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
	if len(h.halted) > 0 {
		s.HaltedTables = make(map[string]string, len(h.halted))
		for t, r := range h.halted {
			s.HaltedTables[t] = r
		}
	}
	if !h.verifiedAt.IsZero() {
		s.VerifiedAgo = time.Since(h.verifiedAt).Seconds()
	}
	return s
}

func (h *Health) Ready() (bool, string) {
	s := h.Snapshot()
	h.mu.Lock()
	serve, tables := h.serveWithHalted, h.tables
	h.mu.Unlock()
	halted := HaltedReason(s.HaltedTables)
	// First, because it is the one condition here that does not pass by
	// itself: bootstrapping ends and lag recovers, a halt waits for an
	// operator. Reporting either of those instead sends them to the wrong
	// place — this used to read "mirror diverged" and, after a restart,
	// "still bootstrapping".
	if halted != "" && tables > 0 && len(s.HaltedTables) >= tables {
		// The switch keeps the HEALTHY tables in service. There are none.
		return false, halted + ". Every table is halted, so nothing is mirrored and the " +
			"replication slot has been dropped. " +
			"To rebuild a halted table, create a file named REBUILD in its mirror directory"
	}
	if halted != "" && !serve {
		return false, halted + ". The other tables are still mirrored; " +
			"this Pod is unready because QS_SERVE_WITH_HALTED_TABLES is not set. " +
			"To rebuild a halted table, create a file named REBUILD in its mirror directory and restart the sidecar"
	}
	ok, why := h.readyApartFromHalts(s)
	if ok && halted != "" {
		why += "; DEGRADED, serving the other tables (QS_SERVE_WITH_HALTED_TABLES): " + halted
	}
	return ok, why
}

func (h *Health) readyApartFromHalts(s Snapshot) (bool, string) {
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

// AddMetrics appends another component's Prometheus text to /metrics. The
// DuckLake publisher reports through it.
func (h *Health) AddMetrics(fn func() string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.extra = append(h.extra, fn)
}

func (h *Health) Metrics() string {
	s := h.Snapshot()
	h.mu.Lock()
	extra := append([]func() string(nil), h.extra...)
	h.mu.Unlock()
	var more strings.Builder
	for _, fn := range extra {
		more.WriteString(fn())
	}
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
	var hb strings.Builder
	hb.WriteString("# HELP quicksilver_halted_tables Tables this Pod has stopped mirroring until an operator rebuilds them.\n" +
		"# TYPE quicksilver_halted_tables gauge\n")
	fmt.Fprintf(&hb, "quicksilver_halted_tables %d\n", len(s.HaltedTables))
	hb.WriteString("# HELP quicksilver_table_halted 1 for each halted table, with the recorded reason.\n" +
		"# TYPE quicksilver_table_halted gauge\n")
	names := make([]string, 0, len(s.HaltedTables))
	for t := range s.HaltedTables {
		names = append(names, t)
	}
	sort.Strings(names)
	for _, t := range names {
		fmt.Fprintf(&hb, "quicksilver_table_halted{table=\"%s\",reason=\"%s\"} 1\n",
			labelEscape(t), labelEscape(truncateReason(s.HaltedTables[t])))
	}
	return fmt.Sprintf(`# HELP quicksilver_lag_seconds Seconds since the mirror was last caught up.
# TYPE quicksilver_lag_seconds gauge
quicksilver_lag_seconds %.3f
# HELP quicksilver_lag_bytes WAL bytes between source head and applied_lsn.
# TYPE quicksilver_lag_bytes gauge
quicksilver_lag_bytes %d
# HELP quicksilver_ready 1 if the readiness probe passes.
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
`, s.LagSeconds, s.LagBytes, ready, s.CompactionBacklog, diverged, boot, s.SlotRetainedBytes) + hb.String() + more.String()
}

// labelEscape escapes a Prometheus label value. A halt reason quotes column
// names and values, so it routinely contains double quotes.
func labelEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
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
