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

	// ReplayHeld says the sidecar holds committed changes back until the local
	// standby replays them (QS_GATE_ON_REPLAY). ReplayLSN is the standby's
	// replay position and NeedLSN the commit end the oldest held change waits
	// for; HeldTransactions counts them.
	ReplayHeld       bool   `json:"replay_held,omitempty"`
	ReplayLSN        string `json:"replay_lsn,omitempty"`
	NeedLSN          string `json:"need_lsn,omitempty"`
	HeldTransactions int    `json:"held_transactions,omitempty"`

	// StoodDown says this node is the primary and the sidecar has stood down:
	// nothing feeds the mirror, so AppliedLSN is empty and the lag fields are
	// zero. It is NOT a freshness claim; the reason in /readyz says so.
	StoodDown bool `json:"stood_down,omitempty"`
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

	// The hold on committed changes the local standby has not replayed yet
	// (see RecordReplayHold). Held is false when nothing is held.
	replayHeld bool
	replayLSN  string
	needLSN    string
	heldTxns   int
	// slotFault is why this Pod's replication slot cannot be read, when that is
	// the case: a stream that fails the same way on every attempt is not fresh,
	// whatever its lag says, and the reason is the first thing an operator needs
	// (slotrecover.go). Empty when there is no such fault.
	slotFault      string
	slotRecoveries int64
	// standingDown is set on a node that is the primary (the sidecar stands
	// down there, main.go). The mirror is not fed, so the applied position,
	// the head and the lag are not reported as if they were current.
	standingDown bool
}

// SetStoodDown records whether this sidecar has stood down because its node is
// the primary (false again when the node is a standby). Set on promotion, so
// that /readyz stops reporting the last applied LSN and its lag as current.
func (h *Health) SetStoodDown(on bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.standingDown = on
}

// StoodDown reports whether SetStoodDown(true) is in effect.
func (h *Health) StoodDown() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.standingDown
}

// SetSlotFault says why the replication slot cannot be read ("" clears it). While
// it is set the Pod is not ready, and the reason is what readiness reports.
func (h *Health) SetSlotFault(reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.slotFault = reason
}

// RecordSlotRecovery counts a slot this Pod dropped and re-created to recover.
func (h *Health) RecordSlotRecovery() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.slotRecoveries++
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

// RecordReplayHold says committed changes are held back because the local
// standby has replayed only through replayLSN, and the oldest of them ends at
// needLSN. While it holds, the mirror is not caught up: the lag keeps growing
// from the last time it was, and readiness says why (readyApartFromHalts).
func (h *Health) RecordReplayHold(replayLSN, needLSN string, held int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.replayHeld, h.replayLSN, h.needLSN, h.heldTxns = true, replayLSN, needLSN, held
}

// ClearReplayHold says the standby has replayed what was held, or nothing was.
func (h *Health) ClearReplayHold() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.replayHeld, h.replayLSN, h.needLSN, h.heldTxns = false, "", "", 0
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
		ReplayHeld:        h.replayHeld, ReplayLSN: h.replayLSN, NeedLSN: h.needLSN,
		HeldTransactions: h.heldTxns,
	}
	if h.standingDown {
		// The last applied LSN is the standby's, and nothing has fed it since
		// the promotion: it is not reported as the mirror's position, and the
		// lag since the last catch-up is not a lag of anything.
		s.StoodDown = true
		s.AppliedLSN, s.HeadLSN = "", ""
		s.LagBytes, s.LagSeconds = 0, 0
		s.ReplayHeld, s.ReplayLSN, s.NeedLSN, s.HeldTransactions = false, "", "", 0
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
	slotFault := h.slotFault
	h.mu.Unlock()
	if s.StoodDown {
		// This is the primary. /readyz is the readiness probe of the whole
		// instance Pod in takeover mode (go/internal/plugin/lifecycle.go), so
		// a 503 here would take the PRIMARY out of its Services. The mirror is
		// not fed, which is the extension's to refuse (it answers from the heap
		// on a node that is not a standby), so nothing here is "not ready".
		return true, "stood down: this node is the primary; its mirror is not fed and is not read here " +
			"(the router answers from the heap on a node that is not a standby)"
	}
	if slotFault != "" {
		// Nothing else is worth saying while the stream cannot be read: the
		// lag and the halts would be stale for the same reason.
		return false, slotFault
	}
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
		if s.ReplayHeld {
			// Not a stall and not a fault: the standby has not replayed the
			// commits this mirror is holding back, and it will not show them
			// until it has. Say so, with the two positions, so that nobody
			// goes looking for a hung sidecar.
			return false, fmt.Sprintf("waiting for local replay: %d change(s) held back until "+
				"the standby replays through %s; standby replay is at %s, lag %.1fs exceeds freshnessSLO %.1fs",
				s.HeldTransactions, s.NeedLSN, s.ReplayLSN, s.LagSeconds, h.slo.Seconds())
		}
		return false, fmt.Sprintf("lag %.1fs exceeds freshnessSLO %.1fs",
			s.LagSeconds, h.slo.Seconds())
	}
	if s.ReplayHeld {
		return true, fmt.Sprintf("lag %.1fs within SLO %.1fs; waiting for local replay: %d change(s) held "+
			"back until the standby replays through %s (replay is at %s)",
			s.LagSeconds, h.slo.Seconds(), s.HeldTransactions, s.NeedLSN, s.ReplayLSN)
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
	stood := 0
	if s.StoodDown {
		stood = 1
	}
	hb.WriteString("# HELP quicksilver_stood_down 1 while this node is the primary and the sidecar has stood down (the mirror is not fed).\n" +
		"# TYPE quicksilver_stood_down gauge\n")
	fmt.Fprintf(&hb, "quicksilver_stood_down %d\n", stood)
	h.mu.Lock()
	recoveries := h.slotRecoveries
	h.mu.Unlock()
	hb.WriteString("# HELP quicksilver_slot_recoveries_total Times this Pod dropped its replication slot because the slot could not be decoded (slotrecover.go).\n" +
		"# TYPE quicksilver_slot_recoveries_total counter\n")
	fmt.Fprintf(&hb, "quicksilver_slot_recoveries_total %d\n", recoveries)
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
