package main

import (
	"fmt"
	"strings"
	"sync"
)

// spoolSource is what the spool gauges read from the change stream. The
// stream's own methods satisfy it; a test supplies a fake.
type spoolSource interface {
	SpoolBytes() int64
	SpoolBudgetBytes() int64
	SpoolPaused() bool
}

// spoolGauges exports the change stream's spool: how much of the WAL it has
// on disk, the budget that bounds it, and whether the receiver has stopped
// reading the socket because the budget is full. A stream is attached for the
// life of one runMirror call and detached when it returns; between two of
// them the gauges read zero.
type spoolGauges struct {
	mu  sync.Mutex
	src spoolSource
}

var spool = &spoolGauges{}

func (g *spoolGauges) attach(s spoolSource) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.src = s
}

// Metrics renders the three gauges in the Prometheus text format, for
// health.AddMetrics.
func (g *spoolGauges) Metrics() string {
	g.mu.Lock()
	src := g.src
	g.mu.Unlock()

	var bytes, budget int64
	var paused bool
	if src != nil {
		bytes, budget, paused = src.SpoolBytes(), src.SpoolBudgetBytes(), src.SpoolPaused()
	}
	var b strings.Builder
	b.WriteString("# HELP quicksilver_spool_bytes Bytes of WAL spooled to disk by the change stream: transactions arriving plus committed ones not yet confirmed.\n")
	b.WriteString("# TYPE quicksilver_spool_bytes gauge\n")
	fmt.Fprintf(&b, "quicksilver_spool_bytes %d\n", bytes)
	b.WriteString("# HELP quicksilver_spool_budget_bytes The bound quicksilver_spool_bytes is held to; negative when unbounded, zero when not yet derived.\n")
	b.WriteString("# TYPE quicksilver_spool_budget_bytes gauge\n")
	fmt.Fprintf(&b, "quicksilver_spool_budget_bytes %d\n", budget)
	b.WriteString("# HELP quicksilver_spool_paused 1 while the change stream has stopped reading the socket because the spool is at its budget.\n")
	b.WriteString("# TYPE quicksilver_spool_paused gauge\n")
	p := 0
	if paused {
		p = 1
	}
	fmt.Fprintf(&b, "quicksilver_spool_paused %d\n", p)
	return b.String()
}
