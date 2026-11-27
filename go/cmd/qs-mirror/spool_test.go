package main

import (
	"strings"
	"testing"
)

type fakeSpool struct {
	bytes, budget int64
	paused        bool
}

func (f fakeSpool) SpoolBytes() int64       { return f.bytes }
func (f fakeSpool) SpoolBudgetBytes() int64 { return f.budget }
func (f fakeSpool) SpoolPaused() bool       { return f.paused }

func hasLine(out, line string) bool {
	for _, l := range strings.Split(out, "\n") {
		if l == line {
			return true
		}
	}
	return false
}

func TestSpoolMetricsReadAttachedStream(t *testing.T) {
	g := &spoolGauges{}
	g.attach(fakeSpool{bytes: 4096, budget: 8192, paused: true})
	out := g.Metrics()
	for _, want := range []string{
		"quicksilver_spool_bytes 4096",
		"quicksilver_spool_budget_bytes 8192",
		"quicksilver_spool_paused 1",
	} {
		if !hasLine(out, want) {
			t.Errorf("missing line %q in:\n%s", want, out)
		}
	}
	for _, name := range []string{"quicksilver_spool_bytes", "quicksilver_spool_budget_bytes", "quicksilver_spool_paused"} {
		if !strings.Contains(out, "# TYPE "+name+" gauge\n") {
			t.Errorf("%s is not declared as a gauge:\n%s", name, out)
		}
	}
}

func TestSpoolMetricsNotPaused(t *testing.T) {
	g := &spoolGauges{}
	g.attach(fakeSpool{bytes: 0, budget: -1, paused: false})
	out := g.Metrics()
	if !hasLine(out, "quicksilver_spool_paused 0") {
		t.Errorf("paused should be 0:\n%s", out)
	}
	if !hasLine(out, "quicksilver_spool_budget_bytes -1") {
		t.Errorf("an unbounded budget should be reported as -1:\n%s", out)
	}
}

func TestSpoolMetricsWithoutStreamReadZero(t *testing.T) {
	g := &spoolGauges{}
	out := g.Metrics()
	for _, want := range []string{
		"quicksilver_spool_bytes 0",
		"quicksilver_spool_budget_bytes 0",
		"quicksilver_spool_paused 0",
	} {
		if !hasLine(out, want) {
			t.Errorf("with no stream attached, missing %q:\n%s", want, out)
		}
	}
	g.attach(fakeSpool{bytes: 7, budget: 9, paused: true})
	g.attach(nil)
	if !hasLine(g.Metrics(), "quicksilver_spool_bytes 0") {
		t.Errorf("after detach the gauges must read zero again")
	}
}
