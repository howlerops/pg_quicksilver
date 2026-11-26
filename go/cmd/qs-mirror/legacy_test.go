package main

import (
	"strings"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

func TestDecideLegacy(t *testing.T) {
	cases := []struct {
		name         string
		legacy       int
		halted, auto bool
		want         legacyAction
	}{
		{"clean table does nothing", 0, false, false, legacyAction{}},
		{"clean table with auto on still does nothing", 0, false, true, legacyAction{}},
		{"legacy, auto off: warn and report only", 2, false, false,
			legacyAction{Warn: true, Metric: true}},
		{"legacy, auto on: halt and request a rebuild", 2, false, true,
			legacyAction{Warn: true, Metric: true, Halt: true, Request: true}},
		{"legacy and already halted for another reason: never auto-rebuilt", 2, true, true,
			legacyAction{Warn: true, Metric: true}},
		{"legacy and already halted, auto off", 1, true, false,
			legacyAction{Warn: true, Metric: true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := decideLegacy(c.legacy, c.halted, c.auto); got != c.want {
				t.Errorf("decideLegacy(%d, halted=%v, auto=%v) = %+v, want %+v",
					c.legacy, c.halted, c.auto, got, c.want)
			}
		})
	}
}

func TestAutoRebuildLegacyEnv(t *testing.T) {
	for v, want := range map[string]bool{
		"":      false,
		"false": false,
		"0":     false,
		"no":    false,
		"true":  true,
		"TRUE":  true,
		" 1 ":   true,
		"yes":   true,
		"on":    true,
	} {
		t.Setenv("QS_AUTO_REBUILD_LEGACY", v)
		if got := autoRebuildLegacy(); got != want {
			t.Errorf("QS_AUTO_REBUILD_LEGACY=%q: got %v, want %v", v, got, want)
		}
	}
}

func TestLegacyReasonAndNames(t *testing.T) {
	cols := []mirror.LegacyColumn{{Column: "ip"}, {Column: "mac"}}
	if got := legacyColumnNames(cols); got != "ip, mac" {
		t.Errorf("column names = %q", got)
	}
	if r := legacyReason("public.hosts", cols); !strings.Contains(r, "public.hosts") ||
		!strings.Contains(r, "ip, mac") {
		t.Errorf("halt reason does not name the table and columns: %q", r)
	}
}

func TestLegacyMetricsSeries(t *testing.T) {
	r := &legacyReport{tables: map[string]bool{}}
	if out := r.Metrics(); strings.Contains(out, "quicksilver_table_needs_rebuild{") {
		t.Errorf("no table marked, but a series was rendered:\n%s", out)
	}
	r.mark("public.b")
	r.mark("public.a")
	out := r.Metrics()
	for _, want := range []string{
		`quicksilver_table_needs_rebuild{table="public.a"} 1`,
		`quicksilver_table_needs_rebuild{table="public.b"} 1`,
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Index(out, "public.a") > strings.Index(out, "public.b") {
		t.Errorf("series are not sorted by table:\n%s", out)
	}
}
