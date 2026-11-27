package main

// Legacy-format tables: data an older writer put in a spelling that no reader
// can vouch for (mirror.LegacyFormatColumns). The mirror cannot tell from the
// files alone whether such a value is wrong, so the only cure is a rebuild.
//
// At start, for each table that reports any:
//
//   - one WARN naming the table, the columns and the remedy;
//   - quicksilver_table_needs_rebuild{table="..."} 1 on /metrics;
//   - and, only when QS_AUTO_REBUILD_LEGACY is true, the table is rebuilt
//     through the path an operator's REBUILD file takes: the table is halted
//     with a reason that says why, the REBUILD marker is written, and the
//     start that follows finds it halted and requested (planHalted), so it is
//     re-snapshotted and put back in service by the same code.
//
// A table that is already halted for another reason is never auto-rebuilt:
// the halt is there to make a person look, and a rebuild would hide it.

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

// legacyAction is what start does for one table.
type legacyAction struct {
	Warn    bool // log the WARN
	Metric  bool // report quicksilver_table_needs_rebuild for the table
	Halt    bool // halt it now, with the legacy reason
	Request bool // write the REBUILD marker so the start that follows rebuilds it
}

// decideLegacy is the decision, pure so it can be tested. legacy is the number
// of columns LegacyFormatColumns reported; halted is whether the table is
// already halted; auto is QS_AUTO_REBUILD_LEGACY.
func decideLegacy(legacy int, halted, auto bool) legacyAction {
	if legacy == 0 {
		return legacyAction{}
	}
	a := legacyAction{Warn: true, Metric: true}
	if auto && !halted {
		a.Halt = true
		a.Request = true
	}
	return a
}

// autoRebuildLegacy reads QS_AUTO_REBUILD_LEGACY. Anything but an explicit yes
// is no, which is the default.
func autoRebuildLegacy() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("QS_AUTO_REBUILD_LEGACY"))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// legacyReason is the halt reason an automatic rebuild records. It is what an
// operator reads in /readyz and in the manifest while the rebuild is pending.
func legacyReason(qualified string, cols []mirror.LegacyColumn) string {
	return fmt.Sprintf("%s needs a rebuild: column(s) %s hold data from an older writer "+
		"(QS_AUTO_REBUILD_LEGACY is on, so the table is being re-snapshotted)",
		qualified, legacyColumnNames(cols))
}

func legacyColumnNames(cols []mirror.LegacyColumn) string {
	names := make([]string, 0, len(cols))
	for _, c := range cols {
		names = append(names, c.Column)
	}
	return strings.Join(names, ", ")
}

// legacyReport holds the tables that need a rebuild, for /metrics.
type legacyReport struct {
	mu     sync.Mutex
	tables map[string]bool
}

var legacy = &legacyReport{tables: map[string]bool{}}

func (r *legacyReport) mark(qualified string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tables[qualified] = true
}

// Metrics renders quicksilver_table_needs_rebuild, one series per table.
func (r *legacyReport) Metrics() string {
	r.mu.Lock()
	names := make([]string, 0, len(r.tables))
	for q := range r.tables {
		names = append(names, q)
	}
	r.mu.Unlock()
	sort.Strings(names)

	var b strings.Builder
	b.WriteString("# HELP quicksilver_table_needs_rebuild 1 for a table holding data from an older writer that needs a rebuild (see the WARN at start).\n")
	b.WriteString("# TYPE quicksilver_table_needs_rebuild gauge\n")
	for _, q := range names {
		fmt.Fprintf(&b, "quicksilver_table_needs_rebuild{table=%q} 1\n", q)
	}
	return b.String()
}

// checkLegacy applies decideLegacy to every table at start, before the halted
// plan is made, so a requested rebuild is seen by that plan.
func checkLegacy(log *slog.Logger, tables map[string]*mirror.Table, auto bool) error {
	qs := make([]string, 0, len(tables))
	for q := range tables {
		qs = append(qs, q)
	}
	sort.Strings(qs)
	for _, q := range qs {
		t := tables[q]
		cols, err := t.LegacyFormatColumns()
		if err != nil {
			// Not reported as clean: the footer that could not be read is named.
			log.Error("cannot check a table for legacy-format data", "table", q, "err", err)
			continue
		}
		a := decideLegacy(len(cols), t.Halted() != "", auto)
		if a.Metric {
			legacy.mark(q)
		}
		if a.Warn {
			log.Warn("table holds data from an older writer that cannot be trusted and needs a rebuild",
				"table", q, "columns", legacyColumnNames(cols),
				"remedy", "rebuild: touch "+filepath.Join(t.Dir, rebuildMarker)+
					" and restart, or set QS_AUTO_REBUILD_LEGACY=true")
		}
		if a.Halt {
			if err := t.Halt(legacyReason(q, cols)); err != nil {
				return fmt.Errorf("halting %s for its automatic rebuild: %w", q, err)
			}
			log.Warn("QS_AUTO_REBUILD_LEGACY is on: rebuilding the table from a snapshot", "table", q)
		}
		if a.Request {
			if err := os.WriteFile(filepath.Join(t.Dir, rebuildMarker), nil, 0o644); err != nil {
				return fmt.Errorf("writing the rebuild request for %s: %w", q, err)
			}
		}
	}
	return nil
}
