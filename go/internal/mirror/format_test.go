package mirror

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

var fmtCols = map[string]string{
	"id": "bigint", "v": "character(5)", "n": "bigint",
}
var fmtOrder = []string{"id", "v", "n"}

func fmtTable(t *testing.T, dir string) *Table {
	t.Helper()
	tbl, err := New(dir, "public", "r", "id", fmtCols, fmtOrder)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tbl.Close)
	return tbl
}

func versions(tbl *Table) string {
	var out []string
	for _, rel := range tbl.dataFilesRel() {
		out = append(out, fmt.Sprintf("%s=%d", filepath.Dir(rel), tbl.State.FileFormat[filepath.Base(rel)]))
	}
	return strings.Join(out, " ")
}

func legacyColumns(t *testing.T, tbl *Table) []LegacyColumn {
	t.Helper()
	got, err := tbl.LegacyFormatColumns()
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// The manifest records which writer format each file is at, and a file made
// from other files is as old as the oldest of them: copying a value does not
// change how it was spelled.
func TestManifestRecordsTheWriterFormatOfEachFile(t *testing.T) {
	dir := t.TempDir()
	// A binary from before versions were recorded: a base and a delta.
	withLegacyWriter(func() {
		tbl, _ := New(dir, "public", "r", "id", fmtCols, fmtOrder)
		applyR(t, tbl, "0/1", rChange(changestream.OpInsert, 1, "ab", 1), rChange(changestream.OpInsert, 2, "cd", 2))
		if _, err := tbl.Compact(); err != nil {
			t.Fatal(err)
		}
		applyR(t, tbl, "0/2", rChange(changestream.OpInsert, 3, "ef   ", 3))
		if got := versions(tbl); got != "base=0 delta=0" {
			t.Fatalf("the legacy writer recorded versions: %s", got)
		}
		tbl.Close()
	})

	tbl := fmtTable(t, dir)
	if st := manifestOf(t, tbl); st.FileFormat != nil {
		t.Fatalf("a legacy manifest has file_format %v", st.FileFormat)
	}
	// What this binary writes from the stream is at its own version.
	applyR(t, tbl, "0/3", rChange(changestream.OpInsert, 4, "gh   ", 4))
	if got := versions(tbl); got != fmt.Sprintf("base=0 delta=0 delta=%d", WriterFormatVersion) {
		t.Fatalf("after an Apply: %s", got)
	}
	if st := manifestOf(t, tbl); len(st.FileFormat) != 1 {
		t.Fatalf("on disk: %v", st.FileFormat)
	}
	// A merge of a legacy delta and a current one holds legacy values.
	if _, err := tbl.MergeDeltas(); err != nil {
		t.Fatal(err)
	}
	if got := versions(tbl); got != "base=0 delta=0" {
		t.Fatalf("after merging a legacy delta with a current one: %s", got)
	}
	// So does a rewrite of a legacy base, by whichever binary.
	applyR(t, tbl, "0/4", rChange(changestream.OpInsert, 5, "ij   ", 5))
	swapCompaction(t, tbl)
	if got := versions(tbl); got != "base=0" {
		t.Fatalf("after rewriting a legacy base: %s", got)
	}
	// It survives a restart, and a delta written after it is current again.
	tbl.Close()
	tbl = fmtTable(t, dir)
	applyR(t, tbl, "0/5", rChange(changestream.OpInsert, 6, "kl   ", 6))
	if got := versions(tbl); got != fmt.Sprintf("base=0 delta=%d", WriterFormatVersion) {
		t.Fatalf("after a restart: %s", got)
	}

	// The report: v is char(5), which ::text and the output function spell
	// differently, and a legacy base may hold snapshot rows.
	got := legacyColumns(t, tbl)
	if len(got) != 1 || got[0].Column != "v" || got[0].PGType != "character(5)" ||
		got[0].WriterVersion != 0 || got[0].Remedy != "rebuild" ||
		len(got[0].Files) != 1 || !strings.HasPrefix(got[0].Files[0], "base/") {
		t.Fatalf("report: %+v", got)
	}
	if !strings.Contains(got[0].Problem, "::text") || !strings.Contains(got[0].Problem, "DIVERGED") {
		t.Fatalf("the report does not say what is wrong: %s", got[0].Problem)
	}

	// The remedy is a rebuild, and after one the report is empty: the rows
	// were read from the source by this binary, whatever they replaced.
	if err := snapshotFrom(tbl, map[string]string{"1": "ab   /1", "2": "cd   /2"}, "0/9"); err != nil {
		t.Fatal(err)
	}
	if got := versions(tbl); got != fmt.Sprintf("base=%d", WriterFormatVersion) {
		t.Fatalf("after a rebuild: %s", got)
	}
	if got := legacyColumns(t, tbl); len(got) != 0 {
		t.Fatalf("after a rebuild the report still lists %+v", got)
	}
	// And a rewrite of current files stays current.
	applyR(t, tbl, "0/A", rChange(changestream.OpInsert, 7, "mn   ", 7))
	swapCompaction(t, tbl)
	if got := versions(tbl); got != fmt.Sprintf("base=%d", WriterFormatVersion) {
		t.Fatalf("after rewriting current files: %s", got)
	}
}

// What is and is not reported.
func TestLegacyFormatColumnsNamesOnlyWhatCannotBeRepaired(t *testing.T) {
	cols := map[string]string{
		"id": "bigint", "c": "char(3)", "ip": "inet", "net": "cidr", "mac": "macaddr",
		"b": "bit(4)", "m": "money", "iv": "interval", "tz": "time with time zone", "ips": "inet[]",
		// handled by read-time conversion, or never different: not reported
		"t": "text", "u": "uuid", "n": "numeric", "d": "numeric(10,2)", "ts": "timestamp with time zone", "vc": "varchar(5)",
	}
	order := []string{"id", "c", "ip", "net", "mac", "b", "m", "iv", "tz", "ips", "t", "u", "n", "d", "ts", "vc"}
	row := map[string]any{"id": "1", "c": "ab ", "ip": "10.0.0.1", "net": "10.0.0.0/8", "mac": "08:00:2b:01:02:03",
		"b": "1010", "m": "$1.00", "iv": "1 day", "tz": "10:00:00+00", "ips": "{10.0.0.1}",
		"t": "x", "u": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "n": "1.50", "d": "2.50",
		"ts": "2025-01-01 00:00:00+00", "vc": "ab"}
	dir := t.TempDir()

	// Legacy deltas only: written from the stream, in the stream's spelling.
	withLegacyWriter(func() {
		tbl, _ := New(dir, "public", "x", "id", cols, order)
		tapply(t, tbl, 1, typedChange(changestream.OpInsert, "x", row))
		tbl.Close()
	})
	tbl, _ := New(dir, "public", "x", "id", cols, order)
	if got := legacyColumns(t, tbl); len(got) != 0 {
		t.Fatalf("a legacy DELTA cannot hold snapshot rows, and %d columns were reported: %+v", len(got), got)
	}
	tbl.Close()

	// A legacy base.
	withLegacyWriter(func() {
		tbl, _ := New(dir, "public", "x", "id", cols, order)
		if _, err := tbl.Compact(); err != nil {
			t.Fatal(err)
		}
		tbl.Close()
	})
	tbl, _ = New(dir, "public", "x", "id", cols, order)
	defer tbl.Close()
	var names []string
	for _, lc := range legacyColumns(t, tbl) {
		names = append(names, lc.Column)
	}
	if got := strings.Join(names, ","); got != "b,c,ip,ips,iv,m,mac,net,tz" {
		t.Fatalf("reported %s", got)
	}

	// A table this binary wrote from the start reports nothing, whatever its
	// types.
	fresh, _ := New(t.TempDir(), "public", "x", "id", cols, order)
	defer fresh.Close()
	tapply(t, fresh, 1, typedChange(changestream.OpInsert, "x", row))
	if _, err := fresh.Compact(); err != nil {
		t.Fatal(err)
	}
	if got := legacyColumns(t, fresh); len(got) != 0 {
		t.Fatalf("a table written by this binary: %+v", got)
	}
}

// A numeric primary key is unique by VALUE: the source cannot hold both 1.5
// and 1.50. An earlier binary stored an unconstrained numeric as
// DECIMAL(38,9), which gives the key the source prints as 1.50 back as
// 1.500000000, and the stream sends 1.50. This test once pinned the defect
// that followed (an update left the old row beside the new one) and the rule
// that flagged such tables for a rebuild. The key is now the canonical numeric
// value wherever one is made, so the update finds its row and nothing is
// flagged.
func TestLegacyNumericPrimaryKeyComparesByValueAndNeedsNoRebuild(t *testing.T) {
	defer func(prev string) { NumericScale = prev }(NumericScale)
	NumericScale = ""
	cols := map[string]string{"k": "numeric", "v": "text"}
	order := []string{"k", "v"}
	keys := []string{"1.50", "12345", "0.000000001", "7.0"}
	for _, compacted := range []bool{true, false} {
		dir := t.TempDir()
		withLegacyWriter(func() {
			tbl, _ := New(dir, "public", "nk", "k", cols, order)
			for i, k := range keys {
				tapply(t, tbl, i+1, typedChange(changestream.OpInsert, "nk", map[string]any{"k": k, "v": "old"}))
			}
			if compacted {
				if _, err := tbl.Compact(); err != nil {
					t.Fatal(err)
				}
			}
			tbl.Close()
		})
		tbl, _ := New(dir, "public", "nk", "k", cols, order)
		if got := legacyColumns(t, tbl); len(got) != 0 {
			t.Fatalf("compacted=%v: a legacy numeric key is flagged for a rebuild it does not need: %+v", compacted, got)
		}

		// Every key is updated once, as the stream would send it.
		for i, k := range keys {
			tapply(t, tbl, 100+i, typedChange(changestream.OpUpdate, "nk", map[string]any{"k": k, "v": "new"}))
		}
		rows, err := tbl.Live()
		if err != nil {
			t.Fatal(err)
		}
		stale := map[string]bool{}
		for _, r := range rows {
			if r["v"] == "old" {
				stale[fmt.Sprint(r["k"])] = true
			}
		}
		if len(rows) != 4 || len(stale) != 0 {
			t.Fatalf("compacted=%v: %d live rows for 4 keys, stale %v", compacted, len(rows), stale)
		}
		// And a delete in yet another spelling of each key finds it too.
		for i, k := range []string{"1.5", "12345.0", "1e-9", "+07"} {
			tapply(t, tbl, 200+i, typedChange(changestream.OpDelete, "nk", map[string]any{"k": k}))
		}
		if rows, _ := tbl.Live(); len(rows) != 0 {
			t.Fatalf("compacted=%v: %d rows survive a delete of every key: %v", compacted, len(rows), rows)
		}
		tbl.Close()
	}

	// A numeric key this binary wrote behaves the same.
	fresh, _ := New(t.TempDir(), "public", "nk", "k", cols, order)
	defer fresh.Close()
	tapply(t, fresh, 1, typedChange(changestream.OpInsert, "nk", map[string]any{"k": "1.50", "v": "old"}))
	tapply(t, fresh, 2, typedChange(changestream.OpUpdate, "nk", map[string]any{"k": "1.50", "v": "new"}))
	if rows, _ := fresh.Live(); len(rows) != 1 || rows[0]["v"] != "new" {
		t.Fatalf("a current numeric key: %v", rows)
	}
	if got := legacyColumns(t, fresh); len(got) != 0 {
		t.Fatalf("a current numeric key is flagged: %+v", got)
	}
}

// A table flagged because of a legacy base stops being flagged when a TRUNCATE
// inside an Apply removes that base: the flag is a statement about the files
// the manifest names, and none of them is legacy any more. The rows the same
// transaction inserts after the TRUNCATE came from the stream, and neither
// they nor a later rewrite of them inherit the dead file's version.
func TestTruncateInAnApplyClearsTheLegacyFlag(t *testing.T) {
	dir := t.TempDir()
	withLegacyWriter(func() {
		tbl, _ := New(dir, "public", "r", "id", fmtCols, fmtOrder)
		applyR(t, tbl, "0/1", rChange(changestream.OpInsert, 1, "ab", 1), rChange(changestream.OpInsert, 2, "cd", 2))
		if _, err := tbl.Compact(); err != nil {
			t.Fatal(err)
		}
		tbl.Close()
	})
	for _, refill := range []bool{false, true} {
		sub := filepath.Join(t.TempDir(), "m")
		if err := os.CopyFS(sub, os.DirFS(dir)); err != nil {
			t.Fatal(err)
		}
		tbl := fmtTable(t, sub)
		if got := legacyColumns(t, tbl); len(got) != 1 || got[0].Column != "v" {
			t.Fatalf("the legacy base is not flagged to begin with: %+v", got)
		}
		changes := []changestream.Change{{Op: changestream.OpTruncate, Schema: "public", Table: "r"}}
		if refill {
			changes = append(changes, rChange(changestream.OpInsert, 7, "xy   ", 7))
		}
		applyR(t, tbl, "0/5", changes...)
		if got := legacyColumns(t, tbl); len(got) != 0 {
			t.Fatalf("refill=%v: flagged after the TRUNCATE removed every legacy file: %+v (%s)", refill, got, versions(tbl))
		}
		if refill {
			if got := versions(tbl); got != fmt.Sprintf("delta=%d", WriterFormatVersion) {
				t.Fatalf("the rows after the TRUNCATE inherited the dead file's version: %s", got)
			}
			swapCompaction(t, tbl)
			if got := versions(tbl); got != fmt.Sprintf("base=%d", WriterFormatVersion) {
				t.Fatalf("after rewriting them: %s", got)
			}
		}
		// It is the manifest that says so, not this process's memory.
		tbl.Close()
		tbl = fmtTable(t, sub)
		if got := legacyColumns(t, tbl); len(got) != 0 {
			t.Fatalf("refill=%v: flagged again after a restart: %+v", refill, got)
		}
	}
}
