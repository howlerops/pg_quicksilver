package mirror

import (
	"fmt"
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

// An unconstrained numeric PRIMARY KEY across the upgrade. The old binary
// stored it as DECIMAL(38,9); this one stores the text PostgreSQL prints. Do
// the two compare equal in the key index? For a key without trailing zeros,
// yes. For one the source prints with them (1.50, 7.0), NO: the decimal lost
// the display scale, the file gives back 1.5, the stream says 1.50, and the
// update lands beside the row it should have replaced.
//
// That cannot be repaired from the files (the scale is gone), so it is
// flagged: LegacyFormatColumns names the key column for a rebuild. This test
// pins both halves — the damage, so that it is not mistaken for fixed, and
// the flag.
func TestLegacyNumericPrimaryKeyIsFlaggedBecauseKeysDoNotCompareEqual(t *testing.T) {
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

		// The flag, before a single change is applied.
		got := legacyColumns(t, tbl)
		if len(got) != 1 || got[0].Column != "k" || got[0].Remedy != "rebuild" ||
			!strings.Contains(got[0].Problem, "primary key") || !strings.Contains(got[0].Problem, "1.50") {
			t.Fatalf("compacted=%v: the legacy numeric key is not flagged: %+v", compacted, got)
		}

		// The damage it warns of: every key is updated once, as the stream
		// would send it.
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
		// 12345 and 0.000000001 found their rows; 1.50 and 7.0 did not.
		if len(rows) != 6 || !stale["1.5"] || !stale["7"] || len(stale) != 2 {
			t.Fatalf("compacted=%v: %d live rows for 4 keys, stale %v. If this is now 4 and none, the keys "+
				"compare equal and the rule in format.go can go", compacted, len(rows), stale)
		}
		tbl.Close()
	}

	// A numeric key this binary wrote is text on both sides, and is not flagged.
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
