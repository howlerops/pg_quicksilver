package mirror

// The generated view is checked by bench/scripts/serve_compare.py, which
// evaluates it in DuckDB against the live source and compares value by value —
// that is the real test and it cannot run from here. These cover what can be
// checked without an engine: that the SQL names the right files, that the
// refusals are refusals, and that a mirror nobody should be reading does not
// hand out a way to read it.

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func TestViewSQLNamesTheManifestAndItsVectors(t *testing.T) {
	tbl := racyTable(t)
	var seed []changestream.Change
	for i := 1; i <= 50; i++ {
		row := map[string]any{"id": itoa(i), "v": "x", "n": "0"}
		seed = append(seed, changestream.Change{Op: changestream.OpInsert,
			Schema: "public", Table: "r", Row: row, Key: row})
	}
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: "0/1", NextLSN: "0/1", Changes: seed,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	// an update, so there is a deletion vector on the base file and a delta
	row := map[string]any{"id": "7", "v": "y", "n": "1"}
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: "0/2", NextLSN: "0/2", Changes: []changestream.Change{{
			Op: changestream.OpUpdate, Schema: "public", Table: "r",
			Row: row, Key: row,
		}},
	}}); err != nil {
		t.Fatal(err)
	}

	sql, err := tbl.ViewSQL()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range tbl.State.BaseFiles {
		if !strings.Contains(sql, filepath.Join(tbl.Dir, "base", f)) {
			t.Errorf("the view does not name base file %s", f)
		}
	}
	for _, d := range tbl.State.DeltaFiles {
		if !strings.Contains(sql, filepath.Join(tbl.Dir, "delta", d)) {
			t.Errorf("the view does not name delta file %s", d)
		}
	}
	// The deletion vector is the difference between the view and a glob, so its
	// absence would be the whole bug rather than a detail.
	if !strings.Contains(sql, ".dv.parquet") {
		t.Error("the view applies no deletion vector, so it would return the " +
			"superseded copy of row 7 as well as the current one")
	}
	if !strings.Contains(sql, "file_row_number NOT IN") {
		t.Error("the deletion vector is named but not applied")
	}
	for _, c := range tbl.Order {
		if !strings.Contains(sql, `"`+c+`"`) {
			t.Errorf("column %s is missing from the view", c)
		}
	}
}

// file_row_number is a generated column, and asking for it stops an engine
// answering count(*) from the Parquet footer. It belongs on the files that have
// a deletion vector and on no others -- getting that backwards either costs
// every query a file open (too many) or resurrects retired rows (too few).
func TestViewSQLAsksForRowNumbersOnlyWhereAVectorNeedsThem(t *testing.T) {
	tbl := racyTable(t)
	var seed []changestream.Change
	for i := 1; i <= 50; i++ {
		row := map[string]any{"id": itoa(i), "v": "x", "n": "0"}
		seed = append(seed, changestream.Change{Op: changestream.OpInsert,
			Schema: "public", Table: "r", Row: row, Key: row})
	}
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: "0/1", NextLSN: "0/1", Changes: seed,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}

	// Insert-only so far: nothing has been superseded, so no file carries a
	// vector and no branch should pay for row numbers.
	sql, err := tbl.ViewSQL()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "file_row_number") {
		t.Errorf("an insert-only mirror asks for file_row_number, which costs a "+
			"metadata-only count(*) a file open per branch:\n%s", sql)
	}

	// Now supersede a row. Exactly the branches carrying a vector gain it.
	row := map[string]any{"id": "7", "v": "y", "n": "1"}
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: "0/2", NextLSN: "0/2", Changes: []changestream.Change{{
			Op: changestream.OpUpdate, Schema: "public", Table: "r",
			Row: row, Key: row,
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	sql, err = tbl.ViewSQL()
	if err != nil {
		t.Fatal(err)
	}
	// Every branch that applies a vector must also have asked for the column it
	// filters on, or the SQL does not even parse.
	if n, m := strings.Count(sql, "file_row_number NOT IN"),
		strings.Count(sql, "file_row_number = true"); n != m {
		t.Errorf("%d branches filter on file_row_number but %d ask for it:\n%s",
			n, m, sql)
	}
	if !strings.Contains(sql, "file_row_number NOT IN") {
		t.Error("the deletion vector is not applied, so row 7 reads twice")
	}
}

func TestViewSQLRefusesAHaltedMirror(t *testing.T) {
	tbl := racyTable(t)
	if err := tbl.Halt("a column was backfilled by a table rewrite"); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.ViewSQL(); err == nil {
		t.Fatal("a halted mirror handed out a view of itself")
	}
}

func TestViewSQLOfAnEmptyMirror(t *testing.T) {
	tbl := racyTable(t)
	sql, err := tbl.ViewSQL()
	if err != nil {
		t.Fatal(err)
	}
	// A view over an empty mirror has to PLAN, which means it has to have the
	// columns. Returning nothing at all makes every query against it a syntax
	// error rather than an empty result.
	for _, c := range tbl.Order {
		if !strings.Contains(sql, `"`+c+`"`) {
			t.Errorf("column %s is missing from the empty view", c)
		}
	}
	if !strings.Contains(sql, "WHERE false") {
		t.Errorf("the empty view is not empty: %s", sql)
	}
}

func TestViewSQLRefusesAManifestOlderThanItsVectors(t *testing.T) {
	tbl := racyTable(t)
	row := map[string]any{"id": "1", "v": "x", "n": "0"}
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: "0/1", NextLSN: "0/1", Changes: []changestream.Change{{
			Op: changestream.OpInsert, Schema: "public", Table: "r",
			Row: row, Key: row,
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	row2 := map[string]any{"id": "1", "v": "y", "n": "1"}
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: "0/2", NextLSN: "0/2", Changes: []changestream.Change{{
			Op: changestream.OpUpdate, Schema: "public", Table: "r",
			Row: row2, Key: row2,
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	// Remove the vector the manifest names, as reclaiming an old generation
	// would for a reader holding a manifest two writes behind. The view must
	// refuse rather than emit SQL that reads the file without it.
	rel := filepath.Join("base", tbl.State.BaseFiles[0])
	if err := os.Remove(tbl.dvPathGen(rel, tbl.State.DVGen[dvStem(rel)])); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.ViewSQL(); !errors.Is(err, ErrStaleManifest) {
		t.Fatalf("ViewSQL returned %v; expected ErrStaleManifest", err)
	}
}

func TestColumnsFromFiles(t *testing.T) {
	tbl := racyTable(t)
	row := map[string]any{"id": "1", "v": "x", "n": "2"}
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: "0/1", NextLSN: "0/1", Changes: []changestream.Change{{
			Op: changestream.OpInsert, Schema: "public", Table: "r",
			Row: row, Key: row,
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	cols, order, err := ColumnsFromFiles(filepath.Dir(tbl.Dir), "public", "r")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(order, ",") != strings.Join(tbl.Order, ",") {
		t.Errorf("column order came back %v, expected %v", order, tbl.Order)
	}
	// The types have to round-trip well enough that arrowType agrees with what
	// the writer used, or a view built from them reads columns as the wrong
	// type.
	for _, c := range order {
		if arrowType(cols[c]) != arrowType(tbl.Columns[c]) {
			t.Errorf("column %s recovered as %q, which is not the type it was "+
				"written as (%q)", c, cols[c], tbl.Columns[c])
		}
	}
}

func itoa(i int) string {
	return strings.TrimSpace(Render(int64(i)))
}
