package mirror

import (
	"fmt"
	"strings"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// The view gives an unconstrained numeric back as the digits PostgreSQL
// printed, whatever their number, and gives the same text for a value whether
// it is read from a file of the previous release (DECIMAL(38,9), which lost
// the display scale) or from the file the next rewrite makes of it. It used
// to cast to DOUBLE: 1e400 read as inf and a 29-digit value kept 17.
func TestUnconstrainedNumericIsExactInTheView(t *testing.T) {
	defer func(prev string) { NumericScale = prev }(NumericScale)
	NumericScale = ""
	cols := map[string]string{"id": "bigint", "n": "numeric"}
	order := []string{"id", "n"}
	dir := t.TempDir()
	huge := "1" + strings.Repeat("0", 400)
	long := "12345678901234567890123456789.123456789"
	old := []string{"1.50", "12.5", "100", "0", "-0.000000001", "0.000"}
	var tbl *Table
	withLegacyWriter(func() {
		tbl, _ = New(dir, "public", "num", "id", cols, order)
		for i, v := range old {
			tapply(t, tbl, i+1, typedChange(changestream.OpInsert, "num", map[string]any{"id": fmt.Sprint(i), "n": v}))
		}
		if _, err := tbl.Compact(); err != nil {
			t.Fatal(err)
		}
		tbl.Close()
	})
	tbl, _ = New(dir, "public", "num", "id", cols, order)
	defer tbl.Close()
	for i, v := range []string{huge, long, "NaN", "1.50"} {
		tapply(t, tbl, 100+i, typedChange(changestream.OpInsert, "num", map[string]any{"id": fmt.Sprint(100 + i), "n": v}))
	}
	if s := sqlType(tbl.typeOf("n")); s != "VARCHAR" {
		t.Fatalf("an unconstrained numeric is presented as %s", s)
	}
	read := func(step string) []string {
		sql, err := tbl.ViewSQL()
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(sql, "DOUBLE") {
			t.Fatalf("%s: the view approximates:\n%s", step, sql)
		}
		if !haveDuck() {
			return nil
		}
		return duck(t, "SELECT id, n, typeof(n) FROM ("+sql+") ORDER BY id")
	}
	want := []string{"0\t1.5\tVARCHAR", "1\t12.5\tVARCHAR", "2\t100\tVARCHAR", "3\t0\tVARCHAR",
		"4\t-0.000000001\tVARCHAR", "5\t0\tVARCHAR",
		"100\t" + huge + "\tVARCHAR", "101\t" + long + "\tVARCHAR", "102\tNaN\tVARCHAR", "103\t1.50\tVARCHAR"}
	mixed := read("old and new files")
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	if ft := fileTypes(t, dataFiles(tbl)[0]); ft["n"] != "VARCHAR" {
		t.Fatalf("after a rewrite the column is stored as %v", ft)
	}
	rewritten := read("after the rewrite")
	if !haveDuck() {
		return
	}
	if strings.Join(mixed, "\n") != strings.Join(want, "\n") {
		t.Errorf("over old and new files DuckDB reads\n%.900s\nexpected\n%.900s", strings.Join(mixed, "\n"), strings.Join(want, "\n"))
	}
	if strings.Join(rewritten, "\n") != strings.Join(mixed, "\n") {
		t.Errorf("the rewrite changed what the view returns:\n%.900s\nbefore:\n%.900s", strings.Join(rewritten, "\n"), strings.Join(mixed, "\n"))
	}
}
