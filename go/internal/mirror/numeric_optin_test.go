package mirror

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// captureLog collects what the package logs for the length of a test.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

var numCols = map[string]string{"id": "bigint", "amount": "numeric", "rate": "numeric"}
var numOrder = []string{"id", "amount", "rate"}

func numTable(t *testing.T, dir string) *Table {
	t.Helper()
	tbl, err := New(dir, "public", "num", "id", numCols, numOrder)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tbl.Close)
	return tbl
}

func numRow(id int, amount, rate string) changestream.Change {
	return typedChange(changestream.OpInsert, "num",
		map[string]any{"id": fmt.Sprint(id), "amount": amount, "rate": rate})
}

func manifestOf(t *testing.T, tbl *Table) State {
	t.Helper()
	var st State
	b, err := os.ReadFile(tbl.statePath())
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatal(err)
	}
	return st
}

// The defect. A column that holds exact text is named in QS_NUMERIC_SCALE
// with a scale one of its values does not fit. The view used to cast the text
// to the decimal at once — an engine's cast rounds, so 0.123456 read as
// 0.1235 — and the next write that carried the value forward halted the
// table. Now the stored values are checked first, the option is refused for
// that column with a line naming the value, and the column goes on being
// exact text: never rounded, never halted.
func TestNumericOptInIsRefusedWhenAStoredValueDoesNotFit(t *testing.T) {
	defer func(prev string) { NumericScale = prev }(NumericScale)
	log := captureLog(t)
	NumericScale = ""
	dir := t.TempDir()
	tbl := numTable(t, dir)
	tapply(t, tbl, 1, numRow(1, "1.50", "2"), numRow(2, "0.123456", "3"), numRow(3, "12.3400", "4"))
	tbl.Close()

	NumericScale = "public.num.amount=4,public.num.rate=2"
	tbl = numTable(t, dir)

	// Before anything has been checked: text, in the type and in the view.
	if got := arrowType(tbl.typeOf("amount")).String(); got != "utf8" {
		t.Fatalf("a column holding unchecked text is exposed as %s the moment the option is set", got)
	}
	sql, err := tbl.ViewSQL()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "DECIMAL") {
		t.Fatalf("the view casts unchecked text to a decimal, which rounds what does not fit:\n%s", sql)
	}
	if got := tbl.NumericOptIns(); len(got) != 2 || got[0].State != "pending" || got[1].State != "pending" {
		t.Fatalf("before the check: %+v", got)
	}

	// The writer's first manifest write is the check.
	tapply(t, tbl, 2, numRow(4, "7", "5"))
	if tbl.Halted() != "" {
		t.Fatalf("the table halted: %s", tbl.Halted())
	}
	out := log.String()
	for _, want := range []string{"REFUSED", "column=amount", `0.123456`, "id = 2", "DECIMAL(38,4)", "table=public.num"} {
		if !strings.Contains(out, want) {
			t.Errorf("the refusal does not say %q:\n%s", want, out)
		}
	}
	opts := tbl.NumericOptIns()
	if opts[0].Column != "amount" || opts[0].State != "refused" || !strings.Contains(opts[0].Why, "0.123456") {
		t.Fatalf("amount: %+v", opts[0])
	}
	// The column beside it fitted, and is a decimal from this manifest on.
	if opts[1].Column != "rate" || opts[1].State != "decimal" || !opts[1].Converting {
		t.Fatalf("rate: %+v", opts[1])
	}
	if got := arrowType(tbl.typeOf("amount")).String(); got != "utf8" {
		t.Fatalf("a refused column is %s", got)
	}
	if got := arrowType(tbl.typeOf("rate")).String(); got != "decimal(38, 2)" {
		t.Fatalf("an accepted column is %s", got)
	}
	st := manifestOf(t, tbl)
	if _, recorded := st.NumericScales["amount"]; recorded || st.NumericScales["rate"] != 2 {
		t.Fatalf("manifest numeric_scales = %v", st.NumericScales)
	}

	// Still exact, and a value that would not fit the refused scale is just
	// another value.
	tapply(t, tbl, 3, numRow(5, "0.00000001", "6"))
	if tbl.Halted() != "" {
		t.Fatalf("a refused column halted the table: %s", tbl.Halted())
	}
	live := liveText(t, tbl)
	for id, want := range map[string]string{"1": "1.50", "2": "0.123456", "3": "12.3400", "5": "0.00000001"} {
		if got := live[id]["amount"]; got != Render(want) { // liveText normalises, as qs-verify does
			t.Errorf("amount of %s read back as %q, want %q", id, got, want)
		}
	}
	sql, _ = tbl.ViewSQL()
	if strings.Contains(sql, `"amount" AS DECIMAL`) {
		t.Fatalf("the view casts a refused column:\n%s", sql)
	}
	if haveDuck() {
		got := duck(t, "SELECT id, amount, typeof(amount), typeof(rate) FROM ("+sql+") WHERE id = 2")
		if len(got) != 1 || got[0] != "2\t0.123456\tVARCHAR\tDECIMAL(38,2)" {
			t.Errorf("DuckDB reads %v", got)
		}
	}

	// A reader follows the manifest and scans nothing: rate was accepted in
	// it, amount has no decision and its files hold text.
	rd := numTable(t, dir)
	if a, r := arrowType(rd.typeOf("amount")).String(), arrowType(rd.typeOf("rate")).String(); a != "utf8" || r != "decimal(38, 2)" {
		t.Fatalf("a reader sees amount as %s and rate as %s", a, r)
	}
}

// The other outcome: every stored value fits. The column becomes the decimal
// at the first manifest write, the decision is in that manifest, the view
// casts the old files exactly meanwhile, and a rewrite is asked for and
// converts them.
func TestNumericOptInIsAcceptedOnceEveryStoredValueFits(t *testing.T) {
	defer func(prev string) { NumericScale = prev }(NumericScale)
	log := captureLog(t)
	NumericScale = ""
	dir := t.TempDir()
	tbl := numTable(t, dir)
	tapply(t, tbl, 1, numRow(1, "1.50", "x"), numRow(2, "0.123456", "x"), numRow(3, "12.3400", "x"))
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	// The one value that would not fit is deleted: it is not a value the
	// table holds, and must not count.
	tapply(t, tbl, 2, typedChange(changestream.OpDelete, "num", map[string]any{"id": "2"}))
	tbl.Close()

	NumericScale = "public.num.amount=4"
	tbl = numTable(t, dir)
	if got := tbl.CompactReason(); got == "numeric-opt-in" {
		t.Fatal("a rewrite was asked for before the column was accepted")
	}
	tapply(t, tbl, 3, numRow(4, "7", "x"))
	if !strings.Contains(log.String(), "every stored value fits") || strings.Contains(log.String(), "REFUSED") {
		t.Fatalf("log:\n%s", log.String())
	}
	if got := arrowType(tbl.typeOf("amount")).String(); got != "decimal(38, 4)" {
		t.Fatalf("after the check the column is %s", got)
	}
	if st := manifestOf(t, tbl); st.NumericScales["amount"] != 4 {
		t.Fatalf("the acceptance is not in the manifest: %v", st.NumericScales)
	}
	sql, err := tbl.ViewSQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, `CAST("amount" AS DECIMAL(38,4))`) {
		t.Fatalf("the view does not present the old files as the decimal:\n%s", sql)
	}
	want := []string{"1\t1.5000\tDECIMAL(38,4)", "3\t12.3400\tDECIMAL(38,4)", "4\t7.0000\tDECIMAL(38,4)"}
	if haveDuck() {
		if got := duck(t, "SELECT id, amount, typeof(amount) FROM ("+sql+") ORDER BY id"); strings.Join(got, "|") != strings.Join(want, "|") {
			t.Errorf("over text files DuckDB reads %v", got)
		}
	}

	// The rewrite is asked for, and converts.
	if got := tbl.CompactReason(); got != "numeric-opt-in" {
		t.Fatalf("CompactReason = %q, want the conversion", got)
	}
	swapCompaction(t, tbl)
	for _, f := range dataFiles(tbl) {
		if ft := fileTypes(t, f); ft["amount"] != "DECIMAL(38,4)" {
			t.Fatalf("after the rewrite %s holds amount as %s", f, ft["amount"])
		}
	}
	if got := tbl.CompactReason(); got == "numeric-opt-in" {
		t.Fatal("the conversion is still asked for after it happened")
	}
	if o := tbl.NumericOptIns(); len(o) != 1 || o[0].State != "decimal" || o[0].Converting {
		t.Fatalf("%+v", o)
	}
	sql, _ = tbl.ViewSQL()
	if strings.Contains(sql, "CAST(") {
		t.Fatalf("converted files are still cast:\n%s", sql)
	}
	if haveDuck() {
		if got := duck(t, "SELECT id, amount, typeof(amount) FROM ("+sql+") ORDER BY id"); strings.Join(got, "|") != strings.Join(want, "|") {
			t.Errorf("after the rewrite DuckDB reads %v", got)
		}
	}

	// A restart does not check again: the manifest says it was.
	tbl.Close()
	before := log.Len()
	tbl = numTable(t, dir)
	if got := arrowType(tbl.typeOf("amount")).String(); got != "decimal(38, 4)" {
		t.Fatalf("reopened, the column is %s", got)
	}
	tapply(t, tbl, 4, numRow(5, "8.25", "x"))
	if strings.Contains(log.String()[before:], "QS_NUMERIC_SCALE") {
		t.Fatalf("an accepted column was examined again:\n%s", log.String()[before:])
	}
	// From here it is a decimal column like any other: what does not fit halts.
	if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: "0/9", NextLSN: "0/9",
		Changes: []changestream.Change{numRow(6, "0.00001", "x")}}}); err == nil {
		t.Fatal("an accepted decimal column took a value it cannot hold")
	}
}

// A file of the previous release holds the column as DECIMAL(38,9). Opting
// into a smaller scale is the same question — does every value fit — asked of
// the digits that file stands for.
func TestNumericOptInChecksLegacyDecimalFiles(t *testing.T) {
	defer func(prev string) { NumericScale = prev }(NumericScale)
	log := captureLog(t)
	NumericScale = ""
	dir := t.TempDir()
	withLegacyWriter(func() {
		tbl, _ := New(dir, "public", "num", "id", numCols, numOrder)
		tapply(t, tbl, 1, numRow(1, "1.50", "0.125"), numRow(2, "3", "0.5"))
		if _, err := tbl.Compact(); err != nil {
			t.Fatal(err)
		}
		tbl.Close()
	})
	NumericScale = "public.num.amount=2,public.num.rate=2"
	tbl := numTable(t, dir)
	tapply(t, tbl, 2, numRow(3, "4", "5"))
	if a, r := arrowType(tbl.typeOf("amount")).String(), arrowType(tbl.typeOf("rate")).String(); a != "decimal(38, 2)" || r != "utf8" {
		t.Fatalf("amount %s (every value fits scale 2), rate %s (0.125 does not)", a, r)
	}
	if !strings.Contains(log.String(), `0.125`) || !strings.Contains(log.String(), "column=rate") {
		t.Fatalf("the refusal does not name the value:\n%s", log.String())
	}
	if haveDuck() {
		sql, _ := tbl.ViewSQL()
		got := duck(t, "SELECT id, amount, rate FROM ("+sql+") ORDER BY id")
		if strings.Join(got, "|") != "1\t1.50\t0.125|2\t3.00\t0.5|3\t4.00\t5" {
			t.Errorf("DuckDB reads %v", got)
		}
	}
	// Scale 9 over DECIMAL(38,9) files is what those files already are:
	// nothing to check, nothing to convert.
	tbl.Close()
	dir2 := t.TempDir()
	NumericScale = ""
	withLegacyWriter(func() {
		l, _ := New(dir2, "public", "num", "id", numCols, numOrder)
		tapply(t, l, 1, numRow(1, "1.50", "0.125"))
		l.Close()
	})
	NumericScale = "9"
	same := numTable(t, dir2)
	if got := arrowType(same.typeOf("rate")).String(); got != "decimal(38, 9)" || same.numericRewriteDue() {
		t.Fatalf("files already in the asked-for form: %s, rewrite due %v", got, same.numericRewriteDue())
	}
}
