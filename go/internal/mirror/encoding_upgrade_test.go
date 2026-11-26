package mirror

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// withFormat1Writer runs fn with the writer producing what the previous
// release produced: the current types, every column OPTIONAL, dictionary and
// zstd everywhere, DECIMAL as a fixed-length byte array, no exactness flags.
func withFormat1Writer(fn func()) {
	legacyEncoding = true
	defer func() { legacyEncoding = false }()
	fn()
}

var ordersCols = map[string]string{"id": "bigint", "tenant": "integer", "amount": "numeric(12,2)", "n9": "numeric(9,2)",
	"n30": "numeric(30,4)", "created": "timestamp with time zone", "d": "date", "status": "text", "doc": "text"}
var ordersOrder = []string{"id", "tenant", "amount", "n9", "n30", "created", "d", "status", "doc"}
var ordersNotNull = []string{"id", "tenant", "amount", "created", "status"}

func orderRow(i, ver int) map[string]any {
	r := map[string]any{
		"id":      fmt.Sprint(i),
		"tenant":  fmt.Sprint(1 + (i+ver)%7),
		"amount":  fmt.Sprintf("%d.%02d", (i*37+ver*1000)%100000-500, (i+ver)%100),
		"n9":      fmt.Sprintf("-%d.%02d", i%9999999, ver%100),
		"n30":     fmt.Sprintf("%d%012d.%04d", 1+ver, i, i%10000),
		"created": fmt.Sprintf("2024-%02d-%02d %02d:%02d:%02d+00", 1+i%12, 1+i%28, ver%24, i%60, (i/60)%60),
		"d":       fmt.Sprintf("20%02d-%02d-%02d", ver%90, 1+i%12, 1+i%28),
		"status":  []string{"new", "paid", "shipped"}[(i+ver)%3],
		"doc":     fmt.Sprintf("doc %d v%d %s", i, ver, strings.Repeat("x", 40)),
	}
	if i%5 == 0 {
		r["n9"], r["d"] = nil, nil
	}
	return r
}

// viewText is the table as DuckDB reads the generated view, keyed by id.
func viewText(t *testing.T, tbl *Table) map[string]map[string]string {
	t.Helper()
	sql, err := tbl.ViewSQL()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]string{}
	for _, line := range duck(t, "SELECT * FROM ("+sql+") ORDER BY 1") {
		f := strings.Split(line, "\t")
		m := map[string]string{}
		for i, c := range tbl.Order {
			// Render drops a decimal's trailing zeros; DuckDB prints the scale.
			m[c] = normalizeNumeric(f[i])
			if f[i] == `\N` {
				m[c] = "<null>"
			}
		}
		if _, dup := out[m[tbl.Key]]; dup {
			t.Errorf("the view returns key %s twice", m[tbl.Key])
		}
		out[m[tbl.Key]] = m
	}
	return out
}

// columnForm is how one file holds one column: its physical type and whether
// it is REQUIRED.
func columnForm(t *testing.T, path, col string) (parquet.Type, bool) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Close()
	s := rdr.MetaData().Schema
	for i := 0; i < s.NumColumns(); i++ {
		if c := s.Column(i); c.Name() == col {
			return c.PhysicalType(), c.SchemaNode().RepetitionType() == parquet.Repetitions.Required
		}
	}
	t.Fatalf("%s has no column %s", path, col)
	return 0, false
}

// A mirror the previous release wrote, carried on by this one: old base, old
// delta, old patch file and old deletion vector, then applies, patch files, a
// delta merge and a base rewrite by each path, a restart, and the generated
// view in DuckDB at every step. Equal to the model, every time.
func TestFormat1MirrorCarriedOnByFormat2(t *testing.T) {
	// It asserts column-partial patch files, which QS_PARTIAL_DELTAS=0 switches off.
	defer func(prev bool) { PartialDeltas = prev }(PartialDeltas)
	PartialDeltas = true
	for _, path := range []string{"sync", "async", "clustered"} {
		t.Run("rewrite="+path, func(t *testing.T) {
			withRowGroup(t, 50)
			switch path {
			case "clustered":
				withClusterBy(t, "public.o=created")
			case "async":
				withClusterBy(t, "public.o=none")
			}
			dir := t.TempDir()
			open := func() *Table {
				tbl, err := New(dir, "public", "o", "id", ordersCols, ordersOrder)
				if err != nil {
					t.Fatal(err)
				}
				return tbl
			}
			want := map[string]map[string]string{}
			put := func(r map[string]any) changestream.Change {
				want[r["id"].(string)] = expectRow(r)
				return changestream.Change{Op: changestream.OpInsert, Schema: "public", Table: "o", Row: r, Key: r}
			}
			// patch is an UPDATE that does not carry doc, as pgoutput omits an
			// unchanged TOASTed value.
			patch := func(i, ver int) changestream.Change {
				r := orderRow(i, ver)
				delete(r, "doc")
				for c, v := range expectRow(r) {
					want[r["id"].(string)][c] = v
				}
				return changestream.Change{Op: changestream.OpUpdate, Schema: "public", Table: "o", Row: r, Key: r}
			}
			del := func(i int) changestream.Change {
				k := map[string]any{"id": fmt.Sprint(i)}
				delete(want, fmt.Sprint(i))
				return changestream.Change{Op: changestream.OpDelete, Schema: "public", Table: "o", Key: k}
			}
			check := func(tbl *Table, step string) {
				t.Helper()
				sameContents(t, step+" (Go reader)", liveText(t, tbl), want)
				checkInvariants(t, tbl, step)
				if haveDuck() {
					sameContents(t, step+" (view in DuckDB)", viewText(t, tbl), want)
				}
			}

			var tbl *Table
			withFormat1Writer(func() {
				tbl = open()
				var batch []changestream.Change
				for i := 0; i < 600; i++ {
					batch = append(batch, put(orderRow(i, 0)))
				}
				tapply(t, tbl, 1, batch...)
				if _, err := tbl.Compact(); err != nil {
					t.Fatal(err)
				}
				batch = nil
				for i := 10; i < 20; i++ {
					batch = append(batch, put(orderRow(i, 1)))
				}
				tapply(t, tbl, 2, batch...)
				tapply(t, tbl, 3, patch(100, 2), patch(101, 2), patch(12, 2), del(200), del(15))
				check(tbl, "as the previous release left it")
				tbl.Close()
			})
			for _, f := range dataFiles(tbl) {
				if strings.Contains(f, "amount") {
					continue
				}
				cols := tbl.partialColsOf(f)
				if cols != nil && !containsStr(cols, "amount") {
					continue
				}
				if pt, req := columnForm(t, f, "amount"); pt != parquet.Types.FixedLenByteArray || req {
					t.Fatalf("%s is not a file of the previous release: amount is %v, required %v", f, pt, req)
				}
			}
			oldFiles := len(dataFiles(tbl))

			tbl = open()
			tbl.SetNotNull(ordersNotNull...)
			if _, err := tbl.BuildIndex(); err != nil {
				t.Fatal(err)
			}
			check(tbl, "opened by this release")
			var batch []changestream.Change
			for i := 20; i < 40; i++ {
				batch = append(batch, put(orderRow(i, 3)))
			}
			for i := 600; i < 660; i++ {
				batch = append(batch, put(orderRow(i, 0)))
			}
			batch = append(batch, del(41), del(11))
			tapply(t, tbl, 4, batch...)
			// Patches on rows whose whole row is in an old base, an old
			// delta, a new delta, and on a row an old patch already amends.
			tapply(t, tbl, 5, patch(300, 4), patch(13, 4), patch(25, 4), patch(100, 4), patch(601, 4))
			check(tbl, "old and new files together")
			if len(dataFiles(tbl)) <= oldFiles {
				t.Fatal("this release wrote no file")
			}
			newest := dataFiles(tbl)[len(dataFiles(tbl))-1]
			if cols := tbl.partialColsOf(newest); cols == nil || containsStr(cols, "doc") {
				t.Fatalf("the last file is not a patch file without doc: %v", cols)
			}
			if pt, req := columnForm(t, newest, "amount"); pt != parquet.Types.Int64 || !req {
				t.Fatalf("patch file: amount is %v, required %v; want INT64 REQUIRED", pt, req)
			}

			// A restart in the middle: nullability is not durable, so the
			// files written until it is given again are OPTIONAL.
			tbl.Close()
			tbl = open()
			if _, err := tbl.BuildIndex(); err != nil {
				t.Fatal(err)
			}
			tapply(t, tbl, 6, put(orderRow(26, 5)), patch(301, 5), del(302))
			if _, req := columnForm(t, dataFiles(tbl)[len(dataFiles(tbl))-1], "amount"); req {
				t.Fatal("a table nobody called SetNotNull on wrote a REQUIRED column")
			}
			check(tbl, "after a restart")
			tbl.SetNotNull(ordersNotNull...)

			// A delta merge over deltas and patch files of both releases.
			deltas := len(tbl.State.DeltaFiles)
			if _, err := tbl.MergeDeltas(); err != nil {
				t.Fatal(err)
			}
			if len(tbl.State.DeltaFiles) >= deltas {
				t.Fatalf("the merge folded nothing: %d delta files before, %d after", deltas, len(tbl.State.DeltaFiles))
			}
			check(tbl, "after a delta merge")
			tapply(t, tbl, 7, patch(320, 9), put(orderRow(27, 9)), del(321))
			check(tbl, "after a delta merge and an apply")

			switch path {
			case "sync":
				if _, err := tbl.Compact(); err != nil {
					t.Fatal(err)
				}
			default:
				before := clusteredRewrites.Load()
				c := tbl.BeginCompaction()
				if c == nil {
					t.Fatal("no compaction")
				}
				tapply(t, tbl, 8, put(orderRow(30, 6)), patch(310, 6), del(311), put(orderRow(700, 0)))
				<-c.done
				if _, err := tbl.FinishCompaction(); err != nil {
					t.Fatal(err)
				}
				if (path == "clustered") != (clusteredRewrites.Load() != before) {
					t.Fatalf("rewrite path %s: clustered rewrites went from %d to %d (%s)", path, before, clusteredRewrites.Load(), c.clusterWhy)
				}
			}
			check(tbl, "after the base rewrite")
			for _, b := range tbl.State.BaseFiles {
				f := filepath.Join(tbl.Dir, "base", b)
				if pt, req := columnForm(t, f, "amount"); pt != parquet.Types.Int64 || !req {
					t.Errorf("rewritten base %s: amount is %v, required %v", b, pt, req)
				}
				if pt, req := columnForm(t, f, "n9"); pt != parquet.Types.Int32 || req {
					t.Errorf("rewritten base %s: n9 is %v, required %v", b, pt, req)
				}
				if pt, _ := columnForm(t, f, "n30"); pt != parquet.Types.FixedLenByteArray {
					t.Errorf("rewritten base %s: n30 is %v", b, pt)
				}
			}
			tapply(t, tbl, 9, put(orderRow(0, 7)), patch(12, 7), patch(610, 7), del(599), put(orderRow(800, 0)))
			check(tbl, "after the rewrite and an apply")

			// DROP NOT NULL: the caller withdraws the claim, a NULL arrives,
			// and the file that holds it sits next to REQUIRED ones.
			tbl.SetNotNull("id")
			r := orderRow(900, 0)
			r["amount"], r["status"] = nil, nil
			tapply(t, tbl, 10, put(r), patch(1, 8))
			check(tbl, "after DROP NOT NULL")
			tbl.Close()
			tbl = open()
			tbl.SetNotNull("id")
			if _, err := tbl.BuildIndex(); err != nil {
				t.Fatal(err)
			}
			tapply(t, tbl, 11, put(orderRow(2, 9)), del(3))
			if _, err := tbl.Compact(); err != nil {
				t.Fatal(err)
			}
			check(tbl, "after a rewrite over REQUIRED and OPTIONAL files")
			tbl.Close()
		})
	}
}

// The contract of SetNotNull: REQUIRED only when said, a NULL for such a
// column fails the batch before anything is written and stores no default,
// and the claim can be withdrawn.
func TestNotNullIsRequiredAndANullIsRefused(t *testing.T) {
	dir := t.TempDir()
	tbl, err := New(dir, "public", "o", "id", ordersCols, ordersOrder)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { tbl.Close() }()
	ins := func(r map[string]any) changestream.Change {
		return changestream.Change{Op: changestream.OpInsert, Schema: "public", Table: "o", Row: r, Key: r}
	}
	tapply(t, tbl, 1, ins(orderRow(1, 0)))
	if _, req := columnForm(t, dataFiles(tbl)[0], "id"); req {
		t.Fatal("nothing was declared NOT NULL and the key was written REQUIRED")
	}

	tbl.SetNotNull(append([]string{"not_a_column"}, ordersNotNull...)...)
	if got := strings.Join(tbl.NotNull(), ","); got != "amount,created,id,status,tenant" {
		t.Fatalf("NotNull() = %s", got)
	}
	tapply(t, tbl, 2, ins(orderRow(2, 0)))
	f := dataFiles(tbl)[len(dataFiles(tbl))-1]
	for _, c := range ordersOrder {
		if _, req := columnForm(t, f, c); req != containsStr(ordersNotNull, c) {
			t.Errorf("column %s: REQUIRED is %v", c, req)
		}
	}

	// A NULL in a NOT NULL column, in the middle of a batch.
	before, _ := filepath.Glob(filepath.Join(dir, "public.o", "*", "*"))
	state, _ := os.ReadFile(tbl.statePath())
	bad := orderRow(4, 0)
	bad["amount"] = nil
	_, err = tbl.Apply([]changestream.Transaction{{CommitLSN: "0/3", NextLSN: "0/3",
		Changes: []changestream.Change{ins(orderRow(3, 0)), ins(bad), ins(orderRow(5, 0))}}})
	var v *NotNullViolation
	if !errors.As(err, &v) || v.Column != "amount" || v.Table != "public.o" || !IsNotNullViolation(err) {
		t.Fatalf("a NULL in a NOT NULL column: err = %v", err)
	}
	if !strings.Contains(err.Error(), `"amount"`) || !strings.Contains(err.Error(), "NOT NULL") {
		t.Fatalf("the error does not say what happened: %v", err)
	}
	after, _ := filepath.Glob(filepath.Join(dir, "public.o", "*", "*"))
	sort.Strings(before)
	sort.Strings(after)
	if strings.Join(before, "\n") != strings.Join(after, "\n") {
		t.Fatalf("the refused batch left files behind:\nbefore %v\nafter  %v", before, after)
	}
	if now, _ := os.ReadFile(tbl.statePath()); string(now) != string(state) {
		t.Fatal("the refused batch changed the manifest")
	}
	if h := tbl.Halted(); h != "" {
		t.Fatalf("the table was halted: %s", h)
	}
	var live map[string]map[string]string
	if live = liveText(t, tbl); len(live) != 2 {
		t.Fatalf("the mirror holds %d rows after a refused batch, want the 2 from before it", len(live))
	}

	// The claim is withdrawn and the same batch is applied: the NULL is a
	// NULL, not a default.
	// The Apply failed, so the Table is reopened, as after any failed write.
	if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: "0/3", NextLSN: "0/3",
		Changes: []changestream.Change{ins(orderRow(3, 0))}}}); err == nil {
		t.Fatal("a Table whose write failed went on applying")
	}
	tbl.Close()
	if tbl, err = New(dir, "public", "o", "id", ordersCols, ordersOrder); err != nil {
		t.Fatal(err)
	}
	tbl.SetNotNull("id")
	tapply(t, tbl, 3, ins(orderRow(3, 0)), ins(bad), ins(orderRow(5, 0)))
	live = liveText(t, tbl)
	if len(live) != 5 || live["4"]["amount"] != "<null>" {
		t.Fatalf("after withdrawing the claim: %d rows, amount of 4 = %q", len(live), live["4"]["amount"])
	}

	// A rewrite under a claim the rows do not meet fails too, and leaves the
	// table as it was.
	tbl.SetNotNull(ordersNotNull...)
	if _, err := tbl.Compact(); !IsNotNullViolation(err) {
		t.Fatalf("a rewrite of a NULL into a NOT NULL column: err = %v", err)
	}
	if h := tbl.Halted(); h != "" {
		t.Fatalf("the table was halted: %s", h)
	}
	c := tbl.BeginCompaction()
	if c == nil {
		t.Fatal("no compaction")
	}
	<-c.done
	if _, err := tbl.FinishCompaction(); !IsNotNullViolation(err) {
		t.Fatalf("a background rewrite of a NULL into a NOT NULL column: err = %v", err)
	}
	if h := tbl.Halted(); h != "" {
		t.Fatalf("the table was halted: %s", h)
	}
	tbl.SetNotNull()
	if len(tbl.NotNull()) != 0 {
		t.Fatal("SetNotNull() did not clear")
	}
	sameLen := liveText(t, tbl)
	if len(sameLen) != 5 || sameLen["4"]["amount"] != "<null>" {
		t.Fatalf("after the refused rewrite: %d rows", len(sameLen))
	}
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	if got := liveText(t, tbl); len(got) != 5 {
		t.Fatalf("after the rewrite: %d rows", len(got))
	}
}

// A column added later, NOT NULL with a default: the files that predate it do
// not have it, read the default back, and a rewrite stores the default in a
// REQUIRED column. Added nullable and then SET NOT NULL: the same, by way of
// OPTIONAL files.
func TestNotNullColumnAddedLater(t *testing.T) {
	withRowGroup(t, 50)
	dir := t.TempDir()
	cols := map[string]string{"id": "bigint", "v": "text"}
	tbl, err := New(dir, "public", "a", "id", cols, []string{"id", "v"})
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	want := map[string]map[string]string{}
	put := func(r map[string]any) changestream.Change {
		want[r["id"].(string)] = expectRow(r)
		return changestream.Change{Op: changestream.OpInsert, Schema: "public", Table: "a", Row: r, Key: r}
	}
	tbl.SetNotNull("id", "qty", "price")
	var batch []changestream.Change
	for i := 0; i < 120; i++ {
		batch = append(batch, put(map[string]any{"id": fmt.Sprint(i), "v": fmt.Sprint("v", i)}))
	}
	tapply(t, tbl, 1, batch...)
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}

	// ADD COLUMN qty integer NOT NULL DEFAULT 7, price numeric(10,2) (nullable for now)
	if err := tbl.Evolve(map[string]string{"id": "bigint", "v": "text", "qty": "integer", "price": "numeric(10,2)"},
		[]string{"id", "v", "qty", "price"}, map[string]string{"qty": "7"}); err != nil {
		t.Fatal(err)
	}
	tbl.SetNotNull("id", "qty")
	for _, r := range want {
		r["qty"], r["price"] = "7", "<null>"
	}
	tapply(t, tbl, 2, put(map[string]any{"id": "500", "v": "new", "qty": "9", "price": "1.50"}),
		put(map[string]any{"id": "3", "v": "upd", "qty": "8", "price": nil}))
	check := func(step string) {
		t.Helper()
		sameContents(t, step+" (Go reader)", liveText(t, tbl), want)
		if haveDuck() {
			sameContents(t, step+" (view in DuckDB)", viewText(t, tbl), want)
		}
	}
	check("a file without the column next to one that has it REQUIRED")
	if _, req := columnForm(t, dataFiles(tbl)[len(dataFiles(tbl))-1], "qty"); !req {
		t.Fatal("qty was not written REQUIRED")
	}
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	check("after a rewrite that stores the default")
	if _, req := columnForm(t, dataFiles(tbl)[0], "qty"); !req {
		t.Fatal("the rewritten base does not hold qty REQUIRED")
	}

	// SET NOT NULL on price, once every row has one.
	batch = nil
	for id, r := range want {
		batch = append(batch, put(map[string]any{"id": id, "v": r["v"], "qty": r["qty"], "price": "2.25"}))
	}
	tapply(t, tbl, 3, batch...)
	tbl.SetNotNull("id", "qty", "price")
	tapply(t, tbl, 4, put(map[string]any{"id": "501", "v": "x", "qty": "1", "price": "3.00"}))
	check("OPTIONAL and REQUIRED files of one column")
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	check("after the rewrite")
	if pt, req := columnForm(t, dataFiles(tbl)[0], "price"); !req || pt != parquet.Types.Int64 {
		t.Fatalf("price: %v required %v", pt, req)
	}
}

// A patch that does not carry a NOT NULL column is not a NULL for it: the
// patch is written without the column, the row keeps the value it has, and
// the same patch with the column set to NULL is refused.
func TestPatchWithoutANotNullColumn(t *testing.T) {
	// It asserts a patch file, which QS_PARTIAL_DELTAS=0 switches off.
	defer func(prev bool) { PartialDeltas = prev }(PartialDeltas)
	PartialDeltas = true
	dir := t.TempDir()
	tbl, err := New(dir, "public", "o", "id", ordersCols, ordersOrder)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { tbl.Close() }()
	ins := func(r map[string]any) changestream.Change {
		return changestream.Change{Op: changestream.OpInsert, Schema: "public", Table: "o", Row: r, Key: r}
	}
	upd := func(r map[string]any) changestream.Change {
		return changestream.Change{Op: changestream.OpUpdate, Schema: "public", Table: "o", Row: r, Key: r}
	}
	tbl.SetNotNull(ordersNotNull...)
	tapply(t, tbl, 1, ins(orderRow(1, 0)), ins(orderRow(2, 0)))
	before := liveText(t, tbl)

	// status (NOT NULL) and doc (nullable) absent from the patch.
	r := orderRow(1, 1)
	delete(r, "status")
	delete(r, "doc")
	tapply(t, tbl, 2, upd(r))
	f := dataFiles(tbl)[len(dataFiles(tbl))-1]
	if cols := tbl.partialColsOf(f); cols == nil || containsStr(cols, "status") {
		t.Fatalf("the last file is not a patch without status: %v", cols)
	}
	after := liveText(t, tbl)
	if after["1"]["amount"] == before["1"]["amount"] {
		t.Fatal("the patch did not change amount")
	}
	if after["1"]["status"] != before["1"]["status"] || after["1"]["doc"] != before["1"]["doc"] {
		t.Fatalf("a patch without status and doc changed them: status %q -> %q, doc %q -> %q",
			before["1"]["status"], after["1"]["status"], before["1"]["doc"], after["1"]["doc"])
	}

	// The same patch with status NULL is a NULL in a NOT NULL column.
	bad := orderRow(2, 2)
	bad["status"] = nil
	if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: "0/3", NextLSN: "0/3",
		Changes: []changestream.Change{upd(bad)}}}); !IsNotNullViolation(err) {
		t.Fatalf("a patch that sets status NULL: err = %v", err)
	}
}
