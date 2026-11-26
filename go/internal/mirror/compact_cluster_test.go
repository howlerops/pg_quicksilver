package mirror

// A rewrite must leave the base file clustered the way it found it.
//
// These tests build a base that is clustered on a column, scatter updates over
// it, rewrite, and COUNT how many row-group ranges overlap — the number a
// reader's pruning actually depends on — rather than checking that some code
// path ran. The same build with clustering turned off is the control: it has to
// come out measurably worse, or the assertion proves nothing.

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func withRowGroup(t *testing.T, n int64) {
	t.Helper()
	old := RowGroupRows
	RowGroupRows = n
	t.Cleanup(func() { RowGroupRows = old })
}

func withClusterBy(t *testing.T, spec string) {
	t.Helper()
	old := ClusterBy
	ClusterBy = spec
	t.Cleanup(func() { ClusterBy = old })
}

func clusterTable(t *testing.T) *Table {
	t.Helper()
	cols := map[string]string{
		"id": "bigint", "ts": "timestamp without time zone", "v": "text", "n": "bigint",
	}
	tbl, err := New(t.TempDir(), "public", "c", "id", cols, []string{"id", "ts", "v", "n"})
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

type clusterModel struct {
	t    *testing.T
	tbl  *Table
	want map[string]map[string]any
	lsn  int
}

func (m *clusterModel) apply(changes []changestream.Change) {
	m.t.Helper()
	m.lsn++
	pos := fmt.Sprintf("0/%X", m.lsn)
	if _, err := m.tbl.Apply([]changestream.Transaction{{CommitLSN: pos, NextLSN: pos, Changes: changes}}); err != nil {
		m.t.Fatal(err)
	}
}

func (m *clusterModel) put(op changestream.Op, row map[string]any) changestream.Change {
	m.want[fmt.Sprint(row["id"])] = copyRow(row)
	name := m.tbl.Qualified[strings.Index(m.tbl.Qualified, ".")+1:]
	return changestream.Change{Op: op, Schema: "public", Table: name, Row: row, Key: row}
}

// rewrite runs one background rewrite to completion and swaps it in.
func (m *clusterModel) rewrite() *Compaction {
	m.t.Helper()
	c := m.tbl.BeginCompaction()
	if c == nil {
		m.t.Fatal("BeginCompaction returned nil")
	}
	<-c.done
	if _, err := m.tbl.FinishCompaction(); err != nil {
		m.t.Fatalf("FinishCompaction: %v", err)
	}
	// The buckets the delta rows were spilled to are the rewrite's own scratch.
	if left, _ := filepath.Glob(filepath.Join(m.tbl.Dir, "index", "*.delta*")); len(left) > 0 {
		m.t.Errorf("the rewrite left its buckets behind: %v", left)
	}
	return c
}

// check compares every live row with the model, and that no key is live twice.
func (m *clusterModel) check(cols ...string) {
	m.t.Helper()
	got := map[string]map[string]any{}
	if err := m.tbl.ForEachLive(func(r map[string]any) error {
		k := fmt.Sprint(r["id"])
		if _, dup := got[k]; dup {
			m.t.Errorf("key %s is live twice", k)
		}
		got[k] = r
		return nil
	}); err != nil {
		m.t.Fatal(err)
	}
	if len(got) != len(m.want) {
		m.t.Errorf("mirror has %d rows, want %d", len(got), len(m.want))
	}
	bad := 0
	for k, w := range m.want {
		g, ok := got[k]
		if !ok {
			if bad++; bad < 4 {
				m.t.Errorf("row %s is missing", k)
			}
			continue
		}
		for _, c := range cols {
			if Render(g[c]) != Render(w[c]) {
				if bad++; bad < 4 {
					m.t.Errorf("row %s column %s: mirror=%v want=%v", k, c, Render(g[c]), w[c])
				}
			}
		}
	}
	checkInvariants(m.t, m.tbl, "after rewrite")
}

// baseRows is the base file in file order, dead rows included: index is position.
func baseRows(t *testing.T, tbl *Table) []map[string]any {
	t.Helper()
	if len(tbl.State.BaseFiles) != 1 {
		t.Fatalf("want one base file, have %v", tbl.State.BaseFiles)
	}
	rows, err := tbl.readParquet(filepath.Join(tbl.Dir, "base", tbl.State.BaseFiles[0]), nil)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// overlapPairs is how many pairs of the base file's row groups have overlapping
// ranges on col, and how many row groups there are.
func overlapPairs(t *testing.T, tbl *Table, col string) (pairs, groups int) {
	t.Helper()
	ranges, groups, err := columnRanges(
		[]string{filepath.Join(tbl.Dir, "base", tbl.State.BaseFiles[0])}, tbl.Order)
	if err != nil {
		t.Fatal(err)
	}
	r := ranges[col]
	if len(r) == 0 {
		t.Fatalf("no statistics for %s", col)
	}
	for i := range r {
		for j := i + 1; j < len(r); j++ {
			if ordCmp(r[i].hi, r[j].lo) > 0 && ordCmp(r[j].hi, r[i].lo) > 0 {
				pairs++
			}
		}
	}
	return pairs, groups
}

const clusterEpoch = "2025-01-01 00:00:00"

func tsAt(minutes int) string {
	t0, _ := time.Parse("2006-01-02 15:04:05", clusterEpoch)
	return t0.Add(time.Duration(minutes) * time.Minute).Format("2006-01-02 15:04:05")
}

// buildScattered loads rows whose ts rises with id (to within `jitter`
// minutes), rewrites them into a base, then scatters updates, moves and new
// rows over it. It returns before the second rewrite.
func buildScattered(t *testing.T, jitter, moves int) (*clusterModel, int) {
	t.Helper()
	rng := rand.New(rand.NewSource(5))
	m := &clusterModel{t: t, tbl: clusterTable(t), want: map[string]map[string]any{}}
	const rows = 4000
	for lo := 1; lo <= rows; lo += 500 {
		var ch []changestream.Change
		for i := lo; i < lo+500; i++ {
			ch = append(ch, m.put(changestream.OpInsert, map[string]any{
				"id": fmt.Sprint(i), "ts": tsAt(i + rng.Intn(jitter+1)), "v": "seed", "n": fmt.Sprint(rng.Intn(1000)),
			}))
		}
		m.apply(ch)
	}
	if c := m.rewrite(); c.clusterBy != "" {
		t.Fatalf("the first rewrite has no base to read a clustering from, yet chose %q", c.clusterBy)
	}
	before, _ := overlapPairs(t, m.tbl, "ts")

	// A fifth of the rows updated in place, scattered over the whole table...
	for round := 0; round < 4; round++ {
		var ch []changestream.Change
		for i := 0; i < 200; i++ {
			id := fmt.Sprint(1 + rng.Intn(rows))
			row := copyRow(m.want[id])
			row["v"] = fmt.Sprintf("u%d", round)
			ch = append(ch, m.put(changestream.OpUpdate, row))
		}
		// ...some rows whose time itself moves...
		for i := 0; i < moves; i++ {
			id := fmt.Sprint(1 + rng.Intn(rows))
			row := copyRow(m.want[id])
			row["ts"] = tsAt(rng.Intn(rows))
			ch = append(ch, m.put(changestream.OpUpdate, row))
		}
		// ...and new rows, later than everything.
		for i := 0; i < 50; i++ {
			id := rows + 1 + round*50 + i
			ch = append(ch, m.put(changestream.OpInsert, map[string]any{
				"id": fmt.Sprint(id), "ts": tsAt(id), "v": "new", "n": "1",
			}))
		}
		m.apply(ch)
	}
	return m, before
}

// TestClusteredRewriteKeepsRanges is the measurement the change exists for, in
// miniature: after scattered updates and a rewrite, the row groups overlap on
// the time column no more than they did before — and with clustering off, the
// same data comes out several times worse.
func TestClusteredRewriteKeepsRanges(t *testing.T) {
	withRowGroup(t, 200)
	for _, tc := range []struct {
		name, spec, want string
		jitter, moves    int
	}{
		// id is exactly sorted and ts only nearly: id overlaps least. Rows go
		// back where they were, which is where their time belongs too.
		{"chosen", "", "id", 30, 0},
		// Both tie at nothing; the time column is the one a filter prunes on.
		// Some rows change their time, and move with it.
		{"tie-goes-to-time", "", "ts", 0, 10},
		// Merging on a column that is only approximately in order.
		{"override-approximate", "public.c=ts", "ts", 30, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withClusterBy(t, "none")
			legacy, before := buildScattered(t, tc.jitter, tc.moves)
			if c := legacy.rewrite(); c.clusterBy != "" {
				t.Fatalf("QS_CLUSTER_BY=none, yet the rewrite clustered on %q", c.clusterBy)
			}
			legacy.check("ts", "v", "n")
			worse, _ := overlapPairs(t, legacy.tbl, "ts")

			ClusterBy = tc.spec
			m, before2 := buildScattered(t, tc.jitter, tc.moves)
			if before2 != before {
				t.Fatalf("the two builds differ before the rewrite: %d and %d", before, before2)
			}
			c := m.rewrite()
			if c.clusterBy != tc.want {
				t.Fatalf("clustered on %q (%s), want %q", c.clusterBy, c.clusterWhy, tc.want)
			}
			m.check("ts", "v", "n")
			after, groups := overlapPairs(t, m.tbl, "ts")
			t.Logf("%s: overlapping row-group pairs on ts: %d before, %d after a clustered rewrite, "+
				"%d after an unclustered one (%d groups); %s", c.clusterBy, before, after, worse, groups, c.clusterWhy)

			const slack = 2 // the new rows add a row group at the end
			if after > before+slack {
				t.Errorf("row groups overlap on %d pairs after the rewrite, %d before it", after, before)
			}
			if worse < 3*max(after, 1) {
				t.Errorf("the unclustered rewrite overlaps on %d pairs and the clustered one on %d: "+
					"this test is not telling them apart", worse, after)
			}
		})
	}
}

// TestClusteredRewriteNewestWins: the old order made the newest copy of a key
// the one at the highest position, and "highest position wins" is what the key
// index does with two records. A clustered rewrite puts a row where its value
// says, so the newest copy can land FIRST — here it always does.
func TestClusteredRewriteNewestWins(t *testing.T) {
	withRowGroup(t, 100)
	withClusterBy(t, "public.r=n")
	before := clusteredRewrites.Load()
	tbl := racyTable(t)

	// A base clustered on n, holding a live row for key 7 at the very end.
	var base []map[string]any
	for i := 1000; i < 2000; i++ {
		base = append(base, map[string]any{"id": fmt.Sprint(i), "v": "base", "n": fmt.Sprint(i * 10)})
	}
	base = append(base, map[string]any{"id": "7", "v": "older", "n": "19990"})
	if err := tbl.writeParquet(filepath.Join(tbl.Dir, "base", "000001.parquet"), base); err != nil {
		t.Fatal(err)
	}
	tbl.State.BaseFiles, tbl.State.BaseRows, tbl.State.Seq = []string{"000001.parquet"}, len(base), 1

	write := func(seq int, rows ...map[string]any) {
		name := fmt.Sprintf("%06d.parquet", seq)
		if err := tbl.writeParquet(filepath.Join(tbl.Dir, "delta", name), rows); err != nil {
			t.Fatal(err)
		}
		tbl.State.DeltaFiles = append(tbl.State.DeltaFiles, name)
		tbl.noteDeltaFile(name, len(rows), nil)
		tbl.State.Seq = seq
	}
	// Key 7 again, live, with a value that sorts in front of the whole base.
	// Key 8 live in two deltas, the newer sorting first; key 9 the other way
	// round; key 10 twice in one file.
	write(2,
		map[string]any{"id": "7", "v": "newer", "n": "5"},
		map[string]any{"id": "8", "v": "older", "n": "15000"},
		map[string]any{"id": "9", "v": "older", "n": "6"},
		map[string]any{"id": "10", "v": "older", "n": "12000"},
		map[string]any{"id": "10", "v": "newer", "n": "7"},
	)
	write(3,
		map[string]any{"id": "8", "v": "newer", "n": "8"},
		map[string]any{"id": "9", "v": "newer", "n": "16000"},
	)
	if err := tbl.ensureIndex(); err != nil {
		t.Fatal(err)
	}

	c := tbl.BeginCompaction()
	<-c.done
	if _, err := tbl.FinishCompaction(); err != nil {
		t.Fatal(err)
	}
	if clusteredRewrites.Load() == before {
		t.Fatalf("the rewrite did not take the clustered path (%s)", c.clusterWhy)
	}

	got := map[string][]string{}
	if err := tbl.ForEachLive(func(r map[string]any) error {
		k := fmt.Sprint(r["id"])
		got[k] = append(got[k], fmt.Sprint(r["v"]))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1004 {
		t.Errorf("%d keys live, want 1004", len(got))
	}
	for _, k := range []string{"7", "8", "9", "10"} {
		if len(got[k]) != 1 || got[k][0] != "newer" {
			t.Errorf("key %s reads %v, want exactly one row, the newer", k, got[k])
		}
	}
	// And the index agrees with the file about where each one is.
	rows := baseRows(t, tbl)
	for _, k := range []string{"7", "8", "9", "10"} {
		l, ok := tbl.index.get(tbl.keyOfString(k))
		if !ok || int(l.Pos) >= len(rows) || fmt.Sprint(rows[l.Pos]["v"]) != "newer" {
			t.Errorf("key %s: the index points at %v, which is not the newer row", k, l)
		}
	}
	checkInvariants(t, tbl, "after the rewrite")
}

// TestUnclusteredRewriteIsUnchanged: with no column the base is clustered on,
// the new base is the old base's live rows and then the deltas', in order —
// the rewrite as it was.
func TestUnclusteredRewriteIsUnchanged(t *testing.T) {
	withRowGroup(t, 100)
	build := func() *clusterModel {
		rng := rand.New(rand.NewSource(9))
		m := &clusterModel{t: t, tbl: racyTable(t), want: map[string]map[string]any{}}
		ids := rng.Perm(2000)
		for lo := 0; lo < len(ids); lo += 400 {
			var ch []changestream.Change
			for _, id := range ids[lo : lo+400] {
				ch = append(ch, m.put(changestream.OpInsert, map[string]any{
					"id": fmt.Sprint(id + 1), "v": fmt.Sprintf("v%d", rng.Intn(50)), "n": fmt.Sprint(rng.Intn(100000)),
				}))
			}
			m.apply(ch)
		}
		m.rewrite()
		for round := 0; round < 3; round++ {
			var ch []changestream.Change
			for i := 0; i < 150; i++ {
				ch = append(ch, m.put(changestream.OpUpdate, map[string]any{
					"id": fmt.Sprint(1 + rng.Intn(2000)), "v": "u", "n": fmt.Sprint(rng.Intn(100000)),
				}))
			}
			m.apply(ch)
		}
		return m
	}
	seq := func(rows []map[string]any) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			out[i] = fmt.Sprintf("%v|%v|%v", Render(r["id"]), Render(r["v"]), Render(r["n"]))
		}
		return out
	}

	m := build()
	live, err := m.tbl.Live() // base then deltas: the order the rewrite always wrote
	if err != nil {
		t.Fatal(err)
	}
	c := m.rewrite()
	if c.clusterBy != "" {
		t.Fatalf("nothing here is clustered, yet the rewrite chose %q (%s)", c.clusterBy, c.clusterWhy)
	}
	if !strings.Contains(c.clusterWhy, "no column is clustered") {
		t.Errorf("reason given: %q", c.clusterWhy)
	}
	got := seq(baseRows(t, m.tbl))
	if want := seq(live); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("the new base is not the old live rows in order (%d rows, want %d)", len(got), len(want))
	}
	m.check("v", "n")

	// And it is what QS_CLUSTER_BY=none writes, row for row.
	withClusterBy(t, "none")
	off := build()
	off.rewrite()
	if strings.Join(seq(baseRows(t, off.tbl)), "\n") != strings.Join(got, "\n") {
		t.Error("the unclustered rewrite and QS_CLUSTER_BY=none wrote different files")
	}
}

// TestClusteredRewriteNulls: a row with no value in the cluster column is not
// placed by it. Base rows stay where they were; delta rows go last, in the
// order they arrived — and none is lost for having nothing to sort by.
func TestClusteredRewriteNulls(t *testing.T) {
	withRowGroup(t, 100)
	withClusterBy(t, "public.r=n")
	m := &clusterModel{t: t, tbl: racyTable(t), want: map[string]map[string]any{}}
	var ch []changestream.Change
	for i := 1; i <= 1000; i++ {
		row := map[string]any{"id": fmt.Sprint(i), "v": "seed", "n": fmt.Sprint(i * 10)}
		if i%50 == 0 {
			row["n"] = nil
		}
		ch = append(ch, m.put(changestream.OpInsert, row))
	}
	m.apply(ch)
	m.rewrite()

	ch = nil
	for _, id := range []int{2000, 2001, 2002, 2003, 2004, 2005} {
		row := map[string]any{"id": fmt.Sprint(id), "v": "new", "n": nil}
		if id%2 == 1 {
			row["n"] = fmt.Sprint((id - 2000) * 1000) // 1000, 3000, 5000
		}
		ch = append(ch, m.put(changestream.OpInsert, row))
	}
	// A base row loses its value, and a NULL one gains one.
	ch = append(ch, m.put(changestream.OpUpdate, map[string]any{"id": "10", "v": "nulled", "n": nil}))
	ch = append(ch, m.put(changestream.OpUpdate, map[string]any{"id": "50", "v": "valued", "n": "4995"}))
	m.apply(ch)
	c := m.rewrite()
	if c.clusterBy != "n" {
		t.Fatalf("clustered on %q (%s)", c.clusterBy, c.clusterWhy)
	}
	m.check("v", "n")

	rows := baseRows(t, m.tbl)
	var ids []string
	for _, r := range rows {
		ids = append(ids, fmt.Sprint(r["id"]))
	}
	at := func(id string) int {
		for i, x := range ids {
			if x == id {
				return i
			}
		}
		t.Fatalf("id %s is not in the base", id)
		return -1
	}
	// Valued delta rows sit between their neighbours...
	for _, c := range [][3]string{{"99", "2001", "101"}, {"299", "2003", "301"}, {"499", "50", "501"}, {"499", "2005", "501"}} {
		if !(at(c[0]) < at(c[1]) && at(c[1]) < at(c[2])) {
			t.Errorf("row %s is at %d, not between %s (%d) and %s (%d)", c[1], at(c[1]), c[0], at(c[0]), c[2], at(c[2]))
		}
	}
	// ...the NULL ones are the tail, in arrival order...
	if tail := strings.Join(ids[len(ids)-4:], ","); tail != "2000,2002,2004,10" {
		t.Errorf("the file ends %s, want the NULL delta rows 2000,2002,2004,10", tail)
	}
	// ...and a NULL base row has not moved.
	if !(at("99") < at("100") && at("100") < at("101")) {
		t.Errorf("NULL base row 100 moved to %d", at("100"))
	}
}

// TestCompactionRacesApplyClustered runs the interleaving model with row groups
// small enough that its second rewrite has a clustered base to merge into: rows
// superseded, deleted and re-inserted while a CLUSTERED rewrite reads them.
func TestCompactionRacesApplyClustered(t *testing.T) {
	withRowGroup(t, 50)
	before := clusteredRewrites.Load()
	t.Run("whole", func(t *testing.T) { testCompactionRaces(t, false) })
	t.Run("partial", func(t *testing.T) { testCompactionRaces(t, true) })
	if clusteredRewrites.Load() == before {
		t.Fatal("no rewrite took the clustered path; this run proved nothing new")
	}
}

func TestOrdCmp(t *testing.T) {
	ts := func(v int64) temporalValue { return temporalValue{k: kindDate, v: v} }
	for _, c := range []struct {
		a, b any
		want int
	}{
		{int64(1), int64(2), -1},
		{int32(5), int64(5), 0},
		{int16(-3), uint8(2), -1},
		{uint64(1 << 63), int64(7), 1},
		{float32(1.5), float64(1.25), 1},
		{"abc", "abd", -1},
		{"b", []byte("a"), 1},
		{[16]byte{0: 1}, [16]byte{0: 2}, -1},
		{[16]byte{15: 9}, [16]byte{15: 9}, 0},
		{ts(10), ts(9), 1},
		{false, true, -1},
	} {
		if ordClass(c.a) == ordNone || ordClass(c.a) != ordClass(c.b) {
			t.Errorf("%T and %T are not in one class", c.a, c.b)
			continue
		}
		if got := ordCmp(c.a, c.b); got != c.want {
			t.Errorf("ordCmp(%v, %v) = %d, want %d", c.a, c.b, got, c.want)
		}
		if got := ordCmp(c.b, c.a); got != -c.want {
			t.Errorf("ordCmp(%v, %v) = %d, want %d", c.b, c.a, got, -c.want)
		}
	}
	for _, v := range []any{nil, struct{}{}, []int{1}, map[string]any{}} {
		if ordClass(v) != ordNone {
			t.Errorf("%T has an order", v)
		}
	}
	if ordClass(int64(1)) == ordClass("1") || ordClass(ts(1)) == ordClass(temporalValue{k: kindTime, v: 1}) {
		t.Error("values of different kinds share a class")
	}
}

func TestChooseCluster(t *testing.T) {
	seq := func(n, width, step int64) []statRange {
		var r []statRange
		for i := int64(0); i < n; i++ {
			r = append(r, statRange{i * step, i*step + width})
		}
		return r
	}
	same := func(n int, lo, hi int64) []statRange {
		r := make([]statRange, n)
		for i := range r {
			r[i] = statRange{lo, hi}
		}
		return r
	}
	order := []string{"id", "status", "flag", "ts", "amount"}
	rank := func(name string) int {
		switch name {
		case "ts":
			return 2
		case "id":
			return 1
		}
		return 0
	}
	ranges := map[string][]statRange{
		"id":     same(20, 0, 1000),  // random keys: every group spans everything
		"status": same(20, 3, 3),     // one value everywhere is not clustering
		"ts":     seq(20, 12, 10),    // nearly sorted: neighbours overlap
		"amount": seq(20, 1000, 10),  // everything overlaps everything
		"flag":   seq(2, 1, 100)[:1], // statistics in one group only
	}
	if col, why := chooseCluster(order, ranges, 20, rank); col != "ts" {
		t.Errorf("chose %q (%s), want ts", col, why)
	}
	// Exactly sorted beats nearly sorted, whatever the rank...
	ranges["id"] = seq(20, 10, 10)
	if col, why := chooseCluster(order, ranges, 20, rank); col != "id" {
		t.Errorf("chose %q (%s), want id", col, why)
	}
	// ...and a tie goes to the time column.
	ranges["ts"] = seq(20, 5, 10)
	if col, why := chooseCluster(order, ranges, 20, rank); col != "ts" {
		t.Errorf("chose %q (%s), want ts", col, why)
	}
	delete(ranges, "ts")
	delete(ranges, "id")
	if col, why := chooseCluster(order, ranges, 20, rank); col != "" || !strings.Contains(why, "no column is clustered") {
		t.Errorf("chose %q (%s), want none", col, why)
	}
	if col, _ := chooseCluster(order, map[string][]statRange{"ts": seq(3, 5, 10)}, 3, rank); col != "" {
		t.Errorf("chose %q from three row groups", col)
	}

	for _, c := range []struct {
		spec, table, col string
		ok               bool
	}{
		{"", "public.o", "", false},
		{"none", "public.o", "none", true},
		{"public.o=ts,*=none", "public.o", "ts", true},
		{"*=none, public.o = ts", "public.o", "ts", true},
		{"public.o=ts", "public.x", "", false},
	} {
		if col, ok := clusterOverride(c.spec, c.table); col != c.col || ok != c.ok {
			t.Errorf("clusterOverride(%q, %q) = %q, %v", c.spec, c.table, col, ok)
		}
	}
}
