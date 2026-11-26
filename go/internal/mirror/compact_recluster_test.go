package mirror

// Re-clustering a base file an older release left declustered.
//
// The file is made here the way that release made it: scattered updates, then
// a rewrite that puts every changed row at the end, twice. What is measured is
// what the clustering is FOR — how many row groups a filter on one month of
// the time column has to read — before, and after the one rewrite that sorts.

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func withRecluster(t *testing.T, overlap, interval float64) {
	t.Helper()
	o, i := ReclusterOverlap, ReclusterIntervalSeconds
	ReclusterOverlap, ReclusterIntervalSeconds = overlap, interval
	t.Cleanup(func() { ReclusterOverlap, ReclusterIntervalSeconds = o, i })
}

// monthGroups is how many of the base file's row groups a filter on
// [from, to) of ts cannot skip: those whose min/max range meets it.
func monthGroups(t *testing.T, tbl *Table, from, to string) (read, groups int) {
	t.Helper()
	rows := baseRows(t, tbl)
	per := int(RowGroupRows)
	for lo := 0; lo < len(rows); lo += per {
		lowest, highest := "", ""
		for _, r := range rows[lo:min(lo+per, len(rows))] {
			ts := Render(r["ts"])
			if lowest == "" || ts < lowest {
				lowest = ts
			}
			if ts > highest {
				highest = ts
			}
		}
		groups++
		if lowest < to && highest >= from {
			read++
		}
	}
	if _, g := overlapPairs(t, tbl, "ts"); g != groups {
		t.Fatalf("the file has %d row groups, counted %d", g, groups)
	}
	return read, groups
}

// declusteredTable is forty thousand rows a year long, in time order, that an
// older release then rewrote twice with a fifth of the rows changed each time.
// random makes a table with no order at all, on any column.
func declusteredTable(t *testing.T, random bool) *clusterModel {
	t.Helper()
	withRowGroup(t, 200)
	withClusterBy(t, "none")
	rng := rand.New(rand.NewSource(11))
	m := &clusterModel{t: t, tbl: clusterTable(t), want: map[string]map[string]any{}}
	const rows = 40_000
	ids := make([]int, rows)
	for i := range ids {
		ids[i] = i + 1
	}
	if random {
		rng.Shuffle(rows, func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	}
	for lo := 0; lo < rows; lo += 5000 {
		var ch []changestream.Change
		for i := lo; i < lo+5000; i++ {
			at := i * 13 // thirteen minutes a row: 361 days
			if random {
				at = rng.Intn(rows * 13)
			}
			ch = append(ch, m.put(changestream.OpInsert, map[string]any{
				"id": fmt.Sprint(ids[i]), "ts": tsAt(at + rng.Intn(20)), "v": "seed", "n": fmt.Sprint(rng.Intn(1000)),
			}))
		}
		m.apply(ch)
	}
	m.rewrite()
	if random {
		return m
	}
	for round := 0; round < 2; round++ {
		var ch []changestream.Change
		for _, i := range rng.Perm(rows)[:rows/5] {
			row := copyRow(m.want[fmt.Sprint(i+1)])
			row["v"] = fmt.Sprintf("old-release-%d", round)
			ch = append(ch, m.put(changestream.OpUpdate, row))
		}
		m.apply(ch)
		if c := m.rewrite(); c.clusterBy != "" {
			t.Fatalf("the old release's rewrite clustered on %q", c.clusterBy)
		}
	}
	return m
}

func TestReclusterSortsADeclusteredBase(t *testing.T) {
	const hour = 3600
	withRecluster(t, 0.10, 6*hour)
	m := declusteredTable(t, false)
	tbl := m.tbl
	// One month, in the middle of the year.
	from, to := tsAt(150*24*60), tsAt(180*24*60)
	before, groups := monthGroups(t, tbl, from, to)
	pairsBefore, _ := overlapPairs(t, tbl, "ts")

	// This release now. Nothing is forced: the column is read off the file.
	ClusterBy = ""
	now := time.Now()
	tbl.clock = func() time.Time { return now }
	if why := tbl.CompactReason(); why != "" {
		t.Fatalf("a base file written a moment ago is rewritten for %q", why)
	}
	now = now.Add(5 * time.Hour)
	if why := tbl.CompactReason(); why != "" {
		t.Fatalf("five hours into a six-hour interval the table is rewritten for %q", why)
	}
	now = now.Add(2 * time.Hour)
	if why := tbl.CompactReason(); why != "recluster" {
		t.Fatalf("a declustered base past the interval: CompactReason is %q", why)
	}

	// Not a quiet table when it happens: updates, some of which move a row's
	// time, a few deletes and some new rows are waiting in a delta.
	rng := rand.New(rand.NewSource(3))
	var ch []changestream.Change
	for i := 0; i < 600; i++ {
		id := fmt.Sprint(1 + rng.Intn(40_000))
		if m.want[id] == nil {
			continue
		}
		row := copyRow(m.want[id])
		switch {
		case i%6 == 0:
			delete(m.want, id)
			ch = append(ch, changestream.Change{Op: changestream.OpDelete, Schema: "public", Table: "c", Key: map[string]any{"id": id}})
			continue
		case i%3 == 0:
			row["ts"] = tsAt(rng.Intn(40_000 * 13))
		}
		row["v"] = "now"
		ch = append(ch, m.put(changestream.OpUpdate, row))
	}
	for i := 0; i < 100; i++ {
		ch = append(ch, m.put(changestream.OpInsert, map[string]any{
			"id": fmt.Sprint(50_000 + i), "ts": tsAt(rng.Intn(40_000 * 13)), "v": "new", "n": "1",
		}))
	}
	// And a row with no time at all, which no sort can place.
	ch = append(ch, m.put(changestream.OpInsert, map[string]any{"id": "60000", "ts": nil, "v": "null", "n": "1"}))
	m.apply(ch)

	runs := sortedRuns.Load()
	c := m.rewrite()
	if c.clusterBy != "ts" || !c.sorted || !strings.Contains(c.clusterWhy, "declustered") {
		t.Fatalf("the rewrite did not re-cluster: column %q, sorted=%v, %s", c.clusterBy, c.sorted, c.clusterWhy)
	}
	nRuns := sortedRuns.Load() - runs
	if nRuns < 100 {
		t.Fatalf("the sort wrote %d runs: forty thousand rows in runs of 200 is 200", nRuns)
	}
	m.check("ts", "v", "n")
	after, groupsAfter := monthGroups(t, tbl, from, to)
	pairsAfter, _ := overlapPairs(t, tbl, "ts")
	// A month is 30/361 of the rows, and a sorted file reads just those groups.
	ideal := (len(m.want)*30/361)/200 + 2
	t.Logf("row groups a one-month filter on ts reads: %d of %d before, %d of %d after (a sorted file: at most %d); "+
		"overlapping pairs %d before, %d after; %d runs; %s",
		before, groups, after, groupsAfter, ideal, pairsBefore, pairsAfter, nRuns, c.clusterWhy)
	if before < 3*ideal {
		t.Fatalf("the declustered file reads %d groups for a month and a sorted one %d: it is not declustered enough to test", before, ideal)
	}
	if after > ideal {
		t.Errorf("after re-clustering a one-month filter reads %d row groups, a sorted file %d", after, ideal)
	}
	// Exactly sorted: the ordered rows rise, and the row with no time is last.
	rows := baseRows(t, tbl)
	for i := 1; i < len(rows)-1; i++ {
		if Render(rows[i-1]["ts"]) > Render(rows[i]["ts"]) {
			t.Fatalf("row %d is before row %d and later in time", i-1, i)
		}
	}
	if last := rows[len(rows)-1]; last["ts"] != nil {
		t.Errorf("the row with no time is not last: %v", last)
	}

	// Once. The file is in order now, and stays so through ordinary rewrites.
	now = now.Add(1000 * time.Hour)
	if why := tbl.CompactReason(); why != "" {
		t.Fatalf("after re-clustering the table is rewritten again for %q", why)
	}
	ch = nil
	for _, i := range rng.Perm(40_000)[:8000] {
		if row := m.want[fmt.Sprint(i+1)]; row != nil {
			row = copyRow(row)
			row["v"] = "later"
			ch = append(ch, m.put(changestream.OpUpdate, row))
		}
	}
	m.apply(ch)
	runs = sortedRuns.Load()
	if c := m.rewrite(); c.clusterBy != "ts" || c.sorted || sortedRuns.Load() != runs {
		t.Fatalf("the next rewrite: column %q, sorted=%v", c.clusterBy, c.sorted)
	}
	m.check("ts", "v", "n")
	if again, _ := monthGroups(t, tbl, from, to); again > ideal+1 {
		t.Errorf("one ordinary rewrite later a one-month filter reads %d row groups", again)
	}
	now = now.Add(1000 * time.Hour)
	if why := tbl.CompactReason(); why == "recluster" {
		t.Fatal("re-clustered twice")
	}
}

// TestReclusterLeavesAnUnclusteredTableAlone: with no column the base is
// clustered on there is nothing to sort by, and no rewrite is started for it.
func TestReclusterLeavesAnUnclusteredTableAlone(t *testing.T) {
	withRecluster(t, 0.10, 1)
	m := declusteredTable(t, true)
	tbl := m.tbl
	ClusterBy = ""
	now := time.Now().Add(1000 * time.Hour)
	tbl.clock = func() time.Time { return now }
	if why := tbl.CompactReason(); why != "" {
		t.Fatalf("a table with no clustered column is rewritten for %q", why)
	}
	var ch []changestream.Change
	for i := 1; i <= 3000; i++ {
		row := copyRow(m.want[fmt.Sprint(i)])
		row["v"] = "u"
		ch = append(ch, m.put(changestream.OpUpdate, row))
	}
	m.apply(ch)
	runs := sortedRuns.Load()
	if c := m.rewrite(); c.clusterBy != "" || c.sorted || sortedRuns.Load() != runs {
		t.Fatalf("a table with no clustered column was sorted on %q (%s)", c.clusterBy, c.clusterWhy)
	}
	m.check("ts", "v", "n")

	// And turned off, a declustered one is left alone too.
	ReclusterOverlap = 0
	d := declusteredTable(t, false)
	ClusterBy = ""
	d.tbl.clock = func() time.Time { return now }
	if why := d.tbl.CompactReason(); why != "" {
		t.Fatalf("QS_RECLUSTER_OVERLAP=0, and the table is rewritten for %q", why)
	}
}
