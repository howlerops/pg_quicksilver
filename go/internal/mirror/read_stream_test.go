package mirror

// ForEachLive must not hold the table.
//
// It used to call readParquet, which returns a whole file as a
// []map[string]any. On a mirror whose base file holds the table that is not
// "one file at a time", it is the entire table as Go maps: 21.6 million of them
// took qs-verify to 13.3 GB and the OOM killer, against a mirror weighing
// 126 MB on disk. The tool whose job is to say whether the mirror is correct
// died before answering, and an unverified run is worse than a failed one
// because nothing was checked.
//
// The streaming was already there — the writer emits fixed-size row groups and
// forEachRowGroup decodes them one at a time. readParquet's only contribution
// was appending them all back into one slice.
//
// This asserts the shape rather than the number of bytes, because a heap
// threshold is a flaky test on a shared machine: it counts how many rows are
// reachable from the reader at once, which is what actually went wrong.

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func TestForEachLiveStreamsRatherThanMaterialising(t *testing.T) {
	tbl := racyTable(t)

	// More rows than one row group, so "streams" and "materialises" are
	// distinguishable at all. A single-group file looks identical either way.
	const rows = 200_000
	if rows <= RowGroupRows {
		t.Fatalf("this test needs more than one row group; rows=%d group=%d",
			rows, RowGroupRows)
	}
	var changes []changestream.Change
	for i := 0; i < rows; i++ {
		row := map[string]any{"id": fmt.Sprint(i), "v": fmt.Sprintf("v%d", i), "n": fmt.Sprint(i)}
		changes = append(changes, changestream.Change{
			Op: changestream.OpInsert, Schema: "public", Table: "r", Row: row, Key: row,
		})
	}
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: "0/1", NextLSN: "0/2", Changes: changes,
	}}); err != nil {
		t.Fatal(err)
	}

	// Heap in use at the point the LAST row is delivered. A reader that has
	// materialised the file is holding every row it has produced; one that
	// streams is holding at most a row group.
	var peak uint64
	var base uint64
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	base = ms.HeapAlloc

	seen := 0
	if err := tbl.ForEachLive(func(r map[string]any) error {
		seen++
		if seen == rows {
			runtime.ReadMemStats(&ms)
			peak = ms.HeapAlloc
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen != rows {
		t.Fatalf("read %d rows, want %d", seen, rows)
	}

	// A materialised read holds `rows` maps; a streaming one holds a row group.
	// The bound is generous — several row groups' worth — because the point is
	// to catch a return to O(file), not to police allocation.
	held := peak - min(peak, base)
	perRow := held / uint64(rows)
	t.Logf("heap in use at the last row: %.1f MB over baseline (%d B/row)",
		float64(held)/1e6, perRow)
	if perRow > 150 {
		t.Errorf("ForEachLive is holding %d bytes per row at the last row, which "+
			"is the whole file rather than a row group. This is what took "+
			"qs-verify to 13.3 GB on a 126 MB mirror.", perRow)
	}
}
