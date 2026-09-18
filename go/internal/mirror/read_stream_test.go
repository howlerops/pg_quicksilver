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
	"path/filepath"
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

	// SELF-CALIBRATING, not an absolute threshold.
	//
	// The first version asserted "under 150 bytes per row", which is a number
	// measured on one machine. It failed under -race (158 B/row, because the
	// detector adds shadow memory), was patched to skip there, and then failed
	// again on a GitHub runner under a plain `go test` — a different machine
	// with different GC timing. Its own comment said a heap threshold is flaky
	// and it used one anyway; skipping -race treated the symptom.
	//
	// So it measures BOTH readers, here, in this process, on whatever machine is
	// running: the streaming path against a deliberately materialising one. Any
	// constant would be a property of the machine; the RATIO is a property of
	// the code, which is what the test is about.
	measure := func(read func(func(map[string]any) error) error) uint64 {
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		base := ms.HeapAlloc
		var peak uint64
		seen := 0
		if err := read(func(r map[string]any) error {
			seen++
			if seen == rows {
				// GC FIRST. HeapAlloc counts garbage that has not been
				// collected yet, not live memory, so without this the streaming
				// reader is charged for rows it already delivered and dropped —
				// entirely at the mercy of when the collector last ran. On this
				// machine that read 32.6 MB against 101.9; on a CI runner the
				// same code read 71.9 against 83.3 and the test failed.
				//
				// A collection here frees exactly what the streaming reader is
				// no longer holding, and cannot free the materialising reader's
				// slice, which is still referenced. That difference IS the
				// property under test.
				runtime.GC()
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
		return peak - min(peak, base)
	}

	streaming := measure(tbl.ForEachLive)

	// The reader as it was before the fix: one file materialised into a slice
	// of maps before a single row is delivered. Kept here, in the test, because
	// a comparison needs both halves and the shipped code must only have one.
	materialising := measure(func(fn func(map[string]any) error) error {
		// The same file set ForEachLive walks. Reading only BaseFiles found
		// nothing at all — the test writes one batch, which lands in a DELTA —
		// and "0 rows" is a comparison against nothing dressed up as a pass.
		for _, set := range [][2]any{
			{"base", tbl.State.BaseFiles}, {"delta", tbl.State.DeltaFiles},
		} {
			sub := set[0].(string)
			for _, f := range set[1].([]string) {
				rel := filepath.Join(sub, f)
				if tbl.partialColsOf(rel) != nil {
					continue
				}
				dead, err := tbl.deadPositions(rel)
				if err != nil {
					return err
				}
				all, err := tbl.readParquet(filepath.Join(tbl.Dir, rel), dead)
				if err != nil {
					return err
				}
				for _, r := range all {
					if err := fn(r); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})

	t.Logf("at the last row: streaming holds %.1f MB, materialising holds %.1f MB",
		float64(streaming)/1e6, float64(materialising)/1e6)

	// Two-to-one is a wide margin on purpose. Measured at roughly three-to-one
	// (113 B/row against 373), and the point is to catch a return to O(file),
	// not to police allocation.
	if streaming*2 > materialising {
		t.Errorf("ForEachLive holds %d bytes at the last row against %d for a "+
			"reader that materialises the whole file — it is not streaming. "+
			"This is what took qs-verify to 13.3 GB on a 126 MB mirror.",
			streaming, materialising)
	}
}
