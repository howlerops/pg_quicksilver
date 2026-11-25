package mirror

// What marking a row dead costs as the vector it joins grows.
//
// markDead's own comment says "marking a position dead is O(1) per key", and
// for the file the row lives in that was true: the data file is never
// rewritten. It was not true of the VECTOR. Every batch re-read the whole
// vector from disk into a map[int]bool, added its handful of positions, sorted
// all of it and wrote all of it back — so the cost of deleting a thousand rows
// tracked the number of rows ALREADY dead in that file rather than the thousand.
//
// Measured here on a 3M-row base, median of seven 1,000-delete Apply calls,
// pinned to one core (taskset -c 1), the two builds run back to back on a
// machine that was doing other work — on a quiet one the left column was 3.5,
// 272 and 590 ms at 7k, 1M and 2M, so read the ratio rather than the figures:
//
//	dead in the base    re-read every batch    held in memory
//	           7,000                8.3 ms            4.6 ms
//	         107,000               62.2 ms           10.6 ms
//	       1,007,000              408.9 ms          100.4 ms
//	       2,007,000              788.1 ms          162.0 ms
//
// About a third of the left column was the read and the map, a third the sort
// and a third the write. The first two are gone (dvcache.go). The third is the
// on-disk contract — every write publishes a whole new immutable generation,
// see State.DVGen — and is what the right column still grows with: about 80 ns
// a position, which is Parquet encoding and zstd over the whole vector. It
// would be a quarter of that again with DELTA_BINARY_PACKED pages (measured:
// 36 ms against 125 ms for two million positions), which is a change to what
// readers are handed and so is not made here.
//
// It is a measurement, not an assertion about speed, and it builds a 3M-row
// mirror to take it, so it does not run unless asked:
//
//	QS_COST=markdead taskset -c 1 go test ./internal/mirror/ -run TestMarkDeadCost -v

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func TestMarkDeadCost(t *testing.T) {
	if testing.Short() || os.Getenv("QS_COST") != "markdead" {
		t.Skip("builds a 3M-row mirror; set QS_COST=markdead")
	}
	const N = 3_000_000
	tbl := racyTable(t)
	lsn := 0
	apply := func(changes []changestream.Change) time.Duration {
		lsn++
		pos := fmt.Sprintf("0/%X", lsn)
		t0 := time.Now()
		if _, err := tbl.Apply([]changestream.Transaction{{
			CommitLSN: pos, NextLSN: pos, Changes: changes,
		}}); err != nil {
			t.Fatal(err)
		}
		return time.Since(t0)
	}

	const per = 50_000
	for lo := 0; lo < N; lo += per {
		ch := make([]changestream.Change, 0, per)
		for i := lo; i < min(lo+per, N); i++ {
			r := map[string]any{"id": fmt.Sprint(i), "v": "x", "n": fmt.Sprint(i)}
			ch = append(ch, changestream.Change{
				Op: changestream.OpInsert, Schema: "public", Table: "r", Row: r, Key: r,
			})
		}
		apply(ch)
	}
	c := tbl.BeginCompaction()
	if c == nil {
		t.Fatal("no compaction started")
	}
	<-c.done
	if _, err := tbl.FinishCompaction(); err != nil {
		t.Fatal(err)
	}
	if len(tbl.State.BaseFiles) != 1 || len(tbl.State.DeltaFiles) != 0 {
		t.Fatalf("expected one base file and no deltas, got %+v", tbl.State)
	}
	baseRel := filepath.Join("base", tbl.State.BaseFiles[0])

	// Scattered, not clustered: 7919 is coprime to N, so this walks every key
	// exactly once in an order with no runs in it.
	next := 0
	dels := func(n int) []changestream.Change {
		ch := make([]changestream.Change, 0, n)
		for i := 0; i < n; i++ {
			k := map[string]any{"id": fmt.Sprint((next * 7919) % N)}
			next++
			ch = append(ch, changestream.Change{
				Op: changestream.OpDelete, Schema: "public", Table: "r", Key: k,
			})
		}
		return ch
	}

	const batch = 1000
	t.Logf("%12s %14s %10s %10s %12s", "dead", "apply median", "min", "max", "vector bytes")
	measure := func() {
		var ds []time.Duration
		for i := 0; i < 7; i++ {
			ds = append(ds, apply(dels(batch)))
		}
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		gen := tbl.State.DVGen[dvStem(baseRel)]
		fi, err := os.Stat(tbl.dvPathGen(baseRel, gen))
		if err != nil {
			t.Fatal(err)
		}
		ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
		t.Logf("%12d %11.2f ms %7.2f ms %7.2f ms %12d",
			next, ms(ds[len(ds)/2]), ms(ds[0]), ms(ds[len(ds)-1]), fi.Size())
	}
	measure()
	for _, target := range []int{100_000, 1_000_000, 2_000_000} {
		for next < target {
			apply(dels(min(250_000, target-next)))
		}
		measure()
	}
}
