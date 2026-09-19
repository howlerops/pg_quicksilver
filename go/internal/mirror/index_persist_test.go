package mirror

// What the persisted key index cost, and why there isn't one any more.
//
// A compaction used to write index/<base>.idx.json — every key in the table, as
// JSON — so that a later ensureIndex could load positions instead of reading
// them back out of the base file. The SNAPSHOT path had stopped writing one
// long ago, and ensureIndex's comment said why: the file already holds
// everything the JSON said. Both rewrite paths kept writing it, which was never
// revisited.
//
// It surfaced in a measurement aimed at something else (docs/32). On the purge
// shape a REWRITTEN mirror came out 2.4x larger on disk than an un-rewritten one
// holding 100,000 more rows, and the base file had shrunk exactly as expected —
// 2.2 MB to 1.6 MB. One file accounted for all of the growth:
//
//	index/000003.idx.json   4,610,794 bytes
//	base/000003.parquet     1,638,400 bytes
//
// An index of a file, 2.8x the size of the file. So it was priced, and it lost
// on both axes at once: larger AND slower to load than rebuilding from the
// Parquet. Which is what a column store should do to a text format — the key
// column is stored compressed and columnar, while JSON holds every key as text
// with punctuation and allocates a map entry per key to parse it.
//
// This test keeps the price on the record, because "it seemed like a cache"
// is how it survived this long, and asserts the two things that make removing
// it safe: nothing writes one now, and the index built from the file is
// identical to the index the JSON would have produced.
//
//	go test ./internal/mirror/ -run TestPersistedIndexCost -v

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func TestPersistedIndexCost(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a few hundred thousand rows")
	}
	const rows = 200_000

	tbl := racyTable(t)
	changes := make([]changestream.Change, 0, rows)
	for i := 1; i <= rows; i++ {
		row := map[string]any{
			"id": fmt.Sprint(i), "v": fmt.Sprintf("v%d", i), "n": fmt.Sprint(i % 997),
		}
		changes = append(changes, changestream.Change{
			Op: changestream.OpInsert, Schema: "public", Table: "r", Row: row, Key: row,
		})
	}
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: "0/1", NextLSN: "0/2", Changes: changes,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	if len(tbl.State.BaseFiles) != 1 {
		t.Fatalf("expected one base file after compaction, got %v", tbl.State.BaseFiles)
	}

	stem := trimParquet(tbl.State.BaseFiles[0])
	idxPath := filepath.Join(tbl.Dir, "index", stem+".idx.json")
	basePath := filepath.Join(tbl.Dir, "base", tbl.State.BaseFiles[0])

	// 1. A compaction must not write one.
	if _, err := os.Stat(idxPath); err == nil {
		t.Errorf("a compaction wrote %s again; it is larger than the file it "+
			"indexes and slower to load than rebuilding from it", idxPath)
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}

	// 2. Rebuilding from the file is the only path, and it has to be right.
	rebuild := func() time.Duration {
		tbl.index, tbl.patch = nil, nil
		t0 := time.Now()
		if err := tbl.ensureIndex(); err != nil {
			t.Fatal(err)
		}
		d := time.Since(t0)
		if got := tbl.index.len(); got != rows {
			t.Fatalf("index holds %d keys, want %d", got, rows)
		}
		return d
	}
	fromParquet := rebuild()
	fromFileIdx := tbl.index

	// 3. The price, measured rather than remembered. The JSON is written HERE,
	// in the test, from the index the file produced — so the comparison is
	// between two ways of loading the same truth, and the shipped code keeps
	// only one of them.
	if err := writeJSON(idxPath, positionsOf(fromFileIdx)); err != nil {
		t.Fatal(err)
	}
	idxStat, err := os.Stat(idxPath)
	if err != nil {
		t.Fatal(err)
	}
	baseStat, err := os.Stat(basePath)
	if err != nil {
		t.Fatal(err)
	}

	id := baseID(tbl.State.BaseFiles[0])
	loadJSON := func() (time.Duration, *keyIndex) {
		idx := newKeyIndex(rows)
		t0 := time.Now()
		m, err := readIndexJSON(idxPath)
		if err != nil {
			t.Fatal(err)
		}
		for k, pos := range m {
			idx.set(tbl.keyOfString(k), loc{File: id, Pos: int32(pos)})
		}
		return time.Since(t0), idx
	}
	fromJSON, jsonIdx := loadJSON()

	// 4. Identical, or the faster path is faster at being wrong. A position
	// that disagrees is how a later markDead tombstones the wrong row.
	mismatch := 0
	fromFileIdx.each(func(k rowKey, want loc) {
		got, ok := jsonIdx.get(k)
		if !ok || got.File != want.File || got.Pos != want.Pos {
			mismatch++
		}
	})
	if mismatch != 0 {
		t.Errorf("%d keys land at different positions depending on which path "+
			"built the index; one of them is wrong", mismatch)
	}

	t.Logf("%d rows", rows)
	t.Logf("  base/%s.parquet    %9d bytes", stem, baseStat.Size())
	t.Logf("  the index as JSON  %9d bytes  (%.1fx the file it indexes)",
		idxStat.Size(), float64(idxStat.Size())/float64(baseStat.Size()))
	t.Logf("  build from the base file  %v", fromParquet.Round(time.Millisecond))
	t.Logf("  load from the JSON        %v", fromJSON.Round(time.Millisecond))

	// Not an assertion on the timing. It is a few tens of milliseconds on a
	// shared machine, and a test that fails when a runner is busy teaches
	// people to re-run it rather than read it. The SIZE is the unarguable half
	// and the numbers above are on the record either way.
	if idxStat.Size() <= baseStat.Size() {
		t.Logf("  (note: the JSON is no longer larger than the base file on "+
			"this shape — %d vs %d — which would be worth re-testing the "+
			"trade on", idxStat.Size(), baseStat.Size())
	}
}

// positionsOf renders an index the way the persisted file used to store it.
func positionsOf(x *keyIndex) map[string]int {
	out := make(map[string]int, x.len())
	x.each(func(k rowKey, l loc) { out[k.String()] = int(l.Pos) })
	return out
}

// readIndexJSON is the loader the shipped code no longer has, kept here so the
// comparison has both halves and the binary has one.
func readIndexJSON(path string) (map[string]int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m map[string]int
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return m, nil
}
