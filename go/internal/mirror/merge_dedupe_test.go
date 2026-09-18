package mirror

// A merge must not put the same key in one file twice.
//
// docs/31. A merge concatenates the live rows of several delta files. If a key
// is live in two of them, both copies used to land in the merged file — and
// there is no way to fix that afterwards, because a deletion vector addresses
// positions within ONE file and both positions are now in the same file. The
// mirror then returns the row twice: count(*) and every aggregate disagree with
// the source while every file on disk is individually valid.
//
// It was found by a concurrency test that reproduced it about once in twenty
// runs, which is the rate at which a real bug gets called a flake. This test is
// DETERMINISTIC: it builds the precondition directly rather than racing for it,
// so the bug cannot come back disguised as timing.

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestMergeNeverDuplicatesAKey(t *testing.T) {
	tbl := racyTable(t)

	// Two delta files that both hold a live row for key 7. The apply path
	// normally prevents this by tombstoning the older copy, so it is built by
	// hand: the point is what MERGE does when handed it, not how it arose.
	write := func(seq int, v string) string {
		name := fmt.Sprintf("%06d.parquet", seq)
		rows := []map[string]any{
			{"id": "7", "v": v, "n": fmt.Sprint(seq)},
			{"id": fmt.Sprint(100 + seq), "v": "other", "n": fmt.Sprint(seq)},
		}
		if err := tbl.writeParquet(filepath.Join(tbl.Dir, "delta", name), rows); err != nil {
			t.Fatal(err)
		}
		tbl.State.DeltaFiles = append(tbl.State.DeltaFiles, name)
		tbl.noteDeltaFile(name, len(rows), nil)
		if tbl.State.Seq < seq {
			tbl.State.Seq = seq
		}
		return name
	}
	a := write(2, "older")
	b := write(3, "newer")

	if err := tbl.ensureIndex(); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.mergeGroup([]string{a, b}, nil); err != nil {
		t.Fatalf("mergeGroup: %v", err)
	}

	seen := map[string]int{}
	var dupKey string
	if err := tbl.ForEachLive(func(r map[string]any) error {
		k := fmt.Sprint(r["id"])
		seen[k]++
		if seen[k] > 1 {
			dupKey = k
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if dupKey != "" {
		t.Errorf("key %s appears %d times after a merge; a deletion vector "+
			"cannot fix this, because both positions are in the same file",
			dupKey, seen[dupKey])
	}

	// And the survivor must be the NEWER one. Files merge oldest-first, so the
	// later row is the newer value — keeping the first would silently roll the
	// mirror back to a superseded value, which is worse than a duplicate
	// because nothing counts wrong.
	var got string
	if err := tbl.ForEachLive(func(r map[string]any) error {
		if fmt.Sprint(r["id"]) == "7" {
			got = fmt.Sprint(r["v"])
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got != "newer" {
		t.Errorf("after the merge key 7 reads %q, want \"newer\" — the merge "+
			"kept a superseded value", got)
	}
}
