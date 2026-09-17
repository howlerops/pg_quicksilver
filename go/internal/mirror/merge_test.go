package mirror

// A bounded merge folds a run of delta files from the MIDDLE of the manifest,
// where the old one always folded the newest run and could therefore append its
// output on the end. Getting the replacement position wrong is the kind of bug
// this project keeps finding: every row is present, every count matches, and
// the mirror answers with a superseded value.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// writeBatches drives n separate batches so that n delta files exist.
func writeBatches(t *testing.T, tbl *Table, batches, perBatch int, val func(b, i int) string) {
	t.Helper()
	for b := 0; b < batches; b++ {
		var changes []changestream.Change
		for i := 0; i < perBatch; i++ {
			row := map[string]any{
				"id": fmt.Sprint(i),
				"v":  val(b, i),
				"n":  fmt.Sprint(b),
			}
			op := changestream.OpUpdate
			if b == 0 {
				op = changestream.OpInsert
			}
			changes = append(changes, changestream.Change{
				Op: op, Schema: "public", Table: "r", Row: row, Key: row,
			})
		}
		if _, err := tbl.Apply([]changestream.Transaction{{
			CommitLSN: fmt.Sprintf("0/%d", b+1),
			NextLSN:   fmt.Sprintf("0/%d", b+2),
			Changes:   changes,
		}}); err != nil {
			t.Fatal(err)
		}
	}
}

// The newest value must survive a bounded merge. Every row is rewritten in
// every batch, so if a merge of older files ever displaces a newer one, the
// mirror answers with a value the source last held several batches ago.
func TestBoundedMergeKeepsTheNewestValue(t *testing.T) {
	defer func(prev int) { MaxMergeFiles = prev }(MaxMergeFiles)
	MaxMergeFiles = 3

	tbl := racyTable(t)
	const batches, rows = 12, 40
	writeBatches(t, tbl, batches, rows, func(b, i int) string {
		return fmt.Sprintf("b%d-r%d", b, i)
	})

	for i := 0; i < 4; i++ {
		if _, err := tbl.MergeDeltas(); err != nil {
			t.Fatal(err)
		}
	}

	seen := 0
	if err := tbl.ForEachLive(func(r map[string]any) error {
		seen++
		id := Render(r["id"])
		want := fmt.Sprintf("b%d-r%s", batches-1, id)
		if got := Render(r["v"]); got != want {
			t.Errorf("id=%s: mirror holds %q, source last wrote %q", id, got, want)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen != rows {
		t.Errorf("read back %d rows, expected %d", seen, rows)
	}
}

// The bound has to actually bind, or the test above proves nothing about the
// middle-of-the-manifest case it exists for.
func TestBoundedMergeFoldsAtMostTheBound(t *testing.T) {
	defer func(prev int) { MaxMergeFiles = prev }(MaxMergeFiles)
	MaxMergeFiles = 3

	tbl := racyTable(t)
	writeBatches(t, tbl, 10, 5, func(b, i int) string { return fmt.Sprintf("v%d", b) })
	before := len(tbl.State.DeltaFiles)
	if before < 5 {
		t.Fatalf("expected several delta files to merge, got %d", before)
	}
	if _, err := tbl.MergeDeltas(); err != nil {
		t.Fatal(err)
	}
	after := len(tbl.State.DeltaFiles)
	// The bound is on files TOUCHED, so the drop is at most that many: folding
	// k files into one removes k-1, and removes all k when every row in them
	// was already superseded and no replacement file is written. Anything more
	// means the bound was ignored and the whole manifest went in one pass.
	if drop := before - after; drop > MaxMergeFiles {
		t.Errorf("one merge removed %d files with the bound at %d; it folded more "+
			"than it was allowed to", drop, MaxMergeFiles)
	}
}

// Repeated bounded merges must converge rather than leaving a prefix that can
// never be folded — which is what taking the NEWEST files each time would do.
func TestBoundedMergeConverges(t *testing.T) {
	defer func(prev int) { MaxMergeFiles = prev }(MaxMergeFiles)
	MaxMergeFiles = 3

	tbl := racyTable(t)
	writeBatches(t, tbl, 16, 5, func(b, i int) string { return fmt.Sprintf("v%d", b) })
	for i := 0; i < 20; i++ {
		n, err := tbl.MergeDeltas()
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 && len(tbl.State.DeltaFiles) < 2 {
			break
		}
	}
	if got := len(tbl.State.DeltaFiles); got > 1 {
		t.Errorf("after repeated merges %d delta files remain; a bounded merge "+
			"that takes the newest run leaves an unmergeable prefix behind", got)
	}
}

// Unbounded is the old behaviour and still has to work.
func TestUnboundedMergeStillFoldsEverything(t *testing.T) {
	defer func(prev int) { MaxMergeFiles = prev }(MaxMergeFiles)
	MaxMergeFiles = 0

	tbl := racyTable(t)
	writeBatches(t, tbl, 8, 5, func(b, i int) string { return fmt.Sprintf("v%d", b) })
	if _, err := tbl.MergeDeltas(); err != nil {
		t.Fatal(err)
	}
	if got := len(tbl.State.DeltaFiles); got != 1 {
		t.Errorf("an unbounded merge left %d delta files, expected 1", got)
	}
}

// The snapshot no longer writes a key -> position index, so ensureIndex has to
// reconstruct it from the base file. That path existed before as a fallback for
// a truncated write; it is now the normal one, and a mirror that reopens with a
// wrong index leaves superseded rows visible forever while every count matches.
func TestIndexRebuildsFromTheBaseFileAlone(t *testing.T) {
	tbl := racyTable(t)
	const rows = 200
	var changes []changestream.Change
	for i := 0; i < rows; i++ {
		row := map[string]any{"id": fmt.Sprint(i), "v": fmt.Sprintf("first-%d", i), "n": "0"}
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

	// Remove every persisted index, which is the state a snapshot now leaves.
	idx, _ := filepath.Glob(filepath.Join(tbl.Dir, "index", "*.idx.json"))
	for _, p := range idx {
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}

	// Reopen and update half the rows. Each update has to find and retire the
	// row the rebuilt index points at; if a position is wrong, the old copy
	// stays live and the mirror holds two rows for one key.
	root := strings.TrimSuffix(tbl.Dir, "/public.r")
	re, err := New(root, "public", "r", "id",
		map[string]string{"id": "bigint", "v": "text", "n": "bigint"},
		[]string{"id", "v", "n"})
	if err != nil {
		t.Fatal(err)
	}
	var updates []changestream.Change
	for i := 0; i < rows; i += 2 {
		row := map[string]any{"id": fmt.Sprint(i), "v": fmt.Sprintf("second-%d", i), "n": "1"}
		updates = append(updates, changestream.Change{
			Op: changestream.OpUpdate, Schema: "public", Table: "r", Row: row, Key: row,
		})
	}
	if _, err := re.Apply([]changestream.Transaction{{
		CommitLSN: "0/3", NextLSN: "0/4", Changes: updates,
	}}); err != nil {
		t.Fatal(err)
	}

	seen := map[string]string{}
	if err := re.ForEachLive(func(r map[string]any) error {
		id := Render(r["id"])
		if prev, dup := seen[id]; dup {
			t.Fatalf("id=%s is live twice (%q and %q): the rebuilt index did not "+
				"point at the row the update had to retire", id, prev, Render(r["v"]))
		}
		seen[id] = Render(r["v"])
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != rows {
		t.Fatalf("read back %d rows, expected %d", len(seen), rows)
	}
	for i := 0; i < rows; i++ {
		want := fmt.Sprintf("first-%d", i)
		if i%2 == 0 {
			want = fmt.Sprintf("second-%d", i)
		}
		if got := seen[fmt.Sprint(i)]; got != want {
			t.Errorf("id=%d: mirror holds %q, want %q", i, got, want)
		}
	}
}
