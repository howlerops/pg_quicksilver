package mirror

// A file that compaction has already deleted must not read as "no rows here".
//
// Compaction saves the new manifest and then removes the old files, so a reader
// that loaded the manifest a moment earlier can open a file that is already
// gone. Skipping it silently returns a fraction of the table: an intact
// 200,302-row mirror read back as 363 rows, and the harness called it a
// divergence.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func TestMissingFileIsAnErrorNotAnEmptyRead(t *testing.T) {
	tbl := racyTable(t)
	var changes []changestream.Change
	for i := 1; i <= 50; i++ {
		row := map[string]any{"id": fmt.Sprint(i), "v": "x", "n": "0"}
		changes = append(changes, changestream.Change{
			Op: changestream.OpInsert, Schema: "public", Table: "r", Row: row, Key: row,
		})
	}
	if _, err := tbl.Apply([]changestream.Transaction{
		{CommitLSN: "0/1", NextLSN: "0/1", Changes: changes},
	}); err != nil {
		t.Fatal(err)
	}

	n := 0
	if err := tbl.ForEachLive(func(map[string]any) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != 50 {
		t.Fatalf("expected 50 rows before the file vanishes, got %d", n)
	}

	// Remove the file the manifest still names, exactly as compaction's cleanup
	// does to a reader holding the previous manifest.
	if len(tbl.State.DeltaFiles) == 0 {
		t.Fatal("expected a delta file to remove")
	}
	victim := filepath.Join(tbl.Dir, "delta", tbl.State.DeltaFiles[0])
	if err := os.Remove(victim); err != nil {
		t.Fatal(err)
	}

	n = 0
	err := tbl.ForEachLive(func(map[string]any) error { n++; return nil })
	if !errors.Is(err, ErrStaleManifest) {
		t.Fatalf("reading a manifest whose file is gone returned (%d rows, err=%v); "+
			"it must return ErrStaleManifest, or a partial read is indistinguishable "+
			"from a correct one", n, err)
	}
}

// TestReadLiveWithRetryResets guards the other half: a retry that does not
// discard the previous attempt double-counts, which looks like a divergence
// caused by the reader rather than the mirror.
func TestReadLiveWithRetryResets(t *testing.T) {
	tbl := racyTable(t)
	var changes []changestream.Change
	for i := 1; i <= 10; i++ {
		row := map[string]any{"id": fmt.Sprint(i), "v": "x", "n": "0"}
		changes = append(changes, changestream.Change{
			Op: changestream.OpInsert, Schema: "public", Table: "r", Row: row, Key: row,
		})
	}
	if _, err := tbl.Apply([]changestream.Transaction{
		{CommitLSN: "0/1", NextLSN: "0/1", Changes: changes},
	}); err != nil {
		t.Fatal(err)
	}

	count := 0
	resets := 0
	if err := tbl.ReadLiveWithRetry(
		func() { count = 0; resets++ },
		func(map[string]any) error { count++; return nil },
	); err != nil {
		t.Fatal(err)
	}
	if resets != 1 {
		t.Errorf("reset called %d times on a clean read, want 1", resets)
	}
	if count != 10 {
		t.Errorf("read %d rows, want 10", count)
	}
}

// TestReaderSeesAConsistentSnapshot is the bug that a benchmark found once in
// seven runs: a mirror that is entirely correct on disk, read by a process that
// is short exactly one row.
//
// qs-verify — and any query engine pointed at this directory — opens the mirror
// independently of the writer. It loads state.json, then reads the files that
// manifest names. A deletion vector overwritten in place between those two
// steps put a row in neither place: dead in the file the reader can see, alive
// only in a file the reader's manifest does not mention.
func TestReaderSeesAConsistentSnapshot(t *testing.T) {
	tbl := racyTable(t)
	var seed []changestream.Change
	for i := 1; i <= 100; i++ {
		row := map[string]any{"id": fmt.Sprint(i), "v": fmt.Sprintf("v%d", i), "n": "0"}
		seed = append(seed, changestream.Change{
			Op: changestream.OpInsert, Schema: "public", Table: "r", Row: row, Key: row,
		})
	}
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: "0/1", NextLSN: "0/1", Changes: seed,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	root := filepath.Dir(tbl.Dir)

	// An independent reader, holding the manifest as it is right now.
	reader, err := New(root, "public", "r", "id", tbl.Columns, tbl.Order)
	if err != nil {
		t.Fatal(err)
	}

	// The writer supersedes a row: the old copy is marked dead in the base file
	// the reader can see, and the new copy goes to a delta the reader's manifest
	// does not name.
	row := map[string]any{"id": "50", "v": "updated", "n": "1"}
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: "0/2", NextLSN: "0/2", Changes: []changestream.Change{{
			Op: changestream.OpUpdate, Schema: "public", Table: "r", Row: row, Key: row,
		}},
	}}); err != nil {
		t.Fatal(err)
	}

	n := 0
	var seen50 string
	if err := reader.ForEachLive(func(r map[string]any) error {
		n++
		if fmt.Sprint(r["id"]) == "50" {
			seen50 = Render(r["v"])
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
	if n != 100 {
		t.Errorf("reader saw %d rows, expected 100 — a row was marked dead by a "+
			"write whose replacement is not in the manifest this reader holds", n)
	}
	// Either value is a correct answer; only the row's absence is not. The
	// reader holds an older manifest, so the older value is what it should see.
	if seen50 != "v50" && seen50 != "updated" {
		t.Errorf("key 50 read back as %q", seen50)
	}
}

// TestReaderRefusesAManifestOlderThanTheVectors covers the other half. Old
// deletion-vector generations cannot be kept forever, so a reader holding a
// manifest old enough that the vector it names has been reclaimed must say so
// and re-read — never answer from a newer vector, which is exactly the wrong
// answer the generations exist to prevent.
func TestReaderRefusesAManifestOlderThanTheVectors(t *testing.T) {
	tbl := racyTable(t)
	var seed []changestream.Change
	for i := 1; i <= 20; i++ {
		row := map[string]any{"id": fmt.Sprint(i), "v": "x", "n": "0"}
		seed = append(seed, changestream.Change{
			Op: changestream.OpInsert, Schema: "public", Table: "r", Row: row, Key: row,
		})
	}
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: "0/1", NextLSN: "0/1", Changes: seed,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	update := func(lsn, id string) {
		t.Helper()
		row := map[string]any{"id": id, "v": "u" + lsn, "n": "1"}
		if _, err := tbl.Apply([]changestream.Transaction{{
			CommitLSN: "0/" + lsn, NextLSN: "0/" + lsn,
			Changes: []changestream.Change{{
				Op: changestream.OpUpdate, Schema: "public", Table: "r",
				Row: row, Key: row,
			}},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	update("2", "1") // base file's vector reaches generation 1
	root := filepath.Dir(tbl.Dir)
	reader, err := New(root, "public", "r", "id", tbl.Columns, tbl.Order)
	if err != nil {
		t.Fatal(err)
	}
	update("3", "2") // generation 2
	update("4", "3") // generation 3, which reclaims generation 1

	err = reader.ForEachLive(func(map[string]any) error { return nil })
	if !errors.Is(err, ErrStaleManifest) {
		t.Fatalf("reader returned %v; expected ErrStaleManifest once the "+
			"deletion vector its manifest names had been reclaimed", err)
	}
	// ...and the documented recovery works.
	n := 0
	if err := reader.ReadLiveWithRetry(func() { n = 0 },
		func(map[string]any) error { n++; return nil }); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if n != 20 {
		t.Errorf("after reloading, the reader saw %d rows, expected 20", n)
	}
}
