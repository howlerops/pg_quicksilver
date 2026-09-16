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
