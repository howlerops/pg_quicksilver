package mirror

// The manifest is the only thing the caller's "confirmed to PostgreSQL" rests
// on, so the property worth testing is an ordering: at the instant the manifest
// is written, every file it names has been fsynced and so has the directory
// that names the file. Nothing outside the process can observe that, which is
// what syncHook is for.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// syncLog records every fsync under one table's directory, in order.
type syncLog struct {
	mu      sync.Mutex
	root    string
	seq     int
	file    map[string]int // data file -> when its contents were last synced
	dir     map[string]int // directory -> when it was last synced
	onState func()         // called as the manifest's temp file is synced
	fail    func(path string, dir bool) error
}

func watchSyncs(t *testing.T, root string) *syncLog {
	t.Helper()
	l := &syncLog{root: root, file: map[string]int{}, dir: map[string]int{}}
	hook := func(path string, dir bool) error {
		if !strings.HasPrefix(path, l.root) {
			return nil // a rewrite some earlier test abandoned
		}
		l.mu.Lock()
		l.seq++
		if dir {
			l.dir[path] = l.seq
		} else {
			// an atomic write syncs its temp file; what matters is the name
			// it is about to be renamed to
			l.file[strings.TrimSuffix(path, ".tmp")] = l.seq
		}
		fail, onState := l.fail, l.onState
		l.mu.Unlock()
		if fail != nil {
			if err := fail(path, dir); err != nil {
				return err
			}
		}
		if !dir && filepath.Base(path) == "state.json.tmp" && onState != nil {
			onState()
		}
		return nil
	}
	syncHook.Store(&hook)
	t.Cleanup(func() { syncHook.Store(nil) })
	return l
}

func TestDataFilesAreSyncedBeforeTheManifestNamesThem(t *testing.T) {
	root := t.TempDir()
	log := watchSyncs(t, root)
	cols := map[string]string{"id": "bigint", "v": "text", "n": "bigint"}
	tbl, err := New(root, "public", "r", "id", cols, []string{"id", "v", "n"})
	if err != nil {
		t.Fatal(err)
	}
	if log.dir[tbl.Dir] == 0 || log.dir[root] == 0 {
		t.Fatalf("New created %s and its subdirectories without syncing the directories that name them", tbl.Dir)
	}

	manifests := 0
	// Runs on the apply goroutine, inside saveState, so tbl.State is exactly
	// the manifest being written.
	log.onState = func() {
		manifests++
		named := map[string]string{} // path -> what it is, for the message
		for _, b := range tbl.State.BaseFiles {
			named[filepath.Join(tbl.Dir, "base", b)] = "base file"
		}
		for _, d := range tbl.State.DeltaFiles {
			named[filepath.Join(tbl.Dir, "delta", d)] = "delta file"
		}
		for stem, gen := range tbl.State.DVGen {
			named[filepath.Join(tbl.Dir, "dv", fmt.Sprintf("%s.%09d.dv.parquet", stem, gen))] = "deletion vector"
		}
		log.mu.Lock()
		defer log.mu.Unlock()
		for path, what := range named {
			if _, err := os.Stat(path); err != nil {
				t.Errorf("manifest %d names a %s that is not on disk: %v", manifests, what, err)
				continue
			}
			at, ok := log.file[path]
			if !ok {
				t.Errorf("manifest %d names %s %s, which was never fsynced",
					manifests, what, filepath.Base(path))
				continue
			}
			if log.dir[filepath.Dir(path)] < at {
				t.Errorf("manifest %d names %s %s, but its directory was not fsynced after it was written",
					manifests, what, filepath.Base(path))
			}
		}
	}
	// The rename of the manifest is itself a directory entry.
	manifestRenameSynced := func(when string) {
		t.Helper()
		log.mu.Lock()
		defer log.mu.Unlock()
		if log.dir[tbl.Dir] < log.file[tbl.statePath()] {
			t.Fatalf("%s: the table directory was not fsynced after the manifest was renamed into it", when)
		}
	}

	lsn := 0
	apply := func(ch []changestream.Change) {
		t.Helper()
		lsn++
		pos := fmt.Sprintf("0/%X", lsn)
		if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: pos, NextLSN: pos, Changes: ch}}); err != nil {
			t.Fatal(err)
		}
		manifestRenameSynced("Apply")
	}
	row := func(op changestream.Op, id int, v string) changestream.Change {
		r := map[string]any{"id": fmt.Sprint(id), "v": v, "n": "1"}
		return changestream.Change{Op: op, Schema: "public", Table: "r", Row: r, Key: r}
	}

	// whole rows, then updates and deletes that put vectors on delta files
	for b := 0; b < 3; b++ {
		var ch []changestream.Change
		for i := 0; i < 50; i++ {
			ch = append(ch, row(changestream.OpInsert, b*50+i, "x"))
		}
		apply(ch)
	}
	apply([]changestream.Change{row(changestream.OpUpdate, 3, "y"), row(changestream.OpDelete, 60, "")})

	// a merge of the deltas
	if _, err := tbl.MergeDeltas(); err != nil {
		t.Fatal(err)
	}
	manifestRenameSynced("MergeDeltas")

	// a background rewrite, with keys moving underneath it so that the new
	// base is installed together with a vector of its own
	c := tbl.BeginCompaction()
	if c == nil {
		t.Fatal("no compaction started")
	}
	apply([]changestream.Change{row(changestream.OpUpdate, 4, "z"), row(changestream.OpDelete, 61, "")})
	<-c.done
	before := manifests
	if _, err := tbl.FinishCompaction(); err != nil {
		t.Fatal(err)
	}
	if manifests == before {
		t.Fatal("the swap wrote no manifest")
	}
	manifestRenameSynced("FinishCompaction")
	if len(tbl.State.BaseFiles) != 1 || tbl.State.DVGen[dvStem(tbl.State.BaseFiles[0])] == 0 {
		t.Fatalf("the swap did not produce a base file with a vector: %+v", tbl.State)
	}

	// and updates against the base file, then the foreground rewrite
	apply([]changestream.Change{row(changestream.OpUpdate, 5, "w"), row(changestream.OpInsert, 900, "x")})
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	manifestRenameSynced("Compact")

	if manifests < 8 {
		t.Fatalf("only %d manifest writes were observed; the hook is not seeing saveState", manifests)
	}
	rows, err := tbl.Live()
	if err != nil || len(rows) != 149 {
		t.Fatalf("mirror holds %d rows, err=%v; want 149", len(rows), err)
	}
}

// A failed fsync is a batch that did not happen: Apply has to say so, and the
// manifest on disk must still carry the previous watermark, because the caller
// confirms to PostgreSQL whatever Apply returns without error.
func TestSyncFailureFailsTheApply(t *testing.T) {
	boom := errors.New("fsync: input/output error")
	for _, target := range []string{"delta file", "delta directory", "deletion vector",
		"deletion vector directory", "manifest", "table directory"} {
		t.Run(target, func(t *testing.T) {
			root := t.TempDir()
			log := watchSyncs(t, root)
			cols := map[string]string{"id": "bigint", "v": "text", "n": "bigint"}
			tbl, err := New(root, "public", "r", "id", cols, []string{"id", "v", "n"})
			if err != nil {
				t.Fatal(err)
			}
			r := map[string]any{"id": "1", "v": "x", "n": "1"}
			if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: "0/1", NextLSN: "0/1",
				Changes: []changestream.Change{{Op: changestream.OpInsert, Schema: "public", Table: "r", Row: r, Key: r}}}}); err != nil {
				t.Fatal(err)
			}

			log.fail = func(path string, dir bool) error {
				hit := false
				switch target {
				case "delta file":
					hit = !dir && filepath.Base(filepath.Dir(path)) == "delta"
				case "delta directory":
					hit = dir && filepath.Base(path) == "delta"
				case "deletion vector":
					hit = !dir && filepath.Base(filepath.Dir(path)) == "dv"
				case "deletion vector directory":
					hit = dir && filepath.Base(path) == "dv"
				case "manifest":
					hit = !dir && filepath.Base(path) == "state.json.tmp"
				case "table directory":
					hit = dir && path == tbl.Dir
				}
				if hit {
					return boom
				}
				return nil
			}
			u := map[string]any{"id": "1", "v": "y", "n": "2"}
			_, err = tbl.Apply([]changestream.Transaction{{CommitLSN: "0/2", NextLSN: "0/2",
				Changes: []changestream.Change{{Op: changestream.OpUpdate, Schema: "public", Table: "r", Row: u, Key: u}}}})
			if !errors.Is(err, boom) {
				t.Fatalf("Apply returned %v after a failed fsync of the %s", err, target)
			}
			if target == "table directory" {
				return // the rename happened; only its durability is in doubt
			}
			var onDisk State
			b, rerr := os.ReadFile(tbl.statePath())
			if rerr != nil {
				t.Fatal(rerr)
			}
			if err := json.Unmarshal(b, &onDisk); err != nil {
				t.Fatal(err)
			}
			if onDisk.AppliedLSN != "0/1" {
				t.Fatalf("manifest on disk advanced to %s although the %s was never made durable",
					onDisk.AppliedLSN, target)
			}
		})
	}
}
