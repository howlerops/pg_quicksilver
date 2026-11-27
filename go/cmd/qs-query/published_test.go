package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

// -published must hand out what the WRITER published, with nothing but the
// directory: no column list, no catalog, no manifest parsing of its own.
func TestPublishedModeReadsWhatTheWriterPublished(t *testing.T) {
	root := t.TempDir()
	cols := map[string]string{"id": "bigint", "v": "text"}
	tbl, err := mirror.New(root, "public", "r", "id", cols, []string{"id", "v"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mirror.ReadPublished(root, "public", "r"); err == nil {
		t.Fatal("a mirror that has never written a manifest has a published snapshot")
	}
	apply := func(lsn string, id int) {
		t.Helper()
		row := map[string]any{"id": fmt.Sprint(id), "v": "x"}
		if _, err := tbl.Apply([]changestream.Transaction{{
			CommitLSN: lsn, NextLSN: lsn, Changes: []changestream.Change{{
				Op: changestream.OpInsert, Schema: "public", Table: "r", Row: row, Key: row,
			}},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	apply("0/1", 1)
	first, err := mirror.ReadPublished(root, "public", "r")
	if err != nil {
		t.Fatal(err)
	}
	apply("0/2", 2)
	second, err := mirror.ReadPublished(root, "public", "r")
	if err != nil {
		t.Fatal(err)
	}

	// The sequence number is the whole change-detection protocol.
	a, _ := publishedScalar(first, true, false)
	b, _ := publishedScalar(second, true, false)
	if a != "1" || b != "2" {
		t.Fatalf("-seq printed %q then %q across one apply; want 1 then 2", a, b)
	}
	if lsn, ok := publishedScalar(second, false, true); !ok || lsn != "0/2" {
		t.Fatalf("-published -lsn printed %q", lsn)
	}
	if _, ok := publishedScalar(second, false, false); ok {
		t.Fatal("plain -published must print the SQL, not a scalar")
	}

	want, err := tbl.ViewSQL()
	if err != nil {
		t.Fatal(err)
	}
	// Rendered where the writer wrote it, the published snapshot is the
	// writer's view of its manifest, path for path.
	if got := publishedSQL(second, root, "", "public", "r"); got != want {
		t.Fatalf("published SQL is not the writer's view of its manifest:\n%s\n--- want\n%s", got, want)
	}
	// What is published names no path of the writer's: a reader that mounts
	// the mirror elsewhere could not have used one.
	if strings.Contains(second.SQL, root) {
		t.Fatalf("the published SQL contains the writer's absolute path %s:\n%s", root, second.SQL)
	}
	if second.Paths != mirror.PathsRelative || second.BaseDir != tbl.Dir {
		t.Fatalf("paths=%q base_dir=%q", second.Paths, second.BaseDir)
	}
	// -root: the same snapshot for an engine that sees the mirror at /mnt/qs.
	moved := publishedSQL(second, root, "/mnt/qs", "public", "r")
	if strings.Contains(moved, root) || strings.Count(moved, "read_parquet('/mnt/qs/public.r/delta/") != 2 {
		t.Fatalf("-root /mnt/qs rendered:\n%s", moved)
	}
	if moved != strings.ReplaceAll(want, root, "/mnt/qs") {
		t.Fatalf("-root changed more than the paths:\n%s\n--- want\n%s", moved, strings.ReplaceAll(want, root, "/mnt/qs"))
	}
	// A snapshot.json from a writer that published absolute paths is still
	// read, and still re-rooted.
	legacy := *second
	legacy.SQL, legacy.Paths, legacy.BaseDir = want, "", ""
	if got := publishedSQL(&legacy, root, "", "public", "r"); got != want {
		t.Fatalf("an absolute-path snapshot rendered at its own root changed:\n%s", got)
	}
	if got := publishedSQL(&legacy, root, "/mnt/qs", "public", "r"); got != moved {
		t.Fatalf("an absolute-path snapshot was not re-rooted:\n%s", got)
	}
	if first.SQL == second.SQL {
		t.Fatal("two manifests published the same SQL")
	}
	h := publishedHeader(second)
	for _, part := range []string{"-- quicksilver mirror public.r", "applied_lsn=0/2", "snapshot_seq=2", "grace_seconds="} {
		if !strings.Contains(h, part) {
			t.Errorf("header %q lacks %q", h, part)
		}
	}
}
