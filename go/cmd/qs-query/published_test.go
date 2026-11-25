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
	if second.SQL != want {
		t.Fatalf("published SQL is not the writer's view of its manifest:\n%s\n--- want\n%s", second.SQL, want)
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
