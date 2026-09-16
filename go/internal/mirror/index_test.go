package mirror

// The index now encodes a file as an integer rather than carrying its path, and
// a key as an int64 when the primary key is an integer type. Both are
// invisible from the outside when they work and produce a silently wrong mirror
// when they do not, so both are pinned here.

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func TestFileIDRoundTrip(t *testing.T) {
	for _, seq := range []int{1, 2, 7, 999, 123456, 1 << 20} {
		for _, dir := range []string{"base", "delta"} {
			rel := filepath.Join(dir, fmt.Sprintf("%06d.parquet", seq))
			id := parseFileID(rel)
			if !id.valid() {
				t.Fatalf("%s did not parse", rel)
			}
			if got := id.name(); got != rel {
				t.Errorf("%s round-tripped to %s", rel, got)
			}
			if id.seq() != seq {
				t.Errorf("%s has seq %d, expected %d", rel, id.seq(), seq)
			}
		}
	}
	// base and delta of the SAME sequence number must be different files, which
	// is the whole reason the id carries a kind bit.
	if baseID("000007.parquet") == deltaID("000007.parquet") {
		t.Error("base/000007 and delta/000007 collide")
	}
	// the zero value has to mean "nowhere", or an empty loc looks like a
	// location in the first file ever written
	if (loc{}).File.valid() {
		t.Error("the zero loc points at a real file")
	}
}

// TestTextPrimaryKey is the path a numeric key does not take. Integer keys are
// most tables, so a bug in the string fallback would sit undetected behind every
// other test in this package.
func TestTextPrimaryKey(t *testing.T) {
	cols := map[string]string{"sku": "text", "qty": "bigint"}
	order := []string{"sku", "qty"}
	tbl, err := New(t.TempDir(), "public", "p", "sku", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	if tbl.numericKey {
		t.Fatal("a text primary key was classified as numeric")
	}
	ch := func(op changestream.Op, row map[string]any) changestream.Change {
		return changestream.Change{Op: op, Schema: "public", Table: "p", Row: row, Key: row}
	}
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: "0/1", NextLSN: "0/1", Changes: []changestream.Change{
			ch(changestream.OpInsert, map[string]any{"sku": "AA-1", "qty": "1"}),
			ch(changestream.OpInsert, map[string]any{"sku": "BB-2", "qty": "2"}),
			// a key that LOOKS numeric must still not collide with a real one
			ch(changestream.OpInsert, map[string]any{"sku": "42", "qty": "3"}),
		},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: "0/2", NextLSN: "0/2", Changes: []changestream.Change{
			ch(changestream.OpUpdate, map[string]any{"sku": "BB-2", "qty": "20"}),
			{Op: changestream.OpDelete, Schema: "public", Table: "p",
				Key: map[string]any{"sku": "42"}},
		},
	}}); err != nil {
		t.Fatal(err)
	}

	got := map[string]string{}
	if err := tbl.ForEachLive(func(r map[string]any) error {
		got[Render(r["sku"])] = Render(r["qty"])
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"AA-1": "1", "BB-2": "20"}
	if len(got) != len(want) {
		t.Fatalf("mirror has %v, expected %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("sku %s = %q, expected %q", k, got[k], v)
		}
	}
}

// TestNumericKeyMatchesAcrossSources: pgoutput hands over "123" and Parquet
// hands back int64(123). If those did not land on the same index entry, an
// update after a compaction would tombstone nothing and leave two live copies.
func TestNumericKeyMatchesAcrossSources(t *testing.T) {
	tbl := racyTable(t)
	if !tbl.numericKey {
		t.Fatal("a bigint primary key was not classified as numeric")
	}
	from := tbl.keyOfString("123")
	back := tbl.keyOf(int64(123))
	if from != back {
		t.Fatalf("text key %v and Parquet key %v differ", from, back)
	}
	if !from.isNum || from.String() != "123" {
		t.Errorf("key rendered as %q", from.String())
	}
}
