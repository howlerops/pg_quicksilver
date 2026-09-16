package mirror

// Column-partial deltas change what is physically on disk for an ordinary
// UPDATE, so these tests check two different things and both of them matter:
// that the mirror still reads back exactly right, and that the bytes actually
// went away. A correct mirror that writes the same amount is the optimisation
// not happening; a smaller mirror that reads back wrong is the worst outcome
// this package can produce.

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// docTable is the jsonb shape from docs/19 in miniature: a key, a small
// counter, and one large column that updates never touch.
func docTable(t *testing.T) *Table {
	t.Helper()
	cols := map[string]string{"id": "bigint", "n": "bigint", "doc": "text"}
	order := []string{"id", "n", "doc"}
	tbl, err := New(t.TempDir(), "public", "d", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func applyAt(t *testing.T, tbl *Table, lsn int, changes ...changestream.Change) {
	t.Helper()
	pos := fmt.Sprintf("0/%X", lsn)
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: pos, NextLSN: pos, Changes: changes,
	}}); err != nil {
		t.Fatal(err)
	}
}

func ins(row map[string]any) changestream.Change {
	return changestream.Change{Op: changestream.OpInsert, Schema: "public",
		Table: "d", Row: row, Key: row}
}

func upd(row map[string]any) changestream.Change {
	return changestream.Change{Op: changestream.OpUpdate, Schema: "public",
		Table: "d", Row: row, Key: row}
}

func del(id string) changestream.Change {
	k := map[string]any{"id": id}
	return changestream.Change{Op: changestream.OpDelete, Schema: "public",
		Table: "d", Row: nil, Key: k}
}

func liveByKey(t *testing.T, tbl *Table) map[string]map[string]any {
	t.Helper()
	out := map[string]map[string]any{}
	if err := tbl.ForEachLive(func(r map[string]any) error {
		k := fmt.Sprint(r["id"])
		if _, dup := out[k]; dup {
			t.Errorf("key %s appears twice in the mirror", k)
		}
		out[k] = r
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func dirBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var n int64
	_ = filepath.Walk(dir, func(_ string, fi os.FileInfo, err error) error {
		if err == nil && !fi.IsDir() {
			n += fi.Size()
		}
		return nil
	})
	return n
}

// TestPartialDeltaDoesNotRewriteTheUntouchedColumn is the whole point: an
// update that omits the large column must not put it back on disk.
func TestPartialDeltaDoesNotRewriteTheUntouchedColumn(t *testing.T) {
	if !PartialDeltas {
		t.Skip("QS_PARTIAL_DELTAS=0")
	}
	tbl := docTable(t)
	doc := strings.Repeat("payload-", 512)

	applyAt(t, tbl, 1, ins(map[string]any{"id": "1", "n": "0", "doc": doc}))
	// Compact so the row lives in a base file; a patch never stands on a delta.
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}

	applyAt(t, tbl, 2, upd(map[string]any{"id": "1", "n": "1"}))

	if len(tbl.State.PartialCols) != 1 {
		t.Fatalf("expected the update to be stored as a patch, but the mirror "+
			"wrote %d partial files (delta files: %v)",
			len(tbl.State.PartialCols), tbl.State.DeltaFiles)
	}
	for name, cols := range tbl.State.PartialCols {
		if strings.Join(cols, ",") != "id,n" {
			t.Errorf("patch file %s carries %v; expected only the columns the "+
				"update sent", name, cols)
		}
		fi, err := os.Stat(filepath.Join(tbl.Dir, "delta", name))
		if err != nil {
			t.Fatal(err)
		}
		if fi.Size() > int64(len(doc)) {
			t.Errorf("patch file is %d bytes for a %d-byte document that was "+
				"never sent; the column is still being rewritten",
				fi.Size(), len(doc))
		}
	}

	got := liveByKey(t, tbl)
	if len(got) != 1 {
		t.Fatalf("mirror has %d rows, expected 1", len(got))
	}
	if Render(got["1"]["doc"]) != doc {
		t.Errorf("doc column came back as %d bytes, expected %d",
			len(Render(got["1"]["doc"])), len(doc))
	}
	if Render(got["1"]["n"]) != "1" {
		t.Errorf("n = %v, expected 1", got["1"]["n"])
	}
}

// TestPartialDeltaIsSmallerThanWholeRows prices the change on the shape it was
// built for, with the SAME workload run both ways.
func TestPartialDeltaIsSmallerThanWholeRows(t *testing.T) {
	run := func(partial bool) int64 {
		defer func(prev bool) { PartialDeltas = prev }(PartialDeltas)
		PartialDeltas = partial
		tbl := docTable(t)
		doc := strings.Repeat("payload-", 512)

		var seed []changestream.Change
		for i := 1; i <= 200; i++ {
			seed = append(seed, ins(map[string]any{
				"id": fmt.Sprint(i), "n": "0", "doc": doc,
			}))
		}
		applyAt(t, tbl, 1, seed...)
		if _, err := tbl.Compact(); err != nil {
			t.Fatal(err)
		}
		for round := 1; round <= 20; round++ {
			var changes []changestream.Change
			for i := 1; i <= 200; i++ {
				changes = append(changes, upd(map[string]any{
					"id": fmt.Sprint(i), "n": fmt.Sprint(round),
				}))
			}
			applyAt(t, tbl, 1+round, changes...)
		}
		got := liveByKey(t, tbl)
		if len(got) != 200 {
			t.Fatalf("partial=%v: mirror has %d rows, expected 200", partial, len(got))
		}
		for i := 1; i <= 200; i++ {
			r := got[fmt.Sprint(i)]
			if Render(r["doc"]) != doc {
				t.Fatalf("partial=%v: row %d lost its document", partial, i)
			}
			if Render(r["n"]) != "20" {
				t.Fatalf("partial=%v: row %d has n=%v, expected 20", partial, i, r["n"])
			}
		}
		return dirBytes(t, tbl.Dir)
	}

	whole := run(false)
	part := run(true)
	t.Logf("20 rounds of 200 updates over a 4 KB document: "+
		"whole rows %d bytes, patches %d bytes (%.1fx smaller)",
		whole, part, float64(whole)/float64(part))
	if part >= whole {
		t.Errorf("column-partial deltas wrote %d bytes against %d for whole "+
			"rows; the optimisation is not happening", part, whole)
	}
}

// TestPartialDeltaRefusesAShrinkingShape covers the rule that keeps a key's
// value to base-plus-one-patch. A second update carrying FEWER columns than the
// patch already standing cannot simply replace it, because the columns it drops
// would have to keep coming from the older patch underneath.
func TestPartialDeltaRefusesAShrinkingShape(t *testing.T) {
	if !PartialDeltas {
		t.Skip("QS_PARTIAL_DELTAS=0")
	}
	cols := map[string]string{"id": "bigint", "a": "text", "b": "text", "c": "text"}
	order := []string{"id", "a", "b", "c"}
	tbl, err := New(t.TempDir(), "public", "d", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	applyAt(t, tbl, 1, ins(map[string]any{"id": "1", "a": "a0", "b": "b0", "c": "c0"}))
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}

	// patch carrying {id,a,b}
	applyAt(t, tbl, 2, upd(map[string]any{"id": "1", "a": "a1", "b": "b1"}))
	// then a change carrying only {id,b}: it does not cover the standing patch,
	// so the mirror must fall back to writing the row whole.
	applyAt(t, tbl, 3, upd(map[string]any{"id": "1", "b": "b2"}))

	got := liveByKey(t, tbl)
	r := got["1"]
	for col, want := range map[string]string{"a": "a1", "b": "b2", "c": "c0"} {
		if Render(r[col]) != want {
			t.Errorf("column %s = %v, expected %s", col, r[col], want)
		}
	}
	if _, standing := tbl.patch.get(tbl.keyOfString("1")); standing {
		t.Errorf("a patch is still standing on key 1; the shrinking update " +
			"should have materialised the row")
	}
}

// TestPartialDeltaThroughMergeAndCompaction drives patches through every file
// transition they can survive — a delta merge, a background rewrite, deletes
// and re-inserts — with an independently maintained expectation.
func TestPartialDeltaThroughMergeAndCompaction(t *testing.T) {
	for _, seed := range []int64{1, 5, 9} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			tbl := docTable(t)
			doc := func(i int) string {
				return strings.Repeat(fmt.Sprintf("d%03d-", i), 64)
			}
			want := map[string]map[string]any{}

			var seedChanges []changestream.Change
			for i := 1; i <= 300; i++ {
				row := map[string]any{"id": fmt.Sprint(i), "n": "0", "doc": doc(i)}
				seedChanges = append(seedChanges, ins(row))
				want[fmt.Sprint(i)] = copyRow(row)
			}
			applyAt(t, tbl, 1, seedChanges...)
			if _, err := tbl.Compact(); err != nil {
				t.Fatal(err)
			}

			next := 301
			var sawPatch, sawPatchDuringRewrite bool
			for round := 1; round <= 50; round++ {
				if (round == 8 || round == 30) && !tbl.Compacting() {
					if c := tbl.BeginCompaction(); c == nil {
						t.Fatal("BeginCompaction returned nil with files present")
					}
				}
				var changes []changestream.Change
				for i := 0; i < 12; i++ {
					switch rng.Intn(10) {
					case 0, 1: // insert
						id := fmt.Sprint(next)
						next++
						row := map[string]any{"id": id, "n": "0", "doc": doc(next)}
						changes = append(changes, ins(row))
						want[id] = copyRow(row)
					case 2, 3, 4, 5, 6: // update leaving the document alone
						id := pickKey(rng, want)
						if id == "" {
							continue
						}
						changes = append(changes, upd(map[string]any{
							"id": id, "n": fmt.Sprint(round),
						}))
						want[id]["n"] = fmt.Sprint(round)
					case 7: // update that DOES rewrite the document
						id := pickKey(rng, want)
						if id == "" {
							continue
						}
						row := map[string]any{
							"id": id, "n": fmt.Sprint(round), "doc": doc(round),
						}
						changes = append(changes, upd(row))
						want[id] = copyRow(row)
					case 8: // delete
						id := pickKey(rng, want)
						if id == "" {
							continue
						}
						changes = append(changes, del(id))
						delete(want, id)
					case 9: // re-insert an early key
						id := fmt.Sprint(1 + rng.Intn(300))
						row := map[string]any{"id": id, "n": "999", "doc": doc(0)}
						changes = append(changes, ins(row))
						want[id] = copyRow(row)
					}
				}
				if len(changes) > 0 {
					applyAt(t, tbl, 1+round, changes...)
				}
				if tbl.patch.len() > 0 {
					sawPatch = true
					if tbl.Compacting() {
						// The window that the swap has to survive: a patch
						// written against a row a rewrite has already folded in.
						sawPatchDuringRewrite = true
					}
				}
				if tbl.Compacting() {
					if _, err := tbl.FinishCompaction(); err != nil {
						t.Fatalf("FinishCompaction: %v", err)
					}
				}
				if tbl.ShouldMergeDeltas() {
					if _, err := tbl.MergeDeltas(); err != nil {
						t.Fatalf("MergeDeltas: %v", err)
					}
				}
			}
			for tbl.Compacting() {
				if _, err := tbl.FinishCompaction(); err != nil {
					t.Fatalf("FinishCompaction: %v", err)
				}
			}
			if PartialDeltas && !sawPatch {
				t.Fatal("no patch was ever written, so this test proved nothing")
			}
			if PartialDeltas && !sawPatchDuringRewrite {
				t.Fatal("no patch was written while a rewrite was in flight, " +
					"which is the case the swap has to get right")
			}

			got := liveByKey(t, tbl)
			if len(got) != len(want) {
				t.Errorf("mirror has %d rows, expected %d", len(got), len(want))
			}
			bad := 0
			for k, w := range want {
				g, ok := got[k]
				if !ok {
					if bad < 3 {
						t.Errorf("row %s missing from the mirror", k)
					}
					bad++
					continue
				}
				for _, c := range []string{"n", "doc"} {
					if Render(g[c]) != Render(w[c]) {
						if bad < 3 {
							t.Errorf("row %s column %s: mirror=%.40v want=%.40v",
								k, c, g[c], w[c])
						}
						bad++
					}
				}
			}
			for k := range got {
				if _, ok := want[k]; !ok {
					if bad < 3 {
						t.Errorf("row %s is in the mirror but was deleted", k)
					}
					bad++
				}
			}
			if bad > 0 {
				t.Errorf("%d discrepancies", bad)
			}
		})
	}
}

// TestPartialDeltaSurvivesReopen checks that the patch bookkeeping is durable.
// It lives in state.json, and a mirror that rebuilds its index from disk after a
// restart has to reconstruct which delta files hold patches — reading a patch
// file as if it held whole rows would blank every column it does not carry.
func TestPartialDeltaSurvivesReopen(t *testing.T) {
	if !PartialDeltas {
		t.Skip("QS_PARTIAL_DELTAS=0")
	}
	tbl := docTable(t)
	doc := strings.Repeat("payload-", 512)
	applyAt(t, tbl, 1, ins(map[string]any{"id": "1", "n": "0", "doc": doc}))
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	applyAt(t, tbl, 2, upd(map[string]any{"id": "1", "n": "7"}))
	root := filepath.Dir(tbl.Dir)

	reopened, err := New(root, "public", "d", "id",
		tbl.Columns, tbl.Order)
	if err != nil {
		t.Fatal(err)
	}
	got := liveByKey(t, reopened)
	if Render(got["1"]["doc"]) != doc {
		t.Errorf("after reopen the document is %d bytes, expected %d",
			len(Render(got["1"]["doc"])), len(doc))
	}
	if Render(got["1"]["n"]) != "7" {
		t.Errorf("after reopen n = %v, expected 7", got["1"]["n"])
	}

	// ...and the index rebuilt from disk has to route the patch to the patch
	// map, not to the row index, or the next update tombstones the wrong row.
	if err := reopened.ensureIndex(); err != nil {
		t.Fatal(err)
	}
	if _, ok := reopened.patch.get(reopened.keyOfString("1")); !ok {
		t.Error("the reopened mirror did not recognise the patch file")
	}
	applyAt(t, reopened, 3, upd(map[string]any{"id": "1", "n": "8", "doc": "small"}))
	got = liveByKey(t, reopened)
	if len(got) != 1 {
		t.Fatalf("mirror has %d rows, expected 1", len(got))
	}
	if Render(got["1"]["doc"]) != "small" || Render(got["1"]["n"]) != "8" {
		t.Errorf("after a whole-row update: n=%v doc=%.20v", got["1"]["n"], got["1"]["doc"])
	}
}
