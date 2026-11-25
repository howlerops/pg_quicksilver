package mirror

// A transaction larger than memory is applied in chunks and published once.
//
// Chunking is what bounds Apply's heap, and every invariant Apply had inside
// one batch now has to hold across the chunks of one transaction, with a
// reader allowed to look at any moment in between. These tests run a
// transaction through a chunk size of a handful of rows, so that every
// interesting boundary falls inside it, and check three things: the result is
// what applying the changes one at a time would give; nothing of the
// transaction can be read until all of it can; and a transaction that dies
// part-way leaves the mirror it started from.

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func withChunk(t *testing.T, n int) {
	t.Helper()
	old := ApplyChunkChanges
	ApplyChunkChanges = n
	t.Cleanup(func() { ApplyChunkChanges = old })
}

var rCols = map[string][]string{"public.r": {"id", "v", "n"}}

// asTxn makes one transaction of the changes, in memory or spooled to disk.
func asTxn(t *testing.T, spooled bool, lsn string, ops []changestream.Change) changestream.Transaction {
	t.Helper()
	if !spooled {
		return changestream.Transaction{CommitLSN: lsn, NextLSN: lsn, Changes: ops}
	}
	// A spool carries the new tuple and the key tuple separately, and insR
	// shares one map between them; either is fine for Apply.
	txn, err := changestream.NewSpooledTransaction(t.TempDir(), lsn, lsn, rCols, ops)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(txn.Release)
	return txn
}

// liveRows reads the mirror the way a reader does: from the manifest on disk,
// in a Table of its own.
func liveRows(t *testing.T, tbl *Table) map[string]string {
	t.Helper()
	rd, err := New(filepath.Dir(tbl.Dir), "public", "r", "id", tbl.Columns, tbl.Order)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	if err := rd.ForEachLive(func(r map[string]any) error {
		id := Render(r["id"])
		if _, dup := out[id]; dup {
			return fmt.Errorf("key %s is live twice", id)
		}
		out[id] = Render(r["v"]) + "/" + Render(r["n"])
		return nil
	}); err != nil {
		t.Fatalf("a reader of the on-disk manifest failed: %v", err)
	}
	return out
}

func sameRows(a, b map[string]string) string {
	keys := map[string]bool{}
	for k := range a {
		keys[k] = true
	}
	for k := range b {
		keys[k] = true
	}
	var diff []string
	for k := range keys {
		if a[k] != b[k] {
			diff = append(diff, fmt.Sprintf("%s: %q vs %q", k, a[k], b[k]))
		}
	}
	sort.Strings(diff)
	if len(diff) > 6 {
		diff = append(diff[:6], "...")
	}
	return fmt.Sprint(diff)
}

func updR(id, v, n string) changestream.Change {
	c := insR(id, v, n)
	c.Op = changestream.OpUpdate
	return c
}

// model is what the source holds after the changes: each applied in order.
func model(start map[string]string, ops []changestream.Change) map[string]string {
	m := map[string]string{}
	for k, v := range start {
		m[k] = v
	}
	for _, c := range ops {
		switch c.Op {
		case changestream.OpDelete:
			delete(m, Render(c.Key["id"]))
		case changestream.OpTruncate:
			m = map[string]string{}
		default:
			m[Render(c.Row["id"])] = Render(c.Row["v"]) + "/" + Render(c.Row["n"])
		}
	}
	return m
}

// seed puts ten rows in a base file and ten more in a delta, so a transaction
// has both kinds of file to supersede rows in.
func seed(t *testing.T, tbl *Table) map[string]string {
	t.Helper()
	var a, b []changestream.Change
	for i := 1; i <= 10; i++ {
		a = append(a, insR(fmt.Sprint(i), "base", "0"))
		b = append(b, insR(fmt.Sprint(i+10), "delta", "0"))
	}
	if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: "0/10", NextLSN: "0/10", Changes: a}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: "0/20", NextLSN: "0/20", Changes: b}}); err != nil {
		t.Fatal(err)
	}
	return liveRows(t, tbl)
}

// The key written in chunk 1 and changed again in a later chunk: it must end
// with exactly one live row, or none.
func TestChunkedApplySupersedesAcrossChunks(t *testing.T) {
	ops := []changestream.Change{
		// chunk 1
		insR("100", "first", "1"), insR("101", "first", "1"), insR("102", "first", "1"),
		// chunk 2
		updR("1", "u", "2"), updR("11", "u", "2"), insR("103", "first", "1"),
		// chunk 3: the key from chunk 1 updated, another deleted
		updR("100", "second", "3"), delR("101"), delR("2"),
		// chunk 4: deleted in chunk 3, back again; updated twice more
		insR("101", "back", "4"), updR("100", "third", "4"), updR("1", "uu", "4"),
		// chunk 5: inserted in 1, deleted here for good; a base row deleted then re-inserted
		delR("102"), delR("3"), insR("3", "re", "5"),
		// tail
		delR("103"), updR("12", "tail", "6"),
	}
	for _, spooled := range []bool{false, true} {
		t.Run(fmt.Sprintf("spooled=%v", spooled), func(t *testing.T) {
			withChunk(t, 3)
			tbl := racyTable(t)
			before := seed(t, tbl)
			chunks := 0
			tbl.afterChunk = func() { chunks++ }
			if _, err := tbl.Apply([]changestream.Transaction{asTxn(t, spooled, "0/30", ops)}); err != nil {
				t.Fatal(err)
			}
			if chunks < 5 {
				t.Fatalf("the transaction was applied in %d chunks; this test needs it split", chunks)
			}
			checkInvariants(t, tbl, "after a chunked transaction")
			want := model(before, ops)
			if got := liveRows(t, tbl); len(got) != len(want) || sameRows(got, want) != "[]" {
				t.Fatalf("mirror differs from applying the changes in order: %s", sameRows(got, want))
			}
		})
	}
}

// The same property against arbitrary histories, and against the unchunked
// Apply this replaced.
func TestChunkedApplyMatchesUnchunked(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for round := 0; round < 20; round++ {
		var ops []changestream.Change
		for i := 0; i < 400; i++ {
			id := fmt.Sprint(1 + rng.Intn(40))
			switch rng.Intn(5) {
			case 0:
				ops = append(ops, delR(id))
			case 1:
				ops = append(ops, updR(id, fmt.Sprintf("u%d", i), fmt.Sprint(i)))
			default:
				ops = append(ops, insR(id, fmt.Sprintf("i%d", i), fmt.Sprint(i)))
			}
		}
		results := map[int]map[string]string{}
		for _, chunk := range []int{0, 7} {
			withChunk(t, chunk)
			tbl := racyTable(t)
			before := seed(t, tbl)
			if _, err := tbl.Apply([]changestream.Transaction{asTxn(t, chunk != 0, "0/30", ops)}); err != nil {
				t.Fatal(err)
			}
			if !checkInvariants(t, tbl, fmt.Sprintf("round %d chunk %d", round, chunk)) {
				t.FailNow()
			}
			results[chunk] = liveRows(t, tbl)
			if want := model(before, ops); sameRows(results[chunk], want) != "[]" {
				t.Fatalf("round %d chunk %d: %s", round, chunk, sameRows(results[chunk], want))
			}
		}
		if d := sameRows(results[0], results[7]); d != "[]" {
			t.Fatalf("round %d: chunked and unchunked disagree: %s", round, d)
		}
	}
}

// A reader that looks between chunks sees the mirror as it was before the
// transaction, complete and readable; after the last chunk it sees all of it.
// The deletion vectors are the part that is easy to get wrong: five chunks
// each supersede rows in the SAME base file, and a generation published per
// chunk would have retired the one the manifest on disk still names.
func TestChunkedApplyIsInvisibleUntilComplete(t *testing.T) {
	withChunk(t, 2)
	tbl := racyTable(t)
	before := seed(t, tbl)
	manifest, err := os.ReadFile(tbl.statePath())
	if err != nil {
		t.Fatal(err)
	}

	var ops []changestream.Change
	for i := 1; i <= 10; i++ { // every row of the base file, two per chunk
		ops = append(ops, updR(fmt.Sprint(i), "new", "9"))
	}
	ops = append(ops, insR("200", "new", "9"), delR("11"))

	looks := 0
	tbl.afterChunk = func() {
		looks++
		now, err := os.ReadFile(tbl.statePath())
		if err != nil {
			t.Fatal(err)
		}
		if string(now) != string(manifest) {
			t.Fatalf("the manifest changed after chunk %d of an unfinished transaction", looks)
		}
		if d := sameRows(liveRows(t, tbl), before); d != "[]" {
			t.Fatalf("after chunk %d a reader saw part of the transaction: %s", looks, d)
		}
	}
	txn := asTxn(t, true, "0/30", ops)
	if _, err := tbl.Apply([]changestream.Transaction{txn}); err != nil {
		t.Fatal(err)
	}
	if looks < 5 {
		t.Fatalf("only %d chunks; the test needs several", looks)
	}
	if d := sameRows(liveRows(t, tbl), model(before, ops)); d != "[]" {
		t.Fatalf("after the transaction: %s", d)
	}
	if tbl.State.AppliedLSN != "0/30" {
		t.Fatalf("applied_lsn = %s", tbl.State.AppliedLSN)
	}
	// One generation per file for the whole transaction, not one per chunk.
	for stem, gen := range tbl.State.DVGen {
		if gen > 2 {
			t.Errorf("vector %s is at generation %d after one transaction on top of the seed", stem, gen)
		}
	}
	checkInvariants(t, tbl, "after")
}

// An update that does not carry a column takes it from the row the mirror
// holds — which, for a key an earlier chunk wrote, is in that chunk's delta
// file and in no manifest yet.
func TestChunkedApplyCarriesForwardFromEarlierChunk(t *testing.T) {
	withChunk(t, 2)
	tbl := toastTable(t)
	doc := `{"big":"` + fmt.Sprint(make([]byte, 64)) + `"}`
	ops := []changestream.Change{
		change(changestream.OpInsert, map[string]any{"id": "1", "status": "new", "doc": doc, "updated_at": "t0"}),
		change(changestream.OpInsert, map[string]any{"id": "2", "status": "new", "doc": doc, "updated_at": "t0"}),
		change(changestream.OpInsert, map[string]any{"id": "3", "status": "new", "doc": doc, "updated_at": "t0"}),
		change(changestream.OpInsert, map[string]any{"id": "4", "status": "new", "doc": doc, "updated_at": "t0"}),
		// doc is absent: "unchanged TOAST"
		change(changestream.OpUpdate, map[string]any{"id": "1", "status": "done", "updated_at": "t1"}),
		change(changestream.OpUpdate, map[string]any{"id": "3", "status": "done", "updated_at": "t1"}),
	}
	if _, err := tbl.Apply([]changestream.Transaction{txn("0/5", ops...)}); err != nil {
		t.Fatal(err)
	}
	n := 0
	if err := tbl.ForEachLive(func(r map[string]any) error {
		n++
		if Render(r["doc"]) != doc {
			return fmt.Errorf("row %v lost its document: %v", r["id"], r["doc"])
		}
		if id := Render(r["id"]); (id == "1" || id == "3") && Render(r["status"]) != "done" {
			return fmt.Errorf("row %s did not take its update", id)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("%d live rows, want 4", n)
	}
}

// A transaction that fails part-way — standing in for kill -9 — leaves the
// previous manifest describing a mirror that is all still there, and the files
// it had written are collected by the Apply that succeeds after the restart.
func TestChunkedApplyCrashLeavesPreviousMirror(t *testing.T) {
	withChunk(t, 2)
	tbl := racyTable(t)
	before := seed(t, tbl)

	var ops []changestream.Change
	for i := 1; i <= 8; i++ {
		ops = append(ops, updR(fmt.Sprint(i), "new", "9"), insR(fmt.Sprint(300+i), "new", "9"))
	}
	chunks := 0
	tbl.afterChunk = func() {
		if chunks++; chunks == 6 {
			panic("kill -9")
		}
	}
	func() {
		defer func() {
			if r := recover(); r != "kill -9" {
				t.Fatalf("expected the injected crash, got %v", r)
			}
		}()
		_, _ = tbl.Apply([]changestream.Transaction{asTxn(t, true, "0/30", ops)})
	}()
	if chunks < 4 {
		t.Fatalf("it failed after %d chunks; the test needs some written first", chunks)
	}
	orphans, _ := os.ReadDir(filepath.Join(tbl.Dir, "delta"))
	if len(orphans) < 4 {
		t.Fatalf("expected the dead transaction's delta files on disk, found %d", len(orphans))
	}
	if d := sameRows(liveRows(t, tbl), before); d != "[]" {
		t.Fatalf("after the failed transaction a reader sees: %s", d)
	}

	// "Restart": a Table built from what is on disk. The slot resends, this
	// time (say) in a different batch shape, so the replay does not simply
	// overwrite every orphan name for name.
	re, err := New(filepath.Dir(tbl.Dir), "public", "r", "id", tbl.Columns, tbl.Order)
	if err != nil {
		t.Fatal(err)
	}
	withChunk(t, 100)
	if _, err := re.Apply([]changestream.Transaction{asTxn(t, true, "0/30", ops)}); err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, re, "after replay")
	if d := sameRows(liveRows(t, re), model(before, ops)); d != "[]" {
		t.Fatalf("after replay: %s", d)
	}
	onDisk, _ := os.ReadDir(filepath.Join(re.Dir, "delta"))
	for _, e := range onDisk {
		if !contains(re.State.DeltaFiles, e.Name()) {
			t.Errorf("delta/%s is on disk and in no manifest", e.Name())
		}
	}
}

// TRUNCATE inside a large transaction: the rows before it go, the rows after
// it stay, and a reader sees the old table until the whole transaction is in.
func TestChunkedApplyTruncateInsideTransaction(t *testing.T) {
	withChunk(t, 2)
	tbl := racyTable(t)
	before := seed(t, tbl)
	ops := []changestream.Change{
		insR("400", "doomed", "1"), insR("401", "doomed", "1"), updR("1", "doomed", "1"),
		{Op: changestream.OpTruncate, Schema: "public", Table: "r"},
		insR("500", "kept", "2"), insR("501", "kept", "2"), insR("1", "kept", "2"),
		updR("500", "kept2", "3"), delR("501"),
	}
	tbl.afterChunk = func() {
		if d := sameRows(liveRows(t, tbl), before); d != "[]" {
			t.Fatalf("mid-transaction a reader saw: %s", d)
		}
	}
	if _, err := tbl.Apply([]changestream.Transaction{asTxn(t, true, "0/30", ops)}); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"500": "kept2/3", "1": "kept/2"}
	if d := sameRows(liveRows(t, tbl), want); d != "[]" {
		t.Fatalf("after: %s", d)
	}
	checkInvariants(t, tbl, "after truncate")
	for _, sub := range []string{"base", "delta"} {
		entries, _ := os.ReadDir(filepath.Join(tbl.Dir, sub))
		for _, e := range entries {
			if !contains(tbl.State.DeltaFiles, e.Name()) && !contains(tbl.State.BaseFiles, e.Name()) {
				t.Errorf("%s/%s survived the truncate and is in no manifest", sub, e.Name())
			}
		}
	}
}

// A halt raised in the middle of a large transaction — a value the column
// cannot hold, several chunks in — records the halt and publishes nothing else.
func TestHaltInsideChunkedApplyPublishesNothing(t *testing.T) {
	withChunk(t, 2)
	tbl := racyTable(t)
	before := seed(t, tbl)
	var ops []changestream.Change
	for i := 1; i <= 8; i++ {
		ops = append(ops, updR(fmt.Sprint(i), "new", "9"), insR(fmt.Sprint(300+i), "new", "9"))
	}
	ops = append(ops, insR("999", "x", "not a number"))
	if _, err := tbl.Apply([]changestream.Transaction{asTxn(t, true, "0/30", ops)}); err == nil {
		t.Fatal("a transaction with an unstorable value applied")
	}
	if d := sameRows(liveRows(t, tbl), before); d != "[]" {
		t.Fatalf("the halt published part of the transaction: %s", d)
	}
	rd, err := New(filepath.Dir(tbl.Dir), "public", "r", "id", tbl.Columns, tbl.Order)
	if err != nil {
		t.Fatal(err)
	}
	if rd.Halted() == "" {
		t.Fatal("the halt is not on disk")
	}
	if rd.State.AppliedLSN != "0/20" {
		t.Fatalf("applied_lsn moved to %s", rd.State.AppliedLSN)
	}
}
