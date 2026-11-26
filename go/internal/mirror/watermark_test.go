package mirror

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// wmTables opens n tables public.t0..t(n-1) under one root.
func wmTables(t *testing.T, root string, n int) []*Table {
	t.Helper()
	cols := map[string]string{"id": "bigint", "v": "text", "n": "bigint"}
	var out []*Table
	for i := 0; i < n; i++ {
		tbl, err := New(root, "public", fmt.Sprintf("t%d", i), "id", cols, []string{"id", "v", "n"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(tbl.Close)
		out = append(out, tbl)
	}
	return out
}

func wmChange(table string, id int, v string) changestream.Change {
	row := map[string]any{"id": itoa(id), "v": v, "n": "1"}
	return changestream.Change{Op: changestream.OpInsert, Schema: "public", Table: table, Row: row, Key: row}
}

// tick is what the pump does with one batch: every table applies it, then the
// position is confirmed — here, the part of Confirm that belongs to the mirror.
func tick(t *testing.T, tables []*Table, lsn uint64, changes ...changestream.Change) {
	t.Helper()
	applyAll(t, tables, lsn, changes...)
	if err := changestream.RunBeforeConfirm(lsnString(lsn + 1)); err != nil {
		t.Fatal(err)
	}
}

func applyAll(t *testing.T, tables []*Table, lsn uint64, changes ...changestream.Change) {
	t.Helper()
	batch := []changestream.Transaction{{CommitLSN: lsnString(lsn), NextLSN: lsnString(lsn + 1), Changes: changes}}
	for _, tbl := range tables {
		if _, err := tbl.Apply(batch); err != nil {
			t.Fatal(err)
		}
	}
}

func manifestLSN(t *testing.T, tbl *Table) string {
	t.Helper()
	b, err := os.ReadFile(tbl.statePath())
	if err != nil {
		t.Fatal(err)
	}
	var st State
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatal(err)
	}
	return st.AppliedLSN
}

func effective(t *testing.T, root, table string) string {
	t.Helper()
	lsn, err := EffectiveAppliedLSN(root, "public", table)
	if err != nil {
		t.Fatal(err)
	}
	return lsn
}

// mayResume is planResume's test (cmd/qs-mirror/resume.go), for one table.
func mayResume(applied string, confirmed uint64) bool {
	at := changestream.ParseLSN(applied)
	return applied != "" && at != 0 && confirmed <= at+1
}

// The whole argument of watermark.go, one crash point at a time: an idle
// table writes nothing, its effective LSN is its manifest's until the
// watermark is durable and the watermark's after, and at every point the slot
// position that could have been confirmed is one the table may resume from.
func TestIdleTableWritesNothingAndTheWatermarkSpeaksForIt(t *testing.T) {
	root := t.TempDir()
	tables := wmTables(t, root, 2)
	busy, idle := tables[0], tables[1]

	tick(t, tables, 0x100, wmChange("t0", 1, "a"), wmChange("t1", 1, "a"))
	if _, err := os.Stat(filepath.Join(root, WatermarkFile)); err == nil {
		t.Fatal("every table wrote its manifest in this tick, and a watermark was written as well")
	}

	// A batch with nothing for t1. Nothing under t1's directory is synced or
	// rewritten; the manifest and the published snapshot stay as they were.
	log := watchSyncs(t, root)
	before, _ := os.ReadFile(filepath.Join(idle.Dir, PublishedFile))
	applyAll(t, tables, 0x200, wmChange("t0", 2, "b"))
	log.mu.Lock()
	for path := range log.file {
		if strings.HasPrefix(path, idle.Dir) {
			t.Errorf("an Apply that changed nothing in t1 fsynced %s", path)
		}
	}
	if log.dir[idle.Dir] != 0 {
		t.Error("an Apply that changed nothing in t1 fsynced its directory")
	}
	log.mu.Unlock()
	if after, _ := os.ReadFile(filepath.Join(idle.Dir, PublishedFile)); string(after) != string(before) {
		t.Error("an Apply that changed nothing in t1 republished its snapshot")
	}
	if got := manifestLSN(t, idle); got != "0/100" {
		t.Fatalf("t1's manifest moved to %s", got)
	}
	if got := idle.EffectiveAppliedLSN(); got != "0/200" {
		t.Fatalf("in memory t1 has applied through %s, want 0/200", got)
	}

	// CRASH HERE: applied, watermark not yet written, so 0/201 was never
	// confirmed and the slot is at 0/101 at most.
	if got := effective(t, root, "t1"); got != "0/100" {
		t.Fatalf("before the watermark t1's durable LSN is %s, want its manifest's 0/100", got)
	}
	if !mayResume(effective(t, root, "t1"), 0x101) || !mayResume(effective(t, root, "t0"), 0x101) {
		t.Fatal("a crash before the watermark must leave both tables resumable from the previous confirm")
	}
	if mayResume(effective(t, root, "t1"), 0x201) {
		t.Fatal("t1 would resume past a position nothing durable says it reached")
	}

	// The watermark: the file fsynced, then the root directory, and only then
	// does the hook return to Confirm.
	if err := changestream.RunBeforeConfirm("0/201"); err != nil {
		t.Fatal(err)
	}
	log.mu.Lock()
	wmSync, rootSync := log.file[filepath.Join(root, WatermarkFile)], log.dir[root]
	log.mu.Unlock()
	if wmSync == 0 || rootSync < wmSync {
		t.Fatalf("the watermark was not made durable before the confirm: file sync %d, root sync %d", wmSync, rootSync)
	}
	w, err := ReadWatermark(root)
	if err != nil || w == nil {
		t.Fatalf("watermark: %v %v", w, err)
	}
	if w.AppliedLSN != "0/200" || w.Tables["public.t1"] != "0/100" || w.Tables["public.t0"] != "0/200" {
		t.Fatalf("watermark %+v", w)
	}
	// CRASH HERE or after the confirm: the slot may be at 0/201.
	for _, name := range []string{"t0", "t1"} {
		if got := effective(t, root, name); got != "0/200" || !mayResume(got, 0x201) {
			t.Fatalf("%s: effective %s after the watermark", name, got)
		}
	}
	if got := manifestLSN(t, idle); got != "0/100" {
		t.Fatalf("the watermark rewrote t1's manifest: %s", got)
	}

	// A restart reads the same thing into State, so planResume and the apply
	// loop's starting position need no change to see it.
	cols := map[string]string{"id": "bigint", "v": "text", "n": "bigint"}
	re, err := New(root, "public", "t1", "id", cols, []string{"id", "v", "n"})
	if err != nil {
		t.Fatal(err)
	}
	defer re.Close()
	if re.State.AppliedLSN != "0/200" || re.ManifestAppliedLSN() != "0/100" {
		t.Fatalf("reopened: applied %s, manifest %s", re.State.AppliedLSN, re.ManifestAppliedLSN())
	}
	// Its first Apply writes the manifest whatever the batch holds: that
	// publish is the one that sweeps what the previous process retired.
	applyAll(t, []*Table{re}, 0x300)
	if got := manifestLSN(t, re); got != "0/300" {
		t.Fatalf("the first Apply of a process left the manifest at %s", got)
	}

	// A table that changes AFTER a watermark uses its own manifest, although
	// the watermark still names it with an older one.
	tick(t, tables, 0x400, wmChange("t1", 5, "later"))
	if got := effective(t, root, "t1"); got != "0/400" {
		t.Fatalf("t1 changed at 0/400 and reports %s", got)
	}
	if got := effective(t, root, "t0"); got != "0/400" || manifestLSN(t, busy) != "0/200" {
		t.Fatalf("t0 was idle through 0/400: effective %s, manifest %s", got, manifestLSN(t, busy))
	}
}

// What the watermark must NOT vouch for.
func TestWatermarkDoesNotSpeakForATableItCannot(t *testing.T) {
	root := t.TempDir()
	tables := wmTables(t, root, 4)
	restored, halted := tables[1], tables[2]
	tick(t, tables, 0x100, wmChange("t0", 1, "a"), wmChange("t1", 1, "a"),
		wmChange("t2", 1, "a"), wmChange("t3", 1, "a"))
	old, err := os.ReadFile(restored.statePath())
	if err != nil {
		t.Fatal(err)
	}
	tick(t, tables, 0x200, wmChange("t1", 2, "b"))

	// t2 halts; t3 is not given the next batch at all (its Apply failed, say).
	if err := halted.Halt("a type the mirror cannot hold"); err != nil {
		t.Fatal(err)
	}
	applyAll(t, []*Table{tables[0], restored}, 0x300, wmChange("t0", 3, "c"))
	if err := changestream.RunBeforeConfirm("0/301"); err != nil {
		t.Fatal(err)
	}
	w, _ := ReadWatermark(root)
	if w == nil || w.AppliedLSN != "0/300" {
		t.Fatalf("watermark %+v", w)
	}
	if _, named := w.Tables["public.t2"]; named {
		t.Error("the watermark names a halted table")
	}
	if _, named := w.Tables["public.t3"]; named {
		t.Error("the watermark names a table that has not applied the batch being confirmed")
	}
	if got := effective(t, root, "t3"); got != "0/100" || mayResume(got, 0x301) {
		t.Errorf("t3 stopped at 0/100 and reports %s", got)
	}
	if got := effective(t, root, "t1"); got != "0/300" {
		t.Fatalf("t1 idle through 0/300 reports %s", got)
	}

	// t1's directory is put back from an older copy. Its manifest no longer
	// holds what the watermark saw, so the watermark is not about this table
	// any more, and the resume check finds it behind — as it did before the
	// watermark existed.
	if err := os.WriteFile(restored.statePath(), old, 0o644); err != nil {
		t.Fatal(err)
	}
	if got := effective(t, root, "t1"); got != "0/100" || mayResume(got, 0x301) {
		t.Fatalf("a restored manifest at 0/100 reports %s; the hole between it and the slot would go unnoticed", got)
	}

	// A table that was never snapshotted has no LSN, whatever a file says.
	if got := effectiveLSN("public.new", "", &Watermark{AppliedLSN: "0/300",
		Tables: map[string]string{"public.new": ""}}); got != "" {
		t.Fatalf("a table with no manifest LSN reports %q", got)
	}
	if got, err := EffectiveAppliedLSN(root, "public", "absent"); got != "" || err != nil {
		t.Fatalf("an absent table: %q %v", got, err)
	}
}

// A watermark that cannot be made durable fails the confirm, and leaves the
// previous one in place.
func TestWatermarkSyncFailureRefusesTheConfirm(t *testing.T) {
	root := t.TempDir()
	tables := wmTables(t, root, 2)
	tick(t, tables, 0x100, wmChange("t0", 1, "a"), wmChange("t1", 1, "a"))
	tick(t, tables, 0x200, wmChange("t0", 2, "b"))

	applyAll(t, tables, 0x300, wmChange("t0", 3, "c"))
	for _, failDir := range []bool{false, true} {
		boom := errors.New("EIO")
		hook := func(path string, dir bool) error {
			if dir == failDir && (path == root || strings.HasPrefix(path, filepath.Join(root, WatermarkFile))) {
				return boom
			}
			return nil
		}
		syncHook.Store(&hook)
		err := changestream.RunBeforeConfirm("0/301")
		syncHook.Store(nil)
		if !errors.Is(err, boom) {
			t.Fatalf("failDir=%v: the hook returned %v", failDir, err)
		}
	}
	// Not confirmed, so the slot is at 0/201; whichever watermark survived,
	// t1 may resume from there and from nowhere later than it has reached.
	if got := effective(t, root, "t1"); !mayResume(got, 0x201) {
		t.Fatalf("t1 reports %s", got)
	}
	if err := changestream.RunBeforeConfirm("0/301"); err != nil {
		t.Fatal(err)
	}
	if got := effective(t, root, "t1"); got != "0/300" {
		t.Fatalf("t1 reports %s", got)
	}
}

// A closed table is not spoken for by later watermarks, and tables on
// different goroutines can apply while a watermark is written.
func TestWatermarkForgetsClosedTablesAndIsSafeAcrossGoroutines(t *testing.T) {
	root := t.TempDir()
	tables := wmTables(t, root, 3)
	tick(t, tables, 0x100, wmChange("t0", 1, "a"), wmChange("t1", 1, "a"), wmChange("t2", 1, "a"))
	tables[2].Close()
	tick(t, tables[:2], 0x200, wmChange("t0", 2, "b"))
	w, _ := ReadWatermark(root)
	if w == nil || w.Tables["public.t1"] != "0/100" {
		t.Fatalf("watermark %+v", w)
	}
	if _, named := w.Tables["public.t2"]; named {
		t.Fatal("the watermark names a table that was closed before the batch")
	}

	var wg sync.WaitGroup
	for i, tbl := range tables[:2] {
		wg.Add(1)
		go func(i int, tbl *Table) {
			defer wg.Done()
			for l := uint64(0x300); l < 0x340; l++ {
				var ch []changestream.Change
				if i == 0 {
					ch = append(ch, wmChange("t0", int(l), "x"))
				}
				if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: lsnString(l), NextLSN: lsnString(l + 1), Changes: ch}}); err != nil {
					t.Error(err)
					return
				}
			}
		}(i, tbl)
	}
	for l := uint64(0x300); l < 0x340; l++ {
		if err := SyncWatermark(root, lsnString(l+1)); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if err := SyncWatermarks("0/340"); err != nil {
		t.Fatal(err)
	}
	if got := effective(t, root, "t1"); got != "0/33F" {
		t.Fatalf("t1 reports %s", got)
	}
}

// TestIdleTablesCostNothingPerTick measures what one tick of the apply loop
// costs with 1, 10 and 100 mirrored tables of which one receives changes,
// with a manifest per table per tick (what Apply did) and with the watermark.
// It asserts the part that is not a matter of hardware — the number of syncs
// — and logs the times. QS_BENCH_DIR puts it on a real disk; the default temp
// directory is usually tmpfs, where an fsync is free.
func TestIdleTablesCostNothingPerTick(t *testing.T) {
	// As a measurement (QS_BENCH_DIR) it runs enough ticks to time; as a test
	// it runs enough to count syncs, which do not vary.
	base, ticks, rows := os.Getenv("QS_BENCH_DIR"), 30, 200
	if base == "" {
		base, ticks, rows = t.TempDir(), 3, 10
	}
	run := func(n int, always bool) (perTick time.Duration, syncs float64) {
		root, err := os.MkdirTemp(base, "ticks")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(root)
		tables := wmTables(t, root, n)
		defer func() {
			for _, tbl := range tables {
				tbl.Close()
			}
		}()
		var seed []changestream.Change
		for i := 0; i < n; i++ {
			for id := 1; id <= rows; id++ {
				seed = append(seed, wmChange(fmt.Sprintf("t%d", i), id, "seed"))
			}
		}
		tick(t, tables, 0x1000, seed...)

		applyAlwaysSaves = always
		defer func() { applyAlwaysSaves = false }()
		count := 0
		hook := func(string, bool) error { count++; return nil }
		syncHook.Store(&hook)
		defer syncHook.Store(nil)
		start := time.Now()
		for i := 0; i < ticks; i++ {
			var ch []changestream.Change
			for id := 1; id <= min(100, rows); id++ {
				row := map[string]any{"id": itoa(id), "v": itoa(i), "n": "2"}
				ch = append(ch, changestream.Change{Op: changestream.OpUpdate, Schema: "public", Table: "t0", Row: row, Key: row})
			}
			tick(t, tables, uint64(0x2000+i*0x10), ch...)
		}
		return time.Since(start) / time.Duration(ticks), float64(count) / float64(ticks)
	}
	var after1 float64
	for _, n := range []int{1, 10, 100} {
		bt, bs := run(n, true)
		at, as := run(n, false)
		t.Logf("%3d tables, 1 changing: manifest per table %7.2f ms/tick %6.1f syncs/tick | watermark %7.2f ms/tick %5.1f syncs/tick",
			n, float64(bt.Microseconds())/1000, bs, float64(at.Microseconds())/1000, as)
		if n == 1 {
			after1 = as
			if as > bs {
				t.Errorf("one table: %.1f syncs per tick, was %.1f", as, bs)
			}
			continue
		}
		// The busy table's own syncs, plus the watermark and its directory.
		if as > after1+2 {
			t.Errorf("%d tables: %.1f syncs per tick; one table costs %.1f and the watermark 2", n, as, after1)
		}
		if bs < float64(n) {
			t.Errorf("%d tables: the baseline made only %.1f syncs per tick, so it is not measuring a manifest per table", n, bs)
		}
	}
}
