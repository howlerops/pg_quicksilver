package mirror

// Where publishing, the retention grace, chunked Apply and per-table halts
// meet. Each was written and tested on its own; every test here is a defect
// that needed two of them, and failed before the fix beside it.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func onDiskState(t *testing.T, tbl *Table) State {
	t.Helper()
	var st State
	b, err := os.ReadFile(tbl.statePath())
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatal(err)
	}
	return st
}

func reopen(t *testing.T, tbl *Table) *Table {
	t.Helper()
	re, err := New(filepath.Dir(tbl.Dir), "public", "r", "id", tbl.Columns, tbl.Order)
	if err != nil {
		t.Fatal(err)
	}
	return re
}

// snapshotFrom is Table.Snapshot with the rows handed over rather than read
// from a source: the same beginSnapshot, the same manifest fields, the same
// finishSnapshot and discard. want is id -> "v/n", as liveRows returns it.
func snapshotFrom(tbl *Table, want map[string]string, lsn string) error {
	old, err := tbl.beginSnapshot()
	if err != nil {
		return err
	}
	if err := writeSnapshotBase(tbl, want, lsn); err != nil {
		return err
	}
	if err := tbl.finishSnapshot(); err != nil {
		return err
	}
	tbl.discard(old)
	return nil
}

// writeSnapshotBase is the part of a snapshot between beginSnapshot and the
// manifest: the base file written, State describing it, nothing saved.
func writeSnapshotBase(tbl *Table, want map[string]string, lsn string) error {
	var rows []map[string]any
	for id, vn := range want {
		v, n, _ := strings.Cut(vn, "/")
		rows = append(rows, map[string]any{"id": id, "v": v, "n": n})
	}
	tbl.State.Seq++
	name := fmt.Sprintf("%06d.parquet", tbl.State.Seq)
	tbl.State.BaseFiles = nil
	if len(rows) > 0 {
		if err := tbl.writeParquet(filepath.Join(tbl.Dir, "base", name), rows); err != nil {
			return err
		}
		tbl.State.BaseFiles = []string{name}
	}
	tbl.State.DeltaFiles = nil
	tbl.State.BaseRows, tbl.State.DeltaRows = len(rows), 0
	tbl.State.AppliedLSN = lsn
	return nil
}

// TRUNCATE inside an Apply removed the files of the snapshot published a
// moment earlier with os.Remove, so a reader on it failed at once.
func TestTruncateInsideApplyKeepsTheGrace(t *testing.T) {
	tbl := racyTable(t)
	clk := manualGrace(tbl, time.Minute)
	seed(t, tbl)
	pub := mustPublished(t, tbl)
	if len(pub.Files) == 0 {
		t.Fatal("no files published")
	}
	ops := []changestream.Change{
		{Op: changestream.OpTruncate, Schema: "public", Table: "r"},
		insR("500", "kept", "2"),
	}
	if _, err := tbl.Apply([]changestream.Transaction{asTxn(t, false, "0/30", ops)}); err != nil {
		t.Fatal(err)
	}
	if m := missing(tbl.Dir, pub.Files); len(m) > 0 {
		t.Fatalf("grace is %v, yet %d of %d files of the snapshot published a moment ago are already gone: %v",
			tbl.publisher().grace, len(m), len(pub.Files), m)
	}
	// And they do go, once the grace is over.
	clk.at = clk.at.Add(2 * time.Minute)
	reapNow(tbl)
	if m := missing(tbl.Dir, pub.Files); len(m) != len(pub.Files) {
		t.Fatalf("truncated files outlived their grace: %d of %d are gone", len(m), len(pub.Files))
	}
	if d := sameRows(liveRows(t, tbl), map[string]string{"500": "kept/2"}); d != "[]" {
		t.Fatalf("after: %s", d)
	}
}

// A halt raised inside a chunked Apply, then made "durable again" the way
// cmd/qs-mirror haltOrFail does it: t.Halt(t.Halted()) after Apply returned.
// That second Halt saved the in-memory manifest — the failed transaction's
// chunks, with none of their tombstones — and, being a publish that names no
// files and the process's first, swept every file of the table.
func TestHaltAfterFailedApplyKeepsTheLastGoodManifest(t *testing.T) {
	withChunk(t, 2)
	tbl := racyTable(t)
	want := seed(t, tbl)
	// A restart, over files older than the grace, as a quiet table's are.
	old := time.Now().Add(-10 * time.Minute)
	for _, sub := range []string{"base", "delta", "dv"} {
		es, _ := os.ReadDir(filepath.Join(tbl.Dir, sub))
		for _, e := range es {
			_ = os.Chtimes(filepath.Join(tbl.Dir, sub, e.Name()), old, old)
		}
	}
	re := reopen(t, tbl)
	before := onDiskState(t, re)
	pub := mustPublished(t, re)

	var ops []changestream.Change
	for i := 1; i <= 6; i++ {
		ops = append(ops, updR(fmt.Sprint(i), "new", "9"))
	}
	ops = append(ops, insR("999", "bad", "not-a-number"))
	_, aerr := re.Apply([]changestream.Transaction{asTxn(t, true, "0/30", ops)})
	if aerr == nil || re.Halted() == "" {
		t.Fatalf("no halt produced (err=%v halted=%q)", aerr, re.Halted())
	}
	if err := re.Halt(re.Halted()); err != nil { // haltOrFail
		t.Fatal(err)
	}
	after := onDiskState(t, re)
	if fmt.Sprint(after.DeltaFiles) != fmt.Sprint(before.DeltaFiles) || after.AppliedLSN != before.AppliedLSN {
		t.Errorf("PARTIAL TXN IN MANIFEST: delta files before %v, after haltOrFail %v, applied_lsn %s, dvgen %v",
			before.DeltaFiles, after.DeltaFiles, after.AppliedLSN, after.DVGen)
	}
	if after.Halted == "" {
		t.Error("the halt is not in the manifest on disk")
	}
	if m := missing(re.Dir, pub.Files); len(m) > 0 {
		t.Errorf("HALT SWEEP: %d of %d files the previous snapshot named were unlinked immediately by the halt: %v",
			len(m), len(pub.Files), m)
	}
	if p := mustPublished(t, re); p.Halted == "" || len(p.Files) != 0 {
		t.Errorf("the published snapshot does not refuse: halted=%q files=%v", p.Halted, p.Files)
	}
	// The evidence: the last good manifest still reads back, whole.
	if d := sameRows(liveRows(t, re), want); d != "[]" {
		t.Errorf("the halted table's last good contents: %s", d)
	}
	// And the Table whose Apply failed says so rather than being trusted.
	if _, err := re.Apply([]changestream.Transaction{asTxn(t, false, "0/31",
		[]changestream.Change{insR("7", "x", "1")})}); !errors.Is(err, ErrNeedsReopen) {
		t.Errorf("Apply on a table whose Apply failed: %v, want ErrNeedsReopen", err)
	}
	if err := re.ClearHalt(); !errors.Is(err, ErrNeedsReopen) {
		t.Errorf("a manifest write from a table whose Apply failed: %v, want ErrNeedsReopen", err)
	}
	if got := onDiskState(t, re); fmt.Sprint(got.DeltaFiles) != fmt.Sprint(before.DeltaFiles) || got.Halted == "" {
		t.Errorf("the refused write changed the manifest: %+v", got)
	}
}

// A halt recorded with nothing uncommitted in memory — the ordinary path,
// through saveState — as a process's first publish, over files older than the
// grace. It named no files, so the sweep took all of them.
func TestHaltAsFirstPublishSweepsNothing(t *testing.T) {
	tbl := racyTable(t)
	want := seed(t, tbl)
	old := time.Now().Add(-10 * time.Minute)
	for _, sub := range []string{"base", "delta", "dv"} {
		es, _ := os.ReadDir(filepath.Join(tbl.Dir, sub))
		for _, e := range es {
			_ = os.Chtimes(filepath.Join(tbl.Dir, sub, e.Name()), old, old)
		}
	}
	re := reopen(t, tbl)
	pub := mustPublished(t, re)
	if err := re.Halt("an operator's reason"); err != nil {
		t.Fatal(err)
	}
	if m := missing(re.Dir, pub.Files); len(m) > 0 {
		t.Fatalf("the halt unlinked %d of %d files of the table it halted: %v", len(m), len(pub.Files), m)
	}
	if d := sameRows(liveRows(t, re), want); d != "[]" {
		t.Fatalf("the halted table's contents: %s", d)
	}
	// Cleared, it serves again, and only then is the restart's sweep due.
	if err := re.ClearHalt(); err != nil {
		t.Fatal(err)
	}
	if p := mustPublished(t, re); p.Halted != "" || len(missing(re.Dir, p.Files)) != 0 || len(p.Files) != len(pub.Files) {
		t.Fatalf("after ClearHalt: halted=%q files=%v", p.Halted, p.Files)
	}
}

// The sweep adopted a crashed Apply's orphan deltas BY PATH; the replay wrote
// the same names again; the timer unlinked them under the replay's manifest.
// This one runs on the real timer, as the defect did.
func TestCrashOrphanIsNotReapedUnderItsReplay(t *testing.T) {
	withChunk(t, 2)
	tbl := racyTable(t)
	before := seed(t, tbl)
	for i, lsn := range []string{"0/21", "0/22"} {
		if _, err := tbl.Apply([]changestream.Transaction{asTxn(t, false, lsn,
			[]changestream.Change{insR(fmt.Sprint(100+i), "d", "0")})}); err != nil {
			t.Fatal(err)
		}
		before[fmt.Sprint(100+i)] = "d/0"
	}
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
		defer func() { _ = recover() }()
		_, _ = tbl.Apply([]changestream.Transaction{asTxn(t, true, "0/30", ops)})
	}()

	re := reopen(t, tbl)
	re.publisher().grace = 2 * time.Second
	// An idle tick's maintain() merges deltas before any Apply arrives, and
	// that is this process's first publish.
	if n, err := re.MergeDeltas(); err != nil || n == 0 {
		t.Fatalf("merge: n=%d err=%v", n, err)
	}
	n := 0
	re.afterChunk = func() {
		if n++; n == 7 { // a long transaction: later chunks still to come
			time.Sleep(3 * time.Second)
		}
	}
	if _, err := re.Apply([]changestream.Transaction{asTxn(t, true, "0/30", ops)}); err != nil {
		t.Errorf("replay Apply returned: %v", err)
	}
	st := onDiskState(t, re)
	if m := missing(re.Dir, re.filesOf(&st)); len(m) > 0 {
		t.Fatalf("DATA LOSS: durable manifest (applied_lsn %s) names files that were unlinked: %v", st.AppliedLSN, m)
	}
	if d := sameRows(liveRows(t, re), model(before, ops)); d != "[]" {
		t.Fatalf("after replay: %s", d)
	}
}

// The same hazard one directory over: a background rewrite that died before
// its swap leaves base/<seq+1>; the sweep adopted it with a deadline, the next
// BeginCompaction wrote that name again, and the deadline fell on the new
// base — which the swap then named.
func TestCrashedRewriteOutputIsNotReapedUnderTheNextRewrite(t *testing.T) {
	tbl := racyTable(t)
	want := seed(t, tbl)
	c := tbl.BeginCompaction()
	if c == nil {
		t.Fatal("no compaction began")
	}
	<-c.done // written, never swapped: the process dies here
	orphan := filepath.Join(tbl.Dir, "base", c.name)
	if _, err := os.Stat(orphan); err != nil {
		t.Fatalf("setup: the crashed rewrite left nothing: %v", err)
	}

	re := reopen(t, tbl)
	clk := manualGrace(re, time.Minute)
	// The first publish of the new process, from something that does not
	// advance the sequence: the orphan is still the NEXT name.
	if err := re.Evolve(re.Columns, re.Order, nil); err != nil {
		t.Fatal(err)
	}
	c2 := re.BeginCompaction()
	if c2 == nil || c2.name != c.name {
		t.Fatalf("setup: the next rewrite does not reuse the orphan's name (%v, want %s)", c2, c.name)
	}
	<-c2.done
	// The grace the orphan would have been given runs out, and a publish
	// reaps, before the swap.
	clk.at = clk.at.Add(2 * time.Minute)
	applyR(t, re, "0/30", rChange(changestream.OpInsert, 9001, "new", 1))
	want["9001"] = "new/1"
	if _, err := re.FinishCompaction(); err != nil {
		t.Fatalf("swap: %v", err)
	}
	st := onDiskState(t, re)
	if m := missing(re.Dir, re.filesOf(&st)); len(m) > 0 {
		t.Fatalf("DATA LOSS: the manifest names a base file that was unlinked: %v", m)
	}
	if d := sameRows(liveRows(t, re), want); d != "[]" {
		t.Fatalf("after the swap: %s", d)
	}
}

// The invariant itself, without the sweep's classification or claim to help:
// a deadline standing against a path does not fall on a file written there
// since, however it came to be written.
func TestDeadlineNeverFallsOnARewrittenPath(t *testing.T) {
	tbl, clk := graceTable(t, time.Minute)
	seedR(t, tbl, 5)
	path := filepath.Join(tbl.Dir, "delta", "000000.parquet")
	for _, how := range []string{"rewritten in place", "replaced by rename"} {
		if err := os.WriteFile(path, []byte("the retired one"), 0o644); err != nil {
			t.Fatal(err)
		}
		tbl.retire(path)
		if tbl.publisher().pending[path] == nil {
			t.Fatal("setup: not retired")
		}
		clk.at = clk.at.Add(time.Second)
		if how == "rewritten in place" {
			if err := os.WriteFile(path, []byte("another, later"), 0o644); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := os.WriteFile(path+".tmp", []byte("another, later"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(path+".tmp", path); err != nil {
				t.Fatal(err)
			}
		}
		clk.at = clk.at.Add(2 * time.Minute)
		if pending, _ := reapNow(tbl); pending != 0 {
			t.Fatalf("%s: the retirement is still pending", how)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("%s: the deadline of the file that was there fell on the one that is: %v", how, err)
		}
	}
	// claim: the same, before the write rather than at the unlink.
	tbl.retire(path)
	tbl.claim(path)
	if tbl.publisher().pending[path] != nil || tbl.publisher().bytes < 0 {
		t.Fatal("claim left the deadline standing")
	}
}

// What a reader of snapshot.json sees while a (re-)snapshot runs. It was an
// empty, servable table at the old applied_lsn, for as long as it took.
func TestSnapshotNeverPublishesAnEmptyTable(t *testing.T) {
	for _, fits := range []bool{true, false} {
		name := "previous contents fit under the cap"
		if !fits {
			name = "previous contents exceed the cap"
		}
		t.Run(name, func(t *testing.T) {
			tbl := racyTable(t)
			clk := manualGrace(tbl, time.Minute)
			before := seed(t, tbl)
			prev := mustPublished(t, tbl)
			if !fits {
				tbl.publisher().maxBytes = 1
			}
			old, err := tbl.beginSnapshot() // the first statement of Snapshot
			if err != nil {
				t.Fatal(err)
			}
			p := mustPublished(t, tbl)
			if len(p.Files) == 0 && p.Halted == "" {
				t.Fatalf("during a re-snapshot the published snapshot is an EMPTY table at applied_lsn=%s, not halted", p.AppliedLSN)
			}
			if fits {
				// The previous snapshot stands, whole, and so does the manifest.
				if p.Seq != prev.Seq || len(missing(tbl.Dir, p.Files)) != 0 {
					t.Fatalf("the previous snapshot was replaced or lost files: seq %d -> %d, missing %v",
						prev.Seq, p.Seq, missing(tbl.Dir, p.Files))
				}
				if d := sameRows(liveRows(t, tbl), before); d != "[]" {
					t.Fatalf("the manifest during the re-snapshot: %s", d)
				}
			} else {
				if p.Halted == "" || len(p.Files) != 0 || !strings.Contains(p.SQL, "must not be served") {
					t.Fatalf("over the cap the snapshot must refuse: %+v", p)
				}
				if st := onDiskState(t, tbl); st.Halted != "" {
					t.Fatalf("the refusal reached the manifest as a halt: %q", st.Halted)
				}
			}

			// The rest of the snapshot, and what it leaves.
			want := map[string]string{"1": "fresh/1", "2": "fresh/2"}
			if err := writeSnapshotBase(tbl, want, "0/40"); err != nil {
				t.Fatal(err)
			}
			if err := tbl.finishSnapshot(); err != nil {
				t.Fatal(err)
			}
			tbl.discard(old)
			p = mustPublished(t, tbl)
			if p.Halted != "" || p.AppliedLSN != "0/40" || len(p.Files) != 1 || len(missing(tbl.Dir, p.Files)) != 0 {
				t.Fatalf("after the snapshot: %+v", p)
			}
			if d := sameRows(liveRows(t, tbl), want); d != "[]" {
				t.Fatalf("after the snapshot: %s", d)
			}
			if fits {
				// A reader that started on the previous snapshot has its grace.
				if m := missing(tbl.Dir, prev.Files); len(m) != 0 {
					t.Fatalf("the previous snapshot's files went with it: %v", m)
				}
			}
			clk.at = clk.at.Add(2 * time.Minute)
			reapNow(tbl)
			if m := missing(tbl.Dir, prev.Files); len(m) != len(prev.Files) {
				t.Fatalf("the previous contents were never collected: %d of %d gone", len(m), len(prev.Files))
			}
			if m := missing(tbl.Dir, p.Files); len(m) != 0 {
				t.Fatalf("the new contents were collected: %v", m)
			}
		})
	}
}

// A first bootstrap publishes nothing until there is a table to publish.
func TestBootstrapPublishesNothingUntilItIsDone(t *testing.T) {
	tbl := racyTable(t)
	if _, err := tbl.beginSnapshot(); err != nil {
		t.Fatal(err)
	}
	if p, err := readPublished(tbl.Dir); err == nil {
		t.Fatalf("a bootstrap that has read no rows published: %+v", p)
	}
}

// A snapshot that fails after keeping the previous contents leaves them: the
// manifest, the published snapshot and the files, and a halt recorded then is
// added to THAT manifest.
func TestFailedSnapshotLeavesThePreviousTable(t *testing.T) {
	tbl := racyTable(t)
	manualGrace(tbl, time.Minute)
	before := seed(t, tbl)
	prev := mustPublished(t, tbl)
	if _, err := tbl.beginSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err := writeSnapshotBase(tbl, map[string]string{"1": "half/1"}, "0/40"); err != nil {
		t.Fatal(err)
	}
	tbl.broken = true // what Snapshot does when snapshotRows returns an error
	if err := tbl.Halt("the source holds something this table cannot"); err != nil {
		t.Fatal(err)
	}
	st := onDiskState(t, tbl)
	if st.Halted == "" || st.AppliedLSN != "0/20" {
		t.Fatalf("manifest after the failed snapshot: halted=%q applied_lsn=%s", st.Halted, st.AppliedLSN)
	}
	if m := missing(tbl.Dir, prev.Files); len(m) != 0 {
		t.Fatalf("the previous contents lost files: %v", m)
	}
	if d := sameRows(liveRows(t, tbl), before); d != "[]" {
		t.Fatalf("the previous contents: %s", d)
	}
}

// A restart, then a first publish that replaces the snapshot the dead process
// left: its files had the mtime of the day they were written, the sweep read
// that as their age, and unlinked them under every reader of that snapshot.
// Found by TestRandomInterleavingsAgainstAModel.
func TestRestartGivesTheLastSnapshotItsGrace(t *testing.T) {
	tbl := racyTable(t)
	want := seed(t, tbl)
	old := time.Now().Add(-10 * time.Minute)
	for _, sub := range []string{"base", "delta", "dv"} {
		es, _ := os.ReadDir(filepath.Join(tbl.Dir, sub))
		for _, e := range es {
			_ = os.Chtimes(filepath.Join(tbl.Dir, sub, e.Name()), old, old)
		}
	}
	re := reopen(t, tbl)
	clk := manualGrace(re, time.Minute)
	prev := mustPublished(t, re)
	swapCompaction(t, re) // this process's first publish
	if m := missing(re.Dir, prev.Files); len(m) != 0 {
		t.Fatalf("the snapshot replaced a moment ago has lost %d of %d files: %v", len(m), len(prev.Files), m)
	}
	clk.at = clk.at.Add(2 * time.Minute)
	reapNow(re)
	if m := missing(re.Dir, prev.Files); len(m) != len(prev.Files) {
		t.Fatalf("its files were never collected: %d of %d gone", len(m), len(prev.Files))
	}
	if d := sameRows(liveRows(t, re), want); d != "[]" {
		t.Fatal(d)
	}
}

// Close stops what an abandoned Table would otherwise go on doing.
func TestCloseStopsTheTimerAndTheRewrite(t *testing.T) {
	tbl := racyTable(t)
	tbl.publisher().grace = time.Hour
	seed(t, tbl)
	applyR(t, tbl, "0/30", rChange(changestream.OpUpdate, 1, "u", 1))
	applyR(t, tbl, "0/31", rChange(changestream.OpUpdate, 2, "u", 2))
	want := liveRows(t, tbl)
	if _, err := tbl.MergeDeltas(); err != nil {
		t.Fatal(err)
	}
	p := tbl.publisher()
	p.mu.Lock()
	armed := p.timer != nil
	p.mu.Unlock()
	if !armed {
		t.Fatal("setup: nothing is waiting out a grace")
	}
	c := tbl.BeginCompaction()
	if c == nil {
		t.Fatal("setup: no rewrite began")
	}
	tbl.Close()
	p.mu.Lock()
	armed = p.timer != nil
	p.arm()
	rearmed := p.timer != nil
	p.mu.Unlock()
	if armed || rearmed {
		t.Fatalf("after Close the timer is live (armed=%v, re-armed=%v)", armed, rearmed)
	}
	select {
	case <-c.done:
	default:
		t.Fatal("Close returned with the rewrite still writing")
	}
	if _, err := os.Stat(filepath.Join(tbl.Dir, "base", c.name)); err == nil {
		t.Fatal("Close left the abandoned rewrite's output, under the name the next Table will write")
	}
	if _, err := tbl.Apply(nil); !errors.Is(err, ErrNeedsReopen) {
		t.Fatalf("Apply on a closed table: %v", err)
	}
	// The table opens again from the disk, intact.
	if d := sameRows(liveRows(t, reopen(t, tbl)), want); d != "[]" {
		t.Fatal(d)
	}
}
