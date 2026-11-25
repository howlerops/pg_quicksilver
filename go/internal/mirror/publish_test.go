package mirror

// What a reader holding a published snapshot is promised, checked without an
// engine: the files it names are there for the grace period and gone after it,
// a restart collects what a dead process left without touching what a live one
// is making, and the snapshot never describes a manifest that is not durable.
// TestPublishedSnapshotInDuckDB is the same promise evaluated by a real engine.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

type fakeClock struct{ at time.Time }

func (c *fakeClock) now() time.Time { return c.at }

// graceTable is racyTable with a publisher under the test's control: a grace
// period on a clock that only moves when told, and no timer.
func graceTable(t *testing.T, grace time.Duration) (*Table, *fakeClock) {
	t.Helper()
	tbl := racyTable(t)
	return tbl, manualGrace(tbl, grace)
}

func manualGrace(tbl *Table, grace time.Duration) *fakeClock {
	clk := &fakeClock{at: time.Now()}
	p := tbl.publisher()
	p.grace, p.manual, p.now, p.maxBytes = grace, true, clk.now, 0
	return clk
}

func rChange(op changestream.Op, id int, v string, n int) changestream.Change {
	row := map[string]any{"id": itoa(id), "v": v, "n": itoa(n)}
	return changestream.Change{Op: op, Schema: "public", Table: "r", Row: row, Key: row}
}

func applyR(t *testing.T, tbl *Table, lsn string, changes ...changestream.Change) {
	t.Helper()
	if _, err := tbl.Apply([]changestream.Transaction{txn(lsn, changes...)}); err != nil {
		t.Fatalf("apply %s: %v", lsn, err)
	}
}

// seedR leaves a base file with a deletion vector on it and one delta: the
// shape whose view used to break two applies later.
func seedR(t *testing.T, tbl *Table, rows int) {
	t.Helper()
	var seed []changestream.Change
	for i := 1; i <= rows; i++ {
		seed = append(seed, rChange(changestream.OpInsert, i, "x", i))
	}
	applyR(t, tbl, "0/1", seed...)
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	applyR(t, tbl, "0/2", rChange(changestream.OpUpdate, 1, "u", 1000))
}

func swapCompaction(t *testing.T, tbl *Table) {
	t.Helper()
	c := tbl.BeginCompaction()
	if c == nil {
		t.Fatal("no compaction began")
	}
	<-c.done
	if _, err := tbl.FinishCompaction(); err != nil {
		t.Fatal(err)
	}
}

func mustPublished(t *testing.T, tbl *Table) *Published {
	t.Helper()
	p, err := readPublished(tbl.Dir)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func missing(dir string, files []string) []string {
	var out []string
	for _, f := range files {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			out = append(out, f)
		}
	}
	return out
}

func onDisk(t *testing.T, dir string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, sub := range []string{"base", "delta", "dv"} {
		m, _ := filepath.Glob(filepath.Join(dir, sub, "*"))
		for _, f := range m {
			out[filepath.Join(sub, filepath.Base(f))] = true
		}
	}
	return out
}

func reapNow(tbl *Table) (pending int, bytes int64) {
	p := tbl.publisher()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reap()
	return len(p.pending), p.bytes
}

// churn is everything that used to break a view made earlier: two further
// applies (which retire the vector generation it names), a delta merge and a
// compaction swap.
func churn(t *testing.T, tbl *Table) {
	t.Helper()
	applyR(t, tbl, "0/3", rChange(changestream.OpUpdate, 2, "u", 2000))
	applyR(t, tbl, "0/4", rChange(changestream.OpUpdate, 3, "u", 3000),
		rChange(changestream.OpDelete, 4, "", 0))
	before := append([]string(nil), tbl.State.DeltaFiles...)
	if _, err := tbl.MergeDeltas(); err != nil {
		t.Fatal(err)
	}
	if len(tbl.State.DeltaFiles) >= len(before) {
		t.Fatalf("the merge merged nothing: %v -> %v", before, tbl.State.DeltaFiles)
	}
	applyR(t, tbl, "0/5", rChange(changestream.OpInsert, 9001, "new", 1))
	swapCompaction(t, tbl)
}

func TestSnapshotFilesOutliveItByTheGraceAndNoLonger(t *testing.T) {
	tbl, clk := graceTable(t, time.Minute)
	seedR(t, tbl, 50)
	held := mustPublished(t, tbl)
	if len(held.Files) < 3 || !strings.Contains(strings.Join(held.Files, " "), "dv/") {
		t.Fatalf("the snapshot under test should name a base, a vector and a delta: %v", held.Files)
	}

	churn(t, tbl)
	clk.at = clk.at.Add(59 * time.Second)
	applyR(t, tbl, "0/6", rChange(changestream.OpUpdate, 5, "u", 5000)) // a publish, which reaps

	now := mustPublished(t, tbl)
	if now.Seq <= held.Seq {
		t.Fatalf("sequence went %d -> %d across six manifest changes", held.Seq, now.Seq)
	}
	for _, f := range held.Files {
		if contains(now.Files, f) {
			t.Fatalf("the churn was meant to replace every file snapshot %d names; %s survives", held.Seq, f)
		}
	}
	if gone := missing(tbl.Dir, held.Files); len(gone) != 0 {
		t.Fatalf("inside the grace period, snapshot %d has lost %v", held.Seq, gone)
	}

	// Past the grace of everything the churn retired.
	clk.at = clk.at.Add(2 * time.Minute)
	left, bytes := reapNow(tbl)
	if gone := missing(tbl.Dir, held.Files); len(gone) != len(held.Files) {
		t.Fatalf("after the grace period %d of %d retired files are still there",
			len(held.Files)-len(gone), len(held.Files))
	}
	if left != 0 || bytes != 0 {
		t.Fatalf("%d files (%d bytes) still pending after everything expired", left, bytes)
	}
	// And nothing but the current manifest is left: the grace costs no disk
	// once it is over.
	want := map[string]bool{}
	for _, f := range mustPublished(t, tbl).Files {
		want[f] = true
	}
	got := onDisk(t, tbl.Dir)
	for f := range got {
		if !want[f] {
			t.Errorf("%s is on disk and in no snapshot", f)
		}
	}
	for f := range want {
		if !got[f] {
			t.Errorf("the current snapshot names %s, which is gone", f)
		}
	}
	rows, err := tbl.Live()
	if err != nil || len(rows) != 50 {
		t.Fatalf("live rows = %d, %v; want 50", len(rows), err)
	}
}

// The clock starts when a file is SUPERSEDED. Compact unlinks its inputs
// before it has written their replacement; a reader on the published snapshot
// must not lose them for however long that write takes.
func TestGraceStartsAtSupersessionNotAtTheCall(t *testing.T) {
	tbl, clk := graceTable(t, time.Minute)
	seedR(t, tbl, 20)
	held := mustPublished(t, tbl)

	for _, f := range held.Files {
		tbl.retire(filepath.Join(tbl.Dir, f))
	}
	clk.at = clk.at.Add(time.Hour) // a very slow rewrite
	reapNow(tbl)
	if gone := missing(tbl.Dir, held.Files); len(gone) != 0 {
		t.Fatalf("files the published snapshot names were unlinked before anything replaced it: %v", gone)
	}

	if _, err := tbl.Compact(); err != nil { // the publish that supersedes them
		t.Fatal(err)
	}
	clk.at = clk.at.Add(59 * time.Second)
	reapNow(tbl)
	if gone := missing(tbl.Dir, held.Files); len(gone) != 0 {
		t.Fatalf("Compact unlinked %v with a grace period still owed", gone)
	}
	clk.at = clk.at.Add(2 * time.Second)
	reapNow(tbl)
	if gone := missing(tbl.Dir, held.Files); len(gone) != len(held.Files) {
		t.Fatalf("still there after the grace: %d of %d", len(held.Files)-len(gone), len(held.Files))
	}
}

// An idle table must still give the disk back: nothing will call into it.
func TestRetiredFilesGoWithoutAnotherWrite(t *testing.T) {
	tbl := racyTable(t)
	tbl.publisher().grace = 300 * time.Millisecond
	seedR(t, tbl, 20)
	held := mustPublished(t, tbl)
	swapCompaction(t, tbl)
	deadline := time.Now().Add(10 * time.Second)
	for len(missing(tbl.Dir, held.Files)) != len(held.Files) {
		if time.Now().After(deadline) {
			t.Fatalf("an idle table still holds %d retired files",
				len(held.Files)-len(missing(tbl.Dir, held.Files)))
		}
		time.Sleep(20 * time.Millisecond)
	}
	if gone := missing(tbl.Dir, mustPublished(t, tbl).Files); len(gone) != 0 {
		t.Fatalf("the timer took live files: %v", gone)
	}
}

func TestRestartCollectsOrphansAndKeepsLiveAndInFlight(t *testing.T) {
	tbl, _ := graceTable(t, time.Minute)
	seedR(t, tbl, 50)
	applyR(t, tbl, "0/3", rChange(changestream.OpUpdate, 2, "u", 2000))
	applyR(t, tbl, "0/4", rChange(changestream.OpUpdate, 3, "u", 3000)) // retires vector generation 1
	root := filepath.Dir(tbl.Dir)
	retiredByDeadProcess := filepath.Join(tbl.Dir, "dv",
		fmt.Sprintf("%s.%09d.dv.parquet", trimParquet(tbl.State.BaseFiles[0]), 1))
	if _, err := os.Stat(retiredByDeadProcess); err != nil {
		t.Fatalf("setup: generation 1 should be waiting out its grace: %v", err)
	}

	old := time.Now().Add(-time.Hour)
	write := func(rel string, age time.Time) string {
		path := filepath.Join(tbl.Dir, rel)
		if err := os.WriteFile(path, []byte("orphan"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, age, age); err != nil {
			t.Fatal(err)
		}
		return path
	}
	oldDelta := write("delta/000900.parquet", old)            // an Apply that crashed before its manifest
	oldVector := write("dv/000900.000000001.dv.parquet", old) // and its vector
	oldTmp := write("dv/000901.000000001.dv.parquet.tmp", old)
	young := write("base/000902.parquet", time.Now()) // unnamed, but inside the grace

	// The restart: a new Table over the same directory, nothing carried over.
	re, err := New(root, "public", "r", "id", tbl.Columns, tbl.Order)
	if err != nil {
		t.Fatal(err)
	}
	clk := manualGrace(re, time.Minute)
	for _, f := range []string{oldDelta, oldVector, oldTmp} {
		if _, err := os.Stat(f); err != nil {
			t.Fatalf("New must not collect anything (a reader calls it too): %s is gone", f)
		}
	}

	// A rewrite in flight when the first manifest of this process is written,
	// and old enough that age alone would condemn its output.
	c := re.BeginCompaction()
	<-c.done
	inflight := filepath.Join(re.Dir, "base", c.name)
	if err := os.Chtimes(inflight, old, old); err != nil {
		t.Fatal(err)
	}
	// A live file is old too, as every base file of a quiet table is.
	if err := os.Chtimes(filepath.Join(re.Dir, mustPublished(t, re).Files[0]), old, old); err != nil {
		t.Fatal(err)
	}

	applyR(t, re, "0/5", rChange(changestream.OpInsert, 9001, "new", 1))

	for _, f := range []string{oldDelta, oldVector, oldTmp} {
		if _, err := os.Stat(f); err == nil {
			t.Errorf("orphan %s survived the restart", filepath.Base(f))
		}
	}
	for what, f := range map[string]string{
		"the in-flight rewrite's output":                   inflight,
		"an unnamed file younger than the grace":           young,
		"a file the dead process retired inside its grace": retiredByDeadProcess,
	} {
		if _, err := os.Stat(f); err != nil {
			t.Errorf("the restart collected %s (%s)", what, filepath.Base(f))
		}
	}
	if gone := missing(re.Dir, mustPublished(t, re).Files); len(gone) != 0 {
		t.Fatalf("the restart collected live files: %v", gone)
	}

	// The rewrite it spared is usable, and the adopted files go on time.
	if _, err := re.FinishCompaction(); err != nil {
		t.Fatal(err)
	}
	rows, err := re.Live()
	if err != nil || len(rows) != 51 {
		t.Fatalf("after the swap: %d rows, %v; want 51", len(rows), err)
	}
	clk.at = clk.at.Add(2 * time.Minute)
	reapNow(re)
	for _, f := range []string{young, retiredByDeadProcess} {
		if _, err := os.Stat(f); err == nil {
			t.Errorf("%s adopted at restart and never collected", filepath.Base(f))
		}
	}
	if gone := missing(re.Dir, mustPublished(t, re).Files); len(gone) != 0 {
		t.Fatalf("live files collected after the grace: %v", gone)
	}
}

// Fail every fsync of an apply, one at a time, and look at what a reader and a
// restart would each find.
func TestPublishedSnapshotIsNeverAheadOfTheManifest(t *testing.T) {
	boom := errors.New("injected fsync failure")
	sawPublishFail, sawManifestFail := false, false
	for k := 0; ; k++ {
		tbl, _ := graceTable(t, time.Minute)
		seedR(t, tbl, 20) // manifest and snapshot both at 0/2

		var n atomic.Int64
		var failed atomic.Value
		hook := func(path string, dir bool) error {
			if int(n.Add(1))-1 == k {
				failed.Store(fmt.Sprintf("%s dir=%v", strings.TrimPrefix(path, tbl.Dir), dir))
				return boom
			}
			return nil
		}
		syncHook.Store(&hook)
		_, err := tbl.Apply([]changestream.Transaction{txn("0/3",
			rChange(changestream.OpUpdate, 2, "u", 2000))})
		syncHook.Store(nil)

		var st State
		b, rerr := os.ReadFile(tbl.statePath())
		if rerr != nil || json.Unmarshal(b, &st) != nil {
			t.Fatalf("k=%d: manifest unreadable: %v", k, rerr)
		}
		pub := mustPublished(t, tbl)
		if pub.AppliedLSN > st.AppliedLSN {
			t.Fatalf("k=%d (%v): snapshot is at %s, manifest at %s",
				k, failed.Load(), pub.AppliedLSN, st.AppliedLSN)
		}
		if gone := missing(tbl.Dir, pub.Files); len(gone) != 0 {
			t.Fatalf("k=%d (%v): the published snapshot names missing files %v", k, failed.Load(), gone)
		}
		if err == nil {
			if failed.Load() != nil {
				t.Fatalf("k=%d: %v failed and Apply reported success", k, failed.Load())
			}
			if pub.AppliedLSN != "0/3" || st.AppliedLSN != "0/3" {
				t.Fatalf("clean apply: snapshot %s, manifest %s", pub.AppliedLSN, st.AppliedLSN)
			}
			t.Logf("an apply is %d fsyncs, each failed in turn", k)
			break
		}
		if !errors.Is(err, boom) {
			t.Fatalf("k=%d: %v", k, err)
		}
		if st.AppliedLSN == "0/3" && pub.AppliedLSN == "0/2" {
			sawPublishFail = true // manifest durable, publish failed: older, never newer
		}
		if st.AppliedLSN == "0/2" {
			sawManifestFail = true
		}
	}
	if !sawPublishFail || !sawManifestFail {
		t.Fatalf("the injection never hit both sides: manifest=%v publish=%v",
			sawManifestFail, sawPublishFail)
	}
}

// Every path that changes the manifest publishes, because they all end in
// saveState. This is the list from the top of publish.go, exercised.
func TestEveryManifestChangePublishes(t *testing.T) {
	tbl, _ := graceTable(t, time.Minute)
	last := uint64(0)
	check := func(what string) {
		t.Helper()
		pub := mustPublished(t, tbl)
		if pub.Seq != last+1 {
			t.Fatalf("%s: sequence %d -> %d, want one step", what, last, pub.Seq)
		}
		last = pub.Seq
		b, _ := os.ReadFile(tbl.statePath())
		var st State
		_ = json.Unmarshal(b, &st)
		if pub.AppliedLSN != st.AppliedLSN {
			t.Fatalf("%s: snapshot at %q, manifest at %q", what, pub.AppliedLSN, st.AppliedLSN)
		}
		if st.Halted != "" {
			return
		}
		for _, f := range append(append([]string{}, st.BaseFiles...), st.DeltaFiles...) {
			if !strings.Contains(pub.SQL, f) {
				t.Fatalf("%s: the snapshot's SQL does not name %s", what, f)
			}
		}
		if n := len(st.BaseFiles) + len(st.DeltaFiles); len(pub.Files) < n {
			t.Fatalf("%s: %d files named, manifest has %d data files", what, len(pub.Files), n)
		}
	}
	var seed []changestream.Change
	for i := 1; i <= 20; i++ {
		seed = append(seed, rChange(changestream.OpInsert, i, "x", i))
	}
	applyR(t, tbl, "0/1", seed...)
	check("apply")
	applyR(t, tbl, "0/2", rChange(changestream.OpUpdate, 1, "u", 1))
	check("apply")
	if _, err := tbl.MergeDeltas(); err != nil {
		t.Fatal(err)
	}
	check("delta merge")
	swapCompaction(t, tbl)
	check("compaction swap")
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	check("compact")
	cols := map[string]string{"id": "bigint", "v": "text", "n": "bigint", "extra": "text"}
	order := []string{"id", "v", "n", "extra"}
	if err := tbl.Evolve(cols, order, map[string]string{"extra": "d"}); err != nil {
		t.Fatal(err)
	}
	check("evolve")
	if !strings.Contains(mustPublished(t, tbl).SQL, `"extra"`) {
		t.Fatal("the snapshot after an ADD COLUMN does not have the column")
	}
	if err := tbl.truncate(); err != nil {
		t.Fatal(err)
	}
	check("truncate")
	if pub := mustPublished(t, tbl); len(pub.Files) != 0 || !strings.Contains(pub.SQL, "WHERE false") {
		t.Fatalf("a truncated table published %v", pub.Files)
	}

	if err := tbl.Halt("schema drift"); err != nil {
		t.Fatal(err)
	}
	check("halt")
	pub := mustPublished(t, tbl)
	if pub.Halted != "schema drift" || !strings.Contains(pub.SQL, "is halted and must not be served: schema drift") ||
		strings.Contains(pub.SQL, "read_parquet") || len(pub.Files) != 0 {
		t.Fatalf("a halted mirror published something readable: %+v", pub)
	}

	// The sequence is carried across a restart.
	re, err := New(filepath.Dir(tbl.Dir), "public", "r", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	manualGrace(re, time.Minute)
	if err := re.saveState(); err != nil {
		t.Fatal(err)
	}
	tbl = re
	check("restart")
}

func TestRetireMaxBytesGivesUpTheOldestFirst(t *testing.T) {
	tbl, clk := graceTable(t, time.Hour)
	p := tbl.publisher()
	var paths []string
	for i := 0; i < 4; i++ {
		path := filepath.Join(tbl.Dir, "delta", fmt.Sprintf("9%05d.parquet", i))
		if err := os.WriteFile(path, make([]byte, 1000), 0o644); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	p.maxBytes = 2500
	for _, path := range paths {
		clk.at = clk.at.Add(time.Second)
		tbl.retire(path)
	}
	var have []bool
	for _, path := range paths {
		_, err := os.Stat(path)
		have = append(have, err == nil)
	}
	if fmt.Sprint(have) != "[false false true true]" {
		t.Fatalf("with room for two of four, kept %v (oldest first)", have)
	}
	if p.bytes != 2000 {
		t.Fatalf("accounting says %d bytes held, 2000 are", p.bytes)
	}
}

// TestPublishedSnapshotInDuckDB evaluates published snapshots in a real engine
// and logs what it says. It needs python with duckdb, so it is opt-in:
//
//	QS_DUCKDB_PYTHON=python3 go test -run TestPublishedSnapshotInDuckDB -v ./internal/mirror/
func TestPublishedSnapshotInDuckDB(t *testing.T) {
	py := os.Getenv("QS_DUCKDB_PYTHON")
	if py == "" {
		t.Skip("set QS_DUCKDB_PYTHON to a python with duckdb installed")
	}
	script, _ := filepath.Abs(filepath.Join("testdata", "snapshot_query.py"))
	scratch := t.TempDir()
	query := func(snapshot string) string {
		out, err := exec.Command(py, script, snapshot).CombinedOutput()
		if err != nil {
			t.Fatalf("%s: %v\n%s", script, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	hold := func(tbl *Table, name string) string { // a reader's own copy of a snapshot
		b, err := os.ReadFile(filepath.Join(tbl.Dir, PublishedFile))
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(scratch, name)
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	answer := func(line string) string {
		if i := strings.Index(line, "count="); i >= 0 {
			return line[i:]
		}
		return line
	}
	expect := func(tbl *Table) string { // the same aggregate, from the in-process reader
		rows, err := tbl.Live()
		if err != nil {
			t.Fatal(err)
		}
		sum := int64(0)
		for _, r := range rows {
			sum += r["n"].(int64)
		}
		return fmt.Sprintf("count=%d sum_n=%d ", len(rows), sum)
	}

	for _, grace := range []time.Duration{0, time.Minute} {
		t.Logf("---- grace period %v", grace)
		tbl, clk := graceTable(t, grace)
		seedR(t, tbl, 1000)
		wantN := expect(tbl)
		snapN := hold(tbl, fmt.Sprintf("N-%v.json", grace))
		before := query(snapN)
		t.Logf("snapshot N,   read at once:               %s", before)
		if !strings.Contains(before, wantN) {
			t.Fatalf("snapshot N disagrees with the mirror's own reader (%s)", wantN)
		}

		applyR(t, tbl, "0/3", rChange(changestream.OpUpdate, 2, "u", 2000))
		applyR(t, tbl, "0/4", rChange(changestream.OpUpdate, 3, "u", 3000),
			rChange(changestream.OpDelete, 4, "", 0))
		after2 := query(snapN)
		t.Logf("snapshot N,   after two applies:          %s", after2)
		if _, err := tbl.MergeDeltas(); err != nil {
			t.Fatal(err)
		}
		applyR(t, tbl, "0/5", rChange(changestream.OpInsert, 9001, "new", 1))
		swapCompaction(t, tbl)
		afterAll := query(snapN)
		t.Logf("snapshot N,   after merge + base rewrite: %s", afterAll)

		cur := query(hold(tbl, fmt.Sprintf("Nk-%v.json", grace)))
		t.Logf("snapshot N+k, current:                    %s", cur)
		if !strings.Contains(cur, expect(tbl)) {
			t.Fatalf("snapshot N+k disagrees with the mirror's own reader (%s)", expect(tbl))
		}
		if answer(cur) == answer(before) {
			t.Fatal("the changes did not change the answer; the scenario proves nothing")
		}

		if grace == 0 {
			// What a view generated earlier did before this change.
			if !strings.Contains(after2, "ERROR") || !strings.Contains(afterAll, "ERROR") {
				t.Fatalf("without a grace period the stale snapshot should fail")
			}
			continue
		}
		if answer(after2) != answer(before) || answer(afterAll) != answer(before) {
			t.Fatalf("snapshot N changed its answer inside the grace period")
		}
		clk.at = clk.at.Add(2 * time.Minute)
		reapNow(tbl)
		expired := query(snapN)
		t.Logf("snapshot N,   after the grace period:     %s", expired)
		if !strings.Contains(expired, "ERROR") {
			t.Fatal("past the grace period snapshot N should fail loudly, not answer")
		}
		if again := query(hold(tbl, "Nk-again.json")); answer(again) != answer(cur) {
			t.Fatalf("the current snapshot changed when the grace expired: %s", again)
		}

		if err := tbl.Halt("schema drift"); err != nil {
			t.Fatal(err)
		}
		halted := query(hold(tbl, "halted.json"))
		t.Logf("halted mirror:                            %s", halted)
		if !strings.Contains(halted, "ERROR") || !strings.Contains(halted, "halted") {
			t.Fatal("a halted mirror's snapshot must refuse")
		}
	}
}
