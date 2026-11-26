package mirror

// Random interleavings of everything that writes a manifest, against a model.
//
// The defects in interact_regress_test.go were each found by putting two
// features in an order nobody had tried: a publish before the first Apply of a
// restarted process, a halt recorded after the Apply that raised it, a rewrite
// begun over a crashed one's leftover. This test stops choosing the order. It
// drives one table through crashes in the middle of a chunked Apply, restarts,
// idle merges and rewrites, replays, TRUNCATE, halts, rebuilds and
// re-snapshots, and after EVERY step holds the mirror to three things:
//
//	(i)   every file the manifest on disk names exists, and reads back;
//	(ii)  every file a published snapshot named exists until that snapshot
//	      has been superseded for a full grace period;
//	(iii) the live rows are the model's.
//
// Time is a clock the test owns and the timer is off (publisher.manual), so
// "until its grace expires" is exact rather than slept for.

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

const interleaveGrace = time.Minute

// seenSnapshot is a published snapshot some reader may have started on.
type seenSnapshot struct {
	seq        uint64
	files      []string
	superseded bool
	at         time.Time // when it was superseded
}

type interleaving struct {
	t     *testing.T
	rng   *rand.Rand
	root  string
	clk   *fakeClock
	tbl   *Table
	model map[string]string
	lsn   int

	// replay is a transaction the "source" committed and the mirror has not:
	// the one a crashed Apply was in the middle of. The stream delivers it
	// again before anything later.
	replay    []changestream.Change
	replayLSN string

	halted bool
	seen   []*seenSnapshot
	steps  []string
}

func (w *interleaving) open() {
	w.t.Helper()
	cols := map[string]string{"id": "bigint", "v": "text", "n": "bigint"}
	tbl, err := New(w.root, "public", "r", "id", cols, []string{"id", "v", "n"})
	if err != nil {
		w.t.Fatal(err)
	}
	p := tbl.publisher()
	p.grace, p.manual, p.now, p.maxBytes = interleaveGrace, true, w.clk.now, 0
	w.tbl = tbl
}

// crash abandons the Table as a killed process does: nothing is closed and
// nothing saved. The one thing waited for is its goroutines, which a real
// crash would have taken with it.
func (w *interleaving) crash() {
	if c := w.tbl.compacting; c != nil {
		<-c.done
	}
	w.tbl.aborting.Wait()
	w.open()
}

func (w *interleaving) nextLSN() string {
	w.lsn++
	return fmt.Sprintf("0/%X", w.lsn)
}

func (w *interleaving) fail(format string, args ...any) {
	w.t.Helper()
	from := 0
	if len(w.steps) > 25 {
		from = len(w.steps) - 25
	}
	for i, s := range w.steps[from:] {
		w.t.Logf("step %3d: %s", from+i, s)
	}
	if b, err := os.ReadFile(w.tbl.statePath()); err == nil {
		w.t.Logf("manifest on disk: %s", b)
	}
	var have []string
	for f := range onDisk(w.t, w.tbl.Dir) {
		have = append(have, f)
	}
	sort.Strings(have)
	w.t.Logf("files on disk: %v", have)
	w.t.Fatalf(format, args...)
}

// check is the three assertions. rows=false skips (iii), for the one moment
// the manifest is not meant to hold the table: between the two halves of a
// re-snapshot that had to release the previous contents first.
func (w *interleaving) check(rows bool) {
	w.t.Helper()
	dir := w.tbl.Dir

	// (i) the manifest on disk, its files and its vectors.
	st := onDiskState(w.t, w.tbl)
	for _, f := range w.tbl.filesOf(&st) {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			w.fail("(i) the manifest (applied_lsn %s) names %s, which is gone", st.AppliedLSN, f)
		}
	}
	for _, sub := range [][]string{st.BaseFiles, st.DeltaFiles} {
		for _, name := range sub {
			if gen := st.DVGen[dvStem(name)]; gen != 0 && w.tbl.dvPathGenAny(name, gen) == "" {
				w.fail("(i) the manifest names generation %d of the vector of %s, which is gone", gen, name)
			}
		}
	}
	rd, err := New(w.root, "public", "r", "id", w.tbl.Columns, w.tbl.Order)
	if err != nil {
		w.t.Fatal(err)
	}
	got := map[string]string{}
	if err := rd.ForEachLive(func(r map[string]any) error {
		id := Render(r["id"])
		if _, dup := got[id]; dup {
			return fmt.Errorf("key %s is live twice", id)
		}
		got[id] = Render(r["v"]) + "/" + Render(r["n"])
		return nil
	}); err != nil {
		w.fail("(i) the manifest on disk does not read back: %v", err)
	}
	if st.Halted != "" != w.halted {
		w.fail("the manifest's halt is %q; the test expects halted=%v", st.Halted, w.halted)
	}

	// (iii) the rows.
	if rows {
		if d := sameRows(got, w.model); d != "[]" {
			w.fail("(iii) live rows differ from the model (got, want): %s", d)
		}
	}

	if p, err := readPublished(dir); err == nil {
		if w.halted && p.Halted == "" {
			w.fail("(ii) the table is halted and its published snapshot serves")
		}
		if rows && !w.halted && p.Halted != "" {
			w.fail("(ii) the table is in service and its published snapshot refuses: %s", p.Halted)
		}
	}
	w.observe()
}

// observe is (ii): every snapshot a reader may still be on has its files.
//
// It also runs before every move of the clock, check or no check, and that is
// what makes the supersession times exact: a snapshot first seen replaced was
// replaced since the last observation, and the clock has not moved since then.
func (w *interleaving) observe() {
	w.t.Helper()
	dir := w.tbl.Dir
	if p, err := readPublished(dir); err == nil {
		if n := len(w.seen); n == 0 || w.seen[n-1].seq != p.Seq {
			if n > 0 && p.Seq < w.seen[n-1].seq {
				w.fail("(ii) the published sequence went back: %d after %d", p.Seq, w.seen[n-1].seq)
			}
			for _, s := range w.seen {
				if !s.superseded {
					s.superseded, s.at = true, w.clk.at
				}
			}
			w.seen = append(w.seen, &seenSnapshot{seq: p.Seq, files: p.Files})
		}
		if p.Halted != "" && len(p.Files) != 0 {
			w.fail("(ii) a refusing snapshot names files: %v", p.Files)
		}
	}
	live := w.seen[:0]
	for _, s := range w.seen {
		if s.superseded && !w.clk.at.Before(s.at.Add(interleaveGrace)) {
			continue // its grace is over; its files may go
		}
		if m := missing(dir, s.files); len(m) != 0 {
			age := "it is the current one"
			if s.superseded {
				age = fmt.Sprintf("superseded %v ago, grace %v", w.clk.at.Sub(s.at), interleaveGrace)
			}
			w.fail("(ii) snapshot %d (%s) has lost %v", s.seq, age, m)
		}
		live = append(live, s)
	}
	w.seen = live
}

func (w *interleaving) changes(n int, truncate bool) []changestream.Change {
	var ops []changestream.Change
	at := -1
	if truncate {
		at = w.rng.Intn(n)
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprint(1 + w.rng.Intn(30))
		switch r := w.rng.Intn(10); {
		case i == at:
			ops = append(ops, changestream.Change{Op: changestream.OpTruncate, Schema: "public", Table: "r"})
		case r < 4:
			ops = append(ops, insR(id, fmt.Sprintf("i%d", w.lsn), fmt.Sprint(w.rng.Intn(1000))))
		case r < 8:
			ops = append(ops, updR(id, fmt.Sprintf("u%d", w.lsn), fmt.Sprint(w.rng.Intn(1000))))
		default:
			ops = append(ops, delR(id))
		}
	}
	return ops
}

// timerFires is the publisher's timer going off at an arbitrary moment — in
// particular between two chunks of an Apply, where the files just written are
// named by no manifest yet.
func (w *interleaving) timerFires() {
	if w.rng.Intn(3) == 0 {
		w.pass(time.Duration(w.rng.Intn(90)) * time.Second)
	}
}

// pass moves the clock and lets the publisher's timer catch up with it.
func (w *interleaving) pass(d time.Duration) {
	w.observe()
	w.clk.at = w.clk.at.Add(d)
	reapNow(w.tbl)
	w.observe()
}

func (w *interleaving) apply(ops []changestream.Change, lsn string) {
	w.t.Helper()
	w.tbl.afterChunk = w.timerFires
	defer func() { w.tbl.afterChunk = nil }()
	if _, err := w.tbl.Apply([]changestream.Transaction{asTxn(w.t, w.rng.Intn(2) == 0, lsn, ops)}); err != nil {
		w.fail("apply at %s: %v", lsn, err)
	}
	w.model = model(w.model, ops)
}

// deliver is the stream redelivering what a crashed Apply did not commit.
func (w *interleaving) deliver() {
	if w.replay == nil {
		return
	}
	ops, lsn := w.replay, w.replayLSN
	w.replay = nil
	w.steps = append(w.steps, "  (replay of "+lsn+" first)")
	w.apply(ops, lsn)
}

// resnapshot is the source read again. What the source holds is the model
// plus whatever it committed that the mirror has not applied, and the
// snapshot's LSN is past that, so the replay is discarded by the floor.
func (w *interleaving) source() (map[string]string, string) {
	rows := w.model
	if w.replay != nil {
		rows = model(rows, w.replay)
	}
	return rows, w.nextLSN()
}

func (w *interleaving) step() {
	w.t.Helper()
	tbl := w.tbl
	say := func(format string, args ...any) { w.steps = append(w.steps, fmt.Sprintf(format, args...)) }

	if w.halted {
		// A halted table is stepped over by every loop of the sidecar. Time
		// passes, the process restarts, and eventually an operator rebuilds it.
		switch r := w.rng.Intn(10); {
		case r < 4:
			d := time.Duration(w.rng.Intn(100)) * time.Second
			say("halted: %v pass", d)
			w.pass(d)
		case r < 7:
			say("halted: restart")
			tbl.Close()
			w.open()
		default:
			say("halted: rebuild from a snapshot")
			tbl.Close()
			w.open()
			rows, lsn := w.source()
			if err := snapshotFrom(w.tbl, rows, lsn); err != nil {
				w.fail("rebuild: %v", err)
			}
			if err := w.tbl.ClearHalt(); err != nil {
				w.fail("clear halt: %v", err)
			}
			w.model, w.replay, w.halted = rows, nil, false
		}
		return
	}

	switch r := w.rng.Intn(100); {
	case r < 18:
		w.deliver()
		lsn := w.nextLSN()
		ops := w.changes(1+w.rng.Intn(3), false)
		say("apply %d changes at %s", len(ops), lsn)
		w.apply(ops, lsn)

	case r < 30:
		w.deliver()
		lsn := w.nextLSN()
		ops := w.changes(5+w.rng.Intn(14), w.rng.Intn(4) == 0)
		say("apply %d changes in chunks at %s", len(ops), lsn)
		w.apply(ops, lsn)

	case r < 45:
		w.deliver()
		lsn := w.nextLSN()
		ops := w.changes(5+w.rng.Intn(14), w.rng.Intn(4) == 0)
		die, chunks := 1+w.rng.Intn(5), 0
		say("crash in chunk %d of an apply of %d changes at %s", die, len(ops), lsn)
		tbl.afterChunk = func() {
			if chunks++; chunks == die {
				panic("kill -9")
			}
			w.timerFires()
		}
		crashed := true
		func() {
			defer func() { _ = recover() }()
			_, err := tbl.Apply([]changestream.Transaction{asTxn(w.t, true, lsn, ops)})
			if err != nil {
				w.fail("apply at %s: %v", lsn, err)
			}
			crashed = false
		}()
		tbl.afterChunk = nil
		if !crashed { // fewer chunks than that: it committed
			w.model = model(w.model, ops)
			return
		}
		w.replay, w.replayLSN = ops, lsn
		w.crash()

	case r < 52:
		w.deliver()
		lsn := w.nextLSN()
		ops := w.changes(2+w.rng.Intn(10), false)
		at := w.rng.Intn(len(ops) + 1)
		ops = append(ops[:at:at], append([]changestream.Change{insR("999", "bad", "not-a-number")}, ops[at:]...)...)
		say("apply at %s meets a value the table cannot hold (change %d of %d): halt", lsn, at+1, len(ops))
		_, err := tbl.Apply([]changestream.Transaction{asTxn(w.t, w.rng.Intn(2) == 0, lsn, ops)})
		if err == nil || tbl.Halted() == "" {
			w.fail("no halt: err=%v halted=%q", err, tbl.Halted())
		}
		if err := tbl.Halt(tbl.Halted()); err != nil { // cmd/qs-mirror haltOrFail
			w.fail("recording the halt again: %v", err)
		}
		if err := tbl.saveState(); !errors.Is(err, ErrNeedsReopen) {
			w.fail("a table whose Apply failed wrote its manifest: %v", err)
		}
		w.halted = true

	case r < 59:
		say("restart")
		tbl.Close()
		w.open()

	case r < 67:
		n, err := tbl.MergeDeltas()
		say("idle: merge deltas (%d)", n)
		if err != nil {
			w.fail("merge: %v", err)
		}

	case r < 74:
		c := tbl.BeginCompaction()
		say("idle: begin a rewrite (%v)", c != nil)

	case r < 81:
		if c := tbl.compacting; c != nil {
			<-c.done
		}
		n, err := tbl.FinishCompaction()
		say("idle: swap the rewrite in (%d rows)", n)
		if err != nil {
			w.fail("swap: %v", err)
		}

	case r < 84:
		_, err := tbl.Compact()
		say("compact in place")
		if err != nil {
			w.fail("compact: %v", err)
		}

	case r < 87:
		say("a manifest write that changes no file (evolve)")
		if err := tbl.Evolve(tbl.Columns, tbl.Order, nil); err != nil {
			w.fail("evolve: %v", err)
		}

	case r < 91:
		rows, lsn := w.source()
		say("re-snapshot at %s, previous contents kept", lsn)
		if err := snapshotFrom(tbl, rows, lsn); err != nil {
			w.fail("re-snapshot: %v", err)
		}
		w.model, w.replay = rows, nil

	case r < 93:
		// Over the cap: the previous contents are released FIRST and the
		// published snapshot refuses meanwhile. Readers of earlier snapshots
		// are told to re-read rather than given their grace — that is what
		// the cap is — so (ii) starts again from the refusal.
		rows, lsn := w.source()
		say("re-snapshot at %s, previous contents over the cap", lsn)
		tbl.publisher().maxBytes = 1
		old, err := tbl.beginSnapshot()
		tbl.publisher().maxBytes = 0
		if err != nil {
			w.fail("re-snapshot: %v", err)
		}
		w.seen = nil
		w.check(false)
		if p, err := readPublished(tbl.Dir); err != nil || p.Halted == "" {
			w.fail("mid-re-snapshot the published snapshot does not refuse: %+v (%v)", p, err)
		}
		if w.rng.Intn(2) == 0 {
			say("  ...and the process dies there; the restart snapshots again")
			w.crash()
			w.check(false)
			if err := snapshotFrom(w.tbl, rows, lsn); err != nil {
				w.fail("re-snapshot after the crash: %v", err)
			}
		} else {
			if err := writeSnapshotBase(tbl, rows, lsn); err != nil {
				w.fail("re-snapshot: %v", err)
			}
			if err := tbl.finishSnapshot(); err != nil {
				w.fail("re-snapshot: %v", err)
			}
			tbl.discard(old)
		}
		w.model, w.replay = rows, nil

	case r < 95:
		rows, lsn := w.source()
		say("crash in a re-snapshot at %s that kept the previous contents", lsn)
		if _, err := tbl.beginSnapshot(); err != nil {
			w.fail("re-snapshot: %v", err)
		}
		if err := writeSnapshotBase(tbl, rows, lsn); err != nil {
			w.fail("re-snapshot: %v", err)
		}
		w.crash()

	default:
		d := time.Duration(w.rng.Intn(100)) * time.Second
		say("%v pass", d)
		w.pass(d)
	}
}

func TestRandomInterleavingsAgainstAModel(t *testing.T) {
	if testing.Short() {
		t.Skip("heavy; run without -short")
	}
	seeds, steps := 10, 150
	if testing.Short() {
		seeds = 3
	}
	withChunk(t, 3)
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			w := &interleaving{
				t: t, rng: rand.New(rand.NewSource(int64(seed))), root: t.TempDir(),
				clk: &fakeClock{at: time.Now()}, model: map[string]string{}, lsn: 0x100,
			}
			w.open()
			// A bootstrap, so that there is a base file from the start.
			rows := map[string]string{}
			for i := 1; i <= 12; i++ {
				rows[fmt.Sprint(i)] = fmt.Sprintf("seed/%d", i)
			}
			if err := snapshotFrom(w.tbl, rows, w.nextLSN()); err != nil {
				t.Fatal(err)
			}
			w.model = rows
			w.check(true)
			for i := 0; i < steps; i++ {
				w.step()
				w.check(true)
			}
			w.tbl.Close()
			t.Logf("%d steps", len(w.steps))
		})
	}
}
