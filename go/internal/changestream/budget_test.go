package changestream

// The spool budget, through the same fake connection and real pgoutput bytes
// as spool_test.go.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
)

func setBudget(s *Streaming, n int64) {
	s.mu.Lock()
	s.SpoolMaxBytes = n
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// A committed transaction is spooled and waiting; the next one would take the
// total past the budget. The receiver stops reading — it does not fail — keeps
// the connection alive, and carries on once a Confirm has deleted the first
// one's file. Without the budget it read both to the end.
func TestSpoolBudgetPausesUntilAConfirmFreesIt(t *testing.T) {
	f := &fakeConn{msgs: make(chan pgproto3.BackendMessage), t: t}
	s := newStreaming(f, map[string]bool{"public.events": true})
	s.MaxBufferedChanges = 0
	s.StatusInterval = 5 * time.Millisecond
	s.SpoolDir = t.TempDir()
	s.SpoolThresholdChanges = 5
	s.run(context.Background())
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	stop := feed(f, append([]pgproto3.BackendMessage{xlog(1, relationMsg())}, txn(0x100, 0, 200)...))
	waitFor(t, "the first transaction", func() bool { return s.BufferedChanges() == 200 })
	stop()
	one := s.SpoolBytes()
	if one <= 0 {
		t.Fatal("a spooled transaction counted no bytes")
	}
	// Room for the first and half of the second.
	setBudget(s, one+one/2)
	if got := s.SpoolBudgetBytes(); got != one+one/2 {
		t.Fatalf("budget %d", got)
	}

	stop = feed(f, txn(0x200, 1000, 200))
	defer stop()
	waitFor(t, "the receiver to pause on the budget", s.SpoolPaused)
	if got := s.SpoolBytes(); got < one+one/2 || got > one+one/2+64 {
		t.Fatalf("paused holding %d spool bytes; the budget is %d and one change is under 64", got, one+one/2)
	}
	readAtPause, statusAtPause := f.received.Load(), f.statusCount()
	time.Sleep(60 * time.Millisecond)
	if got := f.received.Load(); got != readAtPause {
		t.Fatalf("read %d more messages while over the spool budget", got-readAtPause)
	}
	if got := f.statusCount() - statusAtPause; got < 3 {
		t.Fatalf("%d status updates in 12 intervals while paused; the walsender would time out", got)
	}
	// Draining alone frees nothing: the file is the applier's until confirmed.
	got, err := s.Transactions(context.Background())
	if err != nil || len(got) != 1 || !got[0].Spooled() {
		t.Fatalf("drain: %v, %d transactions", err, len(got))
	}
	time.Sleep(20 * time.Millisecond)
	if !s.SpoolPaused() || f.received.Load() != readAtPause {
		t.Fatal("a drain without a confirm resumed the receiver")
	}
	if want := ids(t, got[0]); len(want) != 200 {
		t.Fatalf("the first transaction read back %d changes", len(want))
	}

	if err := s.Confirm(context.Background(), got[0].NextLSN); err != nil {
		t.Fatal(err)
	}
	rest := drain(t, s, 1)
	if n := len(ids(t, rest[0])); n != 200 {
		t.Fatalf("the second transaction arrived with %d changes", n)
	}
	if s.SpoolPaused() {
		t.Fatal("still paused after the confirm")
	}
	// The second transaction's ids are longer, so its file is a little larger.
	if got := s.SpoolBytes(); got < one || got > one+200*4 {
		t.Fatalf("after the confirm %d spool bytes are counted, want only the second transaction's (about %d)", got, one)
	}
	if err := s.Confirm(context.Background(), rest[0].NextLSN); err != nil {
		t.Fatal(err)
	}
	if got := s.SpoolBytes(); got != 0 || len(spoolFiles(t, s.SpoolDir)) != 0 {
		t.Fatalf("everything confirmed, and %d bytes in %d files remain", got, len(spoolFiles(t, s.SpoolDir)))
	}
}

// One transaction that does not fit by itself: nothing can be confirmed to make
// room, so the stream fails, with an error that names the numbers and the
// setting. Its file is gone at Close and nothing of it was handed out.
func TestOneTransactionOverTheSpoolBudgetFailsWithTheNumbers(t *testing.T) {
	for _, streamed := range []bool{true, false} {
		f := &fakeConn{msgs: make(chan pgproto3.BackendMessage), t: t}
		s := newStreaming(f, map[string]bool{"public.events": true})
		s.MaxBufferedChanges = 0
		s.StatusInterval = time.Hour
		s.SpoolDir = t.TempDir()
		s.SpoolThresholdChanges = 5
		s.SpoolMaxBytes = 600
		s.run(context.Background())

		var msgs []pgproto3.BackendMessage
		if streamed {
			p := [][]byte{streamStart(7, true), sRelation(7)}
			for i := 0; i < 500; i++ {
				p = append(p, sInsert(7, i))
			}
			msgs = xl(append(p, streamStop(), streamCommit(7, 0x500))...)
		} else {
			msgs = append([]pgproto3.BackendMessage{xlog(1, relationMsg())}, txn(0x500, 0, 500)...)
		}
		stop := feed(f, msgs)
		var err error
		waitFor(t, "the stream to fail", func() bool {
			var got []Transaction
			got, err = s.Transactions(context.Background())
			if len(got) != 0 {
				t.Fatalf("streamed=%v: a transaction over the budget was handed out", streamed)
			}
			return err != nil
		})
		stop()
		var be *SpoolBudgetError
		if !errors.As(err, &be) {
			t.Fatalf("streamed=%v: failed with %v, want a *SpoolBudgetError", streamed, err)
		}
		if be.Budget != 600 || be.Spooled < 600 || be.Spooled > 700 || be.Open != 1 {
			t.Fatalf("streamed=%v: %+v", streamed, be)
		}
		for _, want := range []string{"QS_SPOOL_MAX_BYTES", "600", "MORE than", "restart"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the error does not say %q: %v", want, err)
			}
		}
		// It stopped at the budget rather than reading the other 400 changes.
		if got := f.received.Load(); got > 200 {
			t.Errorf("streamed=%v: read %d messages before failing", streamed, got)
		}
		if err := s.Confirm(context.Background(), "0/600"); err == nil {
			t.Error("a failed stream accepted a confirm")
		}
		_ = s.Close(context.Background())
		if left := spoolFiles(t, s.SpoolDir); len(left) != 0 || s.SpoolBytes() != 0 {
			t.Fatalf("streamed=%v: Close left %v, %d bytes counted", streamed, left, s.SpoolBytes())
		}
	}
}

// A rolled-back subtransaction and a rolled-back transaction give their bytes
// back to the budget, not only to the disk.
func TestSpoolBytesFollowTruncateAndAbort(t *testing.T) {
	s, f, _ := startSpooling(t)
	stop := feed(f, xl(
		streamStart(7, true), sRelation(7), sInsert(7, 1), streamStop(),
	))
	waitFor(t, "the first block", func() bool { return s.SpoolBytes() > 0 })
	stop()
	kept := s.SpoolBytes()
	stop = feed(f, xl(
		streamStart(7, false), sInsert(8, 2), sInsert(8, 3), streamStop(),
	))
	waitFor(t, "the subtransaction", func() bool { return s.SpoolBytes() > kept })
	stop()
	stop = feed(f, xl(streamAbort(7, 8)))
	waitFor(t, "the subtransaction's bytes to be given back", func() bool { return s.SpoolBytes() == kept })
	stop()
	stop = feed(f, xl(streamAbort(7, 7)))
	defer stop()
	waitFor(t, "the transaction's bytes to be given back", func() bool { return s.SpoolBytes() == 0 })
}

func TestDefaultSpoolBudget(t *testing.T) {
	defer func(p func(string) (int64, bool)) { spoolFree = p }(spoolFree)
	dir := t.TempDir()
	var asked []string
	free := int64(40 << 30)
	spoolFree = func(p string) (int64, bool) {
		asked = append(asked, p)
		if p != dir { // the spool directory itself does not exist yet
			return 0, false
		}
		return free, true
	}
	t.Setenv(SpoolMaxBytesEnv, "")
	if got := DefaultSpoolBudget(dir + "/.spool"); got != 10<<30 {
		t.Fatalf("40 GiB free gave a budget of %d, want a quarter", got)
	}
	if len(asked) != 2 || asked[1] != dir {
		t.Fatalf("probed %v, want the directory and then its parent", asked)
	}
	free = 1 << 50
	if got := DefaultSpoolBudget(dir); got != SpoolBudgetCap {
		t.Fatalf("a huge volume gave %d, want the cap %d", got, SpoolBudgetCap)
	}
	free = 0
	if got := DefaultSpoolBudget(dir); got != 1 {
		t.Fatalf("a full volume gave %d; it must be a bound, not 'unbounded'", got)
	}
	t.Setenv(SpoolMaxBytesEnv, "12345")
	if got := DefaultSpoolBudget(dir); got != 12345 {
		t.Fatalf("the environment was not honoured: %d", got)
	}
	t.Setenv(SpoolMaxBytesEnv, "off")
	if got := DefaultSpoolBudget(dir); got >= 0 {
		t.Fatalf("off gave %d", got)
	}
	spoolFree = func(string) (int64, bool) { return 0, false }
	t.Setenv(SpoolMaxBytesEnv, "")
	if got := DefaultSpoolBudget(dir); got != spoolBudgetUnknown {
		t.Fatalf("unknown free space gave %d", got)
	}
}

// A hook registered with BeforeConfirm runs before the position moves, and
// its failure is the Confirm's: the position stays where it was.
func TestBeforeConfirmRunsFirstAndCanRefuse(t *testing.T) {
	defer func(n int) { beforeConfirm.fns = beforeConfirm.fns[:n] }(len(beforeConfirm.fns))
	s, f := startFake(t, 0, time.Hour)
	var seen []string
	var at []pglogrepl.LSN
	refuse := false
	BeforeConfirm(func(lsn string) error {
		seen = append(seen, lsn)
		s.mu.Lock()
		at = append(at, s.confirmed)
		s.mu.Unlock()
		if refuse {
			return errors.New("the watermark could not be written")
		}
		return nil
	})
	if err := s.Confirm(context.Background(), "0/100"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the status update", func() bool { return f.lastStatus() == 0x100 })
	refuse = true
	if err := s.Confirm(context.Background(), "0/200"); err == nil {
		t.Fatal("the hook failed and Confirm did not")
	}
	time.Sleep(20 * time.Millisecond)
	if f.lastStatus() != 0x100 {
		t.Fatalf("a refused confirm reached the server: %s", f.lastStatus())
	}
	// Going backwards confirms nothing, so there is nothing to make durable.
	refuse = false
	if err := s.Confirm(context.Background(), "0/50"); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || seen[0] != "0/100" || seen[1] != "0/200" || at[0] != 0 || at[1] != 0x100 {
		t.Fatalf("hook saw %v with the position at %v", seen, at)
	}
}

// sBlock is one Stream Start..Stop block of n inserts for xid, ids from first.
func sBlock(xid uint32, firstSegment bool, first, n int) []pgproto3.BackendMessage {
	p := [][]byte{streamStart(xid, firstSegment)}
	if firstSegment {
		p = append(p, sRelation(xid))
	}
	for i := 0; i < n; i++ {
		p = append(p, sInsert(xid, first+i))
	}
	return xl(append(p, streamStop())...)
}

// feedAll hands every message to the receiver and returns once it has read
// the last of them.
func feedAll(t *testing.T, f *fakeConn, msgs []pgproto3.BackendMessage) {
	t.Helper()
	want := f.received.Load() + int64(len(msgs))
	stop := feed(f, msgs)
	defer stop()
	waitFor(t, "the receiver to read what was fed", func() bool { return f.received.Load() >= want })
}

func budgetStream(t *testing.T) (*Streaming, *fakeConn) {
	f := &fakeConn{msgs: make(chan pgproto3.BackendMessage), t: t}
	s := newStreaming(f, map[string]bool{"public.events": true})
	s.MaxBufferedChanges = 0
	s.StatusInterval = 5 * time.Millisecond
	s.SpoolDir = t.TempDir()
	s.SpoolThresholdChanges = 5
	s.SpoolMaxBytes = -1
	s.run(context.Background())
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s, f
}

// Three transactions the server streams at once, each two fifths of the
// budget. Together they pass it while none has committed; this failed with a
// SpoolBudgetError, and a restart would have replayed the same interleaving
// into the same error for ever. None of them is too large: the receiver reads
// on, and when the first commits it is a transaction a confirmation gives
// back, so the receiver pauses there until the applier has drained it.
func TestStreamedTransactionsThatOnlyTogetherPassTheBudgetAreDrainedNotFailed(t *testing.T) {
	s, f := budgetStream(t)
	feedAll(t, f, sBlock(7, true, 1000, 100))
	one := s.SpoolBytes()
	if one <= 0 {
		t.Fatal("a streamed transaction counted no bytes")
	}
	budget := one*5/2 + 1
	setBudget(s, budget)

	feedAll(t, f, sBlock(8, true, 2000, 100))
	// The third takes the total past the budget, and every byte is in flight.
	feedAll(t, f, sBlock(9, true, 3000, 100))
	if got := s.SpoolBytes(); got < budget {
		t.Fatalf("%d spool bytes, and the test needs them over the budget of %d", got, budget)
	}
	// It did not fail, did not pause on nothing, and still reads.
	feedAll(t, f, sBlock(9, false, 3100, 20))
	if got, err := s.Transactions(context.Background()); err != nil || len(got) != 0 {
		t.Fatalf("three open transactions over the budget together: %v, %d handed out", err, len(got))
	}
	if s.SpoolPaused() {
		t.Fatal("paused with nothing a confirmation could give back: nothing would ever wake it")
	}

	// One commits. Now a Confirm frees bytes, so it waits for one.
	feedAll(t, f, xl(streamCommit(8, 0x800)))
	waitFor(t, "the receiver to pause on the committed transaction", s.SpoolPaused)
	at := f.received.Load()
	stop := feed(f, sBlock(9, false, 3200, 10))
	time.Sleep(40 * time.Millisecond)
	if got := f.received.Load(); got != at {
		t.Fatalf("read %d more messages while a drainable transaction held the spool over its budget", got-at)
	}
	got := drain(t, s, 1)
	if n := len(ids(t, got[0])); n != 100 {
		t.Fatalf("the committed transaction arrived with %d changes", n)
	}
	if err := s.Confirm(context.Background(), got[0].NextLSN); err != nil {
		t.Fatal(err)
	}
	// Under the budget again: the block that was waiting is read.
	waitFor(t, "the receiver to resume after the confirm", func() bool { return f.received.Load() >= at+12 })
	stop()
	if s.SpoolPaused() {
		t.Fatal("still paused after the confirm")
	}

	feedAll(t, f, xl(streamCommit(7, 0x900), streamCommit(9, 0xA00)))
	rest := drain(t, s, 2)
	if a, b := len(ids(t, rest[0])), len(ids(t, rest[1])); a != 100 || b != 130 {
		t.Fatalf("the other two arrived with %d and %d changes, want 100 and 130", a, b)
	}
	if err := s.Confirm(context.Background(), rest[1].NextLSN); err != nil {
		t.Fatal(err)
	}
	if got := s.SpoolBytes(); got != 0 || len(spoolFiles(t, s.SpoolDir)) != 0 {
		t.Fatalf("everything confirmed, and %d bytes in %d files remain", got, len(spoolFiles(t, s.SpoolDir)))
	}
}

// The failure that remains: among several open transactions, ONE has by itself
// reached the budget. No draining of the others makes room for it, and the
// error says which bytes are its own.
func TestOneStreamedTransactionAmongSeveralStillFailsWhenItAloneIsTheBudget(t *testing.T) {
	s, f := budgetStream(t)
	feedAll(t, f, sBlock(7, true, 1000, 100))
	one := s.SpoolBytes()
	budget := one*3/2 + 1
	setBudget(s, budget)
	// Two open, over the budget together, neither alone: not a failure.
	feedAll(t, f, sBlock(8, true, 2000, 100))
	if _, err := s.Transactions(context.Background()); err != nil {
		t.Fatalf("two transactions that each fit: %v", err)
	}
	stop := feed(f, sBlock(8, false, 2100, 100))
	defer stop()
	var err error
	waitFor(t, "the stream to fail", func() bool {
		_, err = s.Transactions(context.Background())
		return err != nil
	})
	var be *SpoolBudgetError
	if !errors.As(err, &be) {
		t.Fatalf("failed with %v, want a *SpoolBudgetError", err)
	}
	if be.Budget != budget || be.Open != 2 || be.Spooled < budget || be.Spooled > budget+64 ||
		be.Total < be.Spooled+one || be.Total > be.Spooled+one+64 {
		t.Fatalf("%+v, with one=%d budget=%d", be, one, budget)
	}
	if !strings.Contains(err.Error(), "2 transactions were arriving at once") {
		t.Errorf("the error does not mention the others: %v", err)
	}
}
