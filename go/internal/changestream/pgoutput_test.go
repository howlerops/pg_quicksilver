package changestream

// The receiver's buffering, pausing and status-update behaviour, driven through
// a fake replication connection so none of it needs PostgreSQL. The messages
// are real pgoutput proto v1 bytes, so the decode path under test is the
// production one.

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
)

type fakeConn struct {
	msgs     chan pgproto3.BackendMessage
	received atomic.Int64 // messages handed to the receiver

	mu       sync.Mutex
	statuses []pglogrepl.LSN
	inIO     atomic.Int32 // >1 would mean two goroutines on the connection
	t        *testing.T
}

func (f *fakeConn) enter() {
	if f.inIO.Add(1) != 1 {
		f.t.Error("two goroutines are using the replication connection at once")
	}
}

func (f *fakeConn) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	f.enter()
	defer f.inIO.Add(-1)
	select {
	case m := <-f.msgs:
		f.received.Add(1)
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeConn) SendStatus(ctx context.Context, pos pglogrepl.LSN) error {
	f.enter()
	defer f.inIO.Add(-1)
	f.mu.Lock()
	f.statuses = append(f.statuses, pos)
	f.mu.Unlock()
	return nil
}

func (f *fakeConn) statusCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.statuses)
}

func (f *fakeConn) lastStatus() pglogrepl.LSN {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.statuses) == 0 {
		return 0
	}
	return f.statuses[len(f.statuses)-1]
}

func u64(b []byte, v uint64) []byte  { return binary.BigEndian.AppendUint64(b, v) }
func u32(b []byte, v uint32) []byte  { return binary.BigEndian.AppendUint32(b, v) }
func u16(b []byte, v uint16) []byte  { return binary.BigEndian.AppendUint16(b, v) }
func cstr(b []byte, s string) []byte { return append(append(b, s...), 0) }

func xlog(lsn uint64, payload []byte) pgproto3.BackendMessage {
	b := []byte{pglogrepl.XLogDataByteID}
	b = u64(u64(u64(b, lsn), lsn), 0)
	return &pgproto3.CopyData{Data: append(b, payload...)}
}

const relID = 4242

func relationMsg() []byte {
	b := u32([]byte{'R'}, relID)
	b = cstr(cstr(b, "public"), "events")
	b = u16(append(b, 'd'), 1)
	b = cstr(append(b, 1), "id")
	return u32(u32(b, 20), 0xffffffff)
}

func beginMsg(lsn uint64) []byte { return u32(u64(u64([]byte{'B'}, lsn), 0), 7) }

func insertMsg(id int) []byte {
	v := fmt.Sprint(id)
	b := u16(append(u32([]byte{'I'}, relID), 'N'), 1)
	return append(u32(append(b, 't'), uint32(len(v))), v...)
}

func commitMsg(lsn uint64) []byte {
	return u64(u64(u64([]byte{'C', 0}, lsn), lsn+8), 0)
}

// txn returns the messages of one transaction committing at lsn with n inserts
// whose ids start at firstID.
func txn(lsn uint64, firstID, n int) []pgproto3.BackendMessage {
	out := []pgproto3.BackendMessage{xlog(lsn, beginMsg(lsn))}
	for i := 0; i < n; i++ {
		out = append(out, xlog(lsn, insertMsg(firstID+i)))
	}
	return append(out, xlog(lsn, commitMsg(lsn)))
}

func startFake(t *testing.T, cap int, interval time.Duration) (*Streaming, *fakeConn) {
	t.Helper()
	f := &fakeConn{msgs: make(chan pgproto3.BackendMessage), t: t}
	s := newStreaming(f, map[string]bool{"public.events": true})
	s.MaxBufferedChanges = cap
	s.StatusInterval = interval
	s.run(context.Background())
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s, f
}

// feed offers messages to the receiver from the test's "server" and reports,
// through the returned counter, how many it has accepted.
func feed(f *fakeConn, msgs []pgproto3.BackendMessage) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, m := range msgs {
			select {
			case f.msgs <- m:
			case <-done:
				return
			}
		}
	}()
	return func() { close(done); wg.Wait() }
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// The bug: a consumer that does not drain used to let the buffer grow without
// bound. Now the receiver stops reading at the cap, keeps sending status
// updates while stopped, and delivers everything exactly once after a drain.
func TestReceiverPausesAtCapAndResumesOnDrain(t *testing.T) {
	const cap, perTxn, txns = 100, 10, 200
	s, f := startFake(t, cap, 5*time.Millisecond)

	msgs := []pgproto3.BackendMessage{xlog(1, relationMsg())}
	for i := 0; i < txns; i++ {
		msgs = append(msgs, txn(uint64(1000+i*100), i*perTxn, perTxn)...)
	}
	stop := feed(f, msgs)
	defer stop()

	waitFor(t, "the buffer to fill", func() bool { return s.BufferedChanges() >= cap })

	// Paused: over several status intervals nothing more is read, and status
	// updates keep going out. That is what holds off wal_sender_timeout.
	readAtPause, statusAtPause := f.received.Load(), f.statusCount()
	time.Sleep(60 * time.Millisecond)
	if got := f.received.Load(); got != readAtPause {
		t.Fatalf("receiver read %d more messages while the buffer was full", got-readAtPause)
	}
	if got := s.BufferedChanges(); got != cap {
		t.Fatalf("buffered %d changes, want exactly the cap %d", got, cap)
	}
	if got := f.statusCount() - statusAtPause; got < 3 {
		t.Fatalf("only %d status updates in 12 intervals while paused; the walsender would time out", got)
	}

	// Drain in a loop, as pump does. Every transaction arrives once, in order.
	next := 0
	deadline := time.Now().Add(10 * time.Second)
	for next < txns*perTxn {
		if time.Now().After(deadline) {
			t.Fatalf("stalled after %d of %d changes", next, txns*perTxn)
		}
		got, err := s.Transactions(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, tx := range got {
			if len(tx.Changes) != perTxn {
				t.Fatalf("transaction with %d changes, want %d", len(tx.Changes), perTxn)
			}
			for _, c := range tx.Changes {
				if want := fmt.Sprint(next); c.Row["id"] != want {
					t.Fatalf("got id %v, want %s (lost, duplicated or reordered)", c.Row["id"], want)
				}
				next++
			}
			n += len(tx.Changes)
		}
		// One drain never returns more than the cap plus the transaction
		// that crossed it.
		if n > cap+perTxn {
			t.Fatalf("one drain returned %d changes, cap is %d", n, cap)
		}
		time.Sleep(time.Millisecond)
	}
}

// A transaction larger than the cap cannot be split in proto v1. It must still
// come through whole rather than deadlock the receiver against its own cap.
func TestTransactionLargerThanCapIsDelivered(t *testing.T) {
	s, f := startFake(t, 10, time.Hour)
	msgs := append([]pgproto3.BackendMessage{xlog(1, relationMsg())}, txn(1000, 0, 500)...)
	msgs = append(msgs, txn(2000, 500, 3)...)
	stop := feed(f, msgs)
	defer stop()

	waitFor(t, "the large transaction", func() bool { return s.BufferedChanges() == 500 })
	got, err := s.Transactions(context.Background())
	if err != nil || len(got) != 1 || len(got[0].Changes) != 500 {
		t.Fatalf("got %d transactions, err %v; want the one 500-change transaction", len(got), err)
	}
	waitFor(t, "the transaction behind it", func() bool { return s.BufferedChanges() == 3 })
}

// Confirm does no I/O of its own (fakeConn fails the test if two goroutines
// are ever on the connection); the receiver sends the position promptly, both
// when it is blocked in a read and when it is paused on a full buffer.
func TestConfirmIsSentByTheReceiver(t *testing.T) {
	s, f := startFake(t, 5, time.Hour) // no timer-driven updates at all
	ctx := context.Background()

	waitFor(t, "the receiver to block in a read", func() bool { return f.inIO.Load() == 1 })
	if err := s.Confirm(ctx, "0/100"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "status while reading", func() bool { return f.lastStatus() == 0x100 })

	stop := feed(f, append([]pgproto3.BackendMessage{xlog(1, relationMsg())}, txn(1000, 0, 5)...))
	defer stop()
	waitFor(t, "the buffer to fill", func() bool { return s.BufferedChanges() == 5 })
	if err := s.Confirm(ctx, "0/200"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "status while paused", func() bool { return f.lastStatus() == 0x200 })

	// A position that does not advance is not re-sent, and never moves back.
	n := f.statusCount()
	if err := s.Confirm(ctx, "0/150"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if f.statusCount() != n || f.lastStatus() != 0x200 {
		t.Fatalf("stale confirm produced a status update (last %s)", f.lastStatus())
	}
}

// Many Confirms racing the receiver's reads and timer: run under -race.
func TestConfirmConcurrentWithReceive(t *testing.T) {
	s, f := startFake(t, 50, time.Millisecond)
	msgs := []pgproto3.BackendMessage{xlog(1, relationMsg())}
	for i := 0; i < 300; i++ {
		msgs = append(msgs, txn(uint64(1000+i*100), i, 1)...)
	}
	stop := feed(f, msgs)
	defer stop()

	ctx := context.Background()
	seen := 0
	var last string
	waitFor(t, "all transactions confirmed", func() bool {
		got, err := s.Transactions(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, tx := range got {
			seen++
			last = tx.NextLSN
			if err := s.Confirm(ctx, last); err != nil {
				t.Fatal(err)
			}
		}
		return seen == 300
	})
	want, _ := pglogrepl.ParseLSN(last)
	waitFor(t, "the last position to be sent", func() bool { return f.lastStatus() == want })
}
