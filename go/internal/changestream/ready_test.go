package changestream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// The consumer used to learn about a commit by polling every 200 ms. Ready is
// how it is told: once per "there is something new", never per keepalive.
func TestReadySignalsCommitIdleAndError(t *testing.T) {
	s, f := startFake(t, 0, time.Hour)
	ctx := context.Background()
	send := func(msgs ...pgproto3.BackendMessage) {
		t.Helper()
		before := f.received.Load()
		for _, m := range msgs {
			f.msgs <- m
		}
		waitFor(t, "the receiver to process the messages", func() bool {
			return f.received.Load() == before+int64(len(msgs)) && f.inIO.Load() == 1
		})
	}
	signalled := func() bool {
		select {
		case <-s.Ready():
			return true
		default:
			return false
		}
	}

	if signalled() {
		t.Fatal("signalled before anything was received")
	}
	send(keepalive(0x1000))
	if !signalled() {
		t.Fatal("the first idle position did not signal")
	}
	send(keepalive(0x2000), keepalive(0x2100))
	if signalled() {
		t.Fatal("a keepalive that only moves the idle position signalled")
	}

	// A transaction: nothing while it is assembled, one signal at its commit.
	all := txn(0x3000, 0, 2)
	send(xlog(1, relationMsg()), all[0], all[1], all[2])
	if signalled() {
		t.Fatal("signalled before the commit was seen")
	}
	send(all[3])
	if !signalled() {
		t.Fatal("a buffered commit did not signal")
	}
	// Two more before the consumer looks: one signal stands for both.
	send(txn(0x4000, 2, 1)...)
	send(txn(0x5000, 3, 1)...)
	if !signalled() || signalled() {
		t.Fatal("want exactly one pending signal for two buffered commits")
	}
	got, err := s.Transactions(ctx)
	if err != nil || len(got) != 3 {
		t.Fatalf("Transactions = %d, %v; want all 3", len(got), err)
	}
	// Idle again after data: that transition signals, once.
	send(keepalive(0x6000))
	if !signalled() {
		t.Fatal("the idle position returning after data did not signal")
	}

	// A consumer blocked on Ready is released by a commit, promptly.
	woke := make(chan time.Duration, 1)
	go func() {
		t0 := time.Now()
		<-s.Ready()
		woke <- time.Since(t0)
	}()
	send(txn(0x7000, 4, 1)...)
	select {
	case d := <-woke:
		if d > time.Second {
			t.Fatalf("woken after %s", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a blocked consumer was not woken by a commit")
	}

	// And a failed stream wakes it too, so the error is not found a tick late.
	s.setErr(errors.New("boom"))
	if !signalled() {
		t.Fatal("a stream error did not signal")
	}
}
