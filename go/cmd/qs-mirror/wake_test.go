package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestWaitForWork(t *testing.T) {
	ctx := context.Background()
	never := make(chan time.Time)

	// A backlog does not wait for anything, and does not consume the signal.
	ready := make(chan struct{}, 1)
	ready <- struct{}{}
	t0 := time.Now()
	why, err := waitForWork(ctx, ready, never, true, time.Hour)
	if err != nil || why != wakeBacklog || time.Since(t0) > time.Second {
		t.Fatalf("backlog: %v %v after %s", why, err, time.Since(t0))
	}
	if len(ready) != 1 {
		t.Fatal("a backlog tick consumed the stream's signal")
	}

	// A signal that is already there: woken after the coalescing delay, not
	// before it and not at the fallback.
	const coalesce = 30 * time.Millisecond
	t0 = time.Now()
	why, err = waitForWork(ctx, ready, never, false, coalesce)
	if d := time.Since(t0); err != nil || why != wakeStream || d < coalesce || d > 2*time.Second {
		t.Fatalf("signal: %v %v after %s, want stream after >= %s", why, err, d, coalesce)
	}

	// A signal that arrives while waiting wakes it then, with no ticker at all.
	// This is the path the 200 ms poll used to stand in for.
	go func() {
		time.Sleep(20 * time.Millisecond)
		ready <- struct{}{}
	}()
	t0 = time.Now()
	why, err = waitForWork(ctx, ready, never, false, 0)
	if d := time.Since(t0); err != nil || why != wakeStream || d < 15*time.Millisecond || d > 2*time.Second {
		t.Fatalf("late signal: %v %v after %s", why, err, d)
	}

	// Nothing signalled: the fallback.
	tick := make(chan time.Time, 1)
	tick <- time.Now()
	if why, err = waitForWork(ctx, ready, tick, false, time.Hour); err != nil || why != wakeFallback {
		t.Fatalf("fallback: %v %v", why, err)
	}

	// A nil signal channel is the polling loop: only the ticker wakes it.
	tick <- time.Now()
	if why, err = waitForWork(ctx, nil, tick, false, 0); err != nil || why != wakeFallback {
		t.Fatalf("polling: %v %v", why, err)
	}

	// Cancellation ends every wait: before it, during it, and during the
	// coalescing delay.
	dead, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = waitForWork(dead, ready, never, true, 0); err == nil {
		t.Fatal("a cancelled context with a backlog returned no error")
	}
	if _, err = waitForWork(dead, ready, never, false, 0); err == nil {
		t.Fatal("a cancelled context returned no error")
	}
	mid, cancel2 := context.WithCancel(ctx)
	ready <- struct{}{}
	go func() { time.Sleep(10 * time.Millisecond); cancel2() }()
	t0 = time.Now()
	if _, err = waitForWork(mid, ready, never, false, time.Hour); err == nil || time.Since(t0) > 2*time.Second {
		t.Fatalf("cancel during the coalescing delay: err %v after %s", err, time.Since(t0))
	}
}

func TestWakeCountsMetrics(t *testing.T) {
	var w wakeCounts
	w.note(wakeStream)
	w.note(wakeStream)
	w.note(wakeFallback)
	m := w.Metrics()
	for _, want := range []string{
		`quicksilver_pump_wakeups_total{reason="stream"} 2`,
		`quicksilver_pump_wakeups_total{reason="fallback"} 1`,
		`quicksilver_pump_wakeups_total{reason="backlog"} 0`,
	} {
		if !strings.Contains(m, want) {
			t.Fatalf("metrics lack %q:\n%s", want, m)
		}
	}
}

func TestEnvDuration0(t *testing.T) {
	t.Setenv("QS_TEST_D0", "0")
	if d := envDuration0("QS_TEST_D0", time.Second); d != 0 {
		t.Fatalf("0 must be a value, got %s", d)
	}
	t.Setenv("QS_TEST_D0", "-1s")
	if d := envDuration0("QS_TEST_D0", time.Second); d != time.Second {
		t.Fatalf("negative must fall back, got %s", d)
	}
	t.Setenv("QS_TEST_D0", "5ms")
	if d := envDuration0("QS_TEST_D0", time.Second); d != 5*time.Millisecond {
		t.Fatalf("got %s", d)
	}
}

func TestApplyPacer(t *testing.T) {
	t0 := time.Unix(1000, 0)
	p := newApplyPacer(10, 3, t0) // 10 ticks/s sustained, 3 back to back

	// A commit that arrives alone never waits.
	if d := p.wait(t0); d != 0 {
		t.Fatalf("first tick waits %s", d)
	}
	// The burst: three at once, then the fourth owes a token's worth.
	now := t0
	for i := 0; i < 3; i++ {
		if d := p.wait(now); d != 0 {
			t.Fatalf("tick %d of the burst waits %s", i+1, d)
		}
		p.spend(now)
	}
	if d := p.wait(now); d < 99*time.Millisecond || d > 101*time.Millisecond {
		t.Fatalf("after the burst: wait %s, want ~100ms (1/rate)", d)
	}
	// Sustained: one tick per 100 ms, however fast commits come.
	ticks := 0
	for end := now.Add(10 * time.Second); now.Before(end); {
		if d := p.wait(now); d > 0 {
			now = now.Add(d)
			continue
		}
		p.spend(now)
		ticks++
		now = now.Add(time.Millisecond) // the next commit is already there
	}
	if ticks < 95 || ticks > 105 {
		t.Fatalf("%d ticks in 10 s under constant pressure, want ~100", ticks)
	}
	// Quiet for a while: the burst is back, and no more than the burst.
	now = now.Add(time.Minute)
	free := 0
	for p.wait(now) == 0 && free < 100 {
		p.spend(now)
		free++
	}
	if free != 3 {
		t.Fatalf("%d free ticks after a quiet minute, want the burst of 3", free)
	}
	// Commits slower than the rate are never held back.
	q := newApplyPacer(10, 1, t0)
	now = t0
	for i := 0; i < 50; i++ {
		if d := q.wait(now); d != 0 {
			t.Fatalf("a commit every 150 ms waited %s at tick %d", d, i)
		}
		q.spend(now)
		now = now.Add(150 * time.Millisecond)
	}
	// Ticks the pacer did not start (fallback, backlog) are still charged,
	// but the debt is bounded.
	r := newApplyPacer(10, 1, t0)
	for i := 0; i < 50; i++ {
		r.spend(t0)
	}
	if d := r.wait(t0); d > 200*time.Millisecond {
		t.Fatalf("debt after 50 unpaced ticks is %s, want at most 200ms", d)
	}
	// Off.
	off := newApplyPacer(0, 5, t0)
	for i := 0; i < 100; i++ {
		off.spend(t0)
	}
	if d := off.wait(t0); d != 0 {
		t.Fatalf("rate 0 must not pace, waited %s", d)
	}
	var none *applyPacer
	none.spend(t0)
	if none.wait(t0) != 0 {
		t.Fatal("a nil pacer must not pace")
	}
}
