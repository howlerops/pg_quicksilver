package main

// What wakes the apply loop.
//
// It used to be a 200 ms ticker and nothing else, so a commit waited on
// average 100 ms before the sidecar looked at it, and the routing extension's
// strict default (route only when the mirror has reached this node's replay
// position, waiting at most 20 ms) routed nothing while a mirrored table was
// being written (docs/40). Now the stream's receiver says when a committed
// transaction is buffered (changestream.Streaming.Ready) and the loop runs
// then. The ticker is still there, as the fallback and for the work that has
// no event: maintenance, the role check, the slot guard, the idle confirm.
//
// What did not change is the order inside a tick: apply, which makes the data
// and the manifest durable, and only then confirm. Waking sooner moves when a
// tick starts, not what it does.

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// wakeCoalesce is how long the loop waits, after it is told a transaction is
// buffered, before it takes what is there. It is the whole of the deliberate
// delay on the path and it is what keeps a burst efficient: every tick that
// applies something writes a delta file and fsyncs a manifest per table, so
// commits arriving a millisecond apart are better taken together. 0 is allowed
// and means "take each as it comes". Batching beyond this is what it always
// was: whatever arrives while a tick is applying is taken whole by the next.
var wakeCoalesce = envDuration0("QS_WAKE_COALESCE", 2*time.Millisecond)

// pumpFallbackInterval is the ticker: how often the loop runs when nothing
// woke it.
var pumpFallbackInterval = envDuration("QS_PUMP_FALLBACK_INTERVAL", 200*time.Millisecond)

// pumpEvents turns the event-driven wake-up off (QS_PUMP_EVENTS=off), leaving
// the polling loop it replaced. For measuring the difference, and as a way
// back if the wake-up is ever suspected of something.
var pumpEvents = func() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("QS_PUMP_EVENTS"))) {
	case "off", "0", "false", "no":
		return false
	}
	return true
}()

// primaryPoll is how often a sidecar standing down on the primary asks whether
// it still is one.
var primaryPoll = envDuration("QS_PRIMARY_POLL_INTERVAL", 10*time.Second)

// envDuration0 is envDuration for a setting where zero is a value.
func envDuration0(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return def
	}
	return d
}

type wakeReason int

const (
	wakeBacklog  wakeReason = iota // the previous tick left transactions unapplied
	wakeStream                     // the receiver signalled
	wakeFallback                   // the ticker
)

// waitForWork blocks until the apply loop has a reason to run a tick.
//
// With a backlog it does not block at all. Otherwise it waits for the stream's
// signal or the fallback ticker; after a signal it waits `coalesce` more, so
// that commits arriving together are applied together. A nil `ready` never
// signals, which is the polling loop.
func waitForWork(
	ctx context.Context, ready <-chan struct{}, fallback <-chan time.Time,
	backlog bool, coalesce time.Duration,
) (wakeReason, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if backlog {
		return wakeBacklog, nil
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-fallback:
		return wakeFallback, nil
	case <-ready:
	}
	if coalesce > 0 {
		t := time.NewTimer(coalesce)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-t.C:
		}
	}
	return wakeStream, nil
}

// applyPacer bounds how often the stream may START an apply tick when commits
// keep coming, without delaying one that arrives alone.
//
// Why it has to exist. Every tick that applies something writes a delta file
// per table, and the mirror merges a table's deltas, all of them, each time
// sixteen have piled up: work in proportion to the rows in the deltas, on
// this goroutine. The 200 ms ticker capped that at five files a second without
// anyone having decided it should. Woken per commit, the first build of this
// loop made sixty a second under a steady 1,000 commits/s, and on a 2M-row
// table carrying 200k delta rows it then spent 2.4 of every 3 seconds merging:
// median commit-to-visible went from 22 ms back up to 124 ms, with a 2 s tail.
//
// So: a token bucket. A tick woken by the stream spends a token; tokens come
// back at `rate` a second up to `burst`. A commit that finds a token is applied
// at once. Commits that keep arriving faster than `rate` find none and wait for
// the next, up to 1/rate, during which everything else that commits joins the
// same tick. That is the batching the ticker used to provide, at a cadence
// that is now a setting, and only when there is a burst to batch.
//
// rate <= 0 turns pacing off.
type applyPacer struct {
	rate, burst float64
	tokens      float64
	last        time.Time
}

func newApplyPacer(rate, burst float64, now time.Time) *applyPacer {
	if burst < 1 {
		burst = 1
	}
	return &applyPacer{rate: rate, burst: burst, tokens: burst, last: now}
}

func (p *applyPacer) refill(now time.Time) {
	if d := now.Sub(p.last); d > 0 {
		p.tokens += d.Seconds() * p.rate
		if p.tokens > p.burst {
			p.tokens = p.burst
		}
		p.last = now
	}
}

// wait is how long a stream-woken tick must hold off: zero when there is a
// token.
func (p *applyPacer) wait(now time.Time) time.Duration {
	if p == nil || p.rate <= 0 {
		return 0
	}
	p.refill(now)
	if p.tokens >= 1 {
		return 0
	}
	return time.Duration((1 - p.tokens) / p.rate * float64(time.Second))
}

// spend records a tick that applied something. The balance may go below zero
// (a fallback or backlog tick is not held back, but it still wrote files), and
// is then owed before the stream may start another.
func (p *applyPacer) spend(now time.Time) {
	if p == nil || p.rate <= 0 {
		return
	}
	p.refill(now)
	p.tokens--
	if p.tokens < -1 {
		p.tokens = -1
	}
}

// applyMaxRate is the pacer's sustained rate in apply ticks a second, and
// applyBurst how many may run back to back after a quiet spell.
var (
	applyMaxRate = envFloat0("QS_APPLY_MAX_RATE", 10)
	applyBurst   = envFloat0("QS_APPLY_BURST", 5)
)

func envFloat0(key string, def float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < 0 {
		return def
	}
	return f
}

// wakeCounts is how often each reason started a tick, for /metrics: a loop
// that is only ever woken by the fallback is not being told about commits.
type wakeCounts struct {
	n     [3]atomic.Int64
	paced atomic.Int64 // stream wake-ups the pacer held back
}

var pumpWakes wakeCounts

func (w *wakeCounts) note(r wakeReason) { w.n[r].Add(1) }

func (w *wakeCounts) Metrics() string {
	var b strings.Builder
	b.WriteString("# HELP quicksilver_pump_wakeups_total Apply-loop ticks, by what started them.\n")
	b.WriteString("# TYPE quicksilver_pump_wakeups_total counter\n")
	for i, name := range []string{"backlog", "stream", "fallback"} {
		fmt.Fprintf(&b, "quicksilver_pump_wakeups_total{reason=%q} %d\n", name, w.n[i].Load())
	}
	b.WriteString("# HELP quicksilver_pump_paced_total Stream wake-ups held back by the apply pacer (QS_APPLY_MAX_RATE).\n")
	b.WriteString("# TYPE quicksilver_pump_paced_total counter\n")
	fmt.Fprintf(&b, "quicksilver_pump_paced_total %d\n", w.paced.Load())
	return b.String()
}
