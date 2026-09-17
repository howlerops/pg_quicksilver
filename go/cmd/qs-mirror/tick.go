package main

// What the apply loop spent a tick on.
//
// The loop runs on a 200 ms ticker, so every commit-to-visible p50 this project
// has published is that interval plus a few milliseconds of work (docs/25). The
// interesting number was never the p50; it is the tick that took two seconds,
// because a tick that overruns its interval is the whole of the latency tail.
//
// Attributing one used to mean reading the log and inferring. The wide shape
// was inferable that way -- it logs "merged deltas ... seconds=2.281" right
// next to a 2830 ms sample -- but the narrow shape logged nothing at all and
// its tail stayed unexplained through two documents. A tick has six places it
// can spend time and only two of them said anything.
//
// So it is measured instead. Every phase is timed, and a tick that overruns
// logs what it was doing, in order, worst phase first.

import (
	"log/slog"
	"os"
	"sort"
	"strconv"
	"time"
)

// slowTick is the threshold for saying something. 200 ms is the interval
// itself, so anything past that is a tick that made the next one late.
var slowTick = envDuration("QS_SLOW_TICK", 200*time.Millisecond)

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	if ms, err := strconv.Atoi(v); err == nil {
		return time.Duration(ms) * time.Millisecond
	}
	return def
}

type phase struct {
	name string
	dur  time.Duration
}

// tickTimer accumulates one tick's phases. It is not safe for concurrent use
// and does not need to be: a tick happens on one goroutine, which is the same
// property that lets the compaction swap decide liveness without locking.
type tickTimer struct {
	start  time.Time
	mark   time.Time
	phases []phase
}

func newTickTimer() *tickTimer {
	now := time.Now()
	return &tickTimer{start: now, mark: now}
}

// done closes off a phase and starts the next. Phases with nothing in them are
// dropped rather than logged as 0ms, because a slow-tick line is read by a
// person and five zeroes are noise.
func (t *tickTimer) done(name string) {
	now := time.Now()
	d := now.Sub(t.mark)
	t.mark = now
	if d >= time.Millisecond {
		t.phases = append(t.phases, phase{name, d})
	}
}

func (t *tickTimer) total() time.Duration { return time.Since(t.start) }

// report logs a tick that overran, worst phase first. Under the threshold it is
// silent -- the common case is a tick that did nothing and must not produce a
// line every 200 ms.
func (t *tickTimer) report(log *slog.Logger) {
	total := t.total()
	if total < slowTick {
		return
	}
	sorted := append([]phase(nil), t.phases...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].dur > sorted[j].dur })
	args := []any{"total_ms", total.Milliseconds()}
	for _, p := range sorted {
		args = append(args, p.name+"_ms", p.dur.Milliseconds())
	}
	log.Warn("slow tick: the apply loop overran its interval, "+
		"which is what a latency tail is made of", args...)
}
