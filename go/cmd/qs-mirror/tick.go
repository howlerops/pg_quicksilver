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
	"context"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
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

// MaxTickChanges bounds how many row-changes one tick may apply.
//
// There is a real trade here and the first version of this got it wrong by
// assuming there was not. Smaller batches mean more delta files, more per-batch
// overhead and more merging, so bounding the tick is not free. Measured, drain
// rate against the worst apply tick:
//
//	cap          narrow                    wide
//	50,000        80,021/s    318 ms        52,435/s     508 ms
//	250,000      100,931/s   6303 ms        57,490/s    5993 ms
//	1,000,000     94,477/s   9258 ms        58,367/s   12023 ms
//	unbounded     95,045/s  18650 ms        57,372/s   13069 ms
//
// 250,000 is the knee: it costs no measurable throughput against unbounded --
// it was faster on both shapes, within run-to-run noise -- and it cuts the
// worst stall by about three times. Going to 50,000 bounds the stall to under
// half a second and costs 15% of the drain rate, which is the right choice only
// for a deployment whose freshness SLO is tight enough to care.
//
// The stall matters because readiness gates on freshness. An eighteen-second
// tick against the default 30s SLO is a third of the budget spent in one place
// where nothing else in the loop can run.
//
// Zero disables the cap and restores the old unbounded behaviour for A/B.
var maxTickChanges = int(envInt("QS_MAX_TICK_CHANGES", 250_000))

func envInt(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

// capBatch returns the longest prefix of txns whose total row-changes fit the
// cap, never splitting a transaction.
//
// A transaction is indivisible here for the reason the whole apply path is
// built around: half a transaction in the mirror is a state the source never
// had. So a single transaction larger than the cap is applied whole and
// overruns, which is correct — the alternative is not applying it at all.
func capBatch(txns []changestream.Transaction) []changestream.Transaction {
	if maxTickChanges <= 0 || len(txns) <= 1 {
		return txns
	}
	n := 0
	for i, t := range txns {
		n += len(t.Changes)
		if n >= maxTickChanges {
			return txns[:i+1]
		}
	}
	return txns
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

// currentLSN is where a newly added table's snapshot is anchored. It is the
// write position rather than the slot's, because the snapshot sees the table as
// it is NOW; the stream then replays from the slot's older position and the
// per-table floor in Table.Apply discards everything before this point.
func currentLSN(ctx context.Context, conn *pgx.Conn) (string, error) {
	var lsn string
	err := conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn)
	return lsn, err
}
