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
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
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
//
// The default is derived from the memory limit (memlimit.go): 250,000 from
// about 3 GiB up, about 22,000 at 256Mi, where the old default was OOM-killed
// while draining a backlog.
var maxTickChanges = int(envInt("QS_MAX_TICK_CHANGES", defaultTickChanges(memSizing.Bytes)))

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
//
// What it no longer does is overrun in MEMORY. A transaction past the spool
// threshold is on disk (changestream/spool.go), counted here by Len(), and
// Table.Apply lands it in chunks of mirror.ApplyChunkChanges while keeping it
// invisible until the last one is written. The tick is still long; the heap
// is not.
func capBatch(txns []changestream.Transaction) []changestream.Transaction {
	if maxTickChanges <= 0 || len(txns) <= 1 {
		return txns
	}
	n := 0
	for i, t := range txns {
		n += t.Len()
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

// maxPendingChanges is how much may wait in pump's own queue before the loop
// stops taking from the stream's buffer. It is half of the memory bound; the
// other half is maxStreamBufferedChanges, below.
//
// Four ticks' worth. Enough that a tick never waits on the network for work it
// could already have, and small enough that the queue is a buffer rather than a
// second copy of the backlog: at the default cap that is a million row-changes,
// which is bounded, whereas "whatever the stream has" is not.
//
// The number that matters is not this one but the ratio to maxTickChanges.
// Reading without a bound while applying with one is what put 2.4 GB of
// resident memory behind a 139 MB mirror.
var maxPendingChanges = int(envInt("QS_MAX_PENDING_CHANGES", 4*int64(maxTickChanges)))

// maxStreamBufferedChanges is how much the stream itself may hold, received but
// not yet taken by pump, before it stops reading the replication socket and
// leaves the rest in PostgreSQL's WAL.
//
// maxPendingChanges alone bounded nothing: pump declining to take from the
// stream only moved the backlog into the stream's buffer, which had no cap
// (47 MB to 1.27 GB of heap in seven seconds, against a 256Mi default limit).
// The two caps are one budget. Pump takes the whole stream buffer whenever its
// queue is under maxPendingChanges, so the worst case in memory is
//
//	maxPendingChanges + 2 x maxStreamBufferedChanges (+ one transaction)
//
// where "one transaction" used to mean all of it and now means what init()
// below says: a large one waits on disk and counts here by its length only.
//
// — the queue just under its cap, one full buffer taken on top, and the buffer
// refilled behind it. A quarter of the queue (one tick's worth at the
// defaults) keeps that at 1.5x maxPendingChanges without leaving a tick short
// of work.
var maxStreamBufferedChanges = int(envInt("QS_MAX_STREAM_BUFFERED_CHANGES", int64(maxPendingChanges)/4))

// The three knobs that bound what ONE transaction costs in memory, whatever its
// size (the two above bound many small ones):
//
//	QS_SPOOL_THRESHOLD_CHANGES  a transaction that arrives whole is held in
//	QS_SPOOL_THRESHOLD_BYTES    memory until it crosses either, then spooled to
//	                            <mirror>/.spool; one the server streams in
//	                            progress is spooled from its first change
//	QS_APPLY_CHUNK_CHANGES      how many changes of a larger transaction Apply
//	                            holds at once
//	QS_STREAM_IN_PROGRESS=off   proto_version 1, for PostgreSQL 13 and older
//
// The worst case in memory becomes
//
//	(maxPendingChanges + 2 x maxStreamBufferedChanges) small changes queued
//	+ min(QS_SPOOL_THRESHOLD_CHANGES rows, QS_SPOOL_THRESHOLD_BYTES) assembling
//	+ QS_APPLY_CHUNK_CHANGES rows being applied
//	+ 1 bit per row of each file the transaction supersedes rows in
//
// with no term in the size of the largest transaction.
func init() {
	changestream.DefaultMaxBufferedChanges = maxStreamBufferedChanges
	changestream.SpoolThresholdChanges = int(envInt("QS_SPOOL_THRESHOLD_CHANGES",
		int64(changestream.SpoolThresholdChanges)))
	changestream.SpoolThresholdBytes = int(envInt("QS_SPOOL_THRESHOLD_BYTES",
		int64(changestream.SpoolThresholdBytes)))
	mirror.ApplyChunkChanges = int(envInt("QS_APPLY_CHUNK_CHANGES",
		int64(mirror.ApplyChunkChanges)))
	if v := os.Getenv("QS_STREAM_IN_PROGRESS"); v == "off" || v == "0" || v == "false" {
		changestream.DefaultInProgress = false
	}
}
