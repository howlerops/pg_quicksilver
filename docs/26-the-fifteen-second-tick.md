# 26 — The fifteen-second tick

[docs/25](25-where-the-bootstrap-goes.md) left the narrow shape's latency tail
recorded as **unidentified**, having established only what it was not. That was
the right thing to write down and the wrong place to stop, because the reason it
could not be identified was that the apply loop said almost nothing about itself:
six places to spend time and two of them logged.

So the loop was instrumented — every phase timed, and a tick that overruns its
200 ms interval logs what it was doing, worst phase first. It answered on the
first run, and the answer was not the hypothesis. It was not the compaction
swap, which is what docs/25 guessed at. It was not the delta merge, which is
what docs/25 had confirmed for the wide shape.

**It was `apply`, and it was an order of magnitude worse than anything this
project had reported.**

```
12:16:38  total_ms=849    apply_ms=826
12:16:41  total_ms=2452   apply_ms=2413
12:16:47  total_ms=5706   apply_ms=5229   finish_compaction_ms=442
12:16:57  total_ms=9332   apply_ms=9107   finish_compaction_ms=212
12:17:12  total_ms=15528  apply_ms=14993  finish_compaction_ms=529
```

Five consecutive ticks, each bigger than the last. A tick applies whatever the
stream handed over, and during catch-up the backlog outruns the drain, so every
tick swallows more than the one before it. **Fifteen seconds in which nothing
else in that loop runs** — no confirm, no freshness update, no compaction swap,
no promotion check.

---

## Why nobody had seen it

The latency samples are taken **after** the drain. Every commit-to-visible
number this project has published measures a mirror that has already caught up
and is sitting idle behind a trickle of markers. The phase where the mirror
behaves worst has never been in the sample.

That also explains why docs/25's reading was incomplete rather than wrong. Once
the backlog is gone, the same log shows exactly what docs/25 described:

```
12:17:22  total_ms=2904  merge_during_compaction_ms=2883  apply_ms=15
12:17:28  total_ms=1997  merge_deltas_ms=1779  finish_compaction_ms=204
12:17:33  total_ms=1602  merge_deltas_ms=1594
```

Both findings are true. They are about different windows, and only one of them
was ever measured.

---

## The fix, and the cost it actually has

A tick already has one barrier: a DDL transaction stops the batch so the catalog
can be re-read before anything after it is applied, and whatever is cut off
waits in `pending` for the next tick. A size barrier is the same machinery —
apply at most N row-changes, hold the rest.

A transaction is never split. Half a transaction in the mirror is a state the
source never had, so a single transaction larger than the cap is applied whole
and overruns. The alternative is never applying it.

**The first version of this claimed it cost no throughput. That was wrong**, and
the A/B said so immediately: the stall was fixed and the drain rate was down
13.5%. Two separate things were going on.

### One of them was a connection per tick

`isPrimary` opened a PostgreSQL connection, asked `pg_is_in_recovery()`, and
closed it — **every tick**. Five backends a second at the 200 ms interval, and
far more during a drain, where the ticker refires in a millisecond.

It had never mattered, because one connection amortised over fifteen seconds of
apply is nothing. Bounding the tick made it the dominant per-tick cost, and the
new instrumentation named it in the same line it caused: `is_primary_ms=96`
against `apply_ms=1954`. The connection is now held for the life of the loop.
The question is asked exactly as often as before — the window in which a
promoted node could keep streaming is unchanged — and only the cost of asking is
gone.

### The other one was real

With that fixed, the gap remained. Bounding the tick genuinely costs throughput:
smaller batches mean more delta files, more per-batch overhead, and more
merging. The slow-tick count is the tell — 83 against 7 — and the top phase at a
50,000 cap is no longer apply but `merge_during_compaction`.

So it is a curve, not a win, and here it is. Drain rate against worst apply
tick:

| cap | narrow | wide |
|---|---|---|
| 50,000 | 80,021/s · **318 ms** | 52,435/s · **508 ms** |
| **250,000** | **100,931/s** · 6,303 ms | **57,490/s** · 5,993 ms |
| 1,000,000 | 94,477/s · 9,258 ms | 58,367/s · 12,023 ms |
| unbounded | 95,045/s · 18,650 ms | 57,372/s · 13,069 ms |

**250,000 is the knee**: no measurable throughput cost against unbounded — it
was faster on both shapes, inside run-to-run noise — and about a third of the
worst stall. That is the default.

50,000 bounds the stall to under half a second and costs 15% of the drain rate.
That is the right setting for a deployment whose freshness SLO is tight enough
to care, and the wrong default for one that is not. `QS_MAX_TICK_CHANGES` picks
a point on that curve; `0` restores the unbounded behaviour.

The stall matters because **readiness gates on freshness**. An eighteen-second
tick against the default 30-second SLO spends a third of the budget in one place
where nothing else can run.

---

## What this changes

| | Before | After |
|---|---|---|
| Worst apply tick | 18,650 ms, unbounded and growing | 6,303 ms, bounded by configuration |
| Attributing a tail | read the log and infer | every phase timed, worst logged |
| `isPrimary` | a new connection every tick | one, for the life of the loop |
| narrow's tail | recorded as unidentified (docs/25) | `apply` during catch-up; the merge in steady state |
| The cost of bounding it | claimed to be nothing | measured curve, 15% at the tight end |

## What to do next

1. **Move the delta merge off the tick.** With apply bounded it is the largest
   remaining stall — `merge_during_compaction_ms=5842` on wide. The rewrite is
   already asynchronous and this is not.
2. **Measure latency during the drain, not after it.** Everything above exists
   because the sample window excluded the interesting phase. The harness should
   sample while the workload is running.
3. **`head_lsn` is a second round trip per tick.** Cheaper than the connection
   was, and still a query per tick to ask a question that changes on its own
   schedule.
