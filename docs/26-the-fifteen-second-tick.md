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
| Worst delta merge | 6,073 ms | 3,631 ms, bounded by configuration |
| The temporal halt on bootstrap | never ran — the snapshot bypassed it | inside the writer, where every path goes |
| narrow's bootstrap | 16.5 MB/s | 16.5 MB/s — the per-row map was not it |

---

## Two follow-ons: one that worked, one that did not

### Bounding the merge: does what it says, at an unresolved cost

With apply capped, the largest remaining stall was the delta merge —
`merge_during_compaction_ms=5842` on wide. It was bounded the same way: fold at
most `QS_MAX_MERGE_FILES` delta files per pass instead of every mergeable one.

On the stall it targets, it works. Wide's worst merge went **6,073 ms → 3,631
ms**, about the 8-of-16 the bound implies.

**The throughput effect is inside run-to-run noise and this run cannot resolve
it.** wide drained 48,604/s bounded against 58,198/s unbounded, which looks like
a 16% cost — but wide has ranged 48k–58k across six runs of this session with
the merge untouched. narrow went the other way, 99,602/s bounded against
85,339/s unbounded, and has ranged 80k–101k. Single runs cannot separate a real
10% effect from that. Write amplification per change *is* visible and does move
the right way for the theory: 130 B/change bounded against 116 unbounded.

Taking the oldest files rather than the newest forced a fix to something the old
code got away with. A merge used to fold the newest run and append its output on
the **end** of the manifest, which was only correct because the run was always
newest. A bounded merge folds a run from the middle, and appending there would
move older content after newer — inverting "last occurrence wins" for any key
live in two files. The merged file now takes the position of the run it
replaces. Taking the newest instead would have avoided that and been worse: the
oldest files would never fold, leaving a prefix only a full base rewrite could
clear.

### Removing the snapshot's per-row map: no effect

[docs/25](25-where-the-bootstrap-goes.md) put narrow's bootstrap at 16.5 MB/s on
a saturated core and named the per-row work: the snapshot scanned each row into
a slice, copied it into a `map[string]any`, and the writer looked the values
back out by name. One allocation and N inserts on the way in, N lookups on the
way out, to move a slice to a slice.

That is now gone — the writer buffers rows in column order and the snapshot
hands its scan straight over. The result:

```
before   2.2s -> 185074 rows/s -> 16.5 MB/s
after    2.2s -> 185544 rows/s -> 16.5 MB/s
```

**Nothing.** CPU during the bootstrap went from 1.88s to 1.76s, which is inside
noise at this sample size, and the wall time did not move at all. The map was
real per-row work and it was not enough of the per-row work to matter; what is
left — the scan boxing every value into an `any`, the Arrow builder appends, the
encoding — dominates it.

The change stays, because it is simpler and allocates less, but the honest
record is that the optimisation this was undertaken for did not land. **The
reason it was worth doing anyway is what it found on the way**, which was a
correctness bug rather than a slow path: `checkTemporal` was called from
`writeParquetCols`, one of two ways into the writer, and the snapshot is the
other. Bootstrap never checked. A table carrying a date from 44 BC would have
loaded that column silently NULL — the exact failure [docs/23](23-storing-an-instant.md)
added the halt to prevent, on the one path where such a date is actually likely,
because bootstrap is where data written before this mirror existed arrives.

docs/23 already recorded one version of this: *"a guard that silently has
nothing to guard looks exactly like a guard that passed."* That was a guard
called with an empty column list. This one was not called at all. The fix is
structural rather than a third call site — the check now lives inside the
writer, where every path has to go through it.

---

## What to do next

1. **Actually move the merge off the tick,** rather than bounding it. Bounding
   trades stall for amplification and leaves the work on the apply goroutine;
   the rewrite is already asynchronous and this is the same shape of problem.
2. **Measure latency during the drain, not after it.** Everything above exists
   because the sample window excluded the interesting phase. The harness should
   sample while the workload is running.
3. **`head_lsn` is a second round trip per tick.** Cheaper than the connection
   was, and still a query per tick to ask a question that changes on its own
   schedule.
4. **Find what bootstrap's per-row cost actually is.** Removing the map was a
   reasoned guess and it bought nothing; the remaining candidates are the scan
   boxing every value into an `any` and the Arrow builder appends, and neither
   has been measured on its own.
5. **Repeat the throughput runs.** Several conclusions in this document rest on
   single runs of a benchmark whose drain rate varies by 20% between identical
   builds. That is enough to see a 6-second stall become 3.6; it is not enough
   to see a 10% throughput change.
