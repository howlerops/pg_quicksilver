# 19 — Five table shapes, and what each optimisation actually bought

[docs/18](18-measured-performance.md) measured one table. One table is a
benchmark; five are a map. This document is the map, the changes that came out
of it, and what each change cost.

Harness: [`bench/scripts/workload_matrix.sh`](../bench/scripts/workload_matrix.sh)
and [`go/cmd/qs-matrix`](../go/cmd/qs-matrix/), driving the real `qs-mirror`
sidecar against PostgreSQL 17 with a streaming standby. 200k rows seeded per
shape, 12s of characteristic writes, 4 vCPU / 16 GB. Raw:
[`bench/results/workload_matrix.txt`](../bench/results/workload_matrix.txt).

**Every shape is verified against its source before its throughput is believed.**
That is not ceremony: the shape that motivated this whole exercise was silently
destroying data while passing every performance check.

---

## The map

All five shapes now converge and verify. The jsonb row is the one that changed:
in the first round it never caught up at all.

| shape | what it is | bootstrap | ingest | p50 | p99 | storage | CPU/change | RSS | correct |
|---|---|---|---|---|---|---|---|---|---|
| **narrow** | 4 cols, insert-only | 195k rows/s | **125k/s** | 206 ms | 4.0 s | 3.1× | 25 µs | 4 GB | ✅ |
| **wide** | 12 cols, insert-only | 98k rows/s | **68k/s** | 207 ms | 2.3 s | 3.3× | 44 µs | 5 GB | ✅ |
| **jsonb** | 6 KB doc/row, scalar UPDATEs | 17k rows/s | **162k/s** | 207 ms | 2.0 s | **3.2×** | **15 µs** | 3 GB | ✅ |
| **churn** | UPDATEs on a hot 1% | 195k rows/s | 57k/s *(source-limited)* | 207 ms | **223 ms** | **15.2×** | **9 µs** | **74 MB** | ✅ |
| **deletes** | inserts + deletes 1:1 | 195k rows/s | **242k/s** | 207 ms | **1.8 s** | 4.1× | 11 µs | 5 GB | ✅ |

Four rounds, ingest and p99:

| shape | round 1 | round 2 | round 3 | round 4 |
|---|---|---|---|---|
| narrow | 119k/s · 6.2 s | 132k/s · 9.0 s | 129k/s · 4.9 s | **125k/s · 4.0 s** |
| wide | 67k/s · 224 ms | 69k/s · 5.9 s | 73k/s · 803 ms | **68k/s · 2.3 s** |
| jsonb | never converged | 84k/s · 486 ms | 84k/s · 348 ms | **162k/s · 2.0 s** |
| churn | 59k/s · 234 ms | 59k/s · 217 ms | 59k/s · 223 ms | **57k/s · 223 ms** |
| deletes | 167k/s · 7.6 s | 240k/s · 7.5 s | 256k/s · 1.9 s | **242k/s · 1.8 s** |

Two things in that table are worth reading twice.

**jsonb storage went from 0.6× to 3.2×.** For three rounds the mirror of the
document table was *larger than the PostgreSQL table it mirrored*, because every
UPDATE wrote a complete copy of the row — including the 3.3 KB document it never
touched — into a delta. Round four stopped doing that; the mechanism is below.
Ingest on that shape roughly doubled at the same time, which is the same fact
seen from the other end: those bytes were being compressed and written on the
critical path.

**The p99 column is the least trustworthy number in this document, and should
be read as an order of magnitude.** It is the worst of 60 commit-to-visible
samples in a 12-second window, and across four runs of the *same* code the wide
shape has measured 803 ms, 2.3 s, 4.0 s and 4.8 s. Only differences of several
times over are worth anything here; the p50, the storage ratio, the CPU per
change and the ingest rate are all stable to within a few percent run to run.

### churn is the best case, by a wide margin

Updates concentrated on a hot 1% of rows cost **9 µs of CPU per change, under
100 MB of RSS, and compress ~15×**. Every other shape is 30–70× heavier on
memory. The
reason is that the mirror is a key-addressed store: re-updating the same key
replaces a row rather than adding one, so the working set — index, deltas,
everything — stays the size of the table rather than the size of the traffic.

This is worth stating plainly because it inverts the usual intuition. **A
high-update OLTP table is the cheapest thing to mirror**, not the most expensive.
What is expensive is *growth*.

### jsonb: the shape that did not converge, and why it does now

In round one, 2.9M row-changes against a table with a 6 KB document per row left
the mirror behind after **600 seconds**. It did not converge at all.

My explanation was wrong, and it is worth recording what wrong looked like. I
attributed it to unchanged-TOAST carry-forward: PostgreSQL does not resend a
large value an update did not change, so the mirror fetches the previous one
from its own Parquet, and the smallest unit Parquet can decode is a row group —
~27 MB of decode to recover a few hundred documents. Plausible, arithmetically
sound, and not what was happening.

A CPU profile put reads nowhere near the top. **45% of the sidecar was in
`writeParquet`, 19% in zstd's encoder alone.** The cost was not reading the
document back; it was re-compressing it on the way out, on every single update,
because a delta stores the whole row. Snappy for deltas took the shape from
"never converges" to **84k row-changes/s with a 486 ms p99**.

One artefact from round one is still real and still worth understanding:

```
compacted table=public.m rows=203
```

A rewrite of a 200,000-row table produced a 203-row base. That is not data loss —
by the time the rewrite finished, essentially every key had moved to a newer
delta, so the new base legitimately held almost nothing. **Under heavy churn a
background rewrite finishes stale**, which is a property of the policy, not a
bug, and it is why merging has to be allowed to proceed alongside one.

It also turned out to matter for round four. The first version of column-partial
deltas required the row a patch stands on to be in a *base* file, which sounded
conservative; on this shape `base_rows` was 200 against 200,102 rows in deltas,
so almost every update was refused and the optimisation did not happen at all.
The measurement that caught it was a single `state.json`, not a benchmark number.

---

## What each change bought

Everything here was measured before and after, on the same hardware, with the
same harness.

| change | bought | cost |
|---|---|---|
| **In-memory key→(file,pos) index**, deletion vectors on every file | drain **6.5k → 25k rows/s**, RSS **6.2 GB → 1.2 GB** | ~380 B/row of resident index; a new concurrency surface |
| **Size-aware compaction** (churn, not batch count) | removed the O(table)-per-32-batches rewrite | over-corrected: 203 delta files, analytical median fell to **2.3×** |
| **Tiered delta merging** (file count triggers a merge, not a rewrite) | 203 files → 8, analytical median **2.3× → 5.8×** | merges are still synchronous |
| **Background compaction** (goroutine + atomic swap) | wide-shape p99 **3,517 ms → 224 ms** | a real concurrency bug (below); the swap is still O(rows) on the apply loop |
| **TOAST carry-forward** | stopped silently destroying every large unchanged column | a point read per update; smaller than it looked, see round two |
| **Row-group size 64k → 8k** | drain **78k → 98k/s** on jsonb | none measurable on storage or scans at this size |
| **Bootstrap gate on readiness** | a node with no data stops reporting ready | none |
| **O(changed) compaction swap** | p99: narrow 9.0 s → 4.9 s, wide 5.9 s → 803 ms | the rewrite must be told which keys moved; DDL aborts a rewrite |
| **Streaming Parquet writes** | jsonb RSS 7 GB → 5 GB | none measurable |
| **Column-partial deltas** | jsonb drain **86k → 173k/s**, mirror **2 GB → 330 MB**, CPU **23 → 13 µs/change** | a second index (key → patch); the read path merges; applies only to tables with a TOASTed column |
| **Derived `DeltaRows`** (from per-file counts) | the compaction trigger stopped drifting | a per-file row count in `state.json` |

The row-group sweep is worth calling out as a *negative* result that saved
effort: RSS was identical at 64k, 8k and 2k rows per group. The memory was never
the point reads — it was Go runtime high-water from repeated large allocations,
which a heap profile settled in one command after two wrong guesses.

---

## Goroutines: where concurrency actually helps here

The obvious answer — "decode and apply in parallel" — is already true and was
never the bottleneck. One goroutine receives from the replication stream, another
applies; PostgreSQL's own logical decoder is single-threaded per slot and caps
around 186k rows/s regardless of what the consumer does.

The measurements point somewhere less obvious. Concurrency helped here not by
doing more at once, but by **taking blocking work off the critical path**:

- **Compaction → background goroutine.** p99 3,517 ms → 224 ms on the wide shape.
  This is the entire win so far, and it is a latency win, not a throughput one.
- **Apply itself must stay serial.** Transactions carry LSN order and the
  durable watermark is a single value; parallelising apply across batches would
  mean either applying out of order or maintaining a commit-order barrier that
  costs more than it saves. Parallelism belongs *within* a batch's I/O.
- **Still serial, and now the tail:** the compaction *swap* walks every row to
  decide liveness, on the apply goroutine. That is why the narrow and deletes
  shapes still show 6–8 s p99 while wide shows 224 ms — those shapes had
  millions of rows to walk. Fixing it means making the swap O(changed) instead
  of O(base).

### The concurrency bug, because it is the instructive part

Background compaction reads a snapshot of files while the apply loop keeps
writing. The apply loop supersedes a row in two steps — mark the old position
dead, then write the new row to a delta — and a compactor reading the old file
*between those two steps* holds a stale copy as well as the fresh one.

The first implementation resolved duplicates by taking the first occurrence,
which adopted the stale row and tombstoned the live one. End-to-end it showed up
as a 2.8M-row mirror short by **exactly one row** — small enough to look like
rounding. A deterministic unit test
([`compact_async_test.go`](../go/internal/mirror/compact_async_test.go)) put it
at **162 rows reverted to pre-update values** out of 500, and named the
mechanism. Files are read base-first then deltas in sequence order, so the last
occurrence is the newest; taking it fixes the class.

That test now runs the same interleaving with partial rows (the jsonb pattern)
and passes 15× under `-race`.

---

## Four harness bugs that impersonated product bugs

Recorded because the ratio is the point: in this round, **more measurement bugs
than product bugs**, and each one initially read as a defect in the system.

1. **The jsonb payload compressed.** `repeat('note ', 600)` meant PostgreSQL kept
   documents inline — 8 KB of TOAST across the whole table. It measured a jsonb
   column and tested nothing about TOAST.
2. **The payload was identical per row.** An uncorrelated subquery is evaluated
   once, so Parquet dictionary-encoded 200k documents into 4 MB and reported a
   162× storage win.
3. **The verifier was OOM-killed** on a 5.7M-row mirror, and the harness reported
   the dead process as `DIVERGED`. Both sides now stream.
4. **Two matrix runs raced each other** over one database, one table and one
   output file, producing a 68,234-row "divergence" that was entirely the second
   run recreating the table the first was verifying. The harness now takes a lock.

And a fifth, in the settle logic: waiting for the mirror to catch up requires
*something* to keep committing, and the trickle that makes the watermark
reachable commits one more row after the wait returns — a divergence of exactly
one row, every time. Idempotent markers fix it.

---

## Round two: what the profile said, against what I assumed

The first round ended with four ranked guesses. A CPU profile of the jsonb shape
contradicted the top one.

I expected carry-forward *reads* to dominate — fetching an unchanged document
back out of Parquet. They did not even register. The profile put **45% of the
sidecar in `writeParquet` and 19% in zstd's encoder alone**, because every
UPDATE re-encodes the whole row, and 2.4M updates against 3.3 KB documents is
about 8 GB of unchanged document re-compressed in ten seconds.

| change | bought | cost |
|---|---|---|
| **Snappy for deltas, zstd for base** | jsonb converges: never → **84k/s**, p99 **486 ms** | deltas are bigger; jsonb mirror is now larger than its source |
| **Delta merging allowed during a rewrite** | file count stays bounded while a rewrite is in flight | the guard that keeps a merge off the rewrite's own files is defensive, not proven — removing it also passes the race test |
| **String fast paths in `appendValue`** | removed `fmt.Sprint` from the hot path | none |
| **`ErrStaleManifest` on a vanished file** | a reader can no longer report part of the table as all of it | every read path needs a reset-and-retry loop |

### The bug the matrix found this round

The jsonb shape reported a mirror of **363 rows against a source of 200,302**,
and the mirror was completely intact — reading it moments later returned all
200,302.

Compaction saves the new manifest and then deletes the old files. A reader that
loaded the manifest just before the swap opens files that are already gone, and
`ForEachLive` treated a missing file as "no rows here". So a perfectly healthy
mirror read back as 0.2% of itself, with no error anywhere.

That is a product bug, not a harness one: a serving node doing the same read
would return a fraction of the table and call it a result. A missing file is now
`ErrStaleManifest`, and readers reload the manifest and start over. The retry
API *requires* a reset callback, because forgetting it is equally silent — a
replayed attempt double-counts and invents a divergence the mirror never had.

## Round three: the swap, the writer, and a torn deletion vector

Two of the four ranked items, plus the bug they uncovered.

| change | bought | cost |
|---|---|---|
| **O(changed) compaction swap** | p99: narrow 9.0 s → **4.9 s**, wide 5.9 s → **803 ms**, deletes 7.5 s → **1.9 s** | the rewrite must be told which keys moved; DDL now aborts a rewrite rather than racing its schema |
| **Streaming Parquet writes** (one row group at a time) | jsonb RSS 7 GB → 5 GB | none measurable |
| **Atomic deletion-vector writes** | correctness — see below | none |

The swap used to walk every row of the new base to decide liveness. It now walks
only the keys the apply loop touched while the rewrite ran: the compactor builds
the new index off-loop, and installing it is a pointer assignment. That is the
difference between a tail that tracks table size and one that tracks churn.

### The bug: a deletion vector read while it was being written

With the swap made cheap, the race test started failing — deleted rows coming
back, dozens at a time. Four wrong theories later, the data named it: a key
deleted at round 6, a rewrite that swapped at round 13, and two deleted keys at
**adjacent positions** in the compacted output.

`os.WriteFile` truncates and then writes. Deletion vectors were written that
way, and background compaction reads them on another goroutine. A compactor
reading in that window got a truncated file — and `deadPositions` swallowed the
JSON error and returned an empty map, which means **"nothing in this file is
deleted"**. Every deleted row in it came back.

Both halves were silent, and both are fixed: the write is now temp-file-plus-
rename, and a deletion vector that exists but cannot be parsed is an error. The
distinction that matters is between *"this file has no deleted rows"* and
*"I could not tell"* — the second must never be spelled as the first.

Reverting either half reproduces it within a handful of runs, so both are
load-bearing rather than defensive. This is the same shape as every other bug
this project has found, one level down: an unreadable answer treated as a
negative answer.

## Round four: a delta that stores the change, not the row

The top-ranked item from round three, implemented and priced.

Until now every change was written as a **whole row**. pgoutput does not resend
a large value an UPDATE did not touch — it omits the column entirely — so the
mirror read the old value back out of itself (`carryForward`) and wrote it
again. For the jsonb shape that meant re-compressing a 3.3 KB document on every
update to a status field.

A **column-partial delta** stores the change as it arrived: only the columns the
change carried. The row it supersedes stays exactly where it is, alive, and the
read path merges the two. Three invariants keep this from becoming a chain of
patches, which is what would make reads unbounded:

- a patch stands on a **whole row**, never on another patch;
- a key has **at most one** live patch, and a second partial update may only
  replace it if it carries at least the same columns;
- anything that does not fit falls back to the previous behaviour — carry
  forward and write the row whole.

### What it bought

Same box, same workload, back to back, the flag being the only difference
(`QS_PARTIAL_DELTAS=0` writes every update as a whole row):

| jsonb, 200k rows, 12 s workload | whole rows | column-partial | |
|---|---|---|---|
| drain rate | 86,432/s | **173,404/s** | 2.0× |
| time to drain after the workload | 21.9 s | **4.6 s** | 4.8× |
| mirror size | 2 GB (0.6×) | **330 MB (3.2×)** | 6× smaller |
| sidecar CPU | 68.8 s (23 µs/change) | **37.9 s (13 µs/change)** | 1.8× |
| peak RSS | 5 GB | **3 GB** | |
| correctness | MATCH | MATCH | |

The end state of the two runs says the same thing more plainly than the rates
do. Writing whole rows, the mirror finished with `base_rows: 200` and
`delta_rows: 399,882` — compaction never caught up, because every rewrite was
moving documents. Writing patches, it finished with `base_rows: 200,046` and
`delta_rows: 79`: fully compacted, with time to spare.

### The other four shapes did not move, and that is the result

narrow, wide, churn and deletes are all within run-to-run noise of the whole-row
baseline, and a poll of `state.json` every 500 ms through a full matrix run says
exactly why:

```
  jsonb     max partial files 15 (max delta files 21)
  narrow    max partial files 0
  wide      max partial files 0
  churn     max partial files 0
  deletes   max partial files 0
```

Not one partial delta was written outside the jsonb shape. **The only reason a
column is ever missing from a change is that PostgreSQL stored it out of line
and the UPDATE did not touch it.** Everything else — every narrow column, every
small text field, every timestamp — is sent on every update whether it changed
or not. So this optimisation is precisely a TOAST optimisation, it applies to
exactly the tables that have a large column, and on those tables it is worth
roughly a factor of two in throughput and six in space.

That is a useful thing to know in both directions. It also means the obvious
extension — having the mirror *itself* decide to drop a large value it can see
is unchanged, rather than waiting for pgoutput to omit it — would widen the
benefit to `REPLICA IDENTITY FULL` tables and to values just under the TOAST
threshold. It needs a per-row hash of the large columns to avoid the read it is
trying to avoid, and it is not implemented.

### The bookkeeping bug found on the way

`DeltaRows` drives the compaction trigger, and it was a running total: merges
set it to what they wrote, and a compaction swap subtracted what it had folded.
Both are approximations, and they drift — a swap subtracts a count taken when
the rewrite *began*, so every merge that ran meanwhile is double-counted. The
trigger that decides whether to rewrite a multi-million-row table was therefore
a number nobody was reconciling.

It is now derived: `FileRows` records the row count of each delta file, and
`DeltaRows` is recomputed from the files that actually exist. Cheap — there are
at most a couple of dozen delta files by construction — and it cannot drift.

## What to do next, in order of measured value

1. **Compaction materialises the entire table as Go maps.** This is where the
   4–7 GB of RSS on every growing shape comes from, and it is now the most
   likely source of the remaining multi-second p99 as well: one `map[string]any`
   per row for several million rows is enormous garbage-collector pressure, and
   GC pauses stop the apply goroutine too. The writer already streams a row
   group at a time; the *reader* does not. Reading row group by row group would
   bound the rewrite's memory to one group regardless of table size.
2. **The same is true of bootstrap.** `Snapshot` holds the whole source table in
   memory before writing it, which is most of why the jsonb shape bootstraps at
   17k rows/s against 195k for narrow.
3. **Drop large values the mirror can see are unchanged**, rather than only the
   ones pgoutput omits (see round four above). Needs a per-row hash; widens the
   round-four win to tables whose large column is sent on every update.
4. **Bound carry-forward with a cache of recently-written rows.** Still lower
   priority than it looked: the profile says reads are not where the time goes,
   and column-partial deltas removed most of the carry-forward reads anyway.

Still not on the list: parallel decode. PostgreSQL's decoder caps around
186k rows/s, which is above every ingest number here except the ones already
limited by how fast the source can write — and the jsonb shape has now crossed
into that territory at 162k/s, so it is closer to being the ceiling than it was.
