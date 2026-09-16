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

| shape | what it is | bootstrap | ingest | p50 | p99 | storage | CPU/change | RSS | correct |
|---|---|---|---|---|---|---|---|---|---|
| **narrow** | 4 cols, insert-only | 195k rows/s | **119k/s** | 206 ms | 6.2 s | 3.4× | 24 µs | 6 GB | ✅ |
| **wide** | 12 cols, insert-only | 98k rows/s | **67k/s** | 206 ms | **224 ms** | 3.5× | 38 µs | 6 GB | ✅ |
| **jsonb** | 6 KB doc/row, scalar UPDATEs | 14k rows/s | **never caught up** | — | — | — | — | — | — |
| **churn** | UPDATEs on a hot 1% | 98k rows/s | 59k/s *(source-limited)* | 206 ms | 234 ms | **15.3×** | **9 µs** | **84 MB** | ✅ |
| **deletes** | inserts + deletes 1:1 | 195k rows/s | **167k/s** | 206 ms | 7.6 s | 2.4× | 18 µs | 3 GB | ✅ |

Four of five are correct and comfortably ahead of what a single PostgreSQL
primary would sustain in practice. The fifth is the interesting one.

### churn is the best case, by a wide margin

Updates concentrated on a hot 1% of rows cost **9 µs of CPU per change, 84 MB of
RSS, and compress 15.3×**. Every other shape is 30–70× heavier on memory. The
reason is that the mirror is a key-addressed store: re-updating the same key
replaces a row rather than adding one, so the working set — index, deltas,
everything — stays the size of the table rather than the size of the traffic.

This is worth stating plainly because it inverts the usual intuition. **A
high-update OLTP table is the cheapest thing to mirror**, not the most expensive.
What is expensive is *growth*.

### jsonb does not keep up, and that is the headline

2.9M row-changes in 12 s against a table with a 6 KB document per row, and the
mirror was still behind after **600 seconds**. It does not converge.

The cause is unchanged-TOAST carry-forward. PostgreSQL does not resend a large
value that an update did not change, so for every such update the mirror has to
fetch the previous value from its own Parquet — a point read whose smallest
possible unit is a row group. At 8k rows per group and 3.3 KB per document that
is ~27 MB of decode to recover a few hundred values, and the sidecar log shows
the consequence directly:

```
compacted table=public.m rows=203
```

A rewrite of a 200,000-row table produced a 203-row base. That is not data loss —
by the time the rewrite finished, essentially every key had moved to a newer
delta, so the new base legitimately held almost nothing. **Under heavy churn a
background rewrite finishes stale**, which is a property of the policy, not a bug.

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
| **TOAST carry-forward** | stopped silently destroying every large unchanged column | the jsonb shape no longer converges under load |
| **Row-group size 64k → 8k** | drain **78k → 98k/s** on jsonb | none measurable on storage or scans at this size |
| **Bootstrap gate on readiness** | a node with no data stops reporting ready | none |

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

## What to do next, in order of measured value

1. **Make the compaction swap O(changed).** It is the remaining tail: 6–8 s p99
   on insert-heavy shapes against 224 ms where the base is small. The swap only
   needs to consider keys that moved during the rewrite, which the apply loop
   already knows.
2. **Let delta merging run during a compaction.** Today a rewrite blocks
   merging, so under churn the file count grows while a doomed rewrite finishes.
   This is most of why jsonb never converges.
3. **Bound carry-forward with a cache of recently-written rows.** Hot-set updates
   — the common real pattern, and the cheapest shape in the table above — would
   hit memory instead of Parquet.
4. **Stream the snapshot and compaction writes.** Peak RSS is a materialised
   `[]map[string]any` of the whole table; the writer is already batch-oriented.

Not on this list, deliberately: parallel decode. PostgreSQL's decoder is the
ceiling there, and at 186k rows/s it is above every ingest number in the matrix
except the ones that are already source-limited.
