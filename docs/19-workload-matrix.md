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

| shape | what it is | bootstrap | ingest | p50 | p99 | storage | CPU/change | written/change | correct |
|---|---|---|---|---|---|---|---|---|---|
| **narrow** | 4 cols, insert-only | 195k rows/s | **106k/s** | 209 ms | 2.7 s | 2.7× | 21 µs | — | ✅ |
| **wide** | 12 cols, insert-only | 98k rows/s | **64k/s** | 208 ms | **736 ms** | 2.3× | 37 µs | 105 B | ✅ |
| **jsonb** | 6 KB TOASTed doc, scalar UPDATEs | 16k rows/s | **165k/s** | 208 ms | **648 ms** | **3.2×** | **15 µs** | 239 B | ✅ |
| **inline** | 1.2 KB INLINE doc, resent on every UPDATE | 49k rows/s | **81k/s** | 209 ms | **519 ms** | **13.3×** | 26 µs | 516 B | ✅ |
| **churn** | UPDATEs on a hot 1% | 194k rows/s | 54k/s *(source-limited)* | 208 ms | **229 ms** | **14.7×** | **8 µs** | **6 B** | ✅ |
| **deletes** | inserts + deletes 1:1 | 195k rows/s | **203k/s** | 208 ms | **823 ms** | 3.2× | 12 µs | 22 B | ✅ |

**written/change** is new in round seven and is the most stable thing in this
table. Final mirror size is not a measure of what the write path did —
compaction folds the deltas away, so two runs that wrote wildly different
amounts converge on the same directory — and RSS is a Go runtime high-water
mark that swings by 3× between identical runs. Bytes that actually reached
storage, per row-change, is the number that moves when the writer changes.

Read across it and the shapes stop looking alike: churn writes **6 bytes per
change** and the inline shape writes **516**, an 86× spread on the same
engine. Growth costs, and large values cost; re-updating a row you already
hold costs almost nothing.

Five rounds, ingest and p99:

| shape | round 1 | round 2 | round 3 | round 4 | round 5 | round 6 | round 7 |
|---|---|---|---|---|---|---|---|
| narrow | 119k/s · 6.2 s | 132k/s · 9.0 s | 129k/s · 4.9 s | 125k/s · 4.0 s | 104k/s · 1.1 s | 122k/s · 928 ms | **106k/s · 2.7 s** |
| wide | 67k/s · 224 ms | 69k/s · 5.9 s | 73k/s · 803 ms | 68k/s · 2.3 s | 65k/s · 3.1 s | 72k/s · 2.7 s | **64k/s · 736 ms** |
| jsonb | never converged | 84k/s · 486 ms | 84k/s · 348 ms | 162k/s · 2.0 s | 159k/s · 708 ms | 164k/s · 633 ms | **165k/s · 648 ms** |
| inline | — | — | — | — | — | — | **81k/s · 519 ms** |
| churn | 59k/s · 234 ms | 59k/s · 217 ms | 59k/s · 223 ms | 57k/s · 223 ms | 55k/s · 225 ms | 56k/s · 220 ms | **54k/s · 229 ms** |
| deletes | 167k/s · 7.6 s | 240k/s · 7.5 s | 256k/s · 1.9 s | 242k/s · 1.8 s | 223k/s · 2.1 s | 240k/s · 904 ms | **203k/s · 823 ms** |

Resident memory, which rounds five and six are mostly about:

| shape | round 4 | round 5 | round 6 |
|---|---|---|---|
| wide | 5 GB | 1 GB | **943 MB** |
| jsonb | 3 GB | 560 MB | **206 MB** |
| deletes | 5 GB | 3 GB | **858 MB** |
| churn | 74 MB | 65 MB | **52 MB** |

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
| **Streaming compaction and bootstrap** (row group at a time) | RSS: wide **5 GB → 1 GB**, jsonb **3 GB → 560 MB** | a few percent of ingest on insert-heavy shapes, within run-to-run spread |
| **Generation-numbered deletion vectors** | an independent reader can no longer be short a row; 1-in-7 failures → 10/10 passes | one extra small file per data file, reclaimed after one write |
| **Pointer-free index** (integer file ids, int64 keys) | live heap **1.09 GB → 331 MB**; deletes RSS **3 GB → 858 MB**, p99 **2.1 s → 904 ms**; CPU/change down 10–25% on every shape | a second map for non-integer keys; the id encoding depends on file names staying a single sequence |
| **Eliding unchanged large values** (per-key digest) | inline shape **27% fewer bytes written**, 5% faster drain, no CPU cost | 8 bytes per row of index; equality decided from a 64-bit digest |

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

## Round five: streaming the rewrite, and a reader that was quietly wrong

Two items from round four's list, and the bug that the second one's benchmark
turned up.

### Compaction no longer holds the table in memory

The writer had already been made to emit one row group at a time. The *reader*
had not: a rewrite read every live row into a `[]map[string]any` before writing
a byte, which on the narrow shape is four million Go maps. That is where the
resident memory went, and it is not only memory — allocating and collecting
millions of maps is garbage-collector work, and a GC pause stops the apply
goroutine exactly as it stops the compactor.

Reading row group by row group, straight into the new file, took wide from 5 GB
to 1 GB and jsonb from 3 GB to 560 MB. `Snapshot` got the same treatment, since
it held the whole *source* table before writing it.

Three of the five p99 numbers came down with it (narrow 4.0 s → 1.1 s, jsonb
2.0 s → 708 ms, deletes unchanged within noise), which is consistent with GC
pressure having been part of that tail — but read the p99 caveat above before
believing any single one of those. Ingest is a few percent lower on the
insert-heavy shapes, which may be the per-row-group overhead of the streaming
reader or may be run-to-run variance; it is smaller than the spread between
repeated runs, so this document does not claim it either way.

### The bug: a reader that was short exactly one row

On the run that measured all this, the churn shape came back DIVERGED, missing
one row out of 200,063 — the settle marker. Every previous round had passed it.

Everything on disk was correct. The marker was in `delta/000116`, alive, with no
deletion vector. The mirror's `state.json` named that file. And the verifier had
still failed to see it.

The verifier had printed `applied_lsn=10/665780B0`; the mirror's state said
`10/66578168`. It had loaded the manifest one batch too early — which should be
harmless, because an older manifest is still a valid older snapshot. It was not
harmless, because **deletion vectors were overwritten in place**:

1. the reader loads `state.json`, naming files up to `delta/000115`
2. the writer marks the marker's old copy dead in `dv/000115`, writes the
   replacement to `delta/000116`, and saves the manifest
3. the reader reads `dv/000115` — and gets the *new* vector

The row is now dead in a file the reader can see and alive only in a file the
reader's manifest does not mention. It is in neither place. A manifest and the
vectors it named were two different points in time, so "an older snapshot" was
not a snapshot at all.

The fix is that a deletion vector is never modified. Each write publishes a new
generation, `state.json` records which generation belongs to each file, and a
manifest therefore describes a complete, immutable set. Old generations are kept
for one further write and then reclaimed; a reader holding a manifest older than
that gets `ErrStaleManifest` and re-reads, which is the same distinction this
package has now had to make four times — **"I cannot tell" must never be spelled
as an answer.**

It reproduced about once in seven benchmark runs before the fix, and ten out of
ten runs pass after it. The regression test drives it deterministically: an
independent reader opens the mirror, the writer supersedes a row, and the reader
must come back with either value for that row but never with 99 rows out of 100.
Pointing `deadPositions` back at the newest vector on disk makes it fail with
exactly the production symptom.

This is the most important finding in this document, because of what it is
*about*. The mirror exists to be read by something other than the process
writing it. Every previous bug here was in the writer and showed up as a wrong
mirror; this one was in the *contract between writer and reader*, and the mirror
was right the whole time.

## Round six: an index the garbage collector never looks inside

Round five ended by naming its own next step: the profile said the cost was no
longer Parquet, compression or decoding but the key -> location index itself.
One entry per row of the mirror, `map[string]loc` with `loc{File string, Pos
int}`, held twice during a compaction swap — two pointers per entry, several
million entries, and a collector walking every one of them on every cycle.

Both halves are now integers:

- **A file is an id, not a path.** Data files are named from one monotonic
  sequence and a number is never reused, so `(kind, seq)` already IS the file's
  identity. Making the id a pure function of the name matters twice: there is no
  interning table to grow without bound in a process that runs for weeks, and
  the background compactor can compute an id without touching state the apply
  goroutine is writing.
- **A key is an int64** when the primary key is an integer type, which is most
  tables. pgoutput hands over `"123"` and Parquet hands back `int64(123)`; both
  now land on the same entry without either allocating a string. Text keys fall
  back to a second map, and a table can use both at once without either
  misbehaving — there is a test for a text key that looks numeric.

A Go map whose key and value types contain no pointers is invisible to the
garbage collector: it is scanned as plain memory, not walked entry by entry.
That is the whole of the idea.

| deletes shape | round 5 | round 6 |
|---|---|---|
| live heap | 1.09 GB | **331 MB** |
| `runtime.scanObject` | 14.9% cum | **5.4% cum** |
| peak RSS | 3 GB | **858 MB** |
| p99 | 2.1 s | **904 ms** |
| CPU per change | 13 µs | **10 µs** |
| ingest | 223k/s | **240k/s** |

Every shape moved the same way — CPU per change down 10–25%, ingest up 7–17%,
and four of the five p99s now under a second. The remaining 274 MB of live heap
is the index map itself, which is the irreducible part: one entry per row is
what makes an apply O(change) rather than O(table), and that trade was settled
in docs/18.

The lesson worth keeping is about where to look. Three rounds of this work were
spent on Parquet, compression and buffering, which is where a storage engine's
cost is *supposed* to be. It stopped being there two rounds ago, and the only
reason that was noticed is that a shape refused to follow the others down and
got profiled instead of theorised about.

## Round seven: the value PostgreSQL does not warn you about

Round four works because pgoutput leaves an unchanged TOASTed value out of the
change entirely. That covers exactly one case. PostgreSQL stores a value out of
line only once the whole row passes about 2 KB, so a 1.2 KB document sits
**inline** — and is resent, in full, on every update to any other column in the
row. The mirror wrote it again every time, with no hint that it need not.

So the mirror works the hint out for itself. It keeps, per key, a digest of the
row's large columns; when a change arrives carrying large columns that hash to
the same value, they did not change and are removed from the change before
anything else looks at it. What is left is a short row, and round four's
machinery writes it without expanding it. A sixth shape, **inline**, was added
to measure exactly this.

| inline shape, two runs each | resending | eliding |
|---|---|---|
| bytes written to storage | 674 MB, 629 MB | **477 MB, 475 MB** |
| per row-change | 603 B, 563 B | **433 B, 453 B** |
| drain rate | 81.0k/s, 77.4k/s | **85.7k/s, 81.1k/s** |
| CPU per change | 25 µs, 23 µs | 22 µs, 23 µs |

**27% fewer bytes written, 5% faster, for no CPU.** The other five shapes are
unchanged, and the jsonb shape — which has a large column in every row and can
*never* elide anything — is byte for byte identical either way.

This is the one place in the package that decides data equality from a digest
rather than from the bytes. That is worth being explicit about: a collision
leaves a stale value in the mirror silently and permanently. The odds are 2^-64
per comparison, nothing is compared unless the KEY already matches, the column
name and the value's length are both in the digest, and the alternative is
reading the previous value back out of Parquet — the row-group decode per
update that this whole line of work exists to remove. `QS_ELIDE_UNCHANGED=0`
turns it off.

### Two findings, and both were nearly shipped as the opposite

**The hash function decided whether the optimisation was worth having.** The
first version used `hash/fnv`, whose Go implementation writes a byte at a time.
Measured, it cost **18% more CPU than it saved in bytes**: 16% fewer bytes
written for 24 µs → 28.5 µs per change. That is a trade, not a win, and it
would have gone into the document as one. Swapping to `hash/maphash`, which is
AES-accelerated, made the CPU cost disappear entirely and turned 16% into 27%.
The digests never leave memory — they are not persisted and not compared across
processes — so a per-process seed costs nothing.

**Gating it on the wrong question made an unrelated shape 2× worse.** The first
version enabled the compaction-time digest refresh as soon as a table was seen
to *have* a large column. The jsonb shape has a 6 KB document in every row and
can never elide anything, because pgoutput omits that document on every update
and an omitted column makes the digest unknowable. So it paid 1.2 GB of hashing
per rewrite for nothing, and came out with **24 delta files and a 631 MB mirror
against 4 files and 330 MB**. The gate is now "has an elision ever actually
fired", which is self-correcting: the first elision is bootstrapped by an
ordinary whole-row write, and only then does a rewrite start maintaining
digests.

### The measurement that made both of them visible

Neither would have been noticed from the numbers this document had. Final
mirror size does not measure a write-path change — compaction folds the deltas
away, so both runs converge on ~130 MB. RSS swings 3× between identical runs.
Drain rate on a shape this source-limited moves by less than its own noise.

The harness now reads `write_bytes` from `/proc/<pid>/io` and reports **bytes
that actually reached storage, per row-change**. It is in the main table above,
it is the number that moved, and it immediately said something nobody had
asked: churn writes 6 bytes per change and the inline shape writes 516, an 86×
spread on the same engine.

## What to do next, in order of measured value

1. ~~**Serve a query.**~~ Done — [docs/20](20-serving-the-mirror.md). 11× on a
   group-by, 38× on a filtered aggregate over an inline document, **0.04× on a
   point lookup**, every answer verified against the source, and two silent
   PostgreSQL/DuckDB incompatibilities found by the benchmark failing.
2. ~~**Run against a real CloudNativePG operator.**~~ Done —
   [docs/21](21-against-the-real-operator.md). The real 1.30 operator discovers
   the plugin, completes the handshake, records our capabilities, and puts the
   sidecar in a Pod it built. Rollouts and switchovers still need a cluster that
   can run pods; this sandbox cannot.
3. **Bootstrap is 16k rows/s on jsonb and 49k on inline, against 195k on
   narrow.** Streaming it removed the memory but not the time, so the cost is in
   the source query or in compressing the documents, and those are
   distinguishable by measurement rather than argument.
4. **`narrow` writes the most per change of the growth shapes and has the worst
   p99.** Nothing has been profiled to say why; the `written/change` column is
   new and nobody has looked at it with a profiler yet.

Still not on the list: parallel decode. PostgreSQL's decoder caps around
186k rows/s, which is above every ingest number here except the ones already
limited by how fast the source can write.
