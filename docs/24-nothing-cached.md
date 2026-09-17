# 24 — Nothing cached

Every read-side number this project has published —
[docs/20](20-serving-the-mirror.md), [docs/23](23-storing-an-instant.md) — is a
**warm page cache on both sides**. The data has just been written, it is still
in RAM, and neither engine touches a disk. docs/20 listed measuring a cold cache
as item 2 of what to do next, and called it "a different, and fairer, question".

It is a different question. The answer is not the tidy one — "cold makes the
mirror look even better" — that the expectation had in it.

**Exactly one thing changes consistently, and it is the worst case.** A point
lookup by primary key, which docs/20 published as the honest 20–50× loss, is
1.4–3.4× on a cold cache. Everything else moves in *both* directions depending
on the shape and the query, and which way it moves turns out to be decided by
something more interesting than the storage format.

---

## What "cold" has to mean

Dropping the OS page cache is not enough. PostgreSQL has its own buffer pool,
and a comparison that empties the kernel's cache while leaving `shared_buffers`
full is rigged: PostgreSQL answers from RAM while the mirror goes to disk. So
`serve_compare.py --cold`, before **every single timing**:

1. restarts PostgreSQL, which is the only thing that empties `shared_buffers`;
2. drops the OS page cache;
3. opens a fresh DuckDB, because its buffer manager is a cache too;
4. runs that side **once** — because the second run of a cold query is a warm
   query.

It also records **bytes off the block device** per query, from `/proc/diskstats`
— the whole machine rather than one PID, because a PostgreSQL query fans out to
parallel workers whose I/O never appears in the leader's `/proc/<pid>/io`, and
undercounting one side of a comparison is worse than including a little
background noise. Nothing else runs during a timing.

Bytes read is the point. A column store is not faster because it is clever; it
is faster because it reads fewer bytes, and **on a warm cache that advantage is
nearly free to the row store.**

### What this machine's "cold" is worth

```
cold (page cache dropped)   310 MB/s
warm                        7.3 GB/s
```

A 24× gap, so the measurement is real. But the guest's page cache is what gets
dropped; the **host's is not**, and PostgreSQL's 300 MB scans come back in
130–195 ms — around 2 GB/s, far above the 310 MB/s a `dd` of freshly written
data gets. The device behaves like a fast local NVMe with a warm host cache
behind it.

So: these numbers model **a fast local SSD**. They do not model a network
volume, where PostgreSQL's 300 MB would cost seconds and every ratio below would
move in the mirror's favour. Stating that is cheaper than being caught by it.

---

## Both shapes, both ways

One run, `SHAPES="narrow wide" QS_SERVE_COMPARE=1 QS_COLD=1`. Every result was
compared value by value against PostgreSQL before its timing was believed; all
agreed, warm and cold.

**narrow** — 4.4M rows, four columns:

| query | warm | **cold** | cold: bytes read |
|---|---|---|---|
| `count(*)` | 3.11× | **5.44×** | 302 MB → 41 MB |
| `WHERE ts > now() - interval '1 hour'` | 5.45× | **3.18×** | 303 MB → 51 MB |
| `date_trunc('hour', ts)` group by | 22.52× | **8.22×** | 303 MB → 8.5 MB |
| `sum(amount)` | 11.10× | **8.15×** | 302 MB → 8.9 MB |
| `GROUP BY sku` top 10 | 12.37× | **11.92×** | 432 MB → 45 MB |
| filter + aggregate | 14.57× | **10.39×** | 304 MB → 7.4 MB |
| **point lookup by key** | **0.05×** | **0.73×** | 4.7 MB → 4.0 MB |

**wide** — 2.2M rows, twelve columns:

| query | warm | **cold** | cold: bytes read |
|---|---|---|---|
| `count(*)` | 3.72× | **11.02×** | 348 MB → 62 MB |
| `WHERE ts > now() - interval '1 hour'` | 5.33× | **4.22×** | 348 MB → 72 MB |
| two columns of twelve | 10.54× | **8.50×** | 348 MB → 10 MB |
| three-column group by | 6.20× | **6.42×** | 348 MB → 3.7 MB |
| filter + aggregate (clustered) | 13.22× | **23.72×** | 348 MB → 0.1 MB |
| filter + aggregate (unclustered) | 9.94× | **12.88×** | 348 MB → 0.3 MB |
| **point lookup by key** | **0.03×** | **0.29×** | 4.9 MB → 10.6 MB |

### The point lookup is the one clean result

Read those last rows twice. **Warm, a point lookup on the mirror is 20–30×
slower than PostgreSQL. Cold, it is 1.4× (narrow) and 3.4× (wide).** On narrow
the mirror reads *fewer* bytes to answer it than PostgreSQL's index-plus-heap
path does.

docs/20 published "20–50× slower on a point lookup" as this project's honest
worst case. It turns out to be the honest worst case **of the single most
flattering scenario for PostgreSQL.** An index lookup is 0.3 ms when every page
it needs is already in RAM; when they are not, it is several random reads, and
most of the row store's structural advantage goes with them.

### Everything else moves both ways, and not by shape

`date_trunc` on narrow falls from 22.5× to 8.2×. `count(*)` on wide rises from
3.7× to 11.0×. Same run, same machine, opposite directions — so this is not a
property of the format, and it is not a property of the shape either, since both
shapes contain queries that move each way.

What decides it is **which side was already I/O-bound when warm.** Where the
mirror was doing almost no I/O at all — `date_trunc` reads 8.5 MB — a cold cache
takes away everything it had, and PostgreSQL, whose working set never fit in
`shared_buffers` anyway, loses proportionally less. Where PostgreSQL was the one
getting a free ride from RAM — wide's `count(*)`, 45 ms warm against 332 ms cold
— the cold cache takes it away and the gap opens.

Cold pushes both engines toward being I/O-bound, and once both are, the ratio
walks toward the **bytes-read ratio** instead of the CPU-work ratio. The
bytes-read column is what the timings converge on; on wide's three-column group
by that is 94×, and the timing is 6.4×, so there is a long way left to walk.

---

## Three bugs, all of which produced results rather than errors

### A filter that matched nothing, in a published table

The wide shape's `filter + aggregate` was
`WHERE status = 'shipped'`. The wide shape generates `ok`, `pending` and
`refunded`. **It matched nothing, for as long as it existed.**

Both engines agreed — on `count 0` — so the correctness check passed, and the
timing went into docs/20's table as 3.9×. What it actually measured was how fast
each engine can establish that there is nothing to do: PostgreSQL scans 341 MB
to find out, DuckDB reads a row-group footer and skips every file. That is a
number about **pruning** wearing the label of a number about **filtering**.

An empty result is never *wrong*, which is exactly why it has to be said out
loud. `serve_compare.py` now warns when a query returns empty on both sides.

Fixing the predicate to `status = 'pending'` exposed the next layer: `pending`
exists only in the seeded rows, which sit at the front of the mirror, so DuckDB
still prunes almost every row group from its footers. So the benchmark now
carries **both**:

| wide shape, cold | speedup | bytes read |
|---|---|---|
| `WHERE status = 'pending'` — clustered, prunable | 23.7× | 348 MB → 0.1 MB |
| `WHERE qty = 5` — uniform, nothing to prune | 12.9× | 348 MB → 0.3 MB |

Reporting only the first would credit the format for a property of the data.
The pair also says something the single number could not: even with **nothing**
to prune, reading two narrow columns out of twelve is still 12.9× — so the
clustered case is roughly half pruning and half plain columnar projection.

### A restart that hung on a database already serving queries

`pg_ctl` without `-l` leaves the restarted postmaster holding the benchmark's
stdout and stderr pipes. Python's `capture_output` reads a pipe until EOF, and
EOF never comes while the server runs. `pg_ctl` exits, the child shows up as a
zombie, and the benchmark waits forever — on a PostgreSQL that is up and
answering. Not a crash: a wait, which is the shape almost every bug in this
project's history has taken.

### `file_row_number` on files that have no deletion vector

The view asked `read_parquet(…, file_row_number = true)` on **every** file,
though only branches carrying a deletion vector filter on it. It is a generated
column, so an engine that could have answered `count(*)` from the Parquet footer
has to open the file instead.

It is now asked for only where a vector needs it — two branches of four on the
wide mirror. **This did not move that shape's `count(*)`,** and the reason is
worth keeping: the two branches that still carry vectors dominate the bytes, and
a mirror with deletion vectors cannot answer `count(*)` from metadata at all.
The vectors *are* the difference between the file and the truth. The fix is
still right; the shape that would show it is a compacted, vector-free mirror.

---

## What this changes

| | Before | After |
|---|---|---|
| Read-side evidence | warm cache only | warm and cold, with bytes read |
| The point-lookup worst case | "20–50× slower" | 20–30× warm, **1.4–3.4× cold** |
| The best case | 22.5× on `date_trunc` | 8.2× cold; the warm figure was mostly a cache |
| `filter + aggregate` on wide | 3.9× | was an empty result; now 23.7× clustered / 12.9× not |
| Why the mirror wins | asserted | 348 MB against 3.7 MB, measured per query |

## What to do next

1. **Measure a working set larger than RAM.** Cold-per-query is still a
   best-case disk; a table several times the size of the page cache is the
   scenario the mirror is actually for, and none of these shapes reach it.
2. ~~**Prune on the manifest.**~~ — retracted, with evidence:
   [docs/27](27-the-pruning-that-already-works.md). The engine already prunes at
   row-group granularity and the view is transparent to it; a selective
   predicate reads 1/27 of an unfiltered scan on wide. File-level pruning would
   save opening footers and nothing else.
3. **Run the view through the confined `pg_duckdb` of
   [docs/12](12-s5-serving-path.md)** — still built and measured separately,
   still never put together.
