# 20 — Serving the mirror: the first read-side numbers

Every number in [docs/18](18-measured-performance.md) and
[docs/19](19-workload-matrix.md) is the **write path** — how fast the mirror can
be built and how much it costs to keep. That is the half that had to work first.
It is not the half the project is *for*.

This is the other half, measured for the first time against the Go mirror:
**is reading the mirror actually faster than reading PostgreSQL, and does it
give the same answers?**

Both questions get answered on every run, in that order. A faster answer that
differs from PostgreSQL is not a result.

---

## The problem: a directory is not a table

An engine pointed at a glob of the mirror's directory gets a plausible-looking
answer that is wrong in three separate ways, none of which raises an error:

| | What a glob does | What it should do |
|---|---|---|
| deletion vectors | reads rows that were deleted or superseded | skips them |
| column-partial deltas | reads a patch as a whole row, so every column it does not carry reads as NULL | merges it onto the row it patches |
| the manifest | reads files that are no longer named, misses ones that are | reads exactly the named set |

Only one thing knows the answer to all three, and it is the manifest. So the
mirror publishes a **SELECT that reconstructs itself**:

```
qs-query -mirror /var/lib/postgresql/data/quicksilver -table public.events
```

```sql
-- quicksilver mirror public.events applied_lsn=1D/70ABD3E8
WITH qs_whole AS (
  SELECT "id", "sku", "amount", "ts"
    FROM read_parquet('…/base/000011.parquet', file_row_number = true)
  UNION ALL
  SELECT "id", "sku", "amount", "ts"
    FROM read_parquet('…/delta/000012.parquet', file_row_number = true)
   WHERE file_row_number NOT IN (
     SELECT p FROM read_parquet('…/dv/000012.000000001.dv.parquet'))
)
SELECT …
```

`read_parquet`, nothing else. Any engine with it — DuckDB, DataFusion, Spark,
ClickHouse — can evaluate this, and no engine needs to know anything about the
layout.

Deletion vectors used to be JSON arrays here, and the change is not cosmetic:
parsing 13 MB of decimal positions was over a third of the cost of every query
against a file with deletes, at twenty million rows. See
[docs/29](29-what-a-delete-costs-to-read.md). A mirror written before the change
still has `.dv.json` on disk and the view still emits `read_json` for those,
because a vector a reader cannot find does not raise anything — it reads as
*nothing in this file is dead*.

Two properties are worth spelling out:

- **It is a snapshot of one manifest.** A compaction landing mid-query deletes
  files the query names, and the engine will say so. Ask again and re-run. That
  is the same `ErrStaleManifest` contract the in-process reader has, and for the
  same reason: a view that answers from a mixture of two manifests is the bug
  [round five](19-workload-matrix.md#round-five-streaming-the-rewrite-and-a-reader-that-was-quietly-wrong)
  was about.
- **The applied LSN comes with it**, as the first line. A view of a mirror is
  only usable if the reader can decide whether it is fresh enough, so freshness
  is part of the answer rather than something to go and look up.

---

## The numbers

`bench/scripts/serve_compare.py`, run from the workload matrix
(`QS_SERVE_COMPARE=1`). Same queries against the source and against the mirror
view, median of three, every result compared value by value first.

| shape | rows | `count(*)` | aggregate | group by | filtered aggregate | **point lookup** |
|---|---|---|---|---|---|---|
| **narrow** | 4.4M | 2.2× | 5.5× | **10.9×** | 7.2× | **0.04×** |
| **wide** | 2.3M | 1.7× | 4.1× | 3.3× | 3.9× | **0.02×** |
| **jsonb** | 200k | 4.6× | — | 6.3× | 4.9× | **0.11×** |
| **inline** | 200k | 2.9× | — | **21.5×** | **38.5×** | **0.04×** |
| **churn** | 200k | 2.0× | 2.4× | 6.6× | — | **0.06×** |
| **deletes** | 3.9M | 2.0× | 5.3× | **11.0×** | — | **0.03×** |

Every query agreed with PostgreSQL on every shape.

> **Two corrections since, both from [docs/24](24-nothing-cached.md).** The
> wide shape's "filtered aggregate" — 3.9× above — filtered on a status value
> the shape never generates, so it matched **nothing**: both engines agreed on
> `count 0`, the correctness check passed, and a number about row-group pruning
> went into this table labelled as a number about filtering. And every figure
> here is a warm cache; on a cold one the point lookup is **1.4–3.4× slower,
> not 20–50×**, because an index lookup is only 0.3 ms when its pages are
> already in RAM.

### The best case is the one PostgreSQL is worst at

**inline is the biggest win in the table — 38.5× — and it is the smallest table
in it.** The shape is 200,000 rows with a 1.2 KB document stored *in the heap*.
Asking `count(*) WHERE status = 'ClosedWon'` makes PostgreSQL read 2 GB of pages
to look at a 20-byte column, because the document is sitting in every one of
them. The mirror reads that column and nothing else: 342 ms against 8.9 ms.

That is the columnar argument in one row of a table, and notice what it is
*not*: it is not about the mirror being clever. It is about a row store having
to read bytes it does not want. The 4.4M-row narrow shape — four small columns,
nothing to skip — gets 10.9× on a group-by and only 2.2× on `count(*)`.

### The worst case is worth stating in the same breath

**A point lookup by primary key is 20–50× slower on the mirror.** PostgreSQL
answers it from an index in 0.3 ms; the mirror opens Parquet files and decodes a
row group to find one row, in 5–24 ms. It is the same trade every column store
makes, it is not going to improve much, and any claim about this project that
does not say so out loud is a sales sheet.

The mirror **complements** the primary. It does not replace it, and it is not a
read replica with better numbers.

---

## Two compatibility findings, both silent

The benchmark found both by failing, which is the only reason they are here.

### `ORDER BY … DESC` puts NULLs in different places

```
postgres:  ORDER BY x DESC  ->  [NULL, 3, 1]
duckdb:    ORDER BY x DESC  ->  [3, 1, NULL]
```

PostgreSQL's default for `DESC` is `NULLS FIRST`; DuckDB's is `NULLS LAST`. So
`SELECT sku, sum(amount) FROM … GROUP BY sku ORDER BY 2 DESC LIMIT 10` returns a
**different top ten** from the two engines the moment any group aggregates to
NULL — same data, no error, nothing in either plan to suggest it. On the deletes
shape, 319 rows with a NULL `sku` were enough to change the answer.

This is checked and printed on every run of `serve_compare.py` so that it cannot
quietly stop being true, or quietly become true of something else. A serving
layer that claims PostgreSQL compatibility has to either rewrite `ORDER BY` or
say this out loud; **this project says it out loud.**

### The mirror's timestamps are not timestamps

*(Fixed since this was written — [docs/23](23-storing-an-instant.md). Left here
because it is how the two documents that follow got started.)*

The writer maps anything it does not recognise to a string, so `timestamptz`
lands in Parquet as `VARCHAR`. A query engine can group and compare those as
text — ISO-8601 with a fixed offset happens to sort correctly — but it cannot do
date arithmetic, so `WHERE ts > now() - interval '1 day'` needs an explicit cast
that a query written against PostgreSQL will not have.

It surfaced as a spelling difference (`…+00` from the mirror against `…+00:00`
from a Python datetime), which is only a harness detail. **The fact underneath
was not**, and chasing it found a silent-corruption bug — see
[docs/22](22-one-instant-two-spellings.md). Text is a property of the SESSION
that rendered it, and nothing made the mirror's three sessions agree.

Pinning the rendering (docs/22) was the precondition for parsing the text into
an instant (docs/23), which is what actually closed the gap: `timestamptz`,
`timestamp`, `date` and `time` are now native Arrow types, and the query this
section says needs a cast runs unchanged, 2.84× faster than the source.

---

## What this changes

| | Before | After |
|---|---|---|
| Read-side evidence | the Python reference, docs/11, on a format that no longer exists | six shapes, current format, every answer verified |
| How an engine reads the mirror | it cannot, correctly, without knowing the layout | `qs-query` emits the SQL |
| Freshness | not exposed to a reader | the applied LSN is the first line of the view |
| PostgreSQL compatibility | assumed | two concrete differences documented |

The [docs/12](12-s5-serving-path.md) confinement patch remains the answer to
*who may read the mirror from inside PostgreSQL*; this is the answer to *what
they should read*. The two compose: the view SQL is exactly what a confined
`duckdb_users` role would be given, and it names only files inside the mirror
directory that `duckdb.allowed_directories` permits.

## What to do next

1. ~~**Map temporal types to Arrow timestamps**~~ — done:
   [docs/22](22-one-instant-two-spellings.md) pinned the rendering and
   [docs/23](23-storing-an-instant.md) stores the instant. `interval` is the one
   temporal type still text.
2. ~~**Measure a cold cache.**~~ — done: [docs/24](24-nothing-cached.md). It
   was a different question, and the answer was not the expected one.
3. **Run the view through the confined `pg_duckdb` of docs/12.** The two halves
   have been built and measured separately and have never been put together.
4. **Push predicates into the manifest.** Parquet row-group statistics are
   already written; the view does not yet use them to skip files, so a selective
   filter reads every file the manifest names.
