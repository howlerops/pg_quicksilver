# Publishing the mirror as a DuckLake table

A prototype, to answer one question: can the files the sidecar already writes be
exposed as a DuckLake table — no data rewritten, one DuckLake snapshot per
mirror manifest, kept up on every apply — and what does that cost?

**Yes, with the catalog written directly.** Not through DuckLake's SQL alone,
and not for patch files.

`delete_layouts/` showed DuckLake reading the same data 3 to 4 times faster, but
it rebuilt the table by replaying changes through DuckDB. Here the table is the
mirror's own Parquet files, hard-linked, and the mirror is driven by
`internal/mirror` itself (`gen/`).

## What is here

| file | |
|---|---|
| `publish.py` | the publisher: manifest in, one DuckLake snapshot out. Two modes |
| `gen/main.go` | drives `mirror.Table` one apply at a time, with the sidecar's `maintain()` policy |
| `run.py` | `functional` (small, every case) and `bench` (the numbers below) |
| `read.py` | view SQL against the lake, pinned, one thread |
| `probe.py` | which form of delete DuckLake accepts |
| `raw/` | output of all of the above, as it ran |

Python, because the publisher needs DuckDB 2.0 with the `ducklake` extension
(catalog format 1.1) and that is what is installed; the generator is Go in its
own module so that it can import `internal/mirror` without touching `go/go.mod`.

```
python3 run.py functional --work /tmp/qs-lake
python3 probe.py /tmp/qs-lake
python3 run.py bench --work /tmp/qs-lake-bench                 # ~4 min, 12 GB peak
python3 run.py bench --work /tmp/qs-lake-bench --publish sql
```

## Deletes: three options, in the order asked

`raw/probe.txt`.

1. **Register an existing delete file.** There is no function for it.
   `ducklake_add_data_files` is the only one that registers anything. Putting
   the mirror's vector into `ducklake_delete_file` by hand makes `count(*)`
   right — it is answered from `record_count - delete_count` in the catalog,
   without opening anything — and every scan fail:

       Invalid schema contained in the delete file .../dv/000002.000000002.dv.parquet
       - expected file_name/position/[snapshot_id or row]

   The mirror's vector is one column, `p`. DuckLake wants `(file_path, pos)`.
   A count that is right over a table that cannot be read is the worst of the
   outcomes here, and it is the first one you see.

2. **`DELETE` statements.** Work, and the whole publish fits one transaction and
   one snapshot (`mode="sql"`): `ducklake_add_data_files`, then
   `DELETE ... WHERE filename = F AND file_row_number IN (SELECT p FROM vector)`,
   then a whole-file `DELETE` for each dropped file. Every snapshot is correct.
   Two things are wrong with it:
   - a dropped file that already had deletes is **not retired**. DuckLake records
     its remaining rows as more deletes and goes on listing the file. After the
     run below the manifest named 1 file and the catalog 50.
   - the positional `DELETE` scans, so it costs with the base and the vector:
     median 380 ms, and 3.8 s for the publish after a base rewrite.

3. **The catalog tables, written directly** (`mode="catalog"`). Works. The vector
   is copied into `(file_path, pos)` and its row inserted; the previous one gets
   `end_snapshot`. A dropped file gets `end_snapshot` and is gone. The reader
   does not look at `file_path` — a delete file with `'nonsense'` in it reads
   correctly — which matters below. Inlined deletes
   (`ducklake_inlined_delete_<table>`) also read correctly; not built out.

   Data-file rows are not hand-made: the file is shown to a scratch in-memory
   lake with `ducklake_add_data_files` and the rows it produces are copied over,
   so the statistics are DuckLake's. One trap: a name-mapping id must come from
   the snapshot's `next_file_id` counter, or the reader says
   `Unknown name map id`.

## What maps and what does not

| | |
|---|---|
| Parquet without field ids | maps. `ducklake_add_data_files` writes a `map_by_name` mapping; nothing in the files changes |
| `ADD COLUMN ... DEFAULT` / `MissingVals` | maps. `ALTER TABLE ... ADD COLUMN c DEFAULT v` stores `v` as `initial_default`, which is what an older file reads back. Tested: 19,760 old rows read `emea`, 240 new ones their own value |
| merge, base rewrite | maps, one snapshot, old files ended in it |
| time travel | 51 of 51 snapshots reproduce the manifest they were published from, across five merges; and the last pre-rewrite snapshot after the rewrite |
| column-partial patch files | **do not map.** A DuckLake data file is whole rows; there is no row-level overlay by key. The publisher refuses the manifest. With `QS_PARTIAL_DELTAS=0` the same change is a whole row and publishes (tested) |
| `.dv.json` vectors | not handled; they predate the Parquet ones |
| other DDL (drop, rename, type change) | not tried |
| `uuid` | published as `VARCHAR`, which is how the mirror stores it |

## Measured

2,000,000 rows (uuid key, `numeric(12,2)`, `timestamptz`, status, ~200-byte
text), then 50 applies of 2,000 updates + 400 deletes + 400 inserts, publishing
after each. The mirror merged deltas five times. 4 cores, 15 GB. Publish times
are wall-clock and unpinned; reads are `taskset -c 1`, `threads=1`, warm, best /
median of 5, taken before the final rewrite (1 base + 15 deltas, 117,289 dead
positions).

| | `catalog` | `sql` |
|---|---|---|
| publish per apply, median / p95 / max | 129 / 182 / 204 ms | 380 / 515 / 530 ms |
| first 10 applies / last 10, median | 86 / 129 ms | 203 / 452 ms |
| publish after a merge (+2 −8 files) | 128–189 ms | 375–488 ms |
| publish after a base rewrite (+1 −16) | 65 ms | 3,845 ms |
| mirror's own apply, median (for scale) | 85 ms | 47 ms |
| files the catalog lists after the rewrite | 1 | 50 |
| time travel, snapshots correct | 51 / 51 | 51 / 51 |
| catalog file, first publish → 51 publishes, checkpointed | 4.47 → 7.09 MB | 4.47 → 7.35 MB |
| `count(*)` view → lake | 58.9/60.6 → 2.1/2.1 ms | 61.8/70.1 → 2.1/2.2 ms |
| `GROUP BY status` | 77.9/90.3 → 25.8/27.7 ms | 65.9/67.1 → 37.6/38.2 ms |
| `sum(amount)` | 108.2/109.9 → 50.3/51.5 ms | 106.3/107.8 → 66.9/67.6 ms |
| one-month range, count + sum | 108.9/117.2 → 93.1/94.1 ms | 94.4/98.9 → 87.5/89.2 ms |

All four answers equal between view and lake, in both runs.

**What the publish time is made of** (`catalog`, medians): reading the plan
10 ms, linking and registering the new file 23 ms, converting vectors 40 ms,
the catalog transaction 48 ms. It follows the number of files whose vector
changed in the apply — 54 ms at 0–3, 101 at 4–7, 131 at 8–11, 133 at 12–15 — and
not the table or the number of dead rows: the base rewrite, with no vector to
convert, published in 65 ms. An apply here touches the vectors of nearly every
delta, because updates land on rows that are already in deltas. In `sql` mode
it follows the number of deletes the catalog holds instead (R² 0.72, against
0.22 in `catalog` mode).

**Growth.** Between checkpoints the catalog and its WAL grew 7.4 KB per publish.
The checkpointed file grew 2.6 MB over 51 publishes; DuckDB does not hand
blocks back, so that is an upper bound on what the rows need (53 snapshots, 56
data files, 517 delete files, 280 column-stat rows). The lake directory is
where the real growth is: 517 delete files, 13.0 MB, because the base file's
vector is rewritten whole on every apply (as the mirror itself does), and every
data file the mirror has since dropped stays linked until its snapshots are
expired. Nothing here expires them.

## What the sidecar would have to change

- **Stop writing patch files for a published table** (`QS_PARTIAL_DELTAS=0`), and
  give up what they save on wide-document tables.
- **Not remove a file a snapshot still names.** The prototype hard-links, which
  is same-filesystem only. On object storage the sidecar's `removeDataFile` has
  to wait for snapshot expiry.
- **Write vectors as `(file_path, pos)`.** Then a vector is registered as it is,
  with no copy — the reader ignores `file_path` — and the 40 ms goes. It also
  has to keep a generation for as long as a snapshot names it; today it keeps
  one back.
- **Publish from the apply goroutine**, after `saveState`. The publisher reads
  the manifest and then the files; only there is that one point in time.
- **Carry the column list.** The manifest does not have it; the publisher is
  told.
- **Write catalog rows without DuckDB.** `catalog` mode is ordinary INSERTs and
  UPDATEs, which the sidecar could send to a PostgreSQL-hosted catalog with the
  driver it has. What it borrowed from DuckDB is the per-file statistics, which
  are in the Parquet footer the sidecar just wrote. Not built.
- Nothing else may write the table: DuckLake's own compaction would produce
  files the next manifest does not name.

## Not shown

A PostgreSQL or remote catalog (local DuckDB file only); concurrent readers
during a publish; more than one table; snapshot expiry; a publisher in Go.
