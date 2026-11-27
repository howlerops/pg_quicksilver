# Engine comparison and merged-build validation

Raw output and the scripts that produced it, for three measurements taken on one
4-core / 15 GB container against PostgreSQL 17.12. Nothing here is typed by hand.
Paths inside the scripts and logs are the scratch paths of the run that produced
them (`/tmp/qsb` was a bind mount of the working directory); to re-run, mount
your own directory there or edit the paths.

## four_engines/

PostgreSQL heap, the Quicksilver mirror (commit c5421c5, unpatched), Mason 0.23.4
and a plain DuckDB file, on Mason's published `orders`/`customers` shape at 10M
orders with random UUID keys. Three cores for every engine. Every timed answer was
checked against the heap answer for the same state (`scripts/bench.py`,
`results/ref_*.json`).

- State A: fresh after load. State B: after 200 transactions updating, deleting
  and inserting about 8% of rows (`scripts/workload.py`), no manual compaction.
  State C: after the mirror's own delta merge; PostgreSQL and Quicksilver only.
- Quicksilver and the DuckDB file were timed in-process with python duckdb;
  PostgreSQL and Mason over the wire. That favours the first two slightly.
- The mirror was read through DuckDB directly, not through `pg_duckdb`.
- The plain-DuckDB apply of the workload took 997 s at default settings and 28 s
  with `checkpoint_threshold='4GB'`; the cause was not found.
- One run. Not ClickBench, not TPC-H.

## merged_build/

The same 10M-row run repeated on two cores for the old build and for the build
that merged per-Pod slots, the bounded stream buffer, fsync of data files, the
dead-set cache with idle compaction, and typed value conversion — plus seven
correctness scenarios on a 200k-row table, each ending in `qs-verify`
(`out/a.txt` … `out/g.txt`). Start with `out/p2_summary.txt`.

- The merged build's "after workload" figures come from a second workload round,
  because the first compacted before it could be timed. They are not strictly
  like for like with the old build's.
- These runs predate the constant pad column in the generated view and the fix
  for a rewrite trusting a vector no manifest names.
- `out/p2c_slot.txt` is the reproduction of a caught-up mirror dropping its own
  slot after 4 GiB of WAL that was not for it; fixed afterwards.

## delete_layouts/

How DuckDB's read cost changes with the way the same deletes are represented, on
the churned 10M-row mirror: the generated view, four rewrites of its anti-join,
a fully compacted file, DuckLake, Iceberg v2 and v3, a base split into 16 and 64
files, and a boolean mask. `raw/summary.txt` has every timing at one and two
threads.

- The DuckLake and Iceberg tables were built by replaying the churn by key, so
  their file layouts and encodings differ from the mirror's; the gaps are not
  purely the delete mechanism.
- Iceberg writes needed a REST catalog; `scripts/rest_stub.py` is a local stub.
- Write-side cost of each layout was not measured.
