# The mirror as a DuckLake table: the prototype

**The publisher is now Go, in the sidecar: [`go/internal/ducklake`](../internal/ducklake).**
Turn it on with `QS_DUCKLAKE=on`; [docs/16](../../docs/16-deploying.md#publishing-the-mirror-as-a-ducklake)
says what it does and what was measured. What is left here is the Python
prototype it was built from, kept as a reference and as an oracle, and the
scripts that validated the Go one against a real cluster.

## What the prototype established

`publish.py`, `probe.py`, `run.py`, `read.py`, `gen/`, `raw/` are as they ran.
They answered one question — can the files the sidecar already writes be exposed
as a DuckLake table, no data rewritten, one snapshot per manifest — with **yes,
if the catalog is written directly**, and recorded what does not work:

| tried | result |
|---|---|
| register the mirror's deletion vector (one column, `p`) as a delete file | `count(*)` right, every scan fails: `Invalid schema contained in the delete file` |
| `DELETE` statements through DuckLake | correct, but a dropped file that had deletes is never retired (1 file in the manifest, 50 in the catalog), and 380 ms a publish |
| catalog rows written directly, delete files as `(file_path, pos)` | works; 129 ms a publish, 40 ms of it converting vectors |
| column-partial patch files | cannot be a DuckLake data file |

It left open: a catalog DuckDB does not have to write, concurrent readers,
snapshot expiry, more than one table, and a publisher that is not Python.

## What the Go publisher does differently

| prototype | `internal/ducklake` |
|---|---|
| DuckDB-file catalog, written through the DuckDB library | SQLite catalog (`ducklake:sqlite:`), written with a pure-Go driver; no cgo, no DuckDB in the sidecar |
| statistics borrowed from a scratch DuckLake | computed from the Parquet footer; compared with DuckDB's own in the tests |
| vector file re-read and copied by DuckDB | delete file written from the dead set the apply loop already holds |
| publishes when called | off the apply path, coalescing; reconciles at start |
| never expires anything | expires snapshots after a retention window and unlinks their files |
| refuses a manifest with patch files | turns partial deltas off, refuses the table in the lake (a reader gets the reason), starts the rewrite, publishes after it |
| halted table: raises | the lake table is replaced by a view that fails with the halt reason |

## `validate/`: the Go publisher against a real cluster

PostgreSQL 17 with pg_duckdb, a hot standby, the production `qs-mirror` and two
million rows. Run as root on a machine where the `postgres` user exists; every
process is under `/var/lib/postgresql/qslake`, ports 5570 to 5572.

```
validate/setup.sh                 # cluster + 2M rows
validate/sidecar.sh start on      # QS_DUCKLAKE=on; bootstraps by snapshot
validate/churn.sh 150 0.3         # 150 x (2,000 updates + 400 deletes + 400 inserts)
python3 validate/ticks.py /var/lib/postgresql/qslake/mirror.log   # apply-loop latency
taskset -c 1 python3 validate/read_bench.py /var/lib/postgresql/qslake/mirror
validate/verify.sh                # qs-verify, then the lake against the heap
validate/pg_view.sh 3             # a PostgreSQL view over the lake table, across publishes
validate/kill9.sh 4               # SIGKILL under load, restart, reconcile, verify
validate/sidecar.sh stop; validate/sidecar.sh wipe; validate/sidecar.sh start off   # the baseline
validate/teardown.sh
```

The conformance and concurrency tests are Go tests in the package itself and
need only `python3` with `duckdb` (they skip without it):

```
cd go && go test ./internal/ducklake/
QS_LAKE_STEPS=2000 go test -run TestConformance ./internal/ducklake/
QS_LAKE_CONCURRENT_SECONDS=240 go test -run TestConcurrentReaders ./internal/ducklake/
QS_LAKE_PUBLISHES=5000 go test -run TestCatalogSizeIsStable ./internal/ducklake/
```
