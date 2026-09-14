# Benchmark harness

Reproducible harness backing the spikes in [../docs/08-roadmap-and-spikes.md](../docs/08-roadmap-and-spikes.md).
Results live in `results/` and are analysed in [../docs/11-measured-results.md](../docs/11-measured-results.md).

## What each script answers

| Script | Spike / question | Claim under test |
|---|---|---|
| `scripts/wal_evidence.sh` | docs/03 core claim, OQ-8 | Physical WAL carries no row contents for DELETE, and prefix/suffix-compresses UPDATE. Also quantifies the WAL amplification of `wal_level=logical` and `REPLICA IDENTITY FULL`. |
| `scripts/export_parquet.py` | storage/compression | What the columnar mirror actually costs on disk, per column. |
| `scripts/run_bench.py` | **S2** | Analytical speedup *and* OLTP regression (goal G3). |
| `scripts/concurrency.py` | **OQ-13 / the `C` term** | Does the single-query speedup survive concurrency? This is what drives replica consolidation in docs/10, not the single-query number. |
| `scripts/merge_on_read.py` | **OQ-11** | Merge-on-read cost vs delta backlog — the "never benchmark a freshly compacted mirror" rule. |

## Rules (from docs/08)

1. Always measure against a real PostgreSQL baseline on identical hardware.
2. Always run with a live change stream — a static mirror is not the product.
3. Always report the OLTP-shaped queries too, especially when they look bad.
4. Always report lag alongside latency.

## Running it

Requires PostgreSQL 16 server binaries, Python 3 with `duckdb`, `pyarrow`,
`psycopg[binary]`, `pytz`. The cluster is expected on `/tmp` port 5433.

```bash
# 1. start postgres (see docs/11 for the exact settings used)
# 2. load data
psql -h /tmp -p 5433 -U postgres -f schema.sql
for i in $(seq 0 29); do
  psql -h /tmp -p 5433 -U postgres \
    -v start=$((i*1000000+1)) -v stop=$(((i+1)*1000000)) -f gen_data.sql
done
psql -h /tmp -p 5433 -U postgres \
  -c "ALTER TABLE events ADD PRIMARY KEY (event_id)" \
  -c "CREATE INDEX ON events (user_id, ts DESC)" \
  -c "CREATE INDEX ON events (session_id)" \
  -c "CREATE INDEX ON events (tenant_id, ts)" \
  -c "VACUUM ANALYZE events"

# 3. run
python3 scripts/export_parquet.py
python3 scripts/run_bench.py
python3 scripts/concurrency.py
python3 scripts/merge_on_read.py
bash    scripts/wal_evidence.sh
```

## Known biases in this harness

Stated up front so results aren't over-read:

- **The generated data is unrealistically compressible.** Low-cardinality text
  columns dictionary-encode to near-zero, so the 35x compression ratio measured
  here is optimistic. Real-world 4-10x is the number to plan with.
- **Both engines are effectively memory-resident** (8.7 GB heap, 15 GB RAM).
  This removes I/O from the comparison and is therefore *conservative* for the
  column store — a disk-bound workload favours it more.
- **4 vCPUs.** Both engines are parallelism-limited; a larger box changes the
  absolute numbers, and probably the ratio.
- **Single-table.** No joins across mirrored tables, which is where a real
  workload would stress the design harder.
