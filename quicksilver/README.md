# quicksilver — Phase 1 walking skeleton

A working vertical slice of the ingest → mirror → verify pipeline, run against a
live PostgreSQL.

**Status: reference implementation, not a shipped artifact.** Profiled at ~42,800
rows/s single-threaded, 74% of it Python-side work. The production data plane goes to
Rust and the CNPG-I plugin to Go — see [ADR-0008](../docs/adr/0008-implementation-language.md).
This code stays as the semantic oracle the port must match: it already encodes the
commit-boundary, deletion-vector and DDL-barrier semantics, and `run_slice`/`run_phase1`
become differential tests.

**Before any rewrite**, replace wal2json with `pgoutput`'s binary protocol: JSON parsing
is 56% of the pipeline, and removing it takes *this* Python code to ~96,900 rows/s — a
2.3× gain, larger and cheaper than changing language.

```
python3 -m quicksilver.run_slice --seconds 20
```

| Module | Role |
|---|---|
| `changestream.py` | The interface that makes phase 3 a front-end swap rather than a rewrite ([docs/03](../docs/03-wal-ingestion.md)). `LogicalChangeStream` is the first implementation. |
| `mirror.py` | Columnar writer, **deletion vectors**, compaction, checksum verification ([docs/04](../docs/04-storage-and-query-engine.md) as corrected by [docs/11 Result 5](../docs/11-measured-results.md)). |
| `ddl.py` | DDL as a **barrier in the change stream**, via a source event trigger. Safe changes applied, unsafe ones refused. |
| `health.py` | Freshness, readiness gating on `freshnessSLO`, Prometheus metrics (goal G4). |
| `run_slice.py` | End-to-end convergence proof under concurrent write load. |
| `run_phase1.py` | DDL barriers and readiness gating, tested under live writes. |

## What it establishes

- Transactions are cut on **COMMIT boundaries** and applied whole. Verified by a
  per-batch probe for rows visible in two tables mid-move — the failure mode
  [docs/06 §2](../docs/06-compatibility-and-semantics.md) is about.
- The mirror **converges exactly** — row count and order-independent checksum
  against the source (goal G7).
- Deletes and superseded rows resolve via **deletion vectors**, not a query-time
  `_lsn` window function.
- `applied_lsn` is fsynced **after** the data, and the slot is confirmed only
  after that, so a crash replays rather than loses.

## DDL handling

`pgoutput`/`wal2json` do not replicate DDL, so every CDC product hand-rolls schema
drift detection — and that is where they leak. Here an **event trigger** writes each
DDL statement into `quicksilver.ddl_log`, which is itself mirrored, so DDL arrives
**inside the change stream in commit order** and acts as a barrier: the ingest
applies up to and including the DDL transaction, confirms only that far, re-reads
the catalog, evolves, then continues.

The barrier must *stop the apply*, not merely be noticed. Applying post-DDL rows
with the pre-DDL column list silently drops the new column — row counts still match
and only the values are wrong, which is the worst shape of bug. Verified under live
writes: ADD COLUMN and DROP COLUMN both converge exactly; a type change is refused
and mirroring halts for that table rather than writing a lossy conversion.

Old Parquet files predating an ADD COLUMN are NULL-filled by the read path
(`_projection`), so there is no rewrite and no downtime.

## Readiness gating

`/readyz` returns 503 once staleness exceeds `freshnessSLO`, so Kubernetes drops the
pod from the Service endpoints rather than letting it serve stale answers quietly.
`/metrics` exposes lag, compaction backlog and a divergence flag.

## Known gaps (deliberate, not oversights)

- **Polling, not streaming.** Uses `pg_logical_slot_peek_changes` rather than the
  streaming replication protocol, which caps latency at the poll interval. The
  production version must stream.
- **The key→position index is a JSON dict.** Correct, and would not survive
  production volumes. This is the piece needing real engineering.
- **No DDL handling** — see the catalog/data split in [docs/02](../docs/02-prior-art.md).
- **No failover handling.** Slot sync on PG ≥ 17 is phase 2.
- Single-process, no readiness gating, no metrics export yet.

## Bugs found by running it

Recorded because each would have been silent:

1. **`numeric` via JSON float.** wal2json emits numerics as JSON numbers; routing
   money through `float64` rounds it. Fixed with `json.loads(parse_float=Decimal)`,
   which needs no plugin support (wal2json 2.5 has no numeric-as-string option).
2. **Schema drift.** `pa.Table.from_pylist` infers per batch, so an all-NULL column
   becomes `null`-typed and the union read path breaks. Fixed with an explicit
   Arrow schema derived from declared Postgres types.
3. **Empty take.** When every row of a micro-batch is superseded, `pa.array([])`
   infers null-typed indices and `take` raises.
4. **Confirming at `commit_lsn` stalls the stream permanently.** Postgres'
   `upto_lsn` stops *before* the record at that LSN, so the transaction is re-read
   forever and everything behind it is blocked. Must confirm through wal2json's
   `nextlsn`. Masked in `run_slice` because re-applying the same upsert is
   idempotent — it only became visible once a DDL barrier cut a batch exactly on a
   commit boundary.
5. **Readiness keyed off "time since last apply" makes an idle database look
   stale.** A source with no writes produces no applies, so a perfectly fresh mirror
   would fail its readiness probe and be pulled from the endpoints for no reason.
   Staleness must be measured from when the mirror was last *caught up*, which an
   empty poll establishes.
