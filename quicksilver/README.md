# quicksilver — Phase 1 walking skeleton

A working vertical slice of the ingest → mirror → verify pipeline, run against a
live PostgreSQL. **Prototype**: the production implementation belongs in Go (CNPG-I
is Go) or Rust. This exists to pin the interface and prove the pipeline converges.

```
python3 -m quicksilver.run_slice --seconds 20
```

| Module | Role |
|---|---|
| `changestream.py` | The interface that makes phase 3 a front-end swap rather than a rewrite ([docs/03](../docs/03-wal-ingestion.md)). `LogicalChangeStream` is the first implementation. |
| `mirror.py` | Columnar writer, **deletion vectors**, compaction, checksum verification ([docs/04](../docs/04-storage-and-query-engine.md) as corrected by [docs/11 Result 5](../docs/11-measured-results.md)). |
| `run_slice.py` | End-to-end proof under concurrent write load. |

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
