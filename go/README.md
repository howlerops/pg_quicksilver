# quicksilver (Go)

The production data plane, per [ADR-0009](../docs/adr/0009-implementation-language-revised.md).
Go **1.27.1**.

```bash
go run ./cmd/qs-slice  -seconds 20     # convergence under concurrent writes
go run ./cmd/qs-phase1 -slo 3          # DDL barriers + readiness gating
```

| Package | Role |
|---|---|
| `internal/changestream` | The seam every ingest path produces into. `Logical` is the first implementation; the phase 3 physical-WAL decoder (C/Rust bgworker) emits across this same boundary as a **process**, which is why nothing is written twice. |
| `internal/mirror` | Columnar writer, deletion vectors, compaction, order-independent checksum. |
| `internal/ddl` | DDL as a barrier in the change stream, via a source event trigger. |
| `internal/health` | Freshness, readiness gating on `freshnessSLO`, Prometheus metrics. |

## Parity with the Python reference

Both harnesses pass the identical tests, which is the point — the Python
implementation stays as the semantic oracle ([`../quicksilver/`](../quicksilver/)).

| | Python | Go |
|---|---|---|
| convergence under concurrent writes | PASS | PASS |
| cross-table atomicity (per-batch probe) | 0 violations | 0 violations |
| DDL barrier: ADD / DROP COLUMN | converges | converges |
| DDL: unsafe retype refused | yes | yes |
| readiness flips past SLO / recovers | yes | yes |
| **median apply latency, same workload** | **38.7 ms** | **22.0 ms** |

## Two things this port does better than the reference

**No query engine in the data plane.** The Python version used DuckDB to read
its own Parquet back for verification. Here `mirror.Live()` reads Parquet
directly via arrow-go. A writer should not need a query engine, and removing it
drops a heavyweight CGO dependency from the ingest image.

**Exact decimals are free.** Python needed `parse_float=Decimal`, which disables
CPython's C JSON scanner. Here numerics are simply never unmarshalled into a
float — `decodeValue` keeps the exact decimal text and the writer parses it
against the column's declared scale.

## Phase 2 — streaming, snapshot, failover

`go run ./cmd/qs-phase2` — see [docs/14](../docs/14-phase2-streaming-and-failover.md).

- **`internal/changestream/pgoutput.go`** — `pgoutput` over `START_REPLICATION`.
  No wal2json, no `output_plugin_libraries` allowlisting, no poll-interval floor.
  Measured **36–40 ms** commit-to-visible.
- **`internal/mirror/snapshot.go`** — bootstrap at a slot's consistent point, so
  snapshot and stream join with neither gap nor overlap.
- **Failover** — verified against a real `pg_basebackup` standby promotion. The
  logical slot does **not** survive on PG 16; the mirror detects it, re-snapshots
  against the new primary, and converges.

## Still to do

- the key→position index is still a JSON map; it needs real engineering at
  production volumes
- PG ≥ 17 slot failover (`failover=true` + `sync_replication_slots`) to avoid
  the re-snapshot after promotion
- parallel decode; currently one goroutine receives and one applies
