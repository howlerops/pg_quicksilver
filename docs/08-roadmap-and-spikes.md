# 08 — Roadmap, spikes and kill criteria

The purpose of this document is to make the project **falsifiable**. Each spike has a
question, a method, a threshold and an explicit consequence for failing it — including
consequences that say "stop".

---

## Phase 0 — De-risking spikes (≈4–6 weeks, no product code)

### Status

| Spike | State | Notes |
|---|---|---|
| **S0** workload characterisation | ⬜ **not started — gates everything** | Needs `pg_stat_statements` from real clusters. Nothing else should be funded past prototype until this returns. |
| **S1** CNPG-I capability probe | ⬜ not started | Needs a Kubernetes cluster. |
| **S2** columnar speedup | ✅ **PASSED** | Median 29.9×. [11](11-measured-results.md) |
| **S2b** speedup through `pg_duckdb` | 🟥 **NEW — gap in S2's evidence** | S2 measured **standalone DuckDB**, which bypasses the Postgres executor entirely. The product requires a Postgres front-end. Until this is run, the headline 30× is not a product number. |
| **S3** RLS enforcement | ✅ **PASSED** (with a catch) | RLS and column grants **are** enforced. But ordinary roles cannot reach the Parquet mirror without a dangerous grant — see [11 Result 7](11-measured-results.md#result-7--s3-rls-holds-but-ordinary-roles-cannot-reach-the-mirror-at-all). |
| **S4** physical decode on standby | ⬜ not started | The Architecture C linchpin. Deliberately last. |
| **S5** serving path for the mirror | 🟥 **NEW — blocking** | Result 7 closed off `read_parquet` for ordinary roles. Options: upstream a directory-confined file-access GUC (cheapest), a TAM, or an FDW. Must be scoped before phase 1. |

**S2b is the correction that came out of running S2.** The benchmark answered "is a column
store faster than Postgres on this data" — decisively yes. It did *not* answer "is
Postgres-fronted DuckDB faster than Postgres", which is the actual product claim. The gap
between those two is planner integration, type marshalling across the extension boundary,
per-backend DuckDB instantiation cost, and fallback behaviour. Any of them could erode the
ratio. Treat the 30× as an **upper bound** until S2b lands.

### S0 — Workload characterisation ⭐ **gates the entire project**

**Question:** what fraction of real `-ro` traffic is analytical (columnar wins) vs
OLTP-shaped (columnar loses)?

**Method:** `pg_stat_statements` + sampled `log_min_duration_statement` from 3–5 real CNPG
clusters. Classify each statement by rows scanned, columns projected, presence of aggregation,
and call frequency. Weight by total time *and* by call count — they give different answers and
both matter.

**Threshold:** ≥40% of `-ro` *time* in analytical shapes.

**If it fails:** the "replace read replicas" framing is wrong. Either pivot to Architecture C
immediately (the hybrid node is the only design that survives an OLTP-heavy mix), or pivot the
product to an explicit `app-olap` endpoint and drop the drop-in claim. **Do not proceed to S2
on hope.**

*This is the one spike that can cheaply prevent a wasted quarter. Run it first.*

### S1 — CNPG-I capability probe

**Question:** can a plugin actually (a) retarget the `-ro` Service selector durably across
reconcile loops, (b) create and reconcile a non-instance StatefulSet, (c) tee WAL archives
alongside an existing backup plugin?

**Method:** fork `cnpg-i-hello-world`; implement `OperatorLifecycle` on `Service`,
`ReconcilerHooks.Post` creating a dummy StatefulSet, and a no-op `WAL.Archive`. Run against
CNPG on kind. Verify the Service selector survives a forced reconcile and an operator restart.

**Threshold:** (a) and (b) work. (c) is **expected to fail or conflict** — see
[05](05-cnpg-integration.md#what-cnpg-i-cannot-do), OQ-3.

**If (a) fails:** `serviceMode: takeover` is impossible via CNPG-I; fall back to a separate
service + documented manual cutover, or a mutating webhook outside CNPG-I.
**If (b) fails:** the mirror tier must be a sidecar in instance pods (Architecture A becomes
A′), which changes the resource-isolation story significantly.
**If (c) fails:** Path 1 archive tee needs another mechanism — most likely reading from the
object store the existing archiver already writes to. Cheap to redesign, but plan for it.

### S2 — Columnar speedup, honestly measured  ✅ **PASSED** — see [11](11-measured-results.md)

**Question:** does pg_duckdb-over-Parquet deliver ≥10× p95 on a realistic suite *including*
merge-on-read cost?

**Method:** 100 GB dataset; ClickBench + TPC-H + 10 queries taken from S0's real traffic.
Measure against a like-for-like CNPG hot standby on identical hardware. **Critically:** run
with a continuous change stream applied, so pending deltas and tombstones are realistic —
benchmarking a freshly compacted mirror is self-deception.

**Threshold:** ≥10× p95 on analytical queries with a steady-state delta backlog.

**Result: PASSED.** Median 29.9× on the analytical suite (range 5.8×–68.3×) at 30 M rows.
Merge-on-read overhead is ~1× once deletion vectors replace query-time `_lsn` resolution —
the original docs/04 design cost 33× and would have cancelled the entire speedup. The OLTP
suite regressed by a median of 935×, which is the finding that makes Architecture C
mandatory rather than merely preferable.

**If it fails:** if the gap is compaction tuning, iterate. If DuckDB itself is the limit,
reconsider the engine (ClickHouse as an embedded/sidecar sink) — but note this reopens the
wire-protocol problem from [04](04-storage-and-query-engine.md).

### S2b — Does the speedup survive a Postgres front-end? 🟥 **new, blocking**

**Question:** S2 measured standalone DuckDB. Does the ratio hold when the query arrives over
the Postgres wire protocol, is planned by Postgres, and is executed through `pg_duckdb`?

**Method:** build `pg_duckdb` against PG 16 (this also validates the derived-image strategy
in [07](07-image-strategy.md)). Re-run the full S2 analytical and OLTP suites unchanged,
through `psql`, against both Postgres-heap tables and Parquet. Compare to the standalone
numbers in [11](11-measured-results.md).

**Also capture, because these are the ways it erodes:**
- per-backend DuckDB instantiation cost (does every new connection pay a startup tax?)
- type marshalling across the extension boundary, especially `numeric` and `timestamptz`
- which of the six analytical queries silently fall back to the Postgres executor
- memory per backend at concurrency — the thing that decides pods-per-cluster sizing

**Threshold:** ≥10× median on the analytical suite — i.e. S2's own threshold, re-tested
honestly. Erosion from 30× to, say, 20× is expected and fine.

**If it fails:** if fallback is the cause, the per-table opt-in gate from
[06](06-compatibility-and-semantics.md#3-sql-surface) has to get much stricter. If the
extension boundary itself is the tax, reconsider the serving topology — possibly DuckDB as a
sidecar process rather than in-backend. Either way this is cheaper to learn now than in
phase 1.

### S3 — Security enforcement

**Question:** are RLS policies and column-level grants enforced when the scan executes in
DuckDB rather than the Postgres executor?

**Method:** table with RLS + per-column grants; query as a restricted role via `pg_duckdb`;
attempt to read denied rows and columns.

**Threshold:** enforced, or reliably detectable so we can refuse to mirror such tables.

**If it fails:** the `ValidateClusterCreate` refusal from
[06](06-compatibility-and-semantics.md#4-security) becomes permanent. That narrows the
addressable market — many production tables use RLS — and must be stated plainly in the
README rather than discovered by a user.

### S4 — Physical decode on a standby (the Architecture C linchpin)

**Question:** can a background worker on a hot standby read WAL and resolve heap TIDs against
the local heap, reliably, without falling behind or racing replay?

**Method:** minimal `shared_preload_libraries` bgworker using `XLogReader` (`pg_walinspect`
as the reference). Decode `XLOG_HEAP_INSERT` / `_DELETE` / `_UPDATE` / `_HOT_UPDATE` /
`XLOG_HEAP2_MULTI_INSERT`; resolve old tuples for deletes and prefix/suffix-compressed
updates from the local heap; emit a change stream to stdout. Validate against logical
decoding of the same workload — **byte-identical row sets or it doesn't count.**

**Threshold:** correct output under a mixed OLTP workload including bulk updates, HOT updates,
TOASTed columns, vacuum activity and a checkpoint-heavy period (to force FPIs); sustained
throughput within 2× of the source's change rate.

**If it fails:** Architecture C's ingest is off the table; the target becomes Architecture A
with a logical slot permanently, and the "no load on primary" claim (G5) is dropped from the
pitch. The product still works; it is just less differentiated.

*This is the highest-variance spike. It is deliberately scheduled late, after the product is
proven, so a failure costs a feature rather than the company.*

---

## Phase 1 — Walking skeleton (≈1 quarter)

Goal: **end-to-end, one table, measurably faster, in a real cluster.**

- Internal **change-stream interface** defined first ([03](03-wal-ingestion.md#recommendation)) —
  this is the decision that makes phase 3 a front-end swap rather than a rewrite.
- Logical replication front-end (Path 2).
- Columnar writer: micro-batch → Parquet, `_lsn` on every row, **commit-boundary-atomic
  batches** ([06](06-compatibility-and-semantics.md#2-consistency-and-visibility)).
- Custom image ([07](07-image-strategy.md)), mirror tier only.
- CNPG-I plugin: Identity, Operator (validate + status), Postgres (`EnrichConfiguration`),
  ReconcilerHooks (mirror StatefulSet), Metrics.
- `serviceMode: off` — mirror on a separate `app-olap` service only.
- **Readiness gating on `freshnessSLO`** (G4). Non-negotiable in phase 1; retrofitting a
  safety property is how you get an incident first.
- Lag and freshness metrics; `Cluster.status` integration.

**Exit criterion:** one real table mirrored, ≥10× on its analytical queries, lag under 10 s,
survives a CNPG switchover without divergence.

## Phase 2 — Production-credible (≈1 quarter)

- Multi-table, pattern matching, auto-bootstrap of newly matching tables.
- DDL handling via the catalog/data split (walshadow's design,
  [02](02-prior-art.md#what-to-steal)).
- Compaction + merge-on-read, with compaction backlog as a first-class metric.
- **Continuous checksum verification** (G7) + automatic per-table re-sync on divergence.
- Archive tee (Path 1) for backfill, repair and cold bootstrap.
- `serviceMode: shadow` — traffic replay and comparison. Doubles as the correctness oracle.
- Failover hardening: slot sync on PG ≥ 17, the full lifecycle matrix from
  [05](05-cnpg-integration.md#sequencing-against-cnpg-lifecycle-events-goal-g6) as tests.
- Helm charts; docs; the compatibility matrix published honestly.

**Exit criterion:** a design partner runs it against production traffic in `shadow` mode for
30 days with zero divergence alarms.

## Phase 3 — The differentiated bet (≈2 quarters, gated on S4 and on phase 2 traction)

- Physical WAL ingest on-standby (Path 3c) behind `ingest: physical`.
- Hybrid node: row store + column store co-located.
- Planner routing — GUC → heuristic → cost-based, in that order.
- Admission control / scheduling for mixed workloads (the pgrust scheduler problem).
- `serviceMode: takeover` becomes defensible as a default.

**Do not start phase 3 before phase 2 has a happy design partner.** The physical-WAL work is
the interesting engineering and will therefore attract effort prematurely. It is worthless if
nobody wants the phase-1 product.

## Phase 4 — Optional

- Additional sinks (ClickHouse, Iceberg-for-external-engines) for teams that already run them.
- Multi-node mirror scale-out.
- Quicksilver nodes as valid HA failover targets (a natural consequence of Architecture C, and
  the thing that changes the cost conversation from "2× storage" to "free").

---

## Benchmark harness (build in phase 0, keep forever)

A single reproducible harness used by every spike and every release. Without this, "faster"
is an anecdote.

```
bench/
├── datasets/       clickbench, tpch-sf100, synthetic-wide (60 cols), and an S0-derived trace
├── workloads/      analytical.sql, oltp.sql, mixed.sql (weighted by S0 findings)
├── runners/        cnpg-standby (baseline), quicksilver-A, quicksilver-C
└── report/         p50/p95/p99 per query, per runner, plus lag and compaction backlog
```

Rules, because each has a corresponding way to accidentally cheat:

1. **Always measure against a CNPG hot standby on identical hardware.** Absolute numbers are
   marketing; the ratio is the product.
2. **Always run with a live change stream.** A static mirror is not the product.
3. **Always report the OLTP-shaped queries too**, even when they look bad. Especially when
   they look bad — G3 is a hard constraint and hiding its violations from ourselves is how we
   ship a regression.
4. **Always report lag alongside latency.** A mirror that is fast because it is 10 minutes
   stale has not achieved anything.

---

## Summary of kill criteria

| Spike | Fails if | Consequence |
|---|---|---|
| **S0** | <40% of `-ro` time is analytical | **Reframe or stop.** The premise is wrong. |
| **S1(a)** | `-ro` selector can't be retargeted | Drop `takeover` mode; separate endpoint only |
| **S1(b)** | Can't manage non-instance workloads | Mirror becomes a sidecar; resource isolation suffers |
| **S1(c)** | WAL tee conflicts with backup plugin | Redesign Path 1 around the object store |
| **S2** | <10× on analytical with live deltas | ✅ **PASSED** — median 29.9×, merge overhead ~1× with deletion vectors |
| **S3** | RLS bypassed and undetectable | ✅ **PASSED** — RLS and column grants enforced; the docs/06 refusal is lifted |
| **S5** | No safe serving path exists | Architecture A cannot ship; Quicksilver must own the columnar access path |
| **S4** | Physical decode unreliable on standby | Architecture C ingest dropped; G5 dropped from the pitch |

The project is worth doing if S0 and S2 pass. It is *differentiated* if S4 also passes.
Those are different bets and should be funded separately.
