# 06 — Compatibility and semantics

The honest document. Where the "drop-in read replica" illusion holds, where it leaks, and
where it breaks outright.

Treat this as the risk register for goals **G1 (drop-in)** and **G3 (no regression)**.

---

## 1. The performance shape

The pitch is "faster by default". That is true on average and false pointwise, and the
distinction is the whole ballgame.

| Query shape | Hot standby today | Columnar mirror | Verdict |
|---|---|---|---|
| `SELECT ... WHERE id = $1` | ~0.2 ms, index lookup | 5–50 ms — no B-tree; scan a row group or probe a zone map | **3–100× slower** |
| `SELECT ... WHERE user_id = $1 ORDER BY ts DESC LIMIT 20` | ~1 ms, index scan | 10–100 ms unless clustered on `user_id` | **Much slower** |
| 1000 concurrent short reads | Fine; process-per-connection | DuckDB memory + thread contention | **Worse, possibly much worse** |
| `SELECT count(*), sum(x) ... GROUP BY d WHERE ts > now()-30d` | 20 s, seq scan or bitmap | 0.2 s, vectorised, 3 columns read | **10–100× faster** |
| Wide scan, 3 of 60 columns | Reads all 60 (8 KB pages) | Reads 3, compressed 4–10× | **10–50× faster** |
| Multi-table analytical join | Hash join, tuple-at-a-time | Vectorised | **5–30× faster** |
| `COPY ... TO` of a large result | Decent | Excellent | **Faster** |

**The implication is architectural, not a tuning detail.** A `-ro` endpoint that is
columnar-only will regress every application that uses it for anything other than reporting
— and most do, because `-ro` is where teams send "reads that don't need to be on the
primary", which includes plenty of point lookups.

Two responses, and we should do both:

1. **Measure before switching.** `shadow` service mode (see
   [05](05-cnpg-integration.md#the--ro-takeover-and-why-it-should-be-opt-in)) replays sampled
   `-ro` traffic against the mirror and reports the per-query delta. This is spike **S0**,
   and it gates the project: if a representative workload is 90% point lookups, Architecture
   A has no viable default and we go straight to C or stop.
2. **Keep both engines.** Architecture C's hybrid node makes the fast path *identical* to
   today rather than merely acceptable. This is the strongest argument for C and the reason
   it is the target rather than a nice-to-have.

---

## 2. Consistency and visibility

| Property | CNPG hot standby | Quicksilver mirror |
|---|---|---|
| Snapshot semantics | True MVCC snapshot; every query sees a consistent point in time | Consistent **only if** we cut batches on LSN boundaries — otherwise a query can see a partially-applied transaction |
| Staleness | Replication lag, typically ms | Ingest + batch + compaction lag; seconds by design |
| Read-your-writes | Not guaranteed (already) | Not guaranteed, and worse |
| Cross-table consistency | Guaranteed | **Only if batches are cut on transaction boundaries** |

**The cross-table point is a correctness requirement, not a tuning knob.** If the mirror
applies half of a transaction that moved a row from `orders` to `orders_archive`, a join
across both sees the row twice or not at all. The change stream must therefore carry commit
boundaries and the writer must make batches atomic at those boundaries. Getting this wrong
produces bugs that appear once a week and are unreproducible — the worst possible failure
class. Design it in from the first commit; do not retrofit.

**Mitigations to build:**
- Expose `quicksilver.applied_lsn()` and a `quicksilver.lag` view so applications can assert
  freshness when they care.
- Readiness gating on `freshnessSLO` (G4), so a lagging node leaves the endpoint.
- Continuous per-table checksum verification against the source (G7).

---

## 3. SQL surface

Under Architecture A the data exists *only* as Parquet, so anything DuckDB cannot execute is
an **error**, not a slow path. Under Architecture C the row-store copy exists, so
unsupported constructs degrade to normal Postgres speed — strictly better, and another point
for C.

| Area | Risk | Notes |
|---|---|---|
| Core SQL, joins, aggregates, window functions, CTEs | **Low** | DuckDB's core is strong. |
| `numeric` exact semantics | **Medium** | Postgres `numeric` is arbitrary-precision decimal. Mapping to DECIMAL128 loses range; to DOUBLE loses exactness and silently changes financial results. Must be tested explicitly. |
| `jsonb` | **Medium-High** | Operators (`->`, `->>`, `@>`, `?`), `jsonb_path_query`, GIN-backed containment. Partial coverage at best. |
| Arrays, ranges, enums, domains, composite types | **Medium-High** | Each needs an explicit mapping decision and a fallback story. |
| `tsvector` / full-text search | **High** | Effectively unsupported. Must fall back or be excluded from mirroring. |
| PostGIS | **High** | Out of scope for v1. Exclude such tables explicitly. |
| User-defined functions (PL/pgSQL etc.) | **High** | Cannot run inside DuckDB. Queries calling them must fall back. |
| Stable/volatile functions, `now()`, `current_user` | **Medium** | Semantics must match. |
| `SELECT ... FOR UPDATE/SHARE` | **N/A** | Meaningless on a read mirror; must produce a clear error. |
| Cursors, prepared statements, extended protocol | **Medium** | Driver-level. Must work or ORMs break immediately. |
| `pg_catalog` introspection | **Medium** | Real, because the front end is real Postgres — but mirrored tables must appear with correct types, or `\d` and every migration tool misreport. |

**Design rule:** mirroring is **opt-in per table** (`tables:` / `exclude:` in the plugin
parameters) and the plugin must **refuse to mirror tables whose types it cannot faithfully
represent**, loudly, at admission time. A table that is silently mirrored with lossy types
is worse than one that isn't mirrored at all.

---

## 4. Security

Non-negotiable, and easy to get catastrophically wrong. The mirror is a **complete copy of
production data** in a new place with a new query engine.

| Control | Requirement |
|---|---|
| Roles and grants | Must mirror the source. A user who cannot `SELECT` a column on the primary must not be able to on the mirror. |
| Row-level security | **The hard one.** RLS policies are enforced by the Postgres executor. If DuckDB executes the scan, **RLS may be bypassed entirely.** Until proven otherwise, assume it is. |
| Column-level grants | Same exposure as RLS. |
| `pg_hba` / TLS | Mirror pods need equivalent connection policy to instance pods. |
| Encryption at rest | Mirror PVCs and any object-store bucket need the same posture as the cluster's. |
| Audit | Queries against the mirror must be auditable alongside the primary's. |

**Hard gate:** until RLS and column-privilege enforcement over `pg_duckdb`-executed scans is
*verified by test*, the plugin must **refuse to mirror any table with RLS enabled or
non-trivial column grants**, and say so clearly. This belongs in
`ValidateClusterCreate`. Spike **S3** exists to determine whether the refusal can later be
lifted; assume it cannot until measured.

---

## 5. Operational semantics

| Scenario | Behaviour we must deliver |
|---|---|
| Mirror falls behind SLO | Readiness fails; pod leaves endpoints; metric + event; **no stale answers served silently**. |
| Mirror diverges from source | Checksum verification fails → alarm → automatic re-sync of the affected table; never silent. |
| Primary failover | Lag spike, drain, recover. Logical: slot follows (PG ≥ 17) or re-snapshot. Physical: follow timeline. |
| DDL on a mirrored table | Detect, apply to mirror schema, barrier the stream (walshadow's approach). Unsupported DDL → stop mirroring that table loudly rather than corrupt it. |
| Table added matching `tables:` pattern | Auto-bootstrap via snapshot + catch-up. |
| Mirror pod lost | Rebuild from archive tee + snapshot; must not require primary involvement. |
| Disk pressure on mirror | Compaction backlog is a first-class metric; must degrade before it fails. |

---

## 6. The summary judgement

Quicksilver is **not** a transparent read replica and should never be marketed as one. It is:

> A bounded-staleness, opt-in-per-table, analytics-optimised mirror that speaks Postgres,
> lives inside your CNPG cluster, and is safe to put behind a read endpoint **for query
> shapes it is good at**.

Under **Architecture C** that description tightens considerably — the row store is still
there, so the fallbacks are fast and the unsupported constructs merely run at today's speed.
That is the difference between a product that requires a per-table audit before adoption and
one that can plausibly be turned on by default.

Which is why C is the target, and why the honest version of the pitch in the README is worth
more than the exciting one.
