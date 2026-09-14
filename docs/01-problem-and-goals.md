# 01 — Problem, goals and non-goals

## The problem we are attacking

A typical CloudNativePG cluster looks like this:

```
Cluster/app (instances: 3)
  ├── app-1  primary        ← writes, and whatever reads didn't get routed away
  ├── app-2  hot standby    ← HA failover target, serves `-ro`
  └── app-3  hot standby    ← serves `-ro`
```

Instances 2 and 3 each hold a **complete physical copy of the data**, continuously updated,
consuming a full replica's worth of CPU, memory, storage and IOPS. What do they buy?

- **HA.** Real and valuable — one standby is genuinely load-bearing. But you only need one
  or two for HA; the third, fourth and fifth exist purely for read scale-out.
- **Read throughput.** Real, but only linear: each standby answers the same queries the
  primary would, at the same per-query speed. Adding a replica does not make the slow
  query faster. It just lets you run more of them concurrently.

That second point is the wedge. The queries that make people add read replicas are
overwhelmingly **analytical**: dashboards, reporting, `GROUP BY` over a date range,
funnel/cohort queries, `COUNT(*) FILTER (...)` across millions of rows, exports. Postgres
answers these with a row-store heap and B-trees, reading whole 8 KB pages to get at three
columns, with a tuple-at-a-time executor. A column store with a vectorised executor answers
the same query one to two orders of magnitude faster on the same hardware.

So the proposition: **the copy of the data is already being paid for. Change what shape it
is stored in.**

## Why this is a CNPG plugin and not "just run ClickHouse"

Teams already can run ClickHouse, DuckDB, Snowflake, or a Debezium→Iceberg pipeline next to
Postgres. Most don't, because the cost isn't the engine — it's everything around it:

1. **A second system to operate.** Separate HA, backup, upgrade, monitoring, access control.
2. **A pipeline to own.** CDC connectors are a permanent on-call surface: slot lag, schema
   drift, failover gaps, silent divergence.
3. **A second dialect and driver.** Application code, ORMs, BI tool connections and
   migrations all fork.
4. **A consistency story nobody can explain.** "Is the dashboard stale? By how much? Since
   when?" becomes an unanswerable question.

Quicksilver's bet is that the value is in **erasing items 1–4**, not in the engine. If you
declare it in the `Cluster` spec, the operator owns its lifecycle, it speaks the Postgres
wire protocol on the endpoint your app already uses, and its staleness is a first-class
observable metric with a readiness gate — then and only then does the speedup get consumed
by ordinary application teams rather than by a data platform team.

```yaml
# The entire user-facing surface we are aiming for
spec:
  plugins:
    - name: quicksilver.howlerops.io
      enabled: true
      parameters:
        replicas: "2"
        tables: "public.events,public.orders,analytics.*"
        freshnessSLO: "5s"
```

## Goals

**G1 — Drop-in at the connection level.** An existing application pointed at
`app-ro.namespace.svc:5432` with an unmodified Postgres driver continues to work. Same
protocol, same dialect, same catalog introspection, same table names.

**G2 — Large, *default* speedup on analytical reads.** Target ≥10× p95 on a
TPC-H/ClickBench-shaped suite at 100 GB, with no query rewriting, no hints, no separate
schema, and no user-visible "columnstore table" concept.

**G3 — No regression on non-analytical reads.** This is a hard constraint, not a nice-to-
have. Traffic on `-ro` is a mix; a design that speeds up 20% of queries by 50× and slows
down the other 80% by 3× is a net loss and will be rejected in production. See
[06](06-compatibility-and-semantics.md).

**G4 — Bounded, observable, enforced staleness.** Lag must be exported as a metric, exposed
in `Cluster.status`, and wired to the pod's readiness probe so a lagging mirror is pulled
out of the service endpoint rather than silently serving stale answers.

**G5 — Negligible additional load on the primary.** Ideally the primary sees nothing it
isn't already doing for physical replication. A logical decoding slot is an acceptable
*interim* cost; a permanent one is not (see [03](03-wal-ingestion.md#cost-on-the-primary)).

**G6 — Survives CNPG lifecycle events.** Switchover, failover, rolling minor upgrades,
`pg_basebackup` re-bootstrap of an instance, scale up/down, and node drain must all leave
the mirror correct — repairing automatically if necessary, but never silently diverging.

**G7 — Correctness is auditable.** There must be a cheap, continuous way to prove the mirror
matches the source (per-table checksums over LSN-consistent snapshots), and a loud alarm
when it doesn't. Silent divergence is the failure mode that kills CDC products.

## Non-goals

| Non-goal | Why |
|---|---|
| Accepting writes | Quicksilver is read-path only. Write routing is a proxy problem, not ours. |
| Full ACID/MVCC equivalence with the primary | The mirror is eventually consistent by construction. We make the staleness *bounded and visible*, not zero. |
| Being a general lakehouse | If Iceberg/DuckLake output falls out of the design, good — but "make your data queryable by Spark" is not the pitch. |
| Multi-source ingest | One CNPG cluster in. No cross-database joins, no heterogeneous sources. |
| Replacing HA standbys | At least one ordinary standby remains the failover target. Quicksilver nodes are read scale-out, and (in Architecture C) may *also* be valid failover targets — but that's a bonus, not a requirement. |
| Postgres major-version heterogeneity | Mirror nodes run the same major version as the source. Physical WAL formats are version-specific and this is not negotiable for Architecture C. |

## Freshness budget

Freshness is a *design input*, not an output. Different targets imply genuinely different
architectures, and picking the wrong one wastes a quarter.

| Target | Achievable via | Cost |
|---|---|---|
| **Minutes** | Tee archived WAL segments via the CNPG-I `WAL.Archive` hook; batch-load | Trivial. Zero primary impact. Bounded below by `archive_timeout`. |
| **~1–10 s** | Logical replication slot → micro-batched columnar writes | Moderate. Slot on primary, single-threaded decode, failover-slot handling. |
| **< 1 s** | Physical WAL applied in lockstep with standby replay | Hard. Requires TID-addressed heap state — i.e. Architecture C. |

**Decision: v1 targets 1–10 s, configurable, defaulting to 5 s.** Sub-second is the target
for Architecture C but is explicitly not a v1 requirement. Committing to sub-second up front
forces the hardest architecture first and there is no way to ship incrementally from there.

## Success criteria for the study (not the product)

This study is done when we can answer, with evidence rather than argument:

1. Can a CNPG-I plugin actually take over the `-ro` service endpoint and manage non-instance
   pods? (→ [05](05-cnpg-integration.md), spike S1)
2. What fraction of a realistic `-ro` workload is analytical vs OLTP-shaped? (→ spike S0 — *this gates
   the whole project*)
3. Does pg_duckdb-over-Parquet actually deliver ≥10× on our suite, including the merge-on-read
   cost of pending deletes? (→ spike S2)
4. Can a background worker on a hot standby resolve heap TIDs from the local heap fast enough
   and safely enough to build the mirror in-process? (→ spike S4, the Architecture C linchpin)

Kill criteria for each are in [08](08-roadmap-and-spikes.md).
