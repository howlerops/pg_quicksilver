# 04 — Storage and query engine

Given a change stream (from [03](03-wal-ingestion.md)), two questions remain: **what do we
store the mirror as**, and **what executes the query**?

---

## The constraint that eliminates most options

Goal G1 says an unmodified application, using an unmodified Postgres driver, pointed at the
same `-ro` service, keeps working. That is not a soft preference — it is the entire reason
this is a CNPG plugin rather than "go run ClickHouse". It means whatever serves the query
must provide:

- the **Postgres wire protocol v3**, including the extended query protocol, prepared
  statements, portals, cursors, `COPY`, and correct error codes;
- the **Postgres SQL dialect** as the application's ORM emits it;
- a **credible `pg_catalog`**, because ORMs, migration tools, `psql \d`, and every BI tool
  introspect it;
- **roles, grants and RLS** matching the source, or the mirror becomes a data-exfiltration
  path around the primary's access control.

Reimplementing that surface is a multi-year project (see pgrust in
[02](02-prior-art.md#malisperpgrust) — a serious, well-resourced effort that is at v0.2 and
says "do not put data you care about in it"). Putting a translating proxy in front of
ClickHouse is a smaller version of the same trap: proxies that speak *enough* Postgres to
fool `psql` reliably fail on the third ORM.

**Conclusion: the thing serving queries is a real PostgreSQL backend.** Everything below is
about what happens *underneath* it.

---

## Option A — pg_duckdb over Parquet/DuckLake

Real Postgres process; DuckDB embedded via [`pg_duckdb`](https://github.com/duckdb/pg_duckdb)
(MIT); mirror data as Parquet, catalogued by DuckLake or Iceberg, on a local PVC and/or
object store.

```
client ──PG wire──> postgres backend
                      ├── pg_catalog, roles, RLS, planner    (real Postgres)
                      └── pg_duckdb ──> DuckDB vectorised exec ──> Parquet on PVC/S3
```

**For:**
- Wire protocol, dialect and catalog are genuine, because the front end genuinely is Postgres.
- `pg_duckdb` is actively maintained by DuckDB Labs and MotherDuck, MIT-licensed, >3 k stars,
  >1 M downloads — a dependency with a real future, unlike `pg_analytics` (archived) or
  `pg_mooncake` (acquired).
- Parquet is portable. If Quicksilver dies, the customer's data is still readable.
- DuckLake's catalog-in-a-SQL-database design is an unusually good fit: **that SQL database
  can be the Quicksilver node's own Postgres**, collapsing catalog and serving into one
  process with one backup story.
- Embedded engine → no network hop, no second cluster, no second HA story.

**Against:**
- Query coverage is not 100%. `pg_duckdb` falls back to the Postgres executor for
  unsupported constructs — which is fine when the data is in a Postgres heap and **fatal
  when it only exists as Parquet**. Any construct DuckDB can't execute becomes an error
  rather than a slow query. This is the single largest correctness risk in Option A and is
  spike S2's main job to quantify.
- Type-system edges: `numeric` semantics, arrays, `jsonb`, enums, ranges, domains, PostGIS.
- DuckDB is single-process and memory-hungry; concurrency control on a node serving many
  dashboards needs real attention (cf. pgrust's scheduler).
- Merge-on-read cost: pending deletes/updates must be applied at query time until compaction
  catches up. Benchmarks that ignore this are lying.

---

## Option B — external ClickHouse

Mirror lives in a separate ClickHouse cluster; something translates Postgres queries to it.

**For:** best-in-class OLAP performance; walshadow already targets exactly this sink;
horizontal scale-out is a solved problem there.

**Against:** fails G1 at the first hurdle. A translation layer must reimplement the Postgres
surface described above, and every dialect gap becomes an application bug. It also
reintroduces the second system, second HA story and second backup story that
[01](01-problem-and-goals.md) identifies as the actual cost being attacked.

**Rejected for v1.** Worth revisiting only as an *optional additional sink* for customers who
already run ClickHouse and want the Quicksilver pipeline to feed it — a fine phase-4 feature,
not the product.

---

## Option C — hybrid standby (row + column, co-located)

The node is an ordinary CNPG hot standby — full heap, full indexes, real MVCC — *and*
maintains a local columnar mirror built from the WAL it is already replaying (Path 3c in
[03](03-wal-ingestion.md)). The planner chooses per query.

```
client ──PG wire──> postgres backend (a real hot standby)
                      │
                      ├── planner decides
                      │     ├── point lookup / index scan ──> local heap + B-tree   (today's speed)
                      │     └── scan / aggregate ───────────> columnar mirror       (10–100×)
                      │
                      └── bgworker: WAL ──decode(TID→local heap)──> columnar mirror
```

**This is the recommended target**, for three reasons that compound:

1. **It satisfies G3 by construction.** Point lookups don't need to be "fast enough" — they
   run against the same heap and the same indexes as today, so they are *identical*. The
   no-regression constraint stops being a risk and becomes a property.
2. **It solves the physical-WAL state problem for free.** Per
   [03](03-wal-ingestion.md#3c--put-the-decoder-on-a-real-hot-standby), the TID-addressed
   heap the decoder needs *is* the standby's data directory.
3. **The node can still be an HA failover target.** A Quicksilver node is a valid standby, so
   it counts toward HA rather than being pure overhead — which changes the cost conversation
   entirely.

**Against:**
- Storage is row + column on the same PVC. Roughly 2× a plain standby (mitigated: columnar
  compresses 4–10×, so realistically ~1.2–1.5×).
- Planner routing is the hard part. Options, cheapest first: (i) explicit — a GUC or
  `SET quicksilver.route = column`; (ii) heuristic — route on estimated rows scanned and
  column count; (iii) genuine cost-based integration with custom scan nodes. Start at (i),
  ship (ii), treat (iii) as a research item.
- Mixed workload interference: one 40 s columnar scan must not destabilise a thousand 2 ms
  point lookups. This is exactly the problem pgrust's query scheduler addresses and we
  should expect to need our own answer (resource groups, admission control, or simply
  separate node pools for the two traffic classes).
- Requires the ingest path to be in-process, which is the phase-3 bet.

---

## Option D — native columnar table access method

Write a Postgres TAM that stores tuples column-wise, so the existing planner and executor
work unchanged.

**For:** maximal transparency; no second engine; everything in `pg_catalog` just works.

**Against:** Postgres's executor is tuple-at-a-time. A columnar TAM without a vectorised
executor recovers the I/O win (~2–5×) but not the CPU win (the other 10–50×). Getting the
CPU win means custom scan nodes and a vectorised execution path — i.e. writing a query engine
inside Postgres. That is a multi-year effort, and pgrust exists precisely because doing it
properly meant starting over.

**Out of scope.** Documented so nobody rediscovers it in month six.

---

## Recommendation

**Option A in phase 1, converging on Option C as the target.** They share almost everything:

| Component | Option A (phase 1) | Option C (target) | Shared? |
|---|---|---|---|
| Front end | Real Postgres | Real Postgres | ✅ |
| Execution engine | pg_duckdb / DuckDB | pg_duckdb / DuckDB | ✅ |
| Columnar storage | Parquet + DuckLake | Parquet + DuckLake | ✅ |
| Change stream | logical replication | physical WAL, in-process | ❌ |
| Row-store copy | none | full standby heap | ❌ |
| Query routing | all queries → column | planner chooses | ❌ |

Two of five components change. That is a deliberate, affordable migration, and it means
phase-1 work is not thrown away — which is the property that makes the staged plan credible
rather than a euphemism for "we'll rewrite it later".

## Storage layout sketch

Starting point, to be refined by spike S2:

```
/var/lib/quicksilver/
├── catalog/                        # DuckLake catalog — in the node's own Postgres
├── data/
│   └── public.orders/
│       ├── base/                   # compacted Parquet, sorted on the clustering key
│       │   ├── 000001.parquet
│       │   └── 000002.parquet
│       ├── delta/                  # recent micro-batches, append-only
│       │   └── 000147.parquet
│       └── tombstones/             # deletes pending compaction (position or key deletes)
└── state/
    └── applied_lsn                 # durable watermark; must survive restart
```

- **Micro-batch** the change stream into `delta/` on a size-or-time trigger (target: a few
  seconds, tuneable against the freshness SLO).
- **Compact** `delta/` + `tombstones/` into `base/` in the background. Compaction lag is a
  first-class metric — it is what merge-on-read query cost is a function of.
- **`_lsn` on every row**, per walshadow's design, so out-of-order arrival still converges
  and so any row's provenance is checkable against the source.
- **Verification**: periodic per-table checksums over an LSN-consistent snapshot, compared
  against the source. Exported as a metric, alarmed on mismatch (goal G7). Silent divergence
  is the failure mode that kills CDC products, and the only defence is continuous proof.
