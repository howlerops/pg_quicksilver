# pg_quicksilver

**Status: the study is done and the plugin exists.** The core claims are measured, not
estimated ([docs/11](docs/11-measured-results.md)), and there is now a CNPG-I plugin, a
mirror sidecar, a Helm chart and an end-to-end test that runs the real sidecar against a
live PostgreSQL 17 primary and standby ([docs/16](docs/16-deploying.md)).

The CNPG-I handshake **is** verified — real mutual TLS, CloudNativePG's own dial code and
load sequence, and the lifecycle hook run against instance Pods built by the operator's own
`specs.NewInstance` ([docs/17](docs/17-testing-without-a-cluster.md)). What is left is
Service discovery by label and the operator's reconcile loops; both are listed in
[docs/16](docs/16-deploying.md#what-has-not-been-verified) rather than implied to work.

**Requires PostgreSQL ≥ 17** for `ingest: logical`. Before 17 a logical replication slot does
not survive a failover ([docs/15](docs/15-pg17-slot-failover.md)).

Quicksilver is a proposed [CloudNativePG](https://cloudnative-pg.io/) (CNPG) plugin + Helm
chart that replaces a Postgres cluster's **read replicas** with nodes that maintain a
**columnar, analytics-optimised mirror of the primary built from the WAL** — so that the
same `SELECT`, sent to the same `-ro` endpoint, over the same Postgres wire protocol, comes
back dramatically faster for scan-and-aggregate workloads.

The pitch in one line: *your read replicas are already burning a full copy of your data to
answer `SELECT`s with a row-store and a B-tree. Spend that same copy on a column-store and
get a measured 30× median (up to 68×) on the queries that actually hurt.*

Measured on the **production sidecar** ([docs/18](docs/18-measured-performance.md)): a
12-column, 3.2M-row table gives a **5.8× analytical median** (up to 12.6×), **5.6× smaller**
on disk, **188 ms** p50 commit-to-visible, and **24.6k rows/s** ingest. Speedup scales with
column count — 4 columns gives 7×, 30 columns gives 30× — because a column store's win is
the columns it does not read.

The pitch that matters to whoever signs the invoice: **replace 8 read replicas with 3**, and
have those 3 still count toward HA. See [docs/10](docs/10-scaling-economics.md) for the
consolidation math and the three cases where it doesn't hold.

The bar is lower than it looks: the break-even analytical share is **`f ≈ 1/R`** — 13.3% at 8
replicas, 6.6% at 16 — and is nearly independent of how fast the column store is. Measure
your own with [`bench/s0_workload_profile.sql`](bench/s0_workload_profile.sql)
([docs/13](docs/13-s0-without-customer-data.md)).

---

## Read this first: the headline finding

The naive framing — "swap the read replicas for an OLAP engine, it's faster by default" —
**does not survive contact with the evidence.** Two findings drive the entire design:

1. **A columnar mirror is not uniformly faster than a hot standby.** Measured on 30 M rows:
   **median 30× faster** on analytical queries (up to 68×), and **median 935× *slower*** on
   OLTP-shaped ones (up to 3,445×) — indexed point lookups, short high-concurrency queries,
   `LIMIT 20` ordered fetches. A straight `-ro` takeover doesn't just regress some traffic;
   at 935× it takes production down. See [docs/11](docs/11-measured-results.md#result-3--oltp-regression-is-far-worse-than-predicted).

2. **Physical WAL does not contain enough information to reconstruct row-level changes on
   its own.** Confirmed by experiment: against a row **67× wider**, a `DELETE` record stays
   **54 bytes** and an `UPDATE` record stays **69 bytes** — identical. The row contents are
   simply not there (`xl_heap_delete` carries only a TID; `xl_heap_update` prefix/suffix-
   compresses against the old tuple). Any physical-WAL decoder must therefore maintain its
   own TID-addressed copy of the heap. See
   [docs/11](docs/11-measured-results.md#result-1--the-wal-claims-in-docs03-are-confirmed-exactly).

Finding (2) looks like bad news and is actually the key that unlocks the design. **On a
Postgres hot standby, the TID-addressed copy of the heap already exists — it's the standby's
own data directory.** So the architecture that resolves the hardest technical problem is
also the one that resolves the hardest product problem:

> **A Quicksilver node is a normal CNPG hot standby *plus* a locally-built columnar mirror
> of the same data, with the planner choosing between them per query.**

Point lookups hit the real heap and real indexes at exactly today's speed. Scans and
aggregates hit the column store. The primary sees one ordinary physical replication stream
and no logical decoding slot. Nothing is copied twice over the network.

That is the recommended target. It is *not* the recommended first milestone — see the
[roadmap](docs/08-roadmap-and-spikes.md) for the staged path and the kill criteria.

**A third finding came out of the benchmarks and changed the storage design.** The
merge-on-read scheme originally specified in docs/04 — `_lsn` on every row, newest wins,
resolved at query time — costs **33× even with zero pending deltas**. The tax is the global
window function, not the backlog, and it cancels the entire analytical speedup. Replacing it
with Iceberg-v2-style **deletion vectors** brings it to **~1×**. Measured, and now corrected
in [docs/04](docs/04-storage-and-query-engine.md).

---

## Every number, in one place

**[METRICS.md](METRICS.md)** collects every measurement this project rests on —
ingest, freshness, storage, per-shape behaviour, scale, restart cost, compaction
policy — each with the script that produced it and its raw output checked in
under [`bench/results/`](bench/results/). It also states, in its own section,
what those numbers do **not** say.

Start there if you want the evidence; start below if you want the reasoning.

---

## Document index

| # | Document | What it covers |
|---|---|---|
| 01 | [Problem, goals, non-goals](docs/01-problem-and-goals.md) | What we're replacing, success criteria, explicit non-goals |
| 02 | [Prior art](docs/02-prior-art.md) | walshadow, pg_mooncake, pgrust, pg_duckdb, DuckLake, PeerDB — what to steal, what to avoid, license analysis |
| 03 | [WAL ingestion](docs/03-wal-ingestion.md) | The deep dive. Physical vs logical vs archive-tee. Why physical WAL alone is insufficient, with source evidence |
| 04 | [Storage & query engine](docs/04-storage-and-query-engine.md) | DuckDB/DuckLake vs ClickHouse vs native columnar TAM; the wire-protocol constraint |
| 05 | [CNPG integration](docs/05-cnpg-integration.md) | CNPG-I hook map, service takeover, Cluster spec sketch, what CNPG-I *cannot* do |
| 06 | [Compatibility & semantics](docs/06-compatibility-and-semantics.md) | The honest compatibility matrix; where the illusion breaks |
| 07 | [Image strategy](docs/07-image-strategy.md) | Building our own Postgres image on the CNPG public base |
| 08 | [Roadmap, spikes, kill criteria](docs/08-roadmap-and-spikes.md) | Phased plan with explicit go/no-go gates and a benchmark harness |
| 09 | [Risks & open questions](docs/09-risks-and-open-questions.md) | Risk register and the things we genuinely do not know yet |
| 10 | [Scaling economics](docs/10-scaling-economics.md) | **The business case.** Does this actually reduce replica count? Consolidation math, and the three places it breaks |
| 11 | [Measured results](docs/11-measured-results.md) | **Numbers, not estimates.** 30 M rows, PG 16 vs DuckDB/Parquet. What held, what didn't, and one design that had to be replaced |
| 12 | [S5: the serving path](docs/12-s5-serving-path.md) | The blocker that closed off Architecture A, and the 58-line `pg_duckdb` patch that reopens it |
| 13 | [S0 without customer data](docs/13-s0-without-customer-data.md) | Break-even is `f ≈ 1/R`, not 40%. Why the gating spike stopped gating, and a self-serve script to answer it per cluster |
| 14 | [Streaming, snapshot, failover](docs/14-phase2-streaming-and-failover.md) | pgoutput over streaming replication (36 ms), snapshot bootstrap, and what a **real promotion** does to a logical slot |
| 15 | [PG 17 slot failover](docs/15-pg17-slot-failover.md) | Slot synchronisation works and removes the re-snapshot — and the GUC that silently deadlocks the new primary if promotion doesn't clear it |
| 16 | [Deploying](docs/16-deploying.md) | **The installable part.** Two images, the Helm chart, the Cluster spec, what the plugin refuses, and what has not been verified |
| 17 | [Testing without a cluster](docs/17-testing-without-a-cluster.md) | The CNPG-I handshake is not a Kubernetes thing. Real mTLS, real Pods from CNPG's own builder, real CRD schemas — and the capability bug that found |
| 18 | [Measured performance](docs/18-measured-performance.md) | **The production path, measured.** Bootstrap, drain rate, commit-to-visible, storage, and the three implementation defects the benchmark found before it produced a number worth quoting |
| 19 | [Workload matrix](docs/19-workload-matrix.md) | **Five table shapes.** Narrow, wide, a 6 KB jsonb document per row, hot-set churn, delete-heavy — what each costs, what each optimisation bought, and the one shape that does not keep up |
| 20 | [Serving the mirror](docs/20-serving-the-mirror.md) | **The read path, measured for the first time.** A directory is not a table, so the mirror publishes the SELECT that reconstructs it — 2–38× on six shapes, 0.02× on a point lookup, and two silent PostgreSQL incompatibilities |
| 21 | [Against the real operator](docs/21-against-the-real-operator.md) | The plugin talking to an actual CloudNativePG operator, and the six traps between a passing handshake test and a reconciled Cluster |
| 22 | [One instant, two spellings](docs/22-one-instant-two-spellings.md) | Text is a property of the *session*, not the value. Three sessions render into a mirror and nothing made them agree — a silent-corruption bug found by chasing a harness detail |
| 23 | [Storing an instant](docs/23-storing-an-instant.md) | Temporal columns become real Arrow timestamps: queries that previously would not parse, a value the mirror refuses rather than guesses, and a storage claim that turned out to be false |
| 24 | [Nothing cached](docs/24-nothing-cached.md) | **The read path with an empty cache.** The point-lookup worst case is 20× warm and 1.4× cold; a published benchmark number that was measuring an empty result; and bytes off the block device per query |
| 25 | [Where the bootstrap goes](docs/25-where-the-bootstrap-goes.md) | Two shapes, two different bottlenecks hiding behind one unit; a codec that won on every axis including the one it was supposed to lose; and two published numbers that described the harness rather than the mirror |
| 26 | [The fifteen-second tick](docs/26-the-fifteen-second-tick.md) | Instrumenting the apply loop found a stall an order of magnitude worse than anything reported — and the fix for it cost throughput, which took two more measurements to explain |
| 27 | [The pruning that already works](docs/27-the-pruning-that-already-works.md) | A planned feature, measured before it was built, and retracted — plus the fourth measurement defect of the week and what the four have in common |
| 28 | [The slot is a loaded gun](docs/28-the-slot-is-a-loaded-gun.md) | The large-scale test took the database down — reproducing a risk the register had named and nobody had implemented. Why a logical slot makes a slow mirror the primary's problem, and the two defences that now exist |
| 29 | [What a delete costs to read](docs/29-what-a-delete-costs-to-read.md) | `count(*)` was 23x slower for 5x the data, and every query still agreed with PostgreSQL. Ruling out the SQL shape, `file_row_number` and the forced scan, to find a storage format nobody chose and a compaction trigger that only knew half its own trade-off |
| 30 | [Dropping a database kills the standby](docs/30-dropping-a-database-kills-the-standby.md) | A test script dropped its database and the STANDBY died. Why the configuration this project requires — failover slots plus slot synchronisation — turns an ordinary `DROP DATABASE` into a lost replica, and the two-second ordering that avoids it |
| 31 | [A duplicate row under compaction](docs/31-a-duplicate-row-under-compaction.md) | A key written to one file twice, where no deletion vector can reach it. Two sources, three symptoms that turned out to be one, the wrong diagnosis this document first published, and a diagnostic that was itself lying about positions |
| 32 | [The knob that barely fires](docs/32-the-knob-that-barely-fires.md) | An A/B of the compaction dead-fraction trigger that measured nothing and did not say so — both arms were the same behaviour. Then the workload shape that actually reaches it, both halves of its ledger (2.6× on reads), and the persisted index that turned out to be 2.7× the size of the Parquet file it indexed and slower to load than rebuilding from it |
| — | [ADRs](docs/adr/) | Architecture decision records (template + the decisions still open) |

## The four architectures under consideration

Summarised here, argued in full in [04](docs/04-storage-and-query-engine.md) and
[08](docs/08-roadmap-and-spikes.md).

| | **A — Sidecar mirror** | **B — External OLAP** | **C — Hybrid standby** | **D — Native columnar TAM** |
|---|---|---|---|---|
| Shape | CNPG standby + sidecar builds Parquet from logical replication | Separate ClickHouse cluster fed from WAL; proxy translates | Standby maintains local column store from its own WAL replay; planner routes | New table access method inside Postgres |
| Wire protocol | Postgres ✅ | Needs translation ❌ | Postgres ✅ | Postgres ✅ |
| Load on primary | Logical slot ⚠️ | Logical slot or physical ⚠️ | Ordinary physical stream ✅ | Ordinary physical stream ✅ |
| Point-lookup perf | **935× worse (measured)** ❌ | Worse still ❌ | Unchanged ✅ | Unchanged ✅ |
| Storage cost | 2× | 2× + separate cluster | 2× (row + column, co-located) | ~2× |
| Build effort | **Low** | Medium | Medium-High | **Very High** |
| Recommended as | **Phase 1 — separate `app-olap` endpoint only, never `-ro` takeover** | Rejected for v1 | **Phase 3 target** | Out of scope |

## Non-goals (for now)

- Replacing the OLTP primary. Quicksilver never accepts writes.
- Being a data lake / Iceberg-for-everyone play. Open table formats are a means, not the product.
- Multi-source or heterogeneous ingest. One CNPG cluster in, one mirror out.
- Sub-second freshness in v1. See the [freshness budget](docs/01-problem-and-goals.md#freshness-budget).

## Licensing note

Several inspirations are **AGPL-3.0** (walshadow, pgrust). Reading them for ideas is fine;
vendoring or linking their code would make Quicksilver AGPL and likely kill enterprise
adoption. Intended licence for this project is **Apache-2.0**, matching CNPG. This
constrains which prior art we can actually reuse — see
[docs/02-prior-art.md#licence-analysis](docs/02-prior-art.md#licence-analysis).
