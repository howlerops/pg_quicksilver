# Document index

Numbered roughly chronologically. The 20s and 30s are mostly post-mortems: something
measured that turned out to be wrong, and what it cost to find out.

Start with **[36](36-does-the-projection-pay.md)** for what the project is worth, and
**[11](11-measured-results.md)** for the findings that shaped the design.

| # | Document | What it covers |
|---|---|---|
| 01 | [Problem, goals, non-goals](01-problem-and-goals.md) | What we're replacing, success criteria, explicit non-goals |
| 02 | [Prior art](02-prior-art.md) | walshadow, pg_mooncake, pgrust, pg_duckdb, DuckLake, PeerDB — what to steal, what to avoid, license analysis |
| 03 | [WAL ingestion](03-wal-ingestion.md) | The deep dive. Physical vs logical vs archive-tee. Why physical WAL alone is insufficient, with source evidence |
| 04 | [Storage & query engine](04-storage-and-query-engine.md) | DuckDB/DuckLake vs ClickHouse vs native columnar TAM; the wire-protocol constraint |
| 05 | [CNPG integration](05-cnpg-integration.md) | CNPG-I hook map, service takeover, Cluster spec sketch, what CNPG-I *cannot* do |
| 06 | [Compatibility & semantics](06-compatibility-and-semantics.md) | The honest compatibility matrix; where the illusion breaks |
| 07 | [Image strategy](07-image-strategy.md) | Building our own Postgres image on the CNPG public base |
| 08 | [Roadmap, spikes, kill criteria](08-roadmap-and-spikes.md) | Phased plan with explicit go/no-go gates and a benchmark harness |
| 09 | [Risks & open questions](09-risks-and-open-questions.md) | Risk register and the things we genuinely do not know yet |
| 10 | [Scaling economics](10-scaling-economics.md) | **The business case.** Does this actually reduce replica count? Consolidation math, and the three places it breaks |
| 11 | [Measured results](11-measured-results.md) | **Numbers, not estimates.** 30 M rows, PG 16 vs DuckDB/Parquet. What held, what didn't, and one design that had to be replaced |
| 12 | [S5: the serving path](12-s5-serving-path.md) | The blocker that closed off Architecture A, and the 58-line `pg_duckdb` patch that reopens it |
| 13 | [S0 without customer data](13-s0-without-customer-data.md) | Break-even is `f ≈ 1/R`, not 40%. Why the gating spike stopped gating, and a self-serve script to answer it per cluster |
| 14 | [Streaming, snapshot, failover](14-phase2-streaming-and-failover.md) | pgoutput over streaming replication (36 ms), snapshot bootstrap, and what a **real promotion** does to a logical slot |
| 15 | [PG 17 slot failover](15-pg17-slot-failover.md) | Slot synchronisation works and removes the re-snapshot — and the GUC that silently deadlocks the new primary if promotion doesn't clear it |
| 16 | [Deploying](16-deploying.md) | **The installable part.** Two images, the Helm chart, the Cluster spec, what the plugin refuses, and what has not been verified |
| 17 | [Testing without a cluster](17-testing-without-a-cluster.md) | The CNPG-I handshake is not a Kubernetes thing. Real mTLS, real Pods from CNPG's own builder, real CRD schemas — and the capability bug that found |
| 18 | [Measured performance](18-measured-performance.md) | **The production path, measured.** Bootstrap, drain rate, commit-to-visible, storage, and the three implementation defects the benchmark found before it produced a number worth quoting |
| 19 | [Workload matrix](19-workload-matrix.md) | **Five table shapes.** Narrow, wide, a 6 KB jsonb document per row, hot-set churn, delete-heavy — what each costs, what each optimisation bought, and the one shape that does not keep up |
| 20 | [Serving the mirror](20-serving-the-mirror.md) | **The read path, measured for the first time.** A directory is not a table, so the mirror publishes the SELECT that reconstructs it — 2–38× on six shapes, 0.02× on a point lookup, and two silent PostgreSQL incompatibilities |
| 21 | [Against the real operator](21-against-the-real-operator.md) | The plugin talking to an actual CloudNativePG operator, and the six traps between a passing handshake test and a reconciled Cluster |
| 22 | [One instant, two spellings](22-one-instant-two-spellings.md) | Text is a property of the *session*, not the value. Three sessions render into a mirror and nothing made them agree — a silent-corruption bug found by chasing a harness detail |
| 23 | [Storing an instant](23-storing-an-instant.md) | Temporal columns become real Arrow timestamps: queries that previously would not parse, a value the mirror refuses rather than guesses, and a storage claim that turned out to be false |
| 24 | [Nothing cached](24-nothing-cached.md) | **The read path with an empty cache.** The point-lookup worst case is 20× warm and 1.4× cold; a published benchmark number that was measuring an empty result; and bytes off the block device per query |
| 25 | [Where the bootstrap goes](25-where-the-bootstrap-goes.md) | Two shapes, two different bottlenecks hiding behind one unit; a codec that won on every axis including the one it was supposed to lose; and two published numbers that described the harness rather than the mirror |
| 26 | [The fifteen-second tick](26-the-fifteen-second-tick.md) | Instrumenting the apply loop found a stall an order of magnitude worse than anything reported — and the fix for it cost throughput, which took two more measurements to explain |
| 27 | [The pruning that already works](27-the-pruning-that-already-works.md) | A planned feature, measured before it was built, and retracted — plus the fourth measurement defect of the week and what the four have in common |
| 28 | [The slot is a loaded gun](28-the-slot-is-a-loaded-gun.md) | The large-scale test took the database down — reproducing a risk the register had named and nobody had implemented. Why a logical slot makes a slow mirror the primary's problem, and the two defences that now exist |
| 29 | [What a delete costs to read](29-what-a-delete-costs-to-read.md) | `count(*)` was 23x slower for 5x the data, and every query still agreed with PostgreSQL. Ruling out the SQL shape, `file_row_number` and the forced scan, to find a storage format nobody chose and a compaction trigger that only knew half its own trade-off |
| 30 | [Dropping a database kills the standby](30-dropping-a-database-kills-the-standby.md) | A test script dropped its database and the STANDBY died. Why the configuration this project requires — failover slots plus slot synchronisation — turns an ordinary `DROP DATABASE` into a lost replica, and the two-second ordering that avoids it |
| 31 | [A duplicate row under compaction](31-a-duplicate-row-under-compaction.md) | A key written to one file twice, where no deletion vector can reach it. Two sources, three symptoms that turned out to be one, the wrong diagnosis this document first published, and a diagnostic that was itself lying about positions |
| 32 | [The knob that barely fires](32-the-knob-that-barely-fires.md) | An A/B of the compaction dead-fraction trigger that measured nothing and did not say so — both arms were the same behaviour. Then the workload shape that actually reaches it, both halves of its ledger (2.6× on reads), and the persisted index that turned out to be 2.7× the size of the Parquet file it indexed and slower to load than rebuilding from it |
| 33 | [The probe that gated the wrong thing](33-the-probe-that-gated-the-wrong-thing.md) | A readiness probe that would have taken PostgreSQL out of service in `shadow` mode — the plugin's own default — and the failing test written before the fix |
| 34 | [A setting that outlived its run](34-a-setting-that-outlived-its-run.md) | `synchronized_standby_slots` left behind by one test stalled every later failover slot, silently and with no error to any client |
| 35 | [The port that stopped PostgreSQL](35-the-port-that-stopped-postgresql.md) | The first time the plugin ran inside a real instance Pod it stopped the database from starting. One network namespace, two containers, port 9187 |
| 36 | [Does the projection pay?](36-does-the-projection-pay.md) | **The headline measurement.** 2×–12.7×, as a function of columns touched and data size; 16× worse on point lookups; and a prediction of mine that the data refuted |
| 37 | [The operator that could not roll](37-the-operator-that-could-not-roll.md) | `mode: takeover` did nothing. CloudNativePG 1.25 builds an instance Pod's expected spec without consulting plugins, so no parameter change can ever roll an instance |
| — | [ADRs](adr/) | Architecture decision records (template + the decisions still open) |
