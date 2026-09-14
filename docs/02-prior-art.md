# 02 — Prior art

Everything here was surveyed for this study. For each: what it actually does, what
Quicksilver should steal, and what it tells us not to do.

---

## ClickHouse walshadow

*Source: [github.com/ClickHouse/walshadow](https://github.com/ClickHouse/walshadow),
[announcement blog](https://clickhouse.com/blog/introducing-walshadow). AGPL-3.0.
Status: "Development Preview".*

Replicates Postgres rows into ClickHouse **from the physical WAL stream** — the same stream
a physical standby consumes — rather than via logical decoding. Requires PG ≥ 16, and a
module loaded via `shared_preload_libraries` on the source (not a plain SQL extension).

Three binaries: `walshadow-stream` (the daemon), `walshadow-filter` (offline segment
filtering), `walshadow-classify` (record-level diagnostics).

**The architecture, in four stages:**

1. **Track the live schema.** Filter *catalog* WAL records out of the stream and replay them
   into a **schema-only shadow Postgres instance**. That shadow instance is not a data
   replica — it exists solely to maintain an up-to-date `pg_catalog` so the decoder knows
   what the columns and types currently are.
2. **Decode data changes in parallel.** Heap records are fanned out across a pool of Rust
   decoders. Critically, these *bypass the shadow instance entirely* — no per-record catalog
   round-trip, which is what makes the parallelism worth anything.
3. **Build ClickHouse-native blocks.** A batcher groups decoded rows by table into complete
   native blocks, with no intermediate JSON/Avro hop.
4. **Insert in parallel.** A separate inserter pool writes blocks concurrently, so decode
   and insert scale independently.

Because stages 2–4 are parallel and therefore reorder, every row carries its source WAL
position as an `_lsn` column, and ClickHouse keeps the newest version per key. Operations
that *must* be ordered — schema changes, truncates — insert barriers that drain preceding
work.

**Reported numbers:** ~200 ms commit-to-visible latency and 289 k rows/s sustained on
`c8i.2xlarge` (8 vCPU), against a source producing 290 k rows/s. Quoted comparisons:
PeerDB ~10 s and ~120 k rows/s; a native Postgres physical standby ~50 ms.

### What to steal

- **The catalog/data split.** Catalog records → a stateful schema tracker; heap records →
  stateless parallel decoders. This is the single best idea in the project and it
  generalises to any physical-WAL consumer. It is what makes DDL (`ADD COLUMN`,
  `RENAME COLUMN`, `DROP COLUMN`, `CREATE TABLE`) tractable, which is exactly where naive
  logical-decoding pipelines fall over.
- **`_lsn` as a universal version column + barriers for ordered ops.** Lets you be
  aggressively parallel and still converge. Directly applicable whether the sink is
  ClickHouse, Parquet or a DuckDB table.
- **Native-format writes, no intermediate serialisation.** Our equivalent is writing Arrow
  batches straight into Parquet/DuckDB rather than round-tripping through text or JSON.
- **The framing itself.** "Load profile similar to a physical standby" is precisely the
  pitch Quicksilver makes, and walshadow is strong evidence that the physical-WAL path is
  achievable rather than theoretical.

### What it tells us not to do

- **AGPL-3.0.** We cannot vendor or link this. Ideas only. See [licence analysis](#licence-analysis).
- **It requires a `shared_preload_libraries` module on the source.** In CNPG that means
  mutating the primary's config and restarting it — invasive, and a genuine adoption
  barrier. Quicksilver should aim to need *nothing* on the primary beyond settings CNPG
  already manages.
- **The destination is ClickHouse.** Wrong wire protocol for our goal (G1). We want the same
  ideas pointed at a Postgres-native sink.
- **It is a Development Preview.** Don't treat the reported numbers as a floor we'll hit
  easily; treat them as evidence the approach isn't crazy.

---

## pg_mooncake (Mooncake Labs)

*Source: [github.com/Mooncake-Labs/pg_mooncake](https://github.com/Mooncake-Labs/pg_mooncake).
MIT. **Note: `docs.mooncake.dev` now 301-redirects to `databricks.com` — Mooncake Labs
appears to have been acquired.***

A Postgres extension that maintains a **columnstore mirror of your Postgres tables in
Apache Iceberg**, with claimed sub-second freshness. Two halves:

- **Read path — `pg_duckdb`.** DuckDB's vectorised engine executes analytical queries.
  Claimed top-10 on ClickBench.
- **Write path — `moonlink`.** A Rust library handling streaming and batched
  INSERT/UPDATE/DELETE into the columnstore.

Mirrors are created with `mooncake.create_table()` and kept in sync via **logical
replication** (`wal_level = logical` required). Only metadata lives in Postgres; data lives
in object storage as Parquet with Iceberg metadata.

### What to steal

- **The mirror concept is exactly Quicksilver's, validated.** "Your Postgres table, plus a
  transparently-maintained columnar copy, queried through Postgres" — same product,
  different packaging. That someone built it is a strong positive signal on both demand and
  feasibility.
- **`pg_duckdb` as the read path.** Reuse rather than rebuild. See
  [04](04-storage-and-query-engine.md).
- **Metadata-in-Postgres, data-in-object-store.** Keeps the catalog transactional and the
  bytes cheap.

### What it tells us not to do

- **`mooncake.create_table()` is an explicit, per-table, user-facing action.** That's a
  product seam. Goal G2 says the speedup must be *default* — the user declares intent once
  in the `Cluster` spec, not table-by-table in SQL.
- **Logical replication is the only ingest path**, which imports every logical-slot problem
  (see [03](03-wal-ingestion.md)).
- **Acquisition risk is now realised.** Building on `pg_mooncake` as a dependency means
  building on a project whose maintainers just changed owner and whose docs already point
  at a vendor site. Treat as *reference implementation*, not *dependency*.

---

## malisper/pgrust

*Source: [github.com/malisper/pgrust](https://github.com/malisper/pgrust). AGPL-3.0.
v0.2, explicitly not production-ready.*

A ground-up reimplementation of PostgreSQL in Rust targeting PG 18.3 wire and dialect
compatibility. Reported to pass all 46,066 tests in the Postgres regression suite, with
formal verification (Kani) over ~1,000 user-facing functions. Claimed benchmarks: 18.5%
faster than ClickHouse on ClickBench; 30% higher throughput than PG 18.3 on read-only
sysbench-oltp at 300 GB — tuned for ARM64/Graviton4.

Architecturally: vectorised executor with JIT (compile time ~50 ms → ~5 µs), thread-based
concurrency replacing the process model, a query scheduler that deprioritises long-running
queries, an internal OOM killer, pipelined fsync, and `pgrcolumnar` (dictionary encoding +
compression).

### What to steal

- **`pgrcolumnar` as a design reference** for our own columnar layout — particularly
  dictionary encoding and per-column compression choices.
- **The scheduler idea.** A read node serving both dashboards and point lookups has exactly
  the problem pgrust's scheduler solves: one 40-second scan should not destabilise a
  thousand 2 ms queries. Directly relevant to G3 on a hybrid node.
- **The existence proof.** A single-binary system that is simultaneously competitive on
  ClickBench *and* on sysbench-oltp is the strongest available evidence that the
  "one node, both workloads" premise behind Architecture C is physically possible.

### What it tells us not to do

- **It is not a component we can use.** No stable extension ABI, so no Postgres extensions
  work — which rules out pg_duckdb, our own extension, and most of the CNPG ecosystem.
- **AGPL-3.0** — same constraint as walshadow.
- **"Do not put data you care about in it."** The authors' own words. Not a v1 dependency
  under any reading.
- It is also a useful **cautionary datum on scope**: reimplementing Postgres is a multi-year,
  multi-person effort even when it goes well. Architecture D (a native columnar table access
  method) is a smaller version of the same trap.

---

## Adjacent projects worth knowing

| Project | Relevance |
|---|---|
| **[pg_duckdb](https://github.com/duckdb/pg_duckdb)** (MIT) | The official DuckDB↔Postgres extension. >3 k stars, >1 M downloads, actively maintained by DuckDB Labs + MotherDuck. Executes Postgres queries on DuckDB's vectorised engine, over both Postgres heap tables and object-store Parquet. **The most likely read-path dependency for Quicksilver.** |
| **DuckLake** | DuckDB's lakehouse format that puts the *catalog in a SQL database* rather than in JSON manifests. Interesting because in our case that SQL database can be the Quicksilver node's own Postgres — collapsing catalog and query engine into one process. |
| **[paradedb/pg_analytics](https://github.com/paradedb/pg_analytics)** (ex-`pg_lakehouse`) | **Archived/discontinued**; ParadeDB refocused on `pg_search`. A direct data point that "DuckDB-in-Postgres for lakehouse analytics" is a hard product to sustain as a standalone extension — reinforcing the view that the value is in the *operator integration*, not the extension. |
| **PeerDB** | Postgres→warehouse CDC. Useful as the performance baseline walshadow benchmarks against (~10 s, ~120 k rows/s). |
| **Debezium** | The incumbent logical-decoding CDC pipeline. Its well-documented operational pain (slot lag, failover gaps, schema drift) is the "everything around it" cost from [01](01-problem-and-goals.md) that Quicksilver claims to erase. |
| **ClickHouse `MaterializedPostgreSQL`** | ClickHouse's built-in logical-replication consumer. Long-standing experimental status is a caution about how much tail work CDC correctness carries. |
| **[cnpg-i-hello-world](https://github.com/cloudnative-pg/cnpg-i-hello-world)** | The reference CNPG-I plugin. Demonstrates injecting labels, annotations and a sidecar container into instance pods via lifecycle hooks. Our structural starting point — see [05](05-cnpg-integration.md). |

---

## Licence analysis

| Project | Licence | Can we link/vendor? |
|---|---|---|
| CloudNativePG / cnpg-i / cnpg-i-machinery | Apache-2.0 | ✅ Yes |
| pg_duckdb | MIT | ✅ Yes |
| DuckDB | MIT | ✅ Yes |
| pg_mooncake | MIT | ✅ Yes (but see acquisition risk) |
| ClickHouse (server) | Apache-2.0 | ✅ Yes |
| **walshadow** | **AGPL-3.0** | ❌ **No** — ideas only |
| **pgrust** | **AGPL-3.0** | ❌ **No** — ideas only |
| PostgreSQL itself | PostgreSQL Licence | ✅ Yes |

**Intended Quicksilver licence: Apache-2.0**, matching CNPG.

The two AGPL projects are the two most architecturally interesting ones, which is
unfortunate but not blocking — what we want from them is *design*, and design is not
copyrightable. The practical rule for the team:

> Read walshadow's and pgrust's documentation, blog posts and architecture freely. Do not
> copy their source, and do not have the same person who read their source write our
> equivalent module in the same sitting. Where we implement the same idea — catalog/data
> split, `_lsn` versioning — write it from the published description, and cite the
> description in the commit message.

If at some point vendoring walshadow becomes genuinely attractive, the alternative is to run
it as a **separate, unmodified process communicating over a socket**, which is the standard
AGPL-boundary argument. That should be a deliberate legal decision, not an accident — and it
still leaves Quicksilver's distribution story awkward. Assume "no" unless counsel says
otherwise.
