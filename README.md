# pg_quicksilver

**Status: feasibility study / pre-implementation. No code yet.**

Quicksilver is a proposed [CloudNativePG](https://cloudnative-pg.io/) (CNPG) plugin + Helm
chart that replaces a Postgres cluster's **read replicas** with nodes that maintain a
**columnar, analytics-optimised mirror of the primary built from the WAL** — so that the
same `SELECT`, sent to the same `-ro` endpoint, over the same Postgres wire protocol, comes
back dramatically faster for scan-and-aggregate workloads.

The pitch in one line: *your read replicas are already burning a full copy of your data to
answer `SELECT`s with a row-store and a B-tree. Spend that same copy on a column-store and
get 10–100× on the queries that actually hurt.*

---

## Read this first: the headline finding

The naive framing — "swap the read replicas for an OLAP engine, it's faster by default" —
**does not survive contact with the evidence.** Two findings drive the entire design:

1. **A columnar mirror is not uniformly faster than a hot standby. It is dramatically
   faster on scans and aggregates and meaningfully *slower* on the OLTP-shaped reads that
   also land on `-ro` today** (indexed point lookups, short high-concurrency queries,
   `LIMIT 1` ordered fetches). A straight `-ro` takeover therefore regresses a real
   fraction of production traffic. See [docs/06-compatibility-and-semantics.md](docs/06-compatibility-and-semantics.md).

2. **Physical WAL does not contain enough information to reconstruct row-level changes on
   its own.** `xl_heap_delete` carries only a TID — no column values. `xl_heap_update`
   prefix/suffix-compresses the new tuple *against the old one*. Full-page images suppress
   the per-tuple payload entirely. Any physical-WAL decoder must therefore maintain its own
   TID-addressed copy of the heap. See [docs/03-wal-ingestion.md](docs/03-wal-ingestion.md)
   for the record-by-record evidence.

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
| — | [ADRs](docs/adr/) | Architecture decision records (template + the decisions still open) |

## The four architectures under consideration

Summarised here, argued in full in [04](docs/04-storage-and-query-engine.md) and
[08](docs/08-roadmap-and-spikes.md).

| | **A — Sidecar mirror** | **B — External OLAP** | **C — Hybrid standby** | **D — Native columnar TAM** |
|---|---|---|---|---|
| Shape | CNPG standby + sidecar builds Parquet from logical replication | Separate ClickHouse cluster fed from WAL; proxy translates | Standby maintains local column store from its own WAL replay; planner routes | New table access method inside Postgres |
| Wire protocol | Postgres ✅ | Needs translation ❌ | Postgres ✅ | Postgres ✅ |
| Load on primary | Logical slot ⚠️ | Logical slot or physical ⚠️ | Ordinary physical stream ✅ | Ordinary physical stream ✅ |
| Point-lookup perf | Regresses ❌ | Regresses badly ❌ | Unchanged ✅ | Unchanged ✅ |
| Storage cost | 2× | 2× + separate cluster | 2× (row + column, co-located) | ~2× |
| Build effort | **Low** | Medium | Medium-High | **Very High** |
| Recommended as | **Phase 1 (ships, proves value)** | Rejected for v1 | **Phase 3 target** | Out of scope |

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
