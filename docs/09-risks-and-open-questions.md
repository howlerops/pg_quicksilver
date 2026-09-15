# 09 — Risks and open questions

---

## Risk register

Likelihood × impact, highest first. "Mitigation" means something we would actually do, not a
reassurance.

| # | Risk | L | I | Mitigation |
|---|---|---|---|---|
| R1 | **Real `-ro` traffic is mostly OLTP-shaped**, so a columnar mirror regresses more than it helps | Med | **Fatal** | Spike S0 before anything else. Architecture C exists specifically to survive this outcome. |
| R2 | **Silent divergence** between mirror and source | Med | **Fatal** | Continuous checksum verification (G7) from phase 2; `_lsn` provenance on every row; `shadow` mode as a live oracle. Treat any divergence as a stop-ship. |
| R3 | **Partial transactions visible** across tables (batches not cut on commit boundaries) | Med | High | Commit boundaries carried in the change stream; atomic batch commit. Design in phase 1, not retrofitted. |
| R4 | **RLS / column grants bypassed** by DuckDB-executed scans | **High** | High | Spike S3. Until proven, refuse to mirror RLS tables at admission. Publish the limitation. |
| R5 | **Physical WAL decode on standby proves unreliable** (racing replay, vacuum, TOAST) | Med | High | Spike S4, scheduled late. Failure costs Architecture C, not the product. |
| R6 | **pg_duckdb coverage gaps become hard errors** because data exists only as Parquet | High | Med | Per-table opt-in; admission-time type checks; Architecture C removes this entirely by keeping the row store. |
| R7 | **Logical slot fills `pg_wal` and takes down the primary** | Med | High | `max_slot_wal_keep_size` + alerting + readiness gating; archive tee for recovery without the slot. Long term: Path 3c removes the slot. |
| R8 | **Failover loses the slot** (PG ≤ 16) | High | Med | Slot sync on PG ≥ 17; on ≤ 16, automatic re-snapshot with clear signalling. Consider requiring PG ≥ 17 for `ingest: logical`. |
| R9 | **CNPG-I lacks a hook we need** (esp. WAL tee alongside a backup plugin) | Med | Med | Spike S1. Fallbacks identified per sub-case in [08](08-roadmap-and-spikes.md#s1--cnpg-i-capability-probe). |
| R10 | **Adoption blocked by requiring our image cluster-wide** (Architecture C) | Med | Med | Strictly additive image; mirror-tier-only in phase 1; track CNPG extension-image-volume support (OQ-6). |
| R11 | **Mixed workload interference** — one big scan destabilises many small queries | High | Med | Admission control / resource groups; separate node pools per traffic class as the crude fallback. |
| R12 | **Upstream dependency risk** — pg_mooncake acquired, pg_analytics archived | Med | Med | Depend on `pg_duckdb` (DuckDB Labs + MotherDuck, actively maintained) rather than the acquired/archived options. Keep the storage format portable (Parquet) so an engine swap is possible. |
| R13 | **Type-mapping bugs**, especially `numeric` → silently wrong financial results | Med | High | Exhaustive type-matrix tests in CI, blocking image publish. Refuse unmappable types at admission. |
| R14 | **AGPL contamination** from reading walshadow/pgrust source | Low | High | Ideas-only policy in [02](02-prior-art.md#licence-analysis); implement from published descriptions; cite sources in commits. |
| R15 | **Scope creep into "build a database"** (Architecture D, or a bespoke storage engine for Path 3b) | **High** | High | Architecture D explicitly out of scope. Path 3b explicitly rejected in favour of 3c for exactly this reason. Revisit only with a written ADR. |
| R16 | **2× storage cost** kills the economics | Low | Med | Columnar compresses 4–10×; Architecture C mirror nodes can double as HA standbys, making the marginal cost near zero. |

R15 deserves emphasis. The most likely way this project fails is not that the technology
doesn't work — it is that the physical-WAL decoder is the most intellectually attractive part
of the design and will pull effort away from the boring work that determines whether anyone
buys it. The phase ordering in [08](08-roadmap-and-spikes.md) exists to resist that pull.

---

## Open questions

Things we genuinely do not know and must resolve. Each names who resolves it and how.

### Product

- **OQ-1 — What is the real analytical/OLTP split on `-ro`?**
  → Spike S0. *This is the only question that determines whether the product should exist.*
- **OQ-2 — Do users want "faster `-ro`" or "an analytics endpoint"?** These imply different
  defaults (`takeover` vs `off`) and different marketing. → design-partner interviews, phase 1.

### CNPG

- **OQ-3 — Can multiple plugins implement the WAL service?** If `cnpg-i-barman-cloud` (or
  `barmanObjectStore`) already owns `Archive`, can Quicksilver tee alongside it, or is it
  exclusive? → Spike S1(c); read CNPG's plugin-dispatch source. *Path 1's design depends on
  the answer.*
- **OQ-4 — Does a `LifecycleHook` Service mutation survive operator restart and forced
  reconcile?** → Spike S1(a).
- **OQ-5 — Can a `ReconcilerHooks.Post`-created StatefulSet be properly owned/garbage-collected
  when the Cluster is deleted?** Owner references and `Deregister` semantics. → Spike S1(b).
- **OQ-6 — Is CNPG's extension-image-volume support mature enough to avoid a base-image swap
  in phase 3?** Would materially reduce R10. → track upstream.

### Postgres internals

- **OQ-7 — Can a standby bgworker read the pre-replay heap safely?** Specifically: does it
  decode ahead of replay, hold replay back, or rely on dead tuples surviving until vacuum with
  `hot_standby_feedback`? → Spike S4. *The crux of Architecture C.*
- **OQ-8 — How much does `wal_level = logical` actually cost** in WAL volume and primary CPU
  on a representative write workload? Quantify what Path 3c saves. → measure in phase 1.
- **OQ-9 — TOAST reassembly from physical WAL:** is decoding the TOAST relation's own records
  tractable, or should we always detoast via the local heap on the standby? → Spike S4.
- **OQ-10 — Relfilenode remapping.** After `pg_basebackup` re-bootstrap, `VACUUM FULL`, or
  `CLUSTER`, relfilenodes change. Can a physical mirror remap, or must it rebuild? → Spike S4.

### Engine

- **OQ-11 — What is the true merge-on-read cost** as a function of delta backlog? This sets the
  compaction SLO and therefore the resource budget. → Spike S2.
- **OQ-12 — DuckLake vs Iceberg vs plain Parquet + our own catalog?** DuckLake's
  catalog-in-Postgres is appealing but young. → phase 1 prototype.
- **OQ-13 — Concurrency model.** How many concurrent DuckDB queries can one node sustain
  before memory or thread contention dominates? Sets pods-per-cluster sizing. → Spike S2.
- **OQ-16 — DuckDB spill files.** DuckDB auto-appends its temp directory to the confinement allowlist, so a confined role can read spill files. Are they per-backend, 0600, and removed on completion? If not, that is a cross-tenant disclosure path and the mirror and temp directories need separate treatment. → before shipping the S5 patch.
- **OQ-14 — `numeric` mapping.** DECIMAL128 (loses range) or DOUBLE (loses exactness) or a
  fallback path? There may be no good answer, in which case wide-`numeric` tables are refused.
  → Spike S2, and it is a correctness question, not a performance one.

### Legal / licensing

- **OQ-15 — Is the ideas-only boundary sufficient** for a team that has read AGPL source?
  → counsel, before phase 1 code lands. The answer shapes the contribution policy.

---

## Decisions already taken in this study

Recorded so they are not silently relitigated. Each should become an ADR
([docs/adr/](adr/)) when someone has cause to revisit it.

| Decision | Rationale |
|---|---|
| Serving layer is **real PostgreSQL**, not a proxy or reimplementation | [04](04-storage-and-query-engine.md#the-constraint-that-eliminates-most-options) — the wire/dialect/catalog surface is too large to fake |
| **Apache-2.0**, and no AGPL code | [02](02-prior-art.md#licence-analysis) — enterprise adoption |
| **Logical replication first**, physical WAL later | [03](03-wal-ingestion.md#recommendation) — known risk first, differentiated risk after the product is proven |
| **Architecture A → C**, B and D rejected | [04](04-storage-and-query-engine.md#recommendation) — B fails G1, D is a multi-year engine build |
| **Derived image, never a fork** | [07](07-image-strategy.md#base-derive-never-fork) — Postgres CVE response must remain upstream's job |
| `serviceMode` defaults to **`off`** | [05](05-cnpg-integration.md#the--ro-takeover-and-why-it-should-be-opt-in) — silent `-ro` takeover is an incident generator |
| Mirroring is **opt-in per table** | [06](06-compatibility-and-semantics.md#3-sql-surface) — lossy silent mirroring is worse than none |
| **Readiness gating in phase 1** | G4 — bounded staleness must be enforced, not promised |
