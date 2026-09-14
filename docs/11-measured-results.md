# 11 — Measured results

Everything here was measured, not estimated. Harness and raw output in
[`bench/`](../bench/); reproduce with the commands in
[`bench/README.md`](../bench/README.md).

**Test rig.** 4 vCPU, 15 GB RAM, single NVMe. PostgreSQL 16.13 (16.15 binaries after the
dev-header install), `pg_duckdb` 1.1.1 built from source against PG 16, `shared_buffers=4GB`,
`max_parallel_workers_per_gather=2`. DuckDB 1.5.5 over zstd Parquet. 30 M rows, 30 columns,
~299 B/row → **8.7 GB heap + 2.8 GB indexes**. Identical SQL to both engines.

**Read the biases before the numbers** — [`bench/README.md`](../bench/README.md#known-biases-in-this-harness)
lists four. The two that matter most: generated data is unrealistically compressible, and
both engines are effectively memory-resident, which removes I/O from the comparison and is
therefore *conservative* for the column store.

---

## Result 1 — The WAL claims in docs/03 are confirmed exactly

Controlled experiment: identical `INSERT` / `UPDATE` (of one small column) / `DELETE` against
a **narrow** row (~30 B payload) and a **wide** row (~2000 B payload, 67× wider). If a record
carries the row, its size tracks row width. If not, the sizes are identical.

### `wal_level = replica` — the physical-WAL case

| Operation | Narrow row | Wide row (67× wider) | Verdict |
|---|---|---|---|
| `INSERT` | 92 B | **2065 B** | Row **is** in WAL — size tracks width |
| `UPDATE` | 69 B | **69 B** | **Identical.** Prefix/suffix compression confirmed |
| `DELETE` | 54 B | **54 B** | **Identical.** No row contents whatsoever |

A `DELETE` record is 54 bytes regardless of whether the row was 30 bytes or 2 KB. The column
values — including the primary key — are simply not there. And a `UPDATE` touching one small
column of a 2 KB row emits 69 bytes: the 2000-byte `pad` was **not written**, because it was
prefix/suffix-compressed against the old tuple.

> **Confirmed: a physical-WAL decoder cannot reconstruct row changes without its own
> TID-addressed copy of the heap.** This is the finding that drives Architecture C.

### The cost of switching to logical — quantifying OQ-8

| Config | `UPDATE` wide | `DELETE` wide | Total WAL, 6 statements |
|---|---|---|---|
| `wal_level=replica`, RI DEFAULT | 69 B | 54 B | 3,008 B — baseline |
| `wal_level=logical`, RI DEFAULT | 2,076 B | 64 B | 5,112 B — **+70%** |
| `wal_level=logical`, RI FULL | 4,095 B | 2,073 B | 9,256 B — **+208%** |

Under `logical`, the wide `DELETE` grows only 54→64 B: those 10 bytes are the *key*
(`XLH_DELETE_CONTAINS_OLD_KEY`) — enough to identify the row, still not its contents. Only
`REPLICA IDENTITY FULL` writes the whole old row, at 2,073 B, and triples total WAL.

*Caveat: this microbenchmark is update/delete-heavy on a wide row — near worst case. Real
amplification depends on your insert/update/delete mix and row width.*

---

## Result 2 — Analytical speedup: median 30×, **S2 PASSES**

| Query | PG p50 | DuckDB p50 | Speedup |
|---|---|---|---|
| A1 daily revenue, 30 d window | 2,458 ms | 423 ms | 5.8× |
| A2 top campaigns | 5,002 ms | 118 ms | 42.3× |
| A3 country × device matrix | 3,510 ms | 164 ms | 21.4× |
| A4 full-table aggregate | 3,295 ms | 86 ms | 38.4× |
| A5 distinct users by type | 47,888 ms | 701 ms | **68.3×** |
| A6 cohort funnel | 3,398 ms | 371 ms | 9.2× |

**Median 29.9×, range 5.8×–68.3×.** S2's threshold was ≥10× median — comfortably cleared.

The spread is instructive. The two weakest (A1 5.8×, A6 9.2×) are narrow time-window queries
where Postgres's parallel seq scan does respectably *and* where DuckDB must read `ts`, the
largest column in the file at 33% of total bytes. The strongest (A5, 68×) is
`count(DISTINCT)` — the shape row stores handle worst.

### I/O asymmetry — docs/10 was overstated

| Query | Row store touches | Column store touches | Ratio |
|---|---|---|---|
| A1 | 9,099 MB | 146.4 MB | 62× |
| A2 | 9,099 MB | 64.4 MB | 141× |
| A4 | 9,098 MB | 64.3 MB | 141× |
| A5 | 9,098 MB | 113.8 MB | 80× |

**Measured 62–141×, not the ~500× claimed in docs/10.** That estimate assumed uniform column
sizes; in reality `ts` and `amount` are the high-entropy columns and dominate any query that
touches them. docs/10 has been corrected.

Storage: 8.7 GB heap → **251 MB Parquet (35×)**. This ratio is *inflated* — the generator's
low-cardinality text dictionary-encodes to near-zero. Plan with 4–10×.

---

## Result 3 — OLTP regression is far worse than predicted

docs/06 predicted columnar would be "3–100× slower" on OLTP shapes. Measured:

| Query | PG p50 | DuckDB p50 | Regression |
|---|---|---|---|
| O1 point lookup by PK | 0.09 ms | 52.99 ms | **602× slower** |
| O2 user's last 20 events | 0.12 ms | 396.15 ms | **3,445× slower** |
| O3 session lookup by uuid | 0.10 ms | 277.40 ms | **2,642× slower** |
| O4 tenant + 1-day window | 0.34 ms | 62.65 ms | **182× slower** |
| O5 small per-user aggregate | 0.13 ms | 124.32 ms | **935× slower** |

**Median 935× slower. Range 182×–3,445×.** My estimate was low by more than an order of
magnitude.

The cause is not subtle: Postgres answers these from a B-tree in ~100 µs touching < 2 MB.
DuckDB has no index, and the Parquet is clustered on `ts`, so a `user_id` or `session_id`
predicate prunes nothing and scans all 30 M rows.

**Consequences, and they are not cosmetic:**

1. **`serviceMode: takeover` under Architecture A is indefensible.** Not "risky" — a 935×
   median regression on a traffic class that shares the `-ro` endpoint would be an immediate
   production incident. The default of `off` was correct; it should arguably be the *only*
   option for Architecture A.
2. **Architecture C is no longer merely preferable — it is the only design that can serve
   `-ro`.** Keeping the row store isn't an optimisation; it's what makes the endpoint safe.
3. **Spike S0 is now even more load-bearing.** With a 935× downside, the analytical share of
   traffic has to be high *and* cleanly separable before any takeover is contemplated.

---

## Result 4 — Concurrency holds up: `C ≈ 0.77`, better than assumed

docs/10 modelled consolidation with a pessimistic concurrency-efficiency term `C = 0.5`, and
docs/09 flagged it (OQ-13) as the least-known number in the business case.

| Clients | PG qps | DuckDB qps | Ratio | PG p95 | DuckDB p95 |
|---|---|---|---|---|---|
| 1 | 0.3 | 7.4 | 26.1× | 3,779 ms | 142 ms |
| 2 | 0.4 | 8.0 | 21.1× | 5,128 ms | 290 ms |
| 4 | 0.4 | 8.2 | 23.4× | 15,492 ms | 703 ms |
| 8 | 0.4 | 8.3 | 19.6× | 18,780 ms | 1,416 ms |
| 16 | 0.4 | 8.3 | 20.1× | 38,793 ms | 2,450 ms |

**The ratio does not collapse under concurrency — it holds at ~20×.** `C = 20.1/26.1 = 0.77`.
Both engines saturate at ~4 clients and then queue (throughput flat, p95 rising linearly),
which is the expected and benign behaviour. DuckDB does not degrade *relative* to Postgres.

**Caveat, and it's a big one: 4 vCPUs.** Both engines saturate almost immediately, so this
does not really test concurrency scaling. On a 16–32 core node the process model and the
thread pool may diverge substantially. **OQ-13 is improved, not closed.**

### Re-running the docs/10 consolidation model on measured inputs

docs/10 assumed `S = 50, C = 0.5` → `S·C = 25`. Measured: `S = 30, C = 0.77` → **`S·C = 23`**.
Near-identical, so the conclusion survives unchanged:

| | Analytical need | OLTP need | Total |
|---|---|---|---|
| Today | — | — | **8** |
| Architecture A | 0.24 → HA floor 2 | 2.4 → 3 | **5** |
| Architecture C | 0.24 + 2.4 = 2.64 | (same nodes) | **3** |

**8 → 3 under Architecture C**, with measured rather than assumed inputs.

---

## Result 5 — The docs/04 merge-on-read design is broken, and the fix is measured

The most important negative result. docs/04 specified `_lsn` on every row with newest-version-
wins resolution. Measured against a 137 ms compacted baseline:

| Deltas | Delta rows | S1 `_lsn` global dedup | S2 anti-join | S3 deletion vectors |
|---|---|---|---|---|
| 0 | 0 | 4,544 ms — **33.2×** | 494 ms — 3.6× | 123 ms — **0.9×** |
| 4 | 200 k | 4,594 ms — 33.6× | 567 ms — 4.1× | 116 ms — 0.8× |
| 16 | 800 k | 4,925 ms — 36.0× | 761 ms — 5.6× | 127 ms — 0.9× |
| 64 | 3.2 M | 5,415 ms — **39.5×** | 1,175 ms — 8.6× | 150 ms — **1.1×** |

**S1 costs 33× at *zero* deltas.** The tax is not the backlog — it is the
`row_number() OVER (PARTITION BY event_id ORDER BY _lsn DESC)` over all 30 M rows, plus the
tombstone anti-join. Going from 0 to 3.2 M delta rows adds only 19%.

A 33× tax against a 30× speedup nets out to **roughly zero** — the mirror would be no faster
than the Postgres it replaced. As specified, docs/04 did not work.

**The fix, measured:** precompute deleted/superseded row positions at *compaction* time and
apply them as a position filter at scan time — Iceberg-v2 positional deletes / Delta-style
deletion vectors. No window function, no join. **0.8–1.1× — essentially free**, and nearly
flat in backlog.

S2 (anti-join against only the changed-key set) is the sensible middle ground at 3.6–8.6×:
much better than S1, still eating a third to a quarter of the speedup.

*Caveat: S3 as measured materialises the position map in memory, so a real implementation
reading a bitmap alongside Parquet would be somewhat worse. The structural win — no sort, no
join — is what holds.*

**docs/04 has been corrected to specify deletion vectors.** `_lsn` is retained for ordering
and provenance, which is what walshadow uses it for; it is no longer the query-time
resolution mechanism.

---

## Result 6 — S2b: the speedup survives a Postgres front-end (22.2×), but only because of columnar storage

S2 measured standalone DuckDB, which bypasses the Postgres executor. This re-runs the same
SQL through `pg_duckdb` 1.1.1 built against PG 16 — the real product path. Four
configurations on identical data:

| Query | A: plain PG | B: pg_duckdb / heap | C: pg_duckdb / Parquet | D: raw DuckDB | C vs A | tax (C/D) |
|---|---|---|---|---|---|---|
| A1 daily revenue | 2,458 ms | 3,232 ms | 486 ms | 423 ms | 5.1× | 1.15× |
| A2 top campaigns | 5,002 ms | 8,152 ms | 142 ms | 118 ms | 35.3× | 1.20× |
| A3 country × device | 3,510 ms | 4,282 ms | 198 ms | 164 ms | 17.7× | 1.21× |
| A4 full aggregate | 3,295 ms | 7,021 ms | 124 ms | 86 ms | 26.7× | 1.44× |
| A5 distinct users | 47,888 ms | 5,907 ms | 777 ms | 701 ms | 61.6× | 1.11× |
| A6 cohort funnel | 3,398 ms | 4,132 ms | 536 ms | 371 ms | 6.3× | 1.45× |

**S2b PASSES: median 22.2× through the full Postgres stack**, against a ≥10× threshold.

**The extension-boundary tax is only 1.20× median** (range 0.97–1.45×). The drop from S2's
29.9× standalone to 22.2× in-product is real but modest, and 22.2× is the number to quote
from here on. **No query fell back** to the Postgres executor.

### The finding that matters more than the headline

**Column B is a regression.** Running `pg_duckdb` over the *Postgres heap* — vectorised
execution, row storage — is a **median 1.27× _slower_** than plain Postgres. Five of six
queries got worse; A4 nearly halved in speed. Only A5 (`count(DISTINCT)`) improved, at 8.1×.

> The win is **columnar storage**, not the vectorised engine. An engine swap alone makes
> things worse.

Three consequences:

1. **"Just install pg_duckdb on your existing replica" is not a product.** It would regress
   most analytical queries. The mirror is the entire value.
2. **Architecture C's planner routing must route to the columnar mirror specifically** — not
   merely "execute this in DuckDB". A router that picks the engine without picking the
   storage would make things worse.
3. It independently validates the premise of this whole study: the reason to build a mirror
   is the storage layout, which is exactly the thing a read replica cannot give you.

### Two integration constraints discovered

Both affect the design, neither is fatal:

- **Persistent DuckDB-backed tables require MotherDuck.** `CREATE TABLE ... USING duckdb`
  errors with *"Only TEMP tables are supported in DuckDB if MotherDuck support is not
  enabled"*. So stock `pg_duckdb` cannot give us a persistent, named, columnar table in
  Postgres without a cloud dependency — which is unacceptable for Quicksilver.
- **`read_parquet()` needs `r['colname']` syntax**, not ordinary column references. That
  breaks goal G1 (identical SQL) on its face.

**Both are solved by the same mechanism, and it works:** generate a view that maps
`r['col']::type AS col` for every column. Application SQL then runs unchanged against the
mirror. All Result 6 numbers above were measured through such a view, and
`sum(amount)` matched the Postgres heap **to the cent** — a good early signal on `numeric`
fidelity (OQ-14). **Generating and maintaining that view is now a named plugin
responsibility.**

### Per-connection cost

A fresh backend's first `pg_duckdb` query costs **~18 ms more** than subsequent queries on
the same connection (152 ms vs 134 ms for the same statement). That is the DuckDB
instantiation tax. Amortised to nothing behind a connection pool; a real per-query tax for
an application that connects per request. Worth documenting for users, not worth designing
around.

---

## Result 7 — S3: RLS holds, but ordinary roles cannot reach the mirror at all

Two independent questions, and they came out opposite ways.

### The good half: access control IS enforced under DuckDB execution

With `duckdb.postgres_role` granted, a restricted role querying an RLS-protected table
through `pg_duckdb`'s executor saw **1,000 rows, tenants 1..1** — exactly its policy's
subset — where a superuser sees 3,000 rows across tenants 1..3. Column-level grants were
enforced too: `SELECT secret` was denied.

> **RLS and column privileges survive DuckDB execution.** The hard gate in docs/06 §4 —
> "refuse to mirror any table with RLS enabled" — **can be lifted** for the
> Postgres-heap path.

A methodological note worth keeping: the *first* run of this spike reported 6 of 6 passing,
and every pass was worthless. `pg_duckdb` is deny-by-default for non-superusers
(`duckdb.postgres_role` is empty out of the box), so every test failed closed for the wrong
reason. Green meant untested. The spike script now runs that as an explicit Phase 1 so the
trap is visible rather than repeatable.

### The blocking half: the mirror is unreachable without a dangerous grant

| Attempt | Result |
|---|---|
| Ordinary role queries the mirror **view** (owned by superuser) | **BLOCKED** |
| Ordinary role calls `read_parquet()` directly | **BLOCKED** |
| ...after granting `pg_read_server_files` + `pg_write_server_files` | ALLOWED |

*"Permission Error: File system LocalFileSystem has been disabled by configuration"*

**View ownership does not help.** Unlike table privileges, the filesystem check runs against
the *current* user at plan time, so the usual "wrap it in a view owned by a privileged role"
trick fails. Root cause, from `pg_duckdb`'s `src/pg/permissions.cpp`:

```c
bool AllowRawFileAccess() {
    return is_member_of_role(GetUserId(), ROLE_PG_WRITE_SERVER_FILES) &&
           is_member_of_role(GetUserId(), ROLE_PG_READ_SERVER_FILES);
}
```

So the only supported way to let an application role read the Parquet mirror is to grant it
both server-file roles. **Measured cost of that grant**: the same role could then
`COPY leak FROM '/etc/passwd'` (25 lines read) and read `pg_hba.conf` (126 lines). That is
arbitrary server-file read *and write* handed to an application role — categorically
unacceptable, and strictly worse than the data the mirror holds.

### The workarounds, both unsatisfying

| | Works? | Cost |
|---|---|---|
| **W1** grant both server-file roles | ✅ | Arbitrary server file read/write. **Rejected.** |
| **W2** `SECURITY DEFINER` wrapper function | ✅ | Requires `duckdb.unsafe_allow_execution_inside_functions` (upstream's own name for it), and serves only **fixed** queries — arbitrary application SQL, i.e. goal G1, is impossible this way |
| **W3** patch `pg_duckdb` to allow reads confined to a configured directory | untested | An upstream contribution. **The right long-term fix**, and a plausible one — a `duckdb.allowed_directories` GUC is a small, defensible change. |

### What this does to the architecture

This is the second measured finding pointing the same direction, and together they close off
Architecture A's serving path:

- **Result 6** showed `pg_duckdb` over the *Postgres heap* is 1.27× **slower** than plain
  Postgres — so the safe path is not fast.
- **Result 7** shows the *Parquet* path is fast but not reachable by ordinary roles without
  an unacceptable privilege grant — so the fast path is not safe.

**Quicksilver therefore cannot simply rent `pg_duckdb`'s `read_parquet` as its serving
layer.** It needs to own the columnar access path — as a table access method, a foreign data
wrapper, or an upstream `pg_duckdb` patch (W3). That is a real scope increase over what
docs/04 assumed, and it should be costed before phase 1 rather than discovered inside it.

W3 is the cheapest credible route and should be scoped first: if upstream accepts a
directory-confined file-access GUC, Architecture A's serving path reopens with no fork.

---

## What this changes

| Doc | Change |
|---|---|
| **04** | Storage design now specifies deletion vectors, not `_lsn` global dedup. Compaction is now correctness-critical, not just a tuning knob. |
| **06** | OLTP regression corrected from "3–100×" to a measured 182–3,445×. |
| **10** | I/O asymmetry corrected from ~500× to a measured 62–141×. `C` updated from an assumed 0.5 to a measured 0.77. Consolidation conclusion unchanged. |
| **08** | S2 **PASSED** (median 29.9×). S2b **PASSED** (median 22.2× in-product). OQ-11 answered, OQ-14 partly answered, OQ-13 improved but not closed. |
| **04** | Mirror must be exposed via a **generated view** (`r['col']::type AS col`) — persistent DuckDB-backed tables need MotherDuck, and `read_parquet` does not accept plain column references. Now a named plugin responsibility. **And the view is not sufficient**: Result 7 shows ordinary roles still cannot read it. The serving layer is an open design problem. |
| **06** | The RLS hard gate in §4 **can be lifted** — enforcement is verified. Replaced by a different constraint: the mirror is unreachable by ordinary roles at all. |
| **09** | R4 (RLS bypass) **closed — did not occur**. New blocking risk: no safe serving path for the Parquet mirror with stock `pg_duckdb`. |
| **README** | Headline numbers are now measured. |

## Still unmeasured

Docker is unavailable on this box, so nothing involving Kubernetes ran.

- **S0 (workload characterisation)** — needs real `-ro` traffic. Still the project's gating
  spike, and Result 3 makes it more load-bearing, not less.
- **S1 (CNPG-I capability probe)** — needs a cluster.
- **W3** — whether `pg_duckdb` upstream would accept a directory-confined file-access GUC.
  This is now the cheapest route to a viable Architecture A serving path.
- **S4 (physical WAL decode on a standby)** — needs a C background worker against a live
  standby.
- Anything involving **joins across mirrored tables**, which is where a real workload would
  stress the design harder than this single-table suite does.
