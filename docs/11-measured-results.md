# 11 — Measured results

Everything here was measured, not estimated. Harness and raw output in
[`bench/`](../bench/); reproduce with the commands in
[`bench/README.md`](../bench/README.md).

**Test rig.** 4 vCPU, 15 GB RAM, single NVMe. PostgreSQL 16.13, `shared_buffers=4GB`,
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

## What this changes

| Doc | Change |
|---|---|
| **04** | Storage design now specifies deletion vectors, not `_lsn` global dedup. Compaction is now correctness-critical, not just a tuning knob. |
| **06** | OLTP regression corrected from "3–100×" to a measured 182–3,445×. |
| **10** | I/O asymmetry corrected from ~500× to a measured 62–141×. `C` updated from an assumed 0.5 to a measured 0.77. Consolidation conclusion unchanged. |
| **08** | S2 marked **PASSED** (median 30× ≥ 10× threshold). OQ-11 answered. OQ-13 improved but not closed. |
| **README** | Headline numbers are now measured. |

## Still unmeasured

Docker is unavailable on this box, so nothing involving Kubernetes ran.

- **S0 (workload characterisation)** — needs real `-ro` traffic. Still the project's gating
  spike, and Result 3 makes it more load-bearing, not less.
- **S1 (CNPG-I capability probe)** — needs a cluster.
- **S3 (RLS enforcement through pg_duckdb)** — needs a real pg_duckdb build; this harness used
  DuckDB directly, which bypasses the Postgres executor entirely and so cannot test it.
- **S4 (physical WAL decode on a standby)** — needs a C background worker against a live
  standby.
- Anything involving **joins across mirrored tables**, which is where a real workload would
  stress the design harder than this single-table suite does.
