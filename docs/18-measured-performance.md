# 18 — Measured performance of the production path

Everything below is the **qs-mirror sidecar**, running as its own process,
configured only by the environment [`internal/plugin/lifecycle.go`](../go/internal/plugin/lifecycle.go)
sets, against PostgreSQL 17.11 with a real streaming standby. Harness:
[`bench/scripts/perf_mirror.sh`](../bench/scripts/perf_mirror.sh) and
[`go/cmd/qs-perf`](../go/cmd/qs-perf/). Raw output:
[`bench/results/perf_mirror_pg17.txt`](../bench/results/perf_mirror_pg17.txt).

**Host: 4 vCPU, 16 GB. Table: 3.2M rows, 12 columns, 646 MB heap+indexes.**
Four vCPUs is a small machine and every absolute number below is bounded by it;
the ratios are what travel.

---

## The numbers

| | Measured | |
|---|---|---|
| **Bootstrap** | 3M rows in 36.0s | **83k rows/s**, snapshot to first ready |
| **Drain rate** | 203k-row burst applied in 8.3s | **24.6k rows/s** sustained through the mirror |
| **Commit-to-visible** | p50 **188 ms**, p90 190 ms | p95 2.4s — see *the tail*, below |
| **Storage** | 646 MB → 116 MB | **5.6× smaller**, 38 bytes/row |
| **Sidecar CPU** | 59.5s over the run | 292 µs/row, on a shared 4-vCPU box |
| **Sidecar RSS** | 1.2 GB steady, **5.9 GB peak** | the peak is the snapshot; see *what is still wrong* |

### Query performance, same data, both engines

Deliberately on a **12-column** table. Column count is the single biggest driver
of the columnar advantage — a column store's win is the columns it does not
read — and the 4-column table this benchmark started with measured **7×** where
the 30-column table in [docs/11](11-measured-results.md) measured **30×**. A
benchmark on a narrow table quietly understates the design it is testing.

| Analytical | PostgreSQL | mirror | |
|---|---|---|---|
| `count(*)` | 141 ms | 52 ms | **2.7×** |
| `sum + avg` | 217 ms | 31 ms | **7.0×** |
| `GROUP BY sku` (100k groups) | 1,599 ms | 379 ms | **4.2×** |
| filtered aggregate | 264 ms | 25 ms | **10.3×** |
| `count(DISTINCT sku)` | 3,213 ms | 255 ms | **12.6×** |
| bucketed histogram | 563 ms | 124 ms | **4.5×** |
| **median** | | | **5.8×** |

| OLTP-shaped | PostgreSQL | mirror | |
|---|---|---|---|
| primary-key lookup | 0.32 ms | 61 ms | **191× slower** |
| 100-row range | 0.41 ms | 57 ms | **140× slower** |
| `ORDER BY id LIMIT 20` | 0.26 ms | 155 ms | **593× slower** |

Every row above was checked for **result agreement** before its timing was
believed. A faster engine returning a different answer is not faster.

The OLTP column is the whole reason `mode: takeover` is gated behind an
explicit acknowledgement. It reproduces [docs/11](11-measured-results.md)'s
finding on completely different hardware, a different schema and a different
scale: **two to three orders of magnitude slower on point lookups.**

---

## How these numbers were arrived at, because the first three were wrong

The first run of this harness reported a drain rate of **6,507 rows/s** and
**6.2 GB of RSS** on a 5M-row table. That was real, and it was the
implementation, not the design.

**1. Compaction was triggered by traffic and cost the table.** `Compact()` reads
every live row into `[]map[string]any`, rewrites the whole base file and rebuilds
the index — and it ran every 32 transactions. So its cost scaled with the
*table* while its trigger scaled with *traffic*. A busy 5M-row mirror rewrote 5M
rows every few seconds.

**2. Two more O(table) costs ran on every single batch.** The key→position index
was a JSON file re-read and re-parsed per batch: at 5M rows, a 5M-entry JSON
parse to tombstone a handful of rows. And every delta file was read back and
rewritten per batch to drop superseded rows. Both were fine at the scale the
harnesses ran at and neither survived contact with a real table.

The fix is one idea applied in two places: **an in-memory key → (file, position)
index**, and **deletion vectors on every file** rather than only on base files.
Marking a row dead becomes O(1) and nothing is rewritten on apply. RSS fell from
6.2 GB to 1.2 GB and the drain rate rose ~4×.

**3. Then the fix over-corrected, and the benchmark caught that too.** With
compaction triggered only by churn, 203 delta files accumulated during a burst —
and the analytical median fell to **2.3×**, with a plain `count(*)` coming out
*slower than PostgreSQL*. Opening files dominates once they are small enough.

The resolution is that these are two different costs and they need two different
triggers:

| Trigger | Action | Cost |
|---|---|---|
| delta rows ≥ max(25k, base/5) | rewrite the base | O(table) |
| delta files ≥ 16 | **merge the deltas** | O(delta rows) |

Merging bounds the read path's file count without paying for a base rewrite.
203 files → 8, and the analytical median went 2.3× → **5.8×**.

> Three rounds, and every one of them was a measurement correcting the previous
> conclusion rather than confirming it. The design survived all three; the data
> structures underneath it did not survive any.

---

## The tail, and what is still wrong

**p95 commit-to-visible is 2.4 s against a p50 of 188 ms.** That is not noise, it
is compaction: merges and base rewrites run **synchronously in the apply loop**,
so every commit landing during one waits for it. The fix is to compact on a
separate goroutine and swap the manifest atomically when it finishes. Not done,
and the cost of not doing it is exactly the number above.

**Peak RSS is 5.9 GB, during the snapshot.** `Snapshot()` and `Compact()`
materialise every row as `map[string]any` before writing. At 3M×12 columns that
is gigabytes of Go maps for what is ultimately a streaming operation. Both should
write in batches. Steady-state RSS (1.2 GB) is the in-memory index, which is
inherent and roughly 380 bytes per row — reducible, but a different problem.

**p50 is 188 ms, not the 36–40 ms of [docs/14](14-phase2-streaming-and-failover.md).**
That earlier figure was a single idle table; this is 200 writes/s against 3.2M
rows with compaction running. Both are real; they measure different things, and
the 188 ms is the one to plan a freshness SLO against.

**24.6k rows/s is a ceiling worth stating plainly.** PostgreSQL ingested the same
burst at 118k rows/s. A source sustaining more than ~25k row-changes per second
on this hardware will outrun a single mirror, and the mirror will readiness-gate
itself out of service — correctly, but it will be out of service. Parallel decode
and batched writes are the levers; neither is implemented.

---

## What this means for the thesis

[docs/10](10-scaling-economics.md)'s break-even is `f ≈ 1/R` — the analytical
share of read traffic needed to justify consolidation — and it is **nearly
insensitive to the speedup**. 5.8× median on a 12-column table clears that bar
as comfortably as 30× did, because the bar was never about the multiple.

What these numbers change is the *shape* of the recommendation:

- **Width matters more than anything else.** 4 columns → 7×, 12 columns → 5.8×,
  30 columns → 30×. If the tables worth mirroring are narrow, mirror something
  else.
- **The gate on `mode: takeover` is not caution, it is arithmetic.** 191× slower
  on a primary-key lookup is not a regression to monitor.
- **Ingest, not query, is the scaling limit.** Every query number above is
  comfortable. 24.6k rows/s is not, and it is the number to fix next.
