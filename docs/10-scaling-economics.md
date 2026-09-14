# 10 — Scaling economics: does this actually reduce replica count?

The thesis being tested:

> As the primary grows, a row-store replica gets linearly worse at analytical queries, so you
> add replicas just to keep the same dashboard responsive. A columnar mirror breaks that
> treadmill, so you need fewer readers.

**This is correct, it is the strongest argument for the project, and it is a *throughput*
argument rather than an asymptotic one.** The distinction matters because it tells you which
customers benefit and by how much.

---

## 1. Why replicas get added in the first place

Read replicas do not make a slow query fast. Each replica answers the same query at the same
speed. They buy **concurrency**: more of the same query at once.

```
replicas needed  ≈  (QPS × cost_per_query) / capacity_per_node
```

So if a dashboard query costs 20 s of CPU and you serve 50 of them a minute, you are buying
replicas to absorb `cost_per_query`. Drop that cost 30× (measured) and the capacity
requirement drops ~23× after the concurrency term — bounded below by HA minimums and by the
limits in §4.

This is why the framing "reduce the need for so many readers" is the right one. The win is
not that each query is faster (though it is); it's that **the thing driving replica count
collapses.**

## 2. Three kinds of growth, three different answers

"As the primary grows" hides three distinct pressures, and Quicksilver helps with exactly one
and a half of them.

| Growth in… | Row-store replica | Columnar mirror | Helps? |
|---|---|---|---|
| **Data volume** (bigger tables, same traffic) | Scan cost grows linearly. Add replicas to hold p95 flat. | Cost grows linearly too — but from a **measured 62–141× lower** I/O base, and zone-map pruning can flatten it entirely for windowed queries | ✅ **Yes, strongly** |
| **Read QPS** | Linear in replicas | Linear in nodes, but a **measured ~30× better** constant (~20× under concurrency) | ✅ **Yes** |
| **Write rate** | Replay cost per node grows; single-threaded startup process is the ceiling | Same replay cost, **plus** mirror maintenance | ❌ **No — slightly worse** |

The second row is the ordinary throughput win. The first row is the one worth being precise
about, because it's where the "treadmill" intuition lives.

### The I/O asymmetry

**Measured** on the rig in [11](11-measured-results.md), 30 M rows, 30 columns:

| Query | Row store touches | Column store touches | Ratio |
|---|---|---|---|
| Full-table aggregate (3 cols) | 9,098 MB | 64 MB | **141×** |
| Daily revenue, 30-day window | 9,099 MB | 146 MB | **62×** |
| `count(DISTINCT user_id)` | 9,098 MB | 114 MB | **80×** |

**62–141× less I/O.** An earlier draft of this document estimated ~500× by assuming uniform
column sizes; in reality `ts` and `amount` are the high-entropy columns and dominate any
query touching them. The corrected figure is still decisive, just not as lopsided.

### But be precise about the asymptotics

If the table grows 10× because *ingest rate* grew 10×, the 30-day window also grows 10× and
**both** engines do 10× more work. Columnar keeps its constant-factor lead; it does not have a
better exponent.

If the table grows 10× because of *retention* (more history, same rate), then:

| | Cost |
|---|---|
| Row store, unpartitioned | 10× worse — scans everything |
| Row store, well-partitioned + BRIN | ~flat |
| Columnar with zone maps | ~flat, from a 62–141× lower base |

So the honest claim is:

> Columnar gives a large **constant-factor** win that persists as data grows, and it makes
> partition-pruning behaviour the default rather than something you have to have designed for
> three years ago.

That second clause is where most of the practical value is. Most large Postgres tables are
*not* well-partitioned, because partitioning is a schema decision made before anyone knew the
query patterns. Quicksilver gets pruning behaviour without a migration.

## 3. The consolidation math

Let `R` = current replicas, `f` = fraction of read **time** that is analytical, `S` = speedup,
`C` = concurrency efficiency of a Quicksilver node vs a PG replica (see §4; **measured
0.77**).

**Architecture A** (separate mirror tier — the two tiers each carry their own HA floor):

```
R_new = max(2, ⌈f·R / (S·C)⌉) + max(2, ⌈(1-f)·R⌉)
```

**Architecture C** (hybrid nodes serve both, floors don't stack):

```
R_new = max(2, ⌈ f·R/(S·C) + (1-f)·R ⌉)
```

With `R = 8`, `f = 0.7`, and the **measured** `S = 30`, `C = 0.77` (see
[11](11-measured-results.md#result-4--concurrency-holds-up-c--077) — the original draft
assumed `S = 50, C = 0.5`, giving a near-identical `S·C` of 25 vs 23, so the conclusion is
unchanged):

| | Analytical need | OLTP need | Total |
|---|---|---|---|
| Today | — | — | **8** |
| Architecture A | 0.24 → floor 2 | 2.4 → 3 | **5** |
| Architecture C | 0.24 + 2.4 = 2.64 | (same nodes) | **3** |

**8 → 3, and those 3 are also valid HA failover targets.** That is the number worth putting
in front of a customer — and it's an Architecture C number, which is the clearest economic
argument yet for making C the target rather than a phase-3 nicety. Architecture A only gets
to 5 because the two tiers can't share their HA floors.

### The second-order wins, which are underrated

Consolidation isn't the only saving:

- **Drop the analytics-only indexes on the primary.** Indexes that exist solely to make
  replica dashboards tolerable are pure write-amplification: more WAL, more vacuum, bigger
  heap, slower writes. Moving those queries to columnar lets you drop them — and that speeds
  up the *primary*.
- **Stop trashing the row replicas' buffer cache.** A replica serving both point lookups and
  20 GB scans has its `shared_buffers` continuously evicted by the scans. Remove the scans and
  the remaining row replicas get materially more effective at their actual job — so the
  `(1-f)·R` term above is probably pessimistic.
- **Smaller storage footprint per node.** A 200 GB heap becomes perhaps 30–50 GB columnar, so
  more of the working set fits in page cache.

## 4. Where the thesis breaks

Three limits. All are real; none are fatal; the first is the one to measure.

### 4a. Concurrency — measured, and better than feared

This was the least-known term in the model. Measured, the speedup ratio **holds at ~20×
across 1→16 concurrent clients** rather than collapsing: `C = 0.77`, not the pessimistic 0.5
assumed. Both engines saturate at ~4 clients and then queue cleanly (throughput flat, p95
rising linearly). DuckDB does not degrade *relative* to Postgres under load.

**But the test rig had 4 vCPUs**, so both engines saturated almost immediately and the
scaling question is not really answered. On a 16–32 core node the process model and the
thread pool may diverge substantially. [OQ-13](09-risks-and-open-questions.md#engine) is
improved, not closed.

The structural caveat still stands regardless of `C`: a Quicksilver node absorbs N replicas'
worth of *work*, not of *concurrency*. If your 8 replicas exist because of 5,000 concurrent
connections rather than because queries are expensive, this design saves you very little.

### 4b. Replay is a shared floor that Quicksilver doesn't raise

Every replica — Quicksilver or not — must keep up with the primary's write stream, and
Postgres replay is single-threaded. At high write rates that ceiling determines whether nodes
keep up at all, and it is untouched by any of this.

Worse: an Architecture C node does replay **plus** mirror maintenance, so it is strictly more
loaded than a plain standby. This is survivable because mirror writes are append-mostly and
batched rather than random I/O, but it must be measured, and it means **write-heavy clusters
are the worst fit for this product.** Spike S4 should report mirror-maintenance cost as a
percentage of replay cost.

### 4c. Some replicas aren't there for read throughput at all

If the 8 replicas exist for HA, for geographic distribution, or for connection-count headroom,
then `f·R` is small and the consolidation math returns ~0. The customers who benefit are
specifically those who **scaled past HA minimums because of read load** — 5, 7, 10 replicas
where 2–3 would satisfy HA.

That's a narrower segment than "everyone running CNPG", but it's the segment with the acute
pain and the budget, and Architecture C partly repairs even this case by making mirror nodes
count toward HA instead of being pure overhead.

## 5. What this changes about the plan

1. **The pitch is consolidation, not latency.** "Replace 8 replicas with 3" is a CFO-legible
   number. "Queries are 50× faster" is a benchmark slide. Lead with the first.
2. **It strengthens the case for Architecture C considerably.** §3 shows A gets 8→5 while C
   gets 8→3, purely because C's nodes don't split into two tiers with separate HA floors. C
   was already the technical target; it is now also the economic one.
3. **Spike S0 gets a second output.** It already measures `f` (analytical share of read time).
   It should *also* record why each cluster has the replica count it has — read throughput, HA,
   geography, or connections — because §4c says that determines whether `f` matters at all.
4. **Concurrency measurement is promoted.** `C` is now a headline term in the business case,
   not an engineering footnote. Spike S2 must produce a defensible number for it.
5. **Qualify the target customer explicitly.** Read-heavy, analytics-heavy, scaled past HA
   minimums. Write-heavy clusters and connection-bound clusters are poor fits and we should say
   so rather than discover it in a failed POC.
