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
replicas to absorb `cost_per_query`. Drop that cost 50× and the capacity requirement drops
50× — bounded below by HA minimums and by concurrency limits (§4).

This is why the framing "reduce the need for so many readers" is the right one. The win is
not that each query is faster (though it is); it's that **the thing driving replica count
collapses.**

## 2. Three kinds of growth, three different answers

"As the primary grows" hides three distinct pressures, and Quicksilver helps with exactly one
and a half of them.

| Growth in… | Row-store replica | Columnar mirror | Helps? |
|---|---|---|---|
| **Data volume** (bigger tables, same traffic) | Scan cost grows linearly. Add replicas to hold p95 flat. | Cost grows linearly too — but from a 50–500× lower base, and zone-map/partition pruning can flatten it entirely for windowed queries | ✅ **Yes, strongly** |
| **Read QPS** | Linear in replicas | Linear in nodes, but ~50× better constant | ✅ **Yes** |
| **Write rate** | Replay cost per node grows; single-threaded startup process is the ceiling | Same replay cost, **plus** mirror maintenance | ❌ **No — slightly worse** |

The second row is the ordinary throughput win. The first row is the one worth being precise
about, because it's where the "treadmill" intuition lives.

### The I/O asymmetry

Concrete: `events`, 60 columns, ~2 KB/row.

```sql
SELECT date_trunc('day', ts), count(*), sum(amount)
FROM events WHERE ts > now() - interval '30 days' GROUP BY 1;
```

At 10M rows in the window:

- **Row store:** reads whole 8 KB pages to reach 3 columns → ~20 GB touched.
- **Columnar:** 3 columns, ~24 B/row raw, 4–8× compression → **~40 MB touched.**

That's ~500× less I/O before the vectorised executor does anything. Even conceding a large
chunk back to overheads, you are in 100×+ territory.

### But be precise about the asymptotics

If the table grows 10× because *ingest rate* grew 10×, the 30-day window also grows 10× and
**both** engines do 10× more work. Columnar keeps its constant-factor lead; it does not have a
better exponent.

If the table grows 10× because of *retention* (more history, same rate), then:

| | Cost |
|---|---|
| Row store, unpartitioned | 10× worse — scans everything |
| Row store, well-partitioned + BRIN | ~flat |
| Columnar with zone maps | ~flat, from a 500× lower base |

So the honest claim is:

> Columnar gives a large **constant-factor** win that persists as data grows, and it makes
> partition-pruning behaviour the default rather than something you have to have designed for
> three years ago.

That second clause is where most of the practical value is. Most large Postgres tables are
*not* well-partitioned, because partitioning is a schema decision made before anyone knew the
query patterns. Quicksilver gets pruning behaviour without a migration.

## 3. The consolidation math

Let `R` = current replicas, `f` = fraction of read **time** that is analytical, `S` = speedup,
`C` = concurrency efficiency of a Quicksilver node vs a PG replica (see §4; assume a
pessimistic 0.5).

**Architecture A** (separate mirror tier — the two tiers each carry their own HA floor):

```
R_new = max(2, ⌈f·R / (S·C)⌉) + max(2, ⌈(1-f)·R⌉)
```

**Architecture C** (hybrid nodes serve both, floors don't stack):

```
R_new = max(2, ⌈ f·R/(S·C) + (1-f)·R ⌉)
```

With `R = 8`, `f = 0.7`, `S = 50`, `C = 0.5`:

| | Analytical need | OLTP need | Total |
|---|---|---|---|
| Today | — | — | **8** |
| Architecture A | 0.22 → floor 2 | 2.4 → 3 | **5** |
| Architecture C | 0.22 + 2.4 = 2.6 | (same nodes) | **3** |

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

### 4a. Concurrency, not data volume, is the likely ceiling

DuckDB is an embedded, single-process engine tuned for a modest number of heavy queries. A node
fielding 500 concurrent small-to-medium dashboard queries will hit memory and thread contention
long before it hits any I/O limit — and Postgres's process-per-connection model is genuinely
*better* at high concurrency of small queries.

So one Quicksilver node does **not** cleanly absorb ten replicas' worth of *concurrency*. It
absorbs ten replicas' worth of *work*, provided that work arrives as a manageable number of
large queries. If your 8 replicas exist because of 5,000 concurrent connections rather than
because queries are expensive, this design saves you very little.

That's the `C` term in §3, it's [OQ-13](09-risks-and-open-questions.md#engine), and it's the
one number in the whole model I'd least want to guess at. **Spike S2 must measure it.**

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
