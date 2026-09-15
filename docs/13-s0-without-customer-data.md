# 13 — S0 from a general standpoint

**Question:** S0 (what fraction of `-ro` traffic is analytical?) was the project's gating
spike, and it needs production data we don't have. Can it be answered generally?

**Answer: largely yes — and doing so demotes S0 from a blocking gate to a sizing input.**
Three routes, in descending order of strength.

---

## Route 1 — Break-even analysis (needs no data at all)

The consolidation model from [10](10-scaling-economics.md), using **measured** inputs
(S2b's 22.2× in-product × 0.77 concurrency = `S·C = 17.1`):

```
R_new = max(2, ⌈ f·R/(S·C) + (1-f)·R ⌉)
```

Rather than asking "what is `f`?", ask **"what `f` would we need?"** — the analytical share
at which the first replica is saved:

```
f* = 1 / (R · (1 − 1/(S·C)))
```

| Current replicas `R` | Break-even `f*` |
|---|---|
| 4 | 26.6% |
| 8 | **13.3%** |
| 16 | **6.6%** |
| 32 | **3.3%** |
| 64 | **1.7%** |

**Two things fall out, and both matter more than the exact value of `f`:**

**`f* ≈ 1/R`.** Break-even is governed by *fleet size*, not by how fast the column store is.
Anyone running 8+ read replicas needs only a low-double-digit analytical share to come out
ahead; at 16+ replicas, single digits.

**It is almost insensitive to the speedup.** Even if `S·C` collapsed from the measured 17.1
to 5 — far worse than anything observed — `f*` at R=8 moves only from 13.3% to 15.6%:

| `S·C` | `f*` at R=8 | `f*` at R=16 |
|---|---|---|
| 5 | 15.6% | 7.8% |
| 10 | 13.9% | 6.9% |
| **17.1 (measured)** | **13.3%** | **6.6%** |
| 30 | 12.9% | 6.5% |

Once the speedup is large, the analytical work effectively vanishes from the capacity
equation and what remains is just "is there at least one replica's worth of analytical
load?". Further speedup buys almost nothing. **That means the precise value of `S` — the
number this project spent most of its measurement effort on — barely affects the business
case.** What matters is `f` and `R`.

> **The ≥40% threshold in [08](08-roadmap-and-spikes.md) was arbitrary and about 3× too
> conservative.** It has been corrected to the `R`-dependent break-even.

---

## Route 2 — Published workload traces (indirect, but verified)

No public trace measures the Postgres `-ro` analytical/OLTP split directly. Cloud-warehouse
traces are the closest public data, and they are *warehouse-only*, so `f ≈ 1` by
construction. **They cannot tell us `f`.** What they do establish is something we need
anyway:

**Execution time is extremely concentrated.**

- **Redset** — an Amazon Redshift trace of three months of query metadata from 200 instances
  — shows "extreme skew and long-tail behavior … fewer than **0.1% of queries account for
  roughly 25% of total CPU time**." ([Redbench, arXiv 2511.13059](https://arxiv.org/pdf/2511.13059))
- **Snowflake**, over 667 million production queries in two weeks: filter operators are "only
  10% of operators, [but] they account for **48.2% of CPU time**"; `SELECT` is 47% of
  statements and metadata `SHOW` commands another 31%; the workload is read-dominated at a
  **25:1 read/write ratio**.
  ([VLDB vol.18 p.5126](https://www.vldb.org/pvldb/vol18/p5126-bress.pdf))

Why this matters here: the consolidation model uses **`f` weighted by time**, and heavy
concentration means `f`-by-time is far larger than `f`-by-count. A team looking at its query
log and thinking "we're 99% point lookups" is reasoning about counts, and is likely wrong
about the quantity that governs replica count.

**This effect is large.** On the test workload built for the toolkit below — 800 point
lookups and 3 aggregate scans, deliberately OLTP-heavy:

| Measure | Value |
|---|---|
| `f` by **calls** | **0.4%** |
| `f` by **time** | **18.0%** |

A **45× gap**, and the by-time figure clears the 13.3% break-even while the by-count figure
suggests the idea is hopeless. Three queries out of 825.

---

## Route 3 — Make it self-serve

The remaining uncertainty is per-cluster, so the practical answer is not to research `f` but
to make measuring it trivial: [`bench/s0_workload_profile.sql`](../bench/s0_workload_profile.sql).

```
psql -d yourdb -f s0_workload_profile.sql
```

One file, no dependencies, read-only, **emits no query text** so the output is safe to
share. It reports `f` by time and by calls, the break-even for the given replica count, the
resulting consolidation estimate, a concentration profile, and a sensitivity sweep.

Two design choices worth stating:

**It classifies by execution shape, not SQL text.** A call is analytical if it touches many
8 KB blocks per execution. That is dialect- and ORM-independent; regex over query text is
not, and would misclassify every ORM-generated aggregate.

**It asks why the replicas exist.** Per [10 §4c](10-scaling-economics.md), if they are there
for HA, geography, or connection headroom, `f` is irrelevant and consolidation returns
nothing. The script flags connection-bound clusters explicitly.

*Verified working against a real cluster; sample output in
[`bench/results/s0_sample_output.txt`](../bench/results/s0_sample_output.txt). Two bugs found
by testing it, both of which would have made it silently wrong: `psql`'s `\set` swallows
trailing comments into the value, and Postgres regex uses `\y` for word boundary — `\b` is
backspace, so the DML exclusion matched nothing and silently did nothing.*

---

## The reframing — why S0 no longer blocks

S0 was treated as a go/no-go gate because a wrong answer meant the product regressed
production. **That was only ever true for `serviceMode: takeover`.**

[Result 3](11-measured-results.md#result-3--oltp-regression-is-far-worse-than-predicted) —
the 935× median OLTP regression — already made takeover indefensible under Architecture A,
and the default was set to `off`: the mirror lives on a *separate* `app-olap` endpoint and
`-ro` is untouched. In that mode there is **no correctness or latency downside**. Nothing
regresses. The only cost is the node itself.

So:

| | Old framing | Now |
|---|---|---|
| **Phase 1 (`serviceMode: off`)** | Blocked on S0 | **Not blocked.** `f` is a sizing input: how many mirror nodes, and whether the customer saves money |
| **Phase 3 (`takeover`)** | Blocked on S0 | Still gated on S0 — and rightly so |
| **Sales qualification** | — | `f` is the qualifying question, answered in 5 minutes by the script |

**S0 moves from "research project blocking the roadmap" to "a question each prospect answers
about themselves in five minutes."** The break-even table tells you the bar is low; the
script tells you whether a given cluster clears it.

### What is still genuinely unknown

- The **distribution of `f` across real Postgres fleets.** Routes 1–3 tell us the bar is low
  and how to measure it per cluster; they do not tell us what fraction of the market clears
  it. That needs design partners, and it is a market-sizing question, not an engineering gate.
- Whether replicas are predominantly added for read throughput vs HA/connections. Same
  answer: ask prospects, using section 3 of the script.
