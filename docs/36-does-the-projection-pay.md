# 36 — Does the projection pay?

Yes, between **2× and 12.7×**, and the size of the win is predictable from one
number: how many of the table's columns the query actually reads.

It also **loses by about 16×** on a point lookup, and gets worse at that with
scale.

![Speedup against the PostgreSQL heap, by columns touched and table size](img/projection-benefit.svg)

Everything here comes from
[`bench/results/projection_benefit.json`](../bench/results/projection_benefit.json),
written by [`bench/scripts/projection_benefit.sh`](../bench/scripts/projection_benefit.sh).
The chart is generated from that file by
[`bench/scripts/plot_projection.py`](../bench/scripts/plot_projection.py); no
figure in this document was typed by hand.

---

## Why the old number was not good enough

`serving_pg17.sh` section 9 already reported six ratios, and docs/11 quoted
3.46×–5.07×. Both were measured on a **five-column** table.

A column store's advantage is bytes it does not read. On a five-column table
there is almost nothing to not-read, so that measurement was taken exactly where
the mechanism is weakest — and then generalised. It was also one scale, which
cannot distinguish *faster* from *faster and getting better with size*, and those
two say different things about whether to deploy this.

So this measures the thing the mechanism actually depends on, on a twenty-column
fact table, at three scales.

---

## The measurement

One node, one PostgreSQL 17, 1 GB `shared_buffers` for both sides. The heap and
the mirror are read seconds apart against the same page cache, so there is no
"were the machines comparable" question to argue about. Best of three, because
what is being measured is how fast the engine *can* answer.

The mirror is built by the **production sidecar** on a standby, from a table that
has had deletes and updates applied before the mirror was built — so it carries a
deletion vector and column-partial deltas and has to reconstruct, rather than scan
one freshly compacted file. A serving path measured only on a clean mirror is
measuring something nobody runs.

Every timed query is also **checked for agreement** with the heap. A faster wrong
answer is not a result, and the mirror has three separate ways to be quietly wrong
(retired rows, partial deltas, a stale manifest). All 27 measurements agreed.

Storage, consistently across all three scales: **3.2× smaller** (2936 MiB of heap
became 916 MiB of Parquet).

---

## What it found

Speedup against the heap. **Bold is ≥ 2×**, the threshold this document treats as
material — far enough outside this harness's noise to be real, and big enough to
change what you would build.

| query | cols touched | 0.49M rows | 1.96M rows | 5.88M rows |
|---|---:|---:|---:|---:|
| count(*) | 0 | 0.92× | **3.24×** | **7.50×** |
| sum 1 column | 1 | 1.93× | **6.62×** | **11.19×** |
| sum 2 columns | 2 | 1.52× | **5.80×** | **9.36×** |
| filter + aggregate | 2 | 1.68× | **5.49×** | **8.87×** |
| group by, 2 columns | 2 | **2.27×** | **7.61×** | **12.69×** |
| sum 5 columns | 5 | **2.01×** | **5.04×** | **6.60×** |
| touch 10 columns | 10 | 1.93× | **3.50×** | **4.20×** |
| touch all 19 columns | 19 | 1.08× | 1.98× | **2.00×** |
| point lookup by key | 1 | 0.08× | 0.07× | 0.06× |

### 1. The win is proportional to what you let it skip

At 5.88M rows, reading down the column count:

```
 1 column    11.19x
 2 columns    9.36x
 5 columns    6.60x
10 columns    4.20x
19 columns    2.00x
```

Monotonic, and it is the mechanism rather than a coincidence: a query that reads
everything gives a column store nothing to skip, and the 2× that survives at 19
columns is what encoding and vectorised execution are worth on their own.

This is the number to reach for when someone asks what the mirror will do for
their dashboard. It is not one figure — it is a function of their query.

### 2. It gets better as the data outgrows memory

The same single-column aggregate, across the three scales: **1.93× → 6.62× →
11.19×**.

At 0.49M rows the 245 MiB heap fits inside 1 GB of `shared_buffers`, so
PostgreSQL is reading from RAM and its disadvantage — bytes — costs it almost
nothing. By 5.88M the heap is 2936 MiB and does not fit, and the mirror's 916 MiB
still does. That crossover is the whole argument for the projection, and it is
visible as the gap between the three lines in the chart.

The corollary is the honest caveat: **at small scale this buys you very little.**
Four of the nine queries fail to clear 2× at 0.49M rows, and `count(*)` is
actually *slower* there.

### 3. It is 16× worse at the thing PostgreSQL is for

The point lookup goes **0.08× → 0.07× → 0.06×**: a 2 ms indexed probe against 40 ms
of mirror. It degrades with scale, because an index probe is O(log n) and the
mirror's fixed cost is not.

This is why `mode: takeover` sits behind `acknowledgeOLTPRegression`. Leaving this
row out would have made the table above a sales document; it is in the chart, on
the same axis, so "16× worse" is read off the same ruler as "11× better".

---

## A prediction that was wrong, and why it is worth recording

Before running this, I read `qs-query`'s view generator and predicted the deployed
path would give up much of the benefit. The reasoning looked sound: the generated
view projects **every** column out of `duckdb.query(...)` so the result is a table
to PostgreSQL, so `SELECT sum(amount) FROM events_pq` should make DuckDB hand back
all twenty columns for every row and have PostgreSQL add up one of them.

That is why the benchmark measures a third path — `pushdown`, the same aggregate
pushed inside DuckDB, reading the same files through the same scan (the SELECT is
*lifted back out of the generated view* rather than rebuilt, so the two cannot
drift apart).

The prediction was wrong. At 5.88M rows, in milliseconds:

| query | cols | heap | view | pushdown | view | pushdown |
|---|---:|---:|---:|---:|---:|---:|
| count(*) | 0 | 249 | 33 | 32 | 7.50× | 7.70× |
| sum 1 column | 1 | 538 | 48 | 48 | 11.19× | 11.24× |
| sum 2 columns | 2 | 551 | 59 | 56 | 9.36× | 9.88× |
| group by, 2 columns | 2 | 734 | 58 | 58 | 12.69× | 12.61× |
| sum 5 columns | 5 | 717 | 109 | 106 | 6.60× | 6.79× |
| touch 10 columns | 10 | 1103 | 263 | 260 | 4.20× | 4.25× |
| touch all 19 columns | 19 | 2843 | 1421 | 1422 | 2.00× | 2.00× |

The two columns are the same to within noise. PostgreSQL's planner drops the
unreferenced `r['col']` expressions from the view's target list before execution,
so the columns a query never names cost essentially nothing even though the view
nominally selects them all.

This matters practically: **the deployed path already gets the full benefit**, and
the per-query views that the prediction would have justified building are not
needed. Recording the wrong prediction alongside the measurement is the point —
the reason to measure was that the reasoning was plausible, and plausible is how
this project has been wrong before (docs/32, docs/33).

One caveat on the one place they differ: at 0.49M rows and 19 columns, `view` is
1.08× and `pushdown` 1.58×. That is a single point at the smallest scale, where
every measurement here is closest to the noise floor, and it does not reproduce at
either larger scale. It is not evidence of a real gap.

---

## What this does not measure

- **Warm cache only.** Every timing here has the data in the page cache on both
  sides. That is the dashboard-refresh case, and it is the one that flatters a
  column store *least* — the row store's disadvantage is bytes, and bytes already
  in RAM are nearly free. `serve_compare.py --cold` measures the other one.
- **One row shape.** Twenty columns, five numeric, eleven wide text. A narrower
  table wins less; docs/11's five-column measurement is the same experiment at the
  weak end and agrees with the shape of this curve.
- **One node, four vCPU, 16 GB.** Ratios on a shared runner are worth less than
  ratios on dedicated hardware, which is why the performance sections are opt-in
  (`FULL=1`) rather than part of every CI run.

---

## Reproducing it

```sh
bash bench/scripts/projection_benefit.sh          # ~25 min, writes the JSON
python3 bench/scripts/plot_projection.py          # redraws the SVG
SCALES="8000000" REPS=5 bash bench/scripts/projection_benefit.sh
```

It needs the patched `pg_duckdb` — see [`bench/patches/README.md`](../bench/patches/README.md),
which carries the build recipe, and the `serving` job in `.github/workflows/ci.yml`,
which runs it.
