# 27 — The pruning that already works

Both [docs/23](23-storing-an-instant.md) and [docs/24](24-nothing-cached.md)
list the same next step: *push predicates into the manifest — Parquet row-group
statistics are already written, and the view does not use them to skip files, so
a selective filter reads every file the manifest names.*

**That is wrong, and this document exists to retract it.** The measurement that
should have preceded the feature says the feature would add nothing.

---

## The thing the statement missed

`ViewSQL()` takes no predicate. It cannot: it is generated from the manifest,
before anyone has written a `WHERE` clause. So "the mirror prunes files" would
mean a new API, taking a predicate, evaluating it against per-file statistics,
and emitting a narrower view.

Meanwhile the engine reading that view **already has the predicate and already
has the footers**, and it prunes at ROW GROUP granularity — 55 groups per base
file here, against one decision per file for anything the manifest could do.
The mirror would be doing a coarser version of a job already being done.

The only way that is worth building is if the view's own machinery gets in the
way. It has three candidates: a projection per file, a `CAST` on temporal
columns (added in docs/23), and — where a deletion vector exists — a
`WHERE file_row_number NOT IN (SELECT unnest(v) FROM read_json(...))` semi-join.
Any of those could stop a predicate reaching the scan, and then the statistics
are irrelevant because nothing is pushed down to use them.

So: measure first. `bench/scripts/pruning_check.py`, bytes off the block device,
cold, median of three.

```
narrow — 16 files, predicate id BETWEEN 1000 AND 2000

  raw read_parquet + predicate            1.6 MB   -> (1001,)
  the published view + predicate          1.6 MB   -> (1001,)
  the published view, no predicate       24.4 MB   -> (3845868,)
  same, deletion vectors stripped         1.6 MB   -> (1001,)
```

```
wide — same predicate

  the published view + predicate          1.7 MB
  the published view, no predicate       46.9 MB      27.5x saved
```

**The view costs nothing.** It reads exactly what raw `read_parquet` over the
same files reads. The deletion-vector machinery — eleven of sixteen files
carrying a vector — costs 0.0 MB. And the predicate alone already saves 15× on
narrow and 27.5× on wide.

File-level pruning in the manifest would save opening sixteen footers. That is
the whole of it.

---

## The measurement was wrong first, in a way worth keeping

The first version of this script reported that the deletion-vector machinery
cost **21.9 MB, a 14.58× overhead** — a dramatic, plausible, actionable finding.
It was entirely an artifact.

A fresh DuckDB connection loads its own binaries and extensions from disk. With
the page cache just dropped, that cost lands on **whichever query runs first**,
and the script measured the baseline first. Every subsequent query found DuckDB
already resident and looked cheap by comparison.

The tell was a number that could not be true: adding `file_row_number = true` to
a scan appeared to *save* 24 MB. Adding a column cannot make a query read less.
When an optimisation appears to help in a direction it has no mechanism for, the
measurement is wrong.

Each query now pays its initialisation before the counter starts, and the median
of three is reported. The machinery costs 0.0 MB.

**This is the fourth measurement defect this session**, and they rhyme:

| | what it produced |
|---|---|
| a readiness poll sleeping 1s | a 1.5s bootstrap reported as "1.0s" or "2.0s"; a 2× regression that was a tick |
| a filter matching no rows | a pruning number published as a filtering number (docs/24) |
| `p99` over 60 samples | a maximum printed under a percentile's name, for two documents |
| DuckDB init on the first query | a 14.6× overhead that was a process starting up |

None raised an error. All four produced a plausible number, which is the only
thing they have in common and the only warning available.

---

## What this changes

| | Before | After |
|---|---|---|
| "Push predicates into the manifest" | listed as a next step in two documents | retracted; the engine already prunes finer |
| The view's overhead vs raw files | unmeasured | 0.0 MB, on both shapes |
| The deletion-vector semi-join | suspected of blocking pushdown | measured transparent |
| A selective predicate | assumed to read every file | reads 1/15 (narrow), 1/27 (wide) |

## What to do next

1. **Pruning is not the read path's problem.** If the read path gets more work,
   it should come from a measurement of what is actually slow, not from a
   plausible-sounding gap. The measured worst case remains the point lookup
   ([docs/24](24-nothing-cached.md)), and even that is 1.4× cold rather than the
   20× the warm numbers suggested.
2. **The harness needs a first-run rule.** Four defects, four plausible numbers.
   Anything measured once, first, in a fresh process is not a measurement.
