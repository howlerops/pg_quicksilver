# 29 — What a delete costs to read

The twenty-million-row run in [docs/28](28-the-slot-is-a-loaded-gun.md) was
built to find the things that only break at scale. It found one, and it is not a
correctness bug or a memory bug. It is the project's central claim failing at
exactly the size where the claim matters.

Every query agreed with PostgreSQL. Here is what they cost:

```
count(*)                303.5 ms ->  422.9 ms   0.72x   SLOWER than PostgreSQL
sum one column          635.0 ms ->  743.2 ms   0.85x   SLOWER
group by sku top 10    3204.7 ms ->  945.8 ms   3.39x
point lookup              0.3 ms ->  176.0 ms   0.00x
```

`count(*)` was **18.5 ms at four million rows and 422.9 ms at twenty million** —
twenty-three times slower for five times the data. Something in the read path
scales worse than the data does, and until this document nobody knew what.

---

## Ruling things out, in order

The mirror on disk: one base file of 20,367,209 rows, eight deltas totalling
1,580,080 rows, and a deletion vector on the base holding **1,563,904 dead
positions**.

The first measurement separates the files from the machinery that reads them:

```
raw read_parquet, no view machinery        5.9 ms
the published view                       895.6 ms
```

So the files are fine and the view costs 890 ms. The view does three things a
glob does not: it projects columns, it `CAST`s temporal columns, and it applies
deletion vectors. The third is the one that scales with deletes.

**It is not the SQL shape.** Three formulations of the same anti-join, against
the same base file:

```
no deletion vector at all (wrong answer)    2.4 ms
NOT IN  (what the view emits)             353.1 ms
NOT EXISTS                                368.0 ms
ANTI JOIN                                 358.2 ms
```

Within noise of each other. Rewriting the query was never going to help.

**It is not `file_row_number`.** [docs/27](27-the-pruning-that-already-works.md)
named it as a suspect, and on a cold cache it is one. Warm, it is not:

```
base scan, no file_row_number               1.8 ms
file_row_number materialised (max of it)    2.2 ms
```

0.4 ms. Not the problem.

**It is not that a deletion vector forces a full scan.** This was the best
hypothesis — `count(*)` is answered from the Parquet footer, and any `WHERE`
clause should end that. It is wrong:

```
count(*), no predicate at all (footer)      1.9 ms
count(*), a predicate excluding nothing     2.2 ms
count(*), the deletion vector             257.6 ms

sum(amount), no deletion vector            25.3 ms
sum(amount), a predicate excluding nothing 29.3 ms
sum(amount), the deletion vector          310.4 ms
```

A predicate that excludes nothing is free. The deletion vector adds ~280 ms to
*everything*, whether the query had to scan or not.

---

## Where the 280 ms actually goes

Two pieces, with two unrelated fixes:

```
loading the vector                         127.3 ms      13.1 MB of JSON
the anti-join itself                      ~150    ms
```

### The 127 ms was a format nobody chose

A deletion vector is read by two consumers, and only one of them was ever
considered. The mirror reads it in Go, a few times per compaction. The **query
engine reads it on every query**, because that is how the view resolves deletes.
A JSON array of integers is a reasonable format for the first reader and a bad
one for the second: 13.1 MB of decimal text, parsed from scratch, every time
anyone asks anything.

Written as a one-column Parquet file instead — the same 1,563,904 positions:

```
                        load the vector   count(*) through the view   on disk
JSON array                      95.7 ms                    269.4 ms   13.10 MB
one-column Parquet               0.6 ms                    141.6 ms    1.59 MB
```

That is now the format. Vectors already on disk stay readable as JSON, because a
vector a mirror cannot find does not raise anything — it reads as *nothing in
this file is dead*, and every row it retired comes back.

### Range encoding was measured and rejected

The vector on that mirror collapses from 1,563,904 positions into **77
contiguous runs**. A twenty-thousandfold compression, sitting there for the
taking.

It is an artefact of the benchmark. The workload updates
`id BETWEEN $lo AND $lo + 20000`, so of course the dead positions arrive in
twenty-thousand-row bands; there are 77 runs because there were 77 `UPDATE`
statements. Priced against a scattered vector of identical cardinality, the
anti-join costs the same:

```
clustered (77 runs, what is on disk)      247.3 ms
scattered (1,563,904 isolated positions)  207.7 ms
```

The clustered one is marginally *slower*. Encoding for runs would have shipped a
structure tuned to the shape of a benchmark loop.

This is the same failure [docs/20](20-serving-the-mirror.md) had with a filter that
matched nothing and [docs/27](27-the-pruning-that-already-works.md) had with
DuckDB's startup cost landing on the first measured query: **the measurement
produced a plausible number instead of an error.** Three times now. It is the
house specialty.

---

## The half that no encoding fixes

The anti-join remains, and it does not scale the way a fix would want:

```
dead positions in the vector      count(*)
        0  (no vector at all)       1.9 ms   <- answered from the footer
    1,000                          25.0 ms
   10,000                          48.6 ms
  100,000                          84.8 ms
  400,000                         113.7 ms
1,563,904                         146.8 ms
```

Read that column again. Cutting the vector by 94% — 1.56M positions down to
100,000 — saves 62 ms of 147. Removing it *entirely* saves all 147, because with
no vector the branch emits no `file_row_number` and no filter, and `count(*)` is
a number in the footer.

**Most of the cost is that a vector exists at all, not how large it is.** Which
means trimming dead rows is close to worthless and folding them away is close to
everything. Only compaction does the second.

### The floor is not an artefact of the join, either

One hypothesis survived that table: perhaps the 25 ms at a thousand positions is
the price of expressing a tiny vector as a *join against a second file*, when it
could be a literal list the filter evaluates inline. It is not. Same base file,
same positions, the vector written as SQL literals instead:

```
positions      NOT IN (SELECT p FROM read_parquet(...))      NOT IN (1, 5, 9, ...)
      100                                      44.1 ms                    608.5 ms
    1,000                                      56.0 ms                   5683.3 ms
   10,000                                     103.8 ms          did not finish
```

Fourteen times worse at a hundred positions, a hundred times worse at a
thousand, and at ten thousand it ran for minutes before being killed. DuckDB
turns a literal `IN` list into a chain of comparisons rather than a hash set, so
the cost is quadratic in a way the join never is. The join is not the floor's
cause; it is the reason the floor is as low as it is.

(These four numbers come from a separate run under different load than the table
above — 44.1 ms here against 25.0 ms there for comparable work. The comparison
within each run is sound; across them, only the ratios are.)

---

## Why compaction never ran

It was right not to. `ShouldCompact` fires on `DeltaRows >= BaseRows / 5`, and
the mirror had 1,580,080 delta rows against a threshold of 4,073,441. Working
as designed.

The design is sized entirely by the cost of the **rewrite**. Compaction reads
every live row and writes a new base file, so triggering proportionally to the
base keeps amortised write cost constant — the reasoning in `ShouldCompact`'s
comment is correct, and it is one side of a two-sided ledger. The other side is
that every query pays for the vector the rewrite has not folded away, and that
cost tracks the *absolute* number of dead positions.

So the policy tolerates 80,000 dead rows on a 400k-row mirror and four million on
a twenty-million-row one, while the per-query penalty grows with exactly that
number. **The trigger holds the ratio constant and lets the read penalty grow
without bound.** That is the superlinear scaling, stated plainly.

Here is the number that was missing from the other side of the ledger
(`go test ./internal/mirror/ -run TestCompactionCost -v`):

```
  100,000 rows,  20,000 churned -> 244ms    409 k rows/s
  400,000 rows,  80,000 churned -> 1.015s   394 k rows/s
1,000,000 rows, 200,000 churned -> 3.279s   305 k rows/s
```

Extrapolating, a 20M-row compaction is roughly a minute of background rewrite.
Against ~140 ms per query for carrying the vector, that pays for itself after
about **450 queries** — and a mirror that is not serving hundreds of queries
between compactions is not a mirror anybody needs.

---

## Re-measured end to end

[`bench/results/large_scale_20m.txt`](../bench/results/large_scale_20m.txt),
20,458,270 rows, one base file and eight deltas, vectors on five of them:

```
query                            postgres       mirror   speedup  agree
count(*)                         311.4 ms     269.2 ms     1.16x  yes
date arithmetic                  497.3 ms     298.7 ms     1.67x  yes
truncate to the hour            1125.8 ms     220.7 ms     5.10x  yes
sum one column                   558.7 ms     232.3 ms     2.41x  yes
group by sku top 10             3337.2 ms     755.9 ms     4.41x  yes
filter + aggregate               603.4 ms     134.6 ms     4.48x  yes
point lookup by key                0.3 ms      49.0 ms     0.01x  yes

MATCH (mirror 20458270 rows, checksum 4636486f65f841ef)
PASS
```

Against the run this document opens with:

```
count(*)         422.9 ms  0.72x  ->  269.2 ms  1.16x
sum one column   743.2 ms  0.85x  ->  232.3 ms  2.41x
```

Both results that were slower than PostgreSQL are gone, on comparable data:
1,509,100 dead positions against 1,563,904, and vectors on five files rather
than two. The mirror is not being handed an easier problem.

**This is also the first twenty-million-row run this project has verified.** The
two before it reported `FAIL` because `qs-verify` was OOM-killed, which is worse
than a divergence — a divergence is a finding, an unverified run is nothing at
all. It died at 13.3 GB against a mirror weighing 120 MB, because `ForEachLive`
called `readParquet`, which returns a whole file as a `[]map[string]any`; on a
mirror whose base file holds the table that is 21.6 million Go maps. The row-
group streaming already existed in the writer and the decoder, and `readParquet`
existed only to append the groups back into one slice. `qs-verify`'s own comment
said *both sides stream* — its accumulator did, and the thing feeding it did
not. Peak RSS is now 170 MB and it answers.

## What is still open

The compaction trade-off is now specified in both directions, which it was not
this morning. What has *not* been decided is the policy change itself, and it
should not be decided from one shape on one machine:

- A second trigger on absolute dead rows is the obvious move, but the table
  above says its benefit is sublinear — going from 1.5M dead to 100k buys 62 ms
  of 147. The lever that matters is how often a base file is allowed to carry
  **any** vector, which is a statement about compaction frequency, not about a
  dead-row ceiling.
- Under continuous churn a mirror can never reach zero dead positions, so there
  is a floor to the read cost that no policy removes. The 25 ms at 1,000 dead
  positions is that floor at twenty million rows, and it is twelve times the
  footer answer.
- The sidecar itself holds **2.5 GB of RSS** for a 120 MB mirror, 130 bytes per
  live row, which is the key index. Bounded and paid for, but it is the number
  that decides what memory limit a CNPG Pod needs, and it has not been attacked.

The point lookup — 0.3 ms against 176.0 ms before, 49.0 ms after — is a separate
question this document does not touch. A columnar mirror losing a point lookup
to a B-tree by a hundred times is the expected shape of the trade, and
[docs/20](20-serving-the-mirror.md) already says so. It is in the table because
leaving it out would have been the flattering choice.
