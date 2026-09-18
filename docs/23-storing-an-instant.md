# 23 — Storing an instant, not a sentence about one

[docs/20](20-serving-the-mirror.md) listed one compatibility gap above all the
others: **the mirror's timestamps are not timestamps.** The writer maps what it
does not recognise to a string, and until now that included every temporal type,
so a query engine pointed at the mirror saw `VARCHAR`.

That is enough to `GROUP BY` and enough to `ORDER BY` — ISO-8601 with a fixed
offset happens to sort correctly — and it is not enough for the query people
actually write:

```sql
SELECT count(*) FROM events WHERE ts > now() - interval '1 hour'
```

Against a `VARCHAR` column this does not return a wrong answer. **It does not
parse**, which is worse in a specific way: it means a query written against
PostgreSQL cannot be pointed at the mirror at all. The whole premise of
[docs/20](20-serving-the-mirror.md) — that the mirror publishes a SELECT any
engine can evaluate — stops at the first `WHERE` clause with a date in it.

---

## Why this had to wait for docs/22

Parsing text into an instant means knowing which timezone and which `DateStyle`
produced the text. Before [docs/22](22-one-instant-two-spellings.md) the honest
answer was *"whichever session happened to render this row"*, and three
different sessions render into a mirror. Parsing under that regime is a guess
that is right most of the time, which is the worst kind.

With the rendering pinned, every temporal value entering the mirror is ISO-8601
in UTC. Parsing it is now a fact. The ordering of these two pieces of work was
not a preference; the second is unsound without the first.

---

## What is stored

| PostgreSQL | Arrow | The view calls it |
|---|---|---|
| `timestamptz` | `Timestamp(µs, "UTC")` | `TIMESTAMP WITH TIME ZONE` |
| `timestamp` | `Timestamp(µs)` | `TIMESTAMP` |
| `date` | `Date32` | `DATE` |
| `time` | `Time64(µs)` | `TIME` |

Microseconds because that is PostgreSQL's resolution exactly — not nanoseconds,
which would be a wider type storing no more information and clipping the range
to 1677–2262.

**`time with time zone` is deliberately not in this table.** An offset without a
date is not a point on any timeline, so there is nothing to map it to; it stays
text. PostgreSQL's own documentation recommends against the type.

`interval` is not in the table either, for the same reason: it is a duration
with three independent fields (months, days, microseconds) that cannot be
collapsed into one number without knowing which instant it is measured from.
Arrow has `MonthDayNano`, which is the right shape — this is a gap, not a
refusal, and it is listed at the bottom.

### `infinity` round-trips; a BC date halts

PostgreSQL's temporal range is wider than an Arrow timestamp's, and it contains
two values that are not instants at all.

`infinity` and `-infinity` store as the largest and smallest `int64` — which is
how PostgreSQL stores them too — so they round-trip exactly and sort and compare
exactly as they should, with no engine needing to know anything.

Everything else outside the range **halts the mirror**:

```
halting public.events: column "at" (timestamptz) holds
"0044-03-15 12:00:00+00 BC", which is outside the range an Arrow timestamp
can represent. Storing it as anything else would be silently wrong. Rebuild
the mirror with QS_TEMPORAL_TYPES=0 to store temporal columns as text instead.
```

The halt is durable, names the table, the column and the value, and tells the
operator the one action that recovers. The alternatives were to store `NULL`
(a lie that reads as missing data) or a silently wrapped instant (a lie that
reads as a plausible date in the wrong millennium). **A zero here is
1970-01-01 — a real instant, indistinguishable from data.** Every other line of
this package exists to avoid exactly that, so the check runs *before* the writer
opens the file and the mirror stops with nothing half-written on disk.

---

## The numbers

`bench/scripts/workload_matrix.sh` with `QS_SERVE_COMPARE=1`, median of three,
every result compared value by value against PostgreSQL first.

| shape | query | source | mirror | | agrees |
|---|---|---|---|---|---|
| narrow (4.0M rows) | `WHERE ts > now() - interval '1 hour'` | 93.8 ms | 33.0 ms | **2.84×** | yes |
| narrow | `date_trunc('hour', ts)` group by | 236.9 ms | 20.9 ms | **11.34×** | yes |
| wide (2.2M rows) | `WHERE ts > now() - interval '1 hour'` | 69.2 ms | 23.4 ms | **2.96×** | yes |

The interesting comparison is not against PostgreSQL, though — it is against the
previous column of this project's own results, where **these two queries could
not be run at all.** Going from "no answer" to "the same answer, 2.8× faster" is
the result; the multiplier is a detail.

Note how much better `date_trunc` does than the range filter: 11.3× against
2.8×. Truncating to the hour is arithmetic on the stored integer and touches one
column. The range filter reads more of the column, and the reason is not what this
originally said. It blamed the view for not using row-group statistics to skip
files; [docs/27](27-the-pruning-that-already-works.md) measured that and the
view is transparent — the engine prunes on those footers already. What is left
is that `ts` here is an insert-time clock, so a one-hour window covers a large,
contiguous and recent part of the table, while `date_trunc` touches one column
and skips nothing at all.

### The storage claim, which turned out to be wrong

The obvious expectation is that eight bytes beat twenty-nine characters. Same
million rows, written both ways, so the only variable is the encoding
(`QS_SIZE=… go test ./internal/mirror -run TestTemporalStorageCost -v`):

| timestamps are… | as text | as instants | |
|---|---|---|---|
| **monotone** (an append-only log) | 5.4 MB | 8.1 MB | **50% bigger** |
| **random** over a quarter | 14.8 MB | 12.8 MB | 13% smaller |

Zstd over sorted ISO-8601 is extremely good: consecutive rows share a 20-odd
character prefix, and the compressor eats it. A plain `int64` column has no such
structure to give away. So for the shape the mirror sees *most* — an event log
with an ascending timestamp — **storing the instant costs space rather than
saving it.**

It is still the right trade, because the text was unusable to a query engine and
50% of one column is a small price for a query that runs at all. But "we made it
smaller" would have been a pleasant, plausible, unchecked claim, and it is
false. It is recorded here in the same place as the wins.

---

## Two findings, both silent

**PostgreSQL strips trailing zeros in temporal output.** Ten microseconds prints
as `.00001`, not `.000010`. This was found by a test that asserted the second
spelling, and settled by asking a real PostgreSQL rather than by reasoning about
it — the mirror was right and the test was wrong. Off by one trailing zero, and
`qs-verify` reports every row of the column as a divergence.

**A check that walks a list of columns checks nothing when the list is `nil`.**
`writeParquetCols` takes `cols []string`, where `nil` means "every column" and
is resolved inside the writer. `checkTemporal` was called before that
resolution, so for a whole-row write — which is *most* writes — it iterated over
an empty list and passed. The narrow path (column-partial deltas, which pass an
explicit list) was covered; the common one was not, and a BC timestamp went
straight through. The fix is one line and the lesson is not: **a guard that
silently has nothing to guard looks exactly like a guard that passed.**

---

## The escape hatch

`QS_TEMPORAL_TYPES=0` restores the old behaviour for the whole mirror —
temporal columns stored as the text PostgreSQL rendered, which is what every
mirror written before this holds. It is the documented recovery from a halt, and
it is exercised by the test suite rather than merely offered.

Files written before this change are still readable alongside files written
after it: the view `CAST`s every temporal column, because a `UNION ALL` mixing a
`VARCHAR` branch with a `TIMESTAMP` one either fails to plan or quietly degrades
every branch to `VARCHAR`. The cast is a no-op the engine elides when the types
already agree, and a migration when they do not.

---

## What this changes

| | Before | After |
|---|---|---|
| `WHERE ts > now() - interval '1 hour'` | does not parse | 2.84× faster than the source |
| `date_trunc('hour', ts)` | does not parse | 11.34× faster than the source |
| Row-group statistics on a timestamp | min/max of strings | min/max of instants |
| A value Arrow cannot hold | stored as text, no problem | halts, naming the row |
| `interval`, `timetz` | text | still text, and now on purpose |

## What to do next

1. ~~**`interval` as `MonthDayNano`.**~~ — measured, and it is now a refusal
   with reasons rather than a gap. See
   [the section below](#interval-is-blocked-in-three-places-not-one).
2. ~~**Prune on the statistics.**~~ — retracted:
   [docs/27](27-the-pruning-that-already-works.md) measured it. The engine reads
   those min/max footers already, and the statistics this work made prunable are
   being used — they just are not the mirror's to act on.
3. **Measure a cold cache** — still [docs/20](20-serving-the-mirror.md)'s item
   2, and still the fairer question.


---

## `interval` is blocked in three places, not one

This document called `interval` "a gap, not a refusal" and listed mapping it to
Arrow's `MonthDayNano` as the first thing to do next. Trying it found three
independent blockers, and the third is the one that matters.

**1. arrow-go will not write the type.** `MonthDayNanoInterval` exists in Arrow
and appears in `pqarrow`'s schema *test*, not in its writer:

```
not implemented: support for INTERVAL_MONTH_DAY_NANO
```

**2. Parquet's own `INTERVAL` could not hold it anyway.** The logical type is
(months, days, **milliseconds**), and PostgreSQL's resolution is microseconds.
DuckDB — which does implement it — loses the bottom three digits on its own
round trip:

```
in memory            1 year 2 days 03:04:05.123456
through a parquet file          ...03:04:05.123000
```

So arrow-go not implementing it is closer to a mercy than an omission.

**3. A struct column works, and the MIGRATION does not.** Storing
`struct<months:int32, days:int32, micros:int64>` writes to Parquet, reads back
in DuckDB, and reconstructs losslessly into a real `INTERVAL` that compares
against interval literals:

```sql
to_months(d.months) + to_days(d.days) + to_microseconds(d.micros)
```

That is a working design. What has no answer is the files already on disk.
Every temporal migration in this document works the same way — the view `CAST`s
the old text column to the new type, so a `UNION ALL` of old and new files
plans. For `interval` that cast does not work, in two different ways:

```
CAST('1 year 3 mons 19 days -04:23:45.536088' AS INTERVAL)      OK
CAST('-1 years -4 mons +10 days -12:54:21.024382' AS INTERVAL)  Conversion Error
CAST('-5 mons +12 days 03:46:59.379269' AS INTERVAL)            Conversion Error
CAST('1 mon -1 days' AS INTERVAL)                               29 days   <-- WRONG
```

DuckDB cannot parse the explicit `+` that PostgreSQL prints on a positive
component following a negative one — which, on a corpus of 317 intervals
generated by PostgreSQL 17, is not a rare shape. And where the cast does
succeed it is **wrong**: `1 mon -1 days` becomes `29 days`, collapsing a month
into thirty days, which is the exact collapse the three-field representation
exists to prevent.

So the storage change is possible but it cannot be a cast-based migration. It
needs every existing file rewritten by compaction before the view can serve the
column as `INTERVAL`, and a mirror serving a mixture during that rewrite would
have to pick one representation and be wrong about the other. That is a real
piece of work, not an afternoon, and it is worth doing only for someone who
actually has an `interval` column they want to query as a duration.

**What did land**, in `internal/mirror/interval.go`: a parser and renderer for
PostgreSQL's `IntervalStyle=postgres` text, checked against those 317
PostgreSQL-generated intervals with zero render failures and zero round-trip
failures. That is the part that would have been guessed wrong — the format has
rules that look like bugs until you see enough of them:

```
1 year 3 mons 19 days -04:23:45.536088       a negative part among positives
-1 years -4 mons +10 days -12:54:21.024382   an explicit + on a positive part
-5 mons +12 days 03:46:59.379269             ...but NOT on the time here
-1 years                                     plural, because -1 != 1
```

The sign rule is that a part prints an explicit `+` when it is positive and the
part *immediately before it* was negative — not "any earlier part", which is the
reading that gets the third line wrong. Whoever picks this up next starts with
that solved and a test that proves it.
