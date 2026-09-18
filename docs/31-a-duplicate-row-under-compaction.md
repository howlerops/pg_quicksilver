# 31 — A duplicate row under compaction

A merge put the same key in one file twice, and nothing could say which copy was
dead. Found at about one run in twenty, which is the rate at which a real bug
gets called a flake:

```
--- FAIL: TestCompactionRacesPartialRows/seed3
    compact_async_test.go: key 79 appears twice in the mirror;
                           a compaction swap kept two copies of the same row
```

A duplicate row is not cosmetic. `count(*)` is wrong, every aggregate is wrong,
and the mirror disagrees with the source while every file on disk is
individually valid.

---

## The wrong answer first

This document previously named a suspect, reached by reading rather than by
measuring: an assumption in `mergeGroup`'s comment that "patches never move
during a rewrite", which the code's own concurrency can violate. It was
plausible, it was specific, and it was **wrong**.

The instrumentation that settled it took four lines — on a duplicate, print
where both copies live:

```
index says key 398 lives at delta/000130.parquet pos 2
LIVE COPY in delta/000130.parquet pos 1
LIVE COPY in delta/000130.parquet pos 2
LIVE COPY in delta/000131.parquet pos 7 (partial=true)
```

**Both live copies are in the same file.** Not a base file racing a delta, not
the compaction swap at all, and nothing to do with the rewrite's `touched` set.
The swap was never implicated; the word "compaction" in the test's name sent the
reading in the wrong direction and the reading confirmed itself.

## What it actually was

`mergeGroup` concatenates the live rows of several delta files:

```go
for _, d := range old {
    got, err := t.readParquetCols(filepath.Join(t.Dir, rel), dead, cols)
    rows = append(rows, got...)          // no dedupe
}
...
for i, r := range rows {
    target.set(k, loc{File: id, Pos: int32(i)})   // last wins, in the INDEX
}
```

If a key is live in two of the merged files, both copies land in the merged
file. The index is then set twice and keeps the later position — which is why
the dump shows it pointing at pos 2 while pos 1 is still there, live, with
nothing referring to it.

And nothing *can* refer to it. A deletion vector addresses positions within
**one** file, and after the merge both positions are in that same file. The
older copy cannot be tombstoned without tombstoning a position that the newer
copy might occupy. The merge does not just fail to clean up a mess; it creates
one that the mirror's only cleanup mechanism cannot express.

The comment immediately above the concatenation already names the precondition:

> …would invert "last occurrence wins" for any key that **somehow appears live
> in two files**.

The ordering consequence was handled. The duplication consequence was not.

## The fix

Deduplicate during the merge, keeping the last occurrence — which is what the
index already recorded and what the read path already assumes. Files merge
oldest-first, so the later row is the newer value; keeping the first would
silently roll the mirror back to a superseded value, which is *worse* than a
duplicate because nothing counts wrong.

It is deliberately a fix at the merge rather than at whatever let a key be live
in two files at once. **A merge that cannot express "this row is dead" must not
create the situation that needs it, however the input arose.**

That reasoning stands. The sentence that followed it did not:

> Those two are different questions, and the second one may well have no bug in
> it at all.

It had one, and the next section is it.

---

## Where the duplicates came from

The merge fix was checked and it worked; the duplicates kept coming, about one
run in a thousand instead of one in twenty, in the **whole-row** test the merge
fix said had never failed. So the merge was one source and there was another.

Two things made the second one findable.

The first was admitting the diagnostic was lying. `whereIs` read each file
through its deletion vector — which drops the dead rows and **renumbers the
rest** — and printed that live-subset index next to the index's physical one.
Every capture therefore read as "the index points at a position where the key is
not", a signal that was this function's own arithmetic. Reading with no vector
and reporting dead-or-live as a column made both numbers mean the same thing.

The second was an amplified hunt: `-count=25`, sixty batches, every failing
output kept. One in a thousand needs thousands of runs, and until the rate was
respected the bug looked like a flake.

What came back was unambiguous:

```
key 40 appears twice in the mirror
  index -> delta/000064.parquet#12
  dead in base/000040.parquet#244: v=u3 n=3
  LIVE in delta/000064.parquet#5:  v=re n=9
  LIVE in delta/000064.parquet#12: v=re n=9
```

Two live copies, one file, **identical values**, index on the later. Identical is
the tell. Two racing writers produce two *different* rows; one row written twice
from the same map produces these.

`Apply` collects a batch into `upserts` (key → row) and `order`, and asked
whether a key was already queued by looking it up in `upserts`:

```go
if _, seen := upserts[k]; !seen { order = append(order, k) }
```

A delete removes the key from `upserts` and leaves it in `order`, which is
correct on its own — `writeUpserts` skips a key with no row. But an insert of the
same key **later in the same batch** then finds `upserts` empty, reads that as
"not queued yet", and appends the key to `order` a second time. `writeUpserts`
walks `order` and writes `upserts[k]` per occurrence, so the delta file gets two
identical copies and the index, written last, names only the second.

The fix is one line of bookkeeping: membership of `order` is tracked separately
from `upserts`, because those were never the same question.

This also explains why the merge fix appeared to work. A merge that deduplicates
its output silently *repairs* a file that arrived carrying a duplicate — so the
merge fix was both a real fix and a mask for the thing that fed it.

### Three symptoms, one bug

The hunt had been treating these as possibly three defects:

| symptom | what was reported |
| --- | --- |
| duplicate row | `key 40 appears twice in the mirror` |
| stale value | `row 90 column v: mirror=re want=u31` |
| deleted row present | `row 338 in mirror but deleted at round 51` |

They are all the second copy.

Once a key has an un-indexed live copy, every later write tombstones the copy the
index names and leaves the other one alone. Update the key and the mirror holds
both the new value and the orphan, and a scan returns whichever file it reads
last — **a stale value**. Delete the key and the orphan survives the tombstone
entirely — **a row that was deleted**. Merge the file the orphan lives in and the
index is repointed at it, which is why that capture showed an index entry for a
key that had been deleted and a deletion vector that did not mark it.

The signal was in every capture and I read past it: each one carried `v=re n=9`.
That is the test's round-9 case, which is the only place a key is deleted and
re-inserted **inside one batch**.

## Verification

At a 5% reproduction rate a single green run is exactly what a change that does
nothing looks like, so both fixes are checked deterministically and then at a
sample size chosen from the rate.

`TestMergeNeverDuplicatesAKey` builds two delta files that both hold a live row
for the same key and merges them, rather than racing for the condition. It fails
with the dedupe removed and passes with it, and it asserts the survivor is the
newer value — a fix that deduplicates to the wrong copy passes the count and
corrupts the data.

`TestDeleteThenReinsertInOneBatchWritesOneRow` covers all five interleavings a
single transaction can produce. Before the fix, one delete-and-re-insert wrote
two copies and two wrote three, which is the mechanism counted out rather than
described.

`QS_RACE_INVARIANTS=1` asserts, after **every** apply, swap and merge of the racy
tests, that each key is live in at most one file and that the index names that
copy. Checked only at the end of a sixty-round run, a duplicate is several merges
and a base rewrite away from whatever created it — which is precisely the
distance the first, wrong diagnosis was reasoned across.

The earlier claim here that the concurrency test "ran 120 times after the fix,
0 failures" was weaker than it read. At roughly one in a thousand for the path
that was still broken, 120 runs had about a 90% chance of showing nothing, so a
clean result was mostly a statement about sample size. The number to quote is the
one the rate demands.

## What this cost, as a lesson

Three things, in order of how much they cost.

The first write-up published a mechanism that was not the mechanism, reached by
reading rather than measuring. It was labelled a suspect, which is the only
reason it did no damage.

The second was the diagnostic that lied. Four lines of instrumentation replaced
an afternoon of wrong reasoning — and then the same four lines printed a
renumbered position for a whole corpus of captures, and the "signal" they created
sent the next reading in a new wrong direction. **An instrument is only evidence
once you have checked what it measures.**

The third is the sentence quoted at the top of this section: a fix shipped with a
stated reason to stop looking. The reason was sound about the merge and wrong
about the mirror, and it turned a known-incomplete diagnosis into a closed one.

That is the same shape as every entry in this project's tally of measurement
defects ([docs/27](27-the-pruning-that-already-works.md)): reading produced a
plausible story, and only measurement produced the true one. The difference here
is that the plausible story was mine and I had already written it down — twice.
