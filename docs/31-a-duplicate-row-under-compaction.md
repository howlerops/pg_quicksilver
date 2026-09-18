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
in two files at once. Those two are different questions, and the second one may
well have no bug in it at all — the apply path tombstones a superseded copy, and
the precondition can also arise from a file written before a fix, or a mirror
restored from a backup. **A merge that cannot express "this row is dead" must
not create the situation that needs it, however the input arose.**

## Verification

At a 5% reproduction rate a single green run is exactly what a change that does
nothing looks like, so the fix is checked two ways.

`TestMergeNeverDuplicatesAKey` is **deterministic**: it builds two delta files
that both hold a live row for the same key and merges them, rather than racing
for the condition. Confirmed to fail with the dedupe removed and pass with it.
It also asserts the survivor is the newer value, because a fix that deduplicates
to the wrong copy passes the count and corrupts the data.

The concurrency test that found it ran 120 times after the fix.

```bash
for i in $(seq 120); do
  go -C go test ./internal/mirror/ -run "TestCompactionRaces|TestPartialDelta" -count=1 \
    || echo "FAILED on $i"
done
```

## What this cost, as a lesson

The first write-up of this bug published a mechanism that was not the mechanism.
It was labelled a suspect rather than a diagnosis, which is the only reason it
did no damage — but it also sat in the repository for an afternoon pointing the
next reader at the wrong file. The instrumentation that replaced it with the
answer was four lines and one run.

That is the same shape as every entry in this project's tally of measurement
defects ([docs/27](27-the-pruning-that-already-works.md)): reading produced a
plausible story, and only measurement produced the true one. The difference here
is that the plausible story was mine and I had already written it down.
