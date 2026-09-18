# 31 — A duplicate row under compaction

**This documents an open, unfixed correctness bug.** It is written now, with a
reproduction and a suspect, rather than after a fix, because the failure mode is
one that gets dismissed: it appears in about one run in twenty and the word for
that is usually "flake".

```
--- FAIL: TestCompactionRacesPartialRows/seed3
    compact_async_test.go:214: key 79 appears twice in the mirror;
                               a compaction swap kept two copies of the same row
--- FAIL: TestCompactionRacesPartialRows/seed2
    compact_async_test.go:214: key 91 appears twice in the mirror;
                               a compaction swap kept two copies of the same row
```

Two failures in forty runs of the test binary. Different seeds, different keys.
A duplicate row is not a cosmetic problem: `count(*)` is wrong, every aggregate
is wrong, and the mirror disagrees with the source while every file on disk is
individually valid.

## What is and is not implicated

It is **specific to column-partial deltas**. `TestCompactionRacesApply`, the
whole-row variant of the same test, did not fail once in those forty runs;
`TestCompactionRacesPartialRows` failed twice. Both run the same interleaving of
apply, background rewrite and `MergeDeltas` — the only difference is that the
partial variant writes updates that omit a column, so they land as patches.

It is **not** the read-side compaction trigger added alongside this
investigation. That was the reason the failure was noticed, so it had to be
ruled out first: six consecutive runs with the change and six with it stashed,
all green, and the trigger is off by default and inert in the test.

## The suspect

The swap reconciles only the keys the apply loop reports as `touched`, and
patched keys are deliberately excluded. `partial.go` says why, and the reasoning
is sound as far as it goes:

> Patches ARE created during a background rewrite, and the swap handles them by
> doing nothing: the rewrite folds in the pre-patch row, the patch it never saw
> was written to a delta outside its snapshot, and the read path puts the two
> back together. That is why a patched key is deliberately NOT added to the
> rewrite's `touched` set — the whole row did not move.

`mergeGroup` carries the same assumption one step further:

```go
if t.compacting != nil && cols == nil {
    t.compacting.touched[k] = true
}
// A merge moves keys too, and a rewrite in flight has to know. Patches
// never move during a rewrite (they only exist inside its snapshot,
// which mergeableDeltas excludes), so this is a whole-row question.
```

**That parenthesis is the thing to check.** Patches created *during* a rewrite
are by definition outside the rewrite's snapshot, which is precisely what makes
them mergeable — so "patches never move during a rewrite" is an assumption the
code's own concurrency is able to violate. If a partial merge relocates or folds
a patch while a rewrite is in flight, the whole row's identity has changed and
`touched` was never told, so the swap has no reason to tombstone the copy it
folded into the new base. The base copy and the delta copy both survive, which
is exactly the shape of the failure.

This is a suspect, not a diagnosis. It was reached by reading, and the two
things that would settle it have not been done: instrumenting a failing run to
print which files the duplicated key's two copies live in and whether it was in
`touched`, and then a fix with the test run enough times to make 5% mean
something. Neither is hard; both were out of budget when this was written, and
shipping a guess at a correctness bug is worse than recording it.

## For whoever picks this up

```bash
# reproduces in roughly one run in twenty
for i in $(seq 40); do
  go -C go test ./internal/mirror/ -run TestCompactionRacesPartialRows -count=1 \
    || echo "FAILED on $i"
done
```

Two cautions from this session's other work:

- **A fix has to be measured against the rate, not against one green run.** At
  5%, a single passing run is what you would expect from a change that does
  nothing at all.
- **CI will go red on this intermittently, and that is correct.** It is not a
  flaky test. The test is right and the mirror is wrong; see
  [docs/27](27-the-pruning-that-already-works.md) for this project's running
  tally of measurements that produced a plausible number instead of an error,
  and treat "just a flake" as one more entry waiting to happen.
