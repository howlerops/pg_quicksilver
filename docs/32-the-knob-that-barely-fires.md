# 32 — The knob that barely fires

`QS_COMPACT_DEAD_FRACTION` shipped off with a note saying it would be defaulted
on once an A/B said what it costs the write path. This is that A/B. It did not
find a cost, and the reason is not that there isn't one.

---

## What the flag does

A base file that carries a deletion vector pays for it on every query. Measured
on a 20.5M-row narrow mirror (`bench/scripts/dv_cost_by_shape.py`), `count(*)`
against the base:

| dead in the base | count(\*) | vs a column scan |
| ---: | ---: | ---: |
| 0 | 2.6 ms | — answered from the footer |
| 20,458 | 64.1 ms | 21.2x |
| 204,582 | 112.9 ms | 38.0x |
| 2,045,826 | 220.3 ms | 75.0x |

The step from **no** vector to **any** vector is 24x; a hundred times more dead
rows is only a further 3.5x. So what matters is how long a base is allowed to
carry a vector at all, and only a rewrite removes one. `QS_COMPACT_DEAD_FRACTION`
adds a second compaction trigger: rewrite once the vector covers that fraction
of the base.

That is the read side. The write side — an extra O(table) rewrite — was the
half nobody had measured.

## The A/B, and what it actually measured

Three shapes from the workload matrix, paired runs alternating between arms so
that drift in the machine hits both, 400k rows and 20s of workload each, every
run verified against its source.

Drain rate, `deletes` shape, three runs per arm:

```
frac=0      182,985   183,245   181,741   rows/s
frac=0.01   183,919   179,595   185,991   rows/s
```

CPU per row-change was 14–15 µs in both arms. Bytes written and RSS moved by
more between runs of the *same* arm than between arms. On `jsonb`, `frac=0.01`
came out marginally *faster*. Every run reported MATCH.

Read as a throughput comparison, that is a knob that costs nothing.

It is not. Counting the compactions each run performed:

| shape | frac=0 | frac=0.01 |
| --- | --- | --- |
| deletes | 5 | 5 |
| churn | 0 | 0 |
| jsonb | 3 | 3 |

**Identical.** The second trigger had not caused a single extra rewrite, so both
arms were running the same behaviour and the throughput numbers were comparing a
configuration with itself.

## Why it does not fire

`ShouldCompact` tests churn first and returns early:

```go
if t.State.DeltaRows >= threshold {   // max(BaseRows/5, 25_000)
    return true
}
return t.baseVectorTooExpensive()
```

so the dead-fraction trigger only gets a say while the deltas are below that
bar. Any workload that deletes enough rows to fill a base's vector is usually
also writing enough to clear it — the matrix's `deletes` shape mixes inserts 1:1
with deletes, so the deltas grow with every pair.

Rather than reason about that, the code was changed to say which trigger pulled,
and the runs repeated:

```
deletes   frac=0      6 why=churn
deletes   frac=0.01   5 why=churn   1 why=dead-fraction
jsonb     frac=0      3 why=churn
jsonb     frac=0.01   3 why=churn
churn     both        no compaction at all
```

Sixty seconds of workload across three shapes, and the new trigger moved exactly
one rewrite slightly earlier. The total was unchanged.

## What changed

`CompactReason` replaces the boolean and names the trigger, and the sidecar logs
it (`compaction started ... why=churn`). A base rewrite is the most expensive
thing the process does; "why now" should not require reading the function. It is
also what separates *costs nothing* from *does nothing*, which this A/B could
not do until it existed.

The default stays at 0 — for a better reason than before. It is not that the
write cost is unknown; it is that at these shapes the trigger is shadowed, so
turning it on would buy the measured read win only in a case none of these
benchmarks reach.

## The gap, named

The case this trigger was designed for is a mirror whose **base fills with dead
rows while its deltas stay small** — deletes without accompanying writes, a
retention sweep, a GDPR purge, a partition drop expressed as `DELETE`. No shape
in `bench/scripts/workload_matrix.sh` produces that, which is why the knob's
entire justification rests on a synthetic vector-cost measurement rather than on
a workload.

Until a delete-dominated shape exists, "should this be on by default" is not a
question this repository can answer, and the flag should not be flipped on the
strength of numbers that describe a trigger which did not pull.

## The lesson

An A/B that measures nothing does not announce itself. Six runs, three shapes,
consistent numbers, every one verified — and the comparison was between a
configuration and itself. What caught it was a habit rather than an insight:
asking *did the thing I changed actually happen* before reading what happened
next.

That is the same defect as [docs/27](27-the-pruning-that-already-works.md), where
an optimisation was measured as a win before anyone checked it was running, and
the same as the corpus in [docs/31](31-a-duplicate-row-under-compaction.md) whose
apparent signal was the diagnostic's own arithmetic. Three times now the
measurement has been the thing that was broken.
