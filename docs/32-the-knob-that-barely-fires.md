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

## The gap, named — and then closed

The case this trigger was designed for is a mirror whose **base fills with dead
rows while its deltas stay small** — deletes without accompanying writes, a
retention sweep, a GDPR purge, a partition drop expressed as `DELETE`. No shape
in `bench/scripts/workload_matrix.sh` produced that, which is why the knob's
entire justification rested on a synthetic vector-cost measurement rather than on
a workload.

So the shape was added. `purge` exploits the asymmetry that is the whole point of
a deletion vector: **a DELETE writes no delta row.** It marks a position dead in
the file the row already lives in. Deletes alone therefore drive dead-in-base up
and leave `DeltaRows` where it was, which is the one state where the second
trigger is the only one that can pull.

It is budgeted rather than run flat out — six writers empty a 400k table in under
a second otherwise, and a retention sweep that deletes the whole table is not a
workload anyone runs. Each writer sweeps a quarter of its own slice, then
trickles one insert every 50 ms: enough WAL to keep the drain measurable, far too
little to grow the deltas past the churn threshold.

It lands exactly where intended. 400,000 rows seeded, ~98,000 swept, deltas
merging at one to two thousand rows against a churn threshold of 25,000:

| | `frac=0` | `frac=0.01` |
| --- | --- | --- |
| compactions | **0** | **1**, `why=dead-fraction` |
| correctness | MATCH | MATCH |

The first non-zero result this flag has ever produced. With the knob off, a base
that is a quarter dead is never rewritten at all.

## Both halves of the ledger, at last

`bench/scripts/purge_ledger.py` measures the read side on the mirror each arm
actually produced:

| arm | base rows | dead in base | `count(*)` | `sum` | filtered |
| --- | ---: | ---: | ---: | ---: | ---: |
| off (0) | 400,000 | 100,800 | 15.1 ms | 12.4 ms | 14.6 ms |
| on (0.01) | 299,258 | 0 | 5.7 ms | 4.8 ms | 5.6 ms |

**2.6x on every query**, which is the benefit docs/29 predicted from synthetic
vectors, now confirmed on a mirror a workload built.

The write side, from the matrix: drain rate unchanged (5,075 against 5,092
row-changes/s), latency unchanged, sidecar CPU 14 µs against 28–31 µs per
row-change, bytes written 24 B against 86 B.

So the trade is real and it is not free. But the write-side numbers came with a
confound, and finding it is the better half of this story.

## The mirror that grew by shrinking

The rewritten mirror came out **2.4x larger on disk** than the un-rewritten one,
holding 100,000 *fewer* rows. The base file had shrunk exactly as expected, 2.2
MB to 1.6 MB. One file accounted for all of the growth:

```
index/000003.idx.json   4,610,794 bytes
base/000003.parquet     1,638,400 bytes
```

An index of a file, 2.8x the size of the file. Compaction wrote every key in the
table out as JSON so a later `ensureIndex` could load positions instead of
reading them back from Parquet. The snapshot path had stopped doing this long
ago and `ensureIndex`'s own comment said why — *the file already holds everything
it said* — but both rewrite paths kept writing one, and nobody had looked.

Priced (`TestPersistedIndexCost`, 200k rows), it loses on both axes at once:

```
base/000002.parquet     1,112,975 bytes
the index as JSON       2,977,786 bytes     2.7x the file it indexes

build from the base file   46-60 ms
load from the JSON         61-71 ms
```

Larger *and* slower, consistently. Which is exactly what a column store should do
to a text format: the key column is stored compressed and columnar, while JSON
holds every key as text with punctuation and allocates a map entry per key to
parse it. It reads like a cache, and that is how it survived — a cache that is
bigger than the thing it caches and slower than recomputing it is not a cache.

It is gone. Nothing writes one, nothing reads one, and `removeDataFile` deletes
any a previous version left behind so an upgrading mirror sheds the weight.

That also means **a chunk of the write cost attributed to compaction above was
this file**, not compaction: of the 8 MB the flagged arm wrote, 4.6 MB was the
index JSON.

## Should it default on?

Still no, and now for a reason with a number in it rather than a gap.

The trigger only reaches a workload that deletes without writing. On every other
shape measured — narrow, wide, jsonb, inline, churn, deletes — the churn trigger
fires first and this one changes nothing, so defaulting it on would be a
behaviour change that is inert almost everywhere and meaningful in one case. That
case is real and worth 2.6x on reads, and the operator who has it can set one
environment variable knowing what it buys and what it costs.

What changed is that the sentence is no longer *"nobody has measured this"*. It
is *"measured; it matters for retention-sweep workloads and nothing else, and
here is the trade"* — which is a default decision someone can now disagree with
on evidence.

## The lesson

An A/B that measures nothing does not announce itself. Six runs, three shapes,
consistent numbers, every one verified — and the comparison was between a
configuration and itself. What caught it was a habit rather than an insight:
asking *did the thing I changed actually happen* before reading what happened
next.

The second lesson is the index JSON, and it is a different shape. Nothing was
wrong with that file — it was written correctly, read correctly, and did what it
said. It was simply never priced, and the one word "index" was enough to keep it
unexamined for the life of the project. It cost more than the thing it indexed
and saved less than recomputing it, and the only reason it was found is that a
benchmark measuring something else produced a number that made no sense: a
mirror that got bigger by removing rows. **The anomaly nobody asked about is
worth more than the measurement everyone wanted.**

That is the same defect as [docs/27](27-the-pruning-that-already-works.md), where
an optimisation was measured as a win before anyone checked it was running, and
the same as the corpus in [docs/31](31-a-duplicate-row-under-compaction.md) whose
apparent signal was the diagnostic's own arithmetic. Three times now the
measurement has been the thing that was broken.
