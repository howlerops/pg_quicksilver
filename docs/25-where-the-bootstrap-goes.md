# 25 — Where the bootstrap goes, and what the p50 really is

Two numbers this project has been reporting since [docs/18](18-measured-performance.md)
turn out to describe the harness rather than the mirror, and finding that out
was most of the value of looking.

The starting question was ordinary: the workload matrix reports bootstrap as
rows/s, and across shapes that number spans more than an order of magnitude.

| shape | | rows/s |
|---|---|---|
| narrow | 4 columns | ~193,000 |
| wide | 12 columns | ~98,000 |
| inline | 1.2 KB in the heap | ~49,000 |
| jsonb | 6 KB TOASTed | ~16,000 |

The obvious reading is "bigger rows, fewer rows per second", and it is not
obviously wrong. But **rows/s is the wrong unit to decide that in.** narrow
moves 34 MB in the time it spends; jsonb moves 1.6 GB. In bytes the ranking
inverts, and an inverted ranking means the two shapes are limited by different
things.

---

## The profile that had never run

The matrix already takes a CPU profile. It could not answer this, for a reason
worth writing down: **the profile starts at `readyz`, and `readyz` is exactly
the moment the bootstrap finishes.** Every profile this project has taken has
begun one instruction after the code in question stopped running.

`bench/scripts/bootstrap_profile.sh` starts profiling as soon as pprof answers,
which is before the snapshot begins, and reports bytes/s next to rows/s.

### narrow: one core, spent on rows

```
source        34M (89 B/row)
bootstrap     2.2s -> 185074 rows/s -> 16.5 MB/s
Duration: 30s, Total samples = 1880ms (6.27%)

  cum   cum%
1360ms 72.34%  mirror.(*Table).Snapshot
 800ms 42.55%  mirror.(*parquetWriter).flush
 390ms 20.74%  mirror.appendValue
```

1.88s of CPU inside a 2.2s bootstrap: it is **saturating a single core**, not
waiting on a disk or a socket. And 16.5 MB/s is an absurd rate for a machine
that reads 310 MB/s cold and 7.3 GB/s warm ([docs/24](24-nothing-cached.md)).
Nothing here is I/O. The time goes on per-row work — a `map[string]any` and two
slices allocated per row, then Arrow builders appended one value at a time.

### jsonb: one core, spent on bytes

```
source        1.6G (4249 B/row)
bootstrap     25.4s -> 15763 rows/s -> 67.0 MB/s
Duration: 30s, Total samples = 22030ms (73.43%)

   flat  flat%      cum   cum%
 7740ms 35.13%   9090ms 41.26%  zstd.(*doubleFastEncoder).Encode
 2450ms 11.12%   2450ms 11.12%  syscall.Syscall6
 2390ms 10.85%   2390ms 10.85%  runtime.memmove
```

**41% of the sidecar is inside zstd's encoder.** Four times narrow's byte rate,
a fifth of its row rate, and a completely different bottleneck. The hypothesis
was right: rows/s was hiding two different problems behind one number.

---

## The compression level was not the trade it looked like

A base file is written once and scanned many times, and docs/24 showed the read
path converging on the bytes-read ratio as caches get colder. So cheapening
compression to speed up bootstrap should cost something on every query
afterwards.

Measured — same 20,000-row jsonb batch, **write and read**, median of three
(`QS_CODEC_COST=1 go test ./internal/mirror -run TestCodecCost -v`):

| | size | write | read | ratio |
|---|---|---|---|---|
| snappy | 61.4 MB | 529 ms | 197 ms | 1.2× |
| **zstd level 1** | **33.8 MB** | **258 ms** | **190 ms** | **2.1×** |
| zstd default | 33.4 MB | 712 ms | 222 ms | 2.2× |
| zstd level 6 | 32.7 MB | 1369 ms | 182 ms | 2.2× |

It is not a trade at this end of the curve. **Level 1 writes 2.8× faster than
the default for 1.2% more bytes.** Everything above it buys a rounding error in
size for multiples of the CPU.

### Snappy lost on every axis, including the one it was chosen for

Delta files got Snappy because they are written constantly and read repeatedly
until compaction folds them away, so they should use the codec that
**de**compresses fastest. The reasoning was sound. The premise was false: zstd
level 1 reads *no slower* — 190 ms against 197 ms — because Snappy's file is
1.8× bigger and those bytes have to be fetched and touched too. It wins on all
three axes at once.

### The measurement was wrong the first time

The first version of this benchmark used a synthetic document built only from
repeated structure. It compressed **50×**, zstd had almost nothing to do, and
all four codecs came back within noise of each other — which reads as
"compression is not the bottleneck" and is entirely an artifact of the test
data. The real jsonb row is a structured head plus 3,200 characters of
concatenated md5 hex, chosen by `qs-matrix` precisely so it does not compress
and does not dictionary-encode away. Matching that is what made the differences
appear.

### End to end

`SHAPES="jsonb narrow"`, `QS_SNAPPY_DELTAS=1 QS_ZSTD_LEVEL=-1` against the new
defaults. Both verified against the source.

| | | before | after |
|---|---|---|---|
| **jsonb** | drain rate | 151,872/s | **170,699/s** |
| | mirror on disk | 334 MB | 334 MB |
| **narrow**, 12s run | mirror on disk | 139 MB | 109 MB |
| | written to storage | 195 MB (49 B/change) | 125 MB (32 B/change) |
| **narrow**, 8s run | mirror on disk | 108 MB | 107 MB |
| | written to storage | 146 MB (56 B/change) | 134 MB (52 B/change) |
| | bootstrap | 192,773 rows/s | 192,957 rows/s |

jsonb's drain is **12% faster** and its size does not move at all — exactly what
the micro-benchmark predicts, since its bytes are the base file and level 1
against the default is 1.2% there.

**The narrow storage saving is not a settled number, and the two runs above are
why.** A 12-second workload showed 22% less on disk and 36% fewer bytes written;
an 8-second workload showed 1% and 8%. Both are honest measurements of the same
change. What differs is how much of the mirror was sitting in un-compacted delta
files when the tape was measured — the deltas are where Snappy was, so the
saving is a function of the delta fraction at that instant, not a property of
the mirror. The solid number is the micro-benchmark, where the same data is
61.4 MB under Snappy and 33.8 MB under zstd level 1.

**narrow's bootstrap does not move at all**: 192,773 against 192,957 rows/s.
That is the correct outcome and worth stating, because it confirms the split
this document is about — narrow's bootstrap is per-row bound, so making the
codec 2.8× faster buys it nothing. The codec speedup is jsonb's to collect.

Getting that comparison at all required fixing the harness. Until this work the
readiness poll slept one second, so a 1.5-second bootstrap reported as `1.0s` or
`2.0s` depending on where the tick fell — and an earlier pass of this very A/B
showed narrow "regressing" from 1.0s to 2.0s, which was entirely the tick. Every
bootstrap rows/s in docs/18 and docs/19 carries that error bar.

---

## Two numbers that were about the harness

### `p50 = 209 ms` is a constant

Every commit-to-visible figure in docs/18 and docs/19 has a p50 of about 209 ms
and a p90 within six milliseconds of it. That tightness is the tell. **The apply
loop runs on a 200 ms ticker.** The p50 is the interval plus roughly nine
milliseconds of actual work, and it would read 209 ms if the mirror were ten
times faster or ten times slower.

It is not a wrong number — that *is* the latency a reader sees — but it has been
presented as a measurement of the mirror when it is a measurement of a
configuration constant.

### `p99` with n=60 is the worst of 60

The matrix takes sixty latency samples. The ninety-ninth percentile of sixty
observations is the maximum, and the tables print `p99` and `max` as the same
number every time because they *are* the same number. What those rows report is
"the worst sample seen", which is a useful thing to know and is not a p99.

### What the tail actually is, where it was findable

On the wide and deletes shapes the tail is a **synchronous delta merge blocking
the apply tick**:

```
wide   merged deltas rows=368786 seconds=2.281   ->  reported max 2830 ms
```

Compaction is asynchronous ([docs/19](19-workload-matrix.md) round five); this
merge is not. It happens inline on the tick, and it is the entire tail on those
shapes.

**On the narrow shape it is not that, and I have not established what it is.**
narrow logged no such merges in the run that reported a 1106 ms tail.

Its tail is consistently lower under the new codec — 5041 → 1092 ms on the
12-second run, 1961 → 792 ms on the 8-second one — and "smaller deltas, less
work per tick" is a plausible mechanism. But each of those is a single sample
(see above: n=60 makes the "p99" a maximum), the two runs disagree by 2.5×, and
a plausible mechanism with no measurement behind it is exactly what this
document is about. **Unidentified**, recorded as such.

---

## What this changes

| | Before | After |
|---|---|---|
| Bootstrap bottleneck | "bigger rows are slower" | narrow is per-row CPU (16.5 MB/s), jsonb is the codec (41% in zstd) |
| Profiling coverage | started at `readyz` | starts before the snapshot |
| Base file codec | zstd default | zstd level 1 — 2.8× faster write, +1.2% size |
| Delta file codec | Snappy | zstd level 1 — 45% smaller, 51% faster write, no slower to read |
| narrow storage | — | 1–22% less, depending entirely on the delta fraction at that instant |
| What `p50` means | the mirror's latency | a 200 ms ticker plus ~9 ms |
| What `p99` means | a percentile | the worst of 60 samples |
| Bootstrap timing resolution | quantised to 1s by the readiness poll | 20 ms |

## What to do next

1. **Take the bootstrap off one core.** Both shapes saturate a single core for
   different reasons, and the snapshot is a straight-line loop: read rows,
   build maps, append, encode. Reading and encoding could be pipelined, and
   encoding could be sharded by row range.
2. **Stop building a `map[string]any` per row.** The snapshot scans into a
   slice, copies it into a map keyed by column name, and the writer then reads
   it back out by column name. For a 4-column row that is most of the work.
3. **Move the delta merge off the apply tick,** the way the rewrite already is.
   It is a 2.3-second stall on a 200 ms loop.
4. **Sample latency properly.** Sixty samples cannot support a p99; either take
   enough of them or report the maximum and call it that.
5. **Find narrow's tail.** It is not the merge, and everything above is guessing
   until it is instrumented.
