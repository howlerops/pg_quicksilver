# 0008 — Implementation language: Go for the plugin, Rust for the data plane

- **Status:** Accepted
- **Date:** 2026-09-15
- **Deciders:** —

## Context

The Phase 1 walking skeleton is Python, flagged in its own README as a prototype. The
question raised: should the data plane be Go or Rust for better concurrency?

**Measured first** — profile in [`bench/scripts/profile_ingest.py`](../../bench/scripts/profile_ingest.py),
40,000 change rows / 15.8 MB of wal2json JSON, single-threaded:

| Stage | Seconds | % of pipeline | Runs in |
|---|---|---|---|
| fetch from slot (libpq + PG decode) | 0.222 | 23.7% | PG + C |
| **`json.loads` with `parse_float=Decimal`** | **0.521** | **55.8%** | **Python** |
| decode to dicts | 0.056 | 6.0% | Python |
| group by key | 0.009 | 0.9% | Python |
| Python objects → Arrow | 0.106 | 11.4% | boundary |
| Parquet write (zstd) | 0.020 | 2.2% | C++ |
| **Total** | **0.934** | | **≈ 42,800 rows/s** |

**74% of the pipeline is Python-side work**, so there is real headroom in a rewrite. But
the shape of the cost is the interesting part:

- The dominant stage is **JSON parsing**, and it is expensive *specifically because
  correctness requires `parse_float=Decimal`*, which disables CPython's C scanner
  (2.6× slower than the C path: 0.204s → 0.521s). Money must not round-trip through
  `float64`.
- The parts people assume are hot — Parquet encoding, compression — are **2.2%**. Those
  are already C++ and a rewrite would not touch them.

Reference points: PeerDB ≈ 120k rows/s (Go), walshadow ≈ 289k rows/s (Rust, physical WAL).
So this pipeline is ~3× off Go and ~7× off Rust.

### The finding that reorders the work

**Removing JSON entirely is worth more than changing language.** `pgoutput`'s binary
protocol has no JSON stage at all. Dropping that 0.521s takes the *existing Python*
pipeline from 42.8k to ~96.9k rows/s — a **2.3× gain, into PeerDB territory, with no
rewrite.**

Protocol and language are separable, and the protocol change is both cheaper and larger.

### Constraints that are not about performance

1. **The CNPG-I plugin must be Go.** `cnpg-i-machinery` is Go. Reimplementing the gRPC
   contract elsewhere is pure cost.
2. **The Phase 3 physical-WAL decoder must run inside Postgres as a background worker**
   ([docs/03](../03-wal-ingestion.md) Path 3c), so it is C or Rust-via-`pgrx`. Go is not
   a practical option for a PG bgworker.

So the project already has Go, and will already have C/Rust. The only open question is
which of those the data plane joins.

## Options considered

### Option 1 — Go everywhere except the PG extension
One language for plugin and data plane; `arrow-go` is adequate; easiest to hire and
operate.
**Against:** Phase 3 still needs a third language, and the decode/write core would then
be written twice — once in Go for the standalone ingest, once in Rust/C for the
in-process bgworker. That duplication is exactly what the change-stream interface exists
to avoid.

### Option 2 — Go for the plugin, Rust for the data plane
**For:** `arrow-rs`/`parquet-rs` are stronger than the Go equivalents. `pgrx` makes the
Phase 3 bgworker tractable. Crucially, the **decode + columnar-write core is written
once** and used by both the standalone ingest (Phase 1) and the in-process decoder
(Phase 3) — the seam is already designed. It is also what walshadow and pg_mooncake's
moonlink both chose for the same workload.
**Against:** two languages, smaller hiring pool, and the plugin/data-plane boundary must
be a clean process or FFI boundary.

### Option 3 — Keep Python
**For:** already works; has proven the semantics and surfaced five real bugs.
**Against:** 42.8k rows/s with no parallel-decode path (the GIL blocks walshadow's
decoder-pool design), and a second runtime in the image.

## Decision

**Option 2**, sequenced so the cheap win comes first:

1. **Replace wal2json with the `pgoutput` binary protocol.** Language-independent, ~2.3×,
   and it also removes the `output_plugin_libraries` allowlisting and the wal2json
   dependency. Do this before any rewrite.
2. **CNPG-I plugin in Go.** Forced by the ecosystem.
3. **Data plane (change stream, columnar writer, compaction, verification) in Rust**, as a
   library with a thin binary around it, so Phase 3's `pgrx` bgworker links the same core
   rather than reimplementing it.
4. **Keep the Python implementation as the reference oracle**, not as a shipped artifact.
   It already encodes the semantics the Rust port must match, and `run_slice` / `run_phase1`
   become differential tests against it.

## Consequences

**Positive.** The decode/write core is written once for both ingest paths. Best-in-class
Arrow/Parquet libraries. Concurrency model that supports parallel decoders when Phase 3
needs them. Python survives as a semantic oracle rather than being thrown away.

**Negative.** Two languages plus a C/Rust extension. A Go↔Rust boundary to design. Slower
initial velocity than continuing in Python.

**Neutral.** Python's 42.8k rows/s is *adequate for many clusters* — at 5k writes/s that
is 8× headroom. The rewrite is required before GA and before Phase 3, **not** to finish
Phase 1. And [docs/10 §4b](../10-scaling-economics.md) already identifies write-heavy
clusters as the worst fit for the product regardless of ingest language.

## Revisit if

- The `pgoutput` change alone lands the pipeline comfortably above real customer write
  rates, and Phase 3 is abandoned after S4 — then Go-everywhere (Option 1) becomes the
  simpler choice and this decision should be reopened.
- A credible Go Postgres-bgworker path appears, removing constraint 2.
