# 0008 — Implementation language: Go for the plugin, Rust for the data plane

- **Status:** Accepted
- **Date:** 2026-09-15 (context corrected 2026-09-15 — see *Correction*)
- **Deciders:** —

## Correction

The first version of this ADR contained a **measurement error and a reasoning error**,
both caught by review. Corrected below; the decision stands, but for different reasons.

1. **The `parse_float=Decimal` penalty was reported as 2.6×. It is 1.3×.** The profile
   compared parse-and-*discard* (C scanner) against parse-and-*retain-in-a-list*
   (Decimal), so the Decimal figure also carried 40k dict allocations. Measured
   consistently: 0.424s vs 0.553s. The JSON stage is expensive because materialising
   16 MB of JSON as Python objects is expensive — not because of a correctness tax.

2. **"Protocol change is worth more than language change" was wrong as stated.** It was
   measured entirely within Python, which assumes the JSON stage stays slow. It does not
   in a native implementation. `orjson` (Rust) parses the same payload **2.2× faster than
   CPython's C scanner**, and a genuine Rust pipeline skips Python-object materialisation
   altogether — which is the part that actually dominates.

The corrected reading strengthens the case for Rust and removes the performance
justification for sequencing `pgoutput` first.

## Context

The Phase 1 walking skeleton is Python, flagged in its own README as a prototype. The
question raised: should the data plane be Go or Rust for better concurrency?

**Measured first** — profile in [`bench/scripts/profile_ingest.py`](../../bench/scripts/profile_ingest.py),
40,000 change rows / 15.8 MB of wal2json JSON, single-threaded:

| Stage | Seconds | % of pipeline | Runs in |
|---|---|---|---|
| fetch from slot (libpq + PG decode) | 0.215 | 22.8% | PG + C |
| **`json.loads` with `parse_float=Decimal`** | **0.553** | **58.6%** | **Python** |
| decode to dicts | 0.053 | 5.6% | Python |
| group by key | 0.009 | 0.9% | Python |
| Python objects → Arrow | 0.094 | 9.9% | boundary |
| Parquet write (zstd) | 0.021 | 2.2% | C++ |
| **Total** | **0.944** | | **≈ 42,400 rows/s** |

**JSON parser comparison**, same payload, consistent methodology
([`json_parser_shootout.py`](../../bench/scripts/json_parser_shootout.py)):

| Parser | MB/s | vs CPython C scanner |
|---|---|---|
| CPython `json`, C scanner | 85 | 1.0× |
| CPython `json`, `parse_float=Decimal` | 64 | 0.75× |
| **`orjson` (Rust)** | **191** | **2.2× faster** |

**75% of the pipeline is Python-side work**, and the dominant cost is materialising JSON
as Python objects. The parts people assume are hot — Parquet encoding, compression — are
**2.2%**, already C++, and untouched by any rewrite.

Reference points: PeerDB ≈ 120k rows/s (Go), walshadow ≈ 289k rows/s (Rust, physical WAL).

### The ceiling that actually matters

The `fetch` stage — Postgres' own logical decoding plus libpq — is **0.215s, and is
language-independent**. It caps Path 2 at roughly **186,000 rows/s on this box no matter
what the consumer is written in.**

*Estimate* (labelled as such — not measured): a full Rust pipeline that builds Arrow
arrays directly, with no intermediate objects, should reach **~130–150k rows/s**, putting
it within ~25% of that server-side floor. That is consistent with PeerDB's ~120k in Go.

So Rust buys roughly **3× over Python** and then runs into Postgres' single-threaded
logical decoder. Going past it requires physical WAL (Path 3c) — which needs Rust or C
anyway. This is precisely walshadow's argument, and it is the strongest reason to put the
data plane in a language that can also host the Phase 3 decoder.

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

**Option 2**:

1. **Replace wal2json with the `pgoutput` binary protocol** — but for **dependency and
   de-risking reasons, not performance.** It removes a third-party extension from the
   image and the `output_plugin_libraries` allowlisting, it is maintained by the Postgres
   project, and it is what the Rust port will consume anyway, so doing it first settles
   the decoder semantics in the language where they are cheapest to iterate. The earlier
   "2.3×, larger than the language change" justification was wrong (see *Correction*).
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
