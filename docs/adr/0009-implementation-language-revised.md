# 0009 — Implementation language: Go for plugin *and* data plane

- **Status:** Accepted — **supersedes [0008](0008-implementation-language.md)**
- **Date:** 2026-09-15
- **Deciders:** —

## Context

[ADR-0008](0008-implementation-language.md) chose Go for the CNPG-I plugin and **Rust for
the data plane**, on two arguments. Both were then tested and **both failed.**

### Argument 1: "Rust is needed for throughput." — Refuted by measurement.

0008 never benchmarked Go; it cited PeerDB's published ~120k rows/s. Measured here, on
the identical 16.0 MB wal2json payload used for every other number in this project
([`bench/gobench/`](../../bench/gobench/)):

| Parser | MB/s | Notes |
|---|---|---|
| Go `encoding/json` (stdlib, typed struct) | 50 | reflection-based |
| CPython `json` + `parse_float=Decimal` | 64 | current implementation |
| CPython `json`, C scanner | 85 | |
| **`orjson` (Rust)** | **191** | |
| **`goccy/go-json` (typed struct)** | **207** | |

**Go with a decent JSON library matches Rust** — 207 vs 191 MB/s, within noise. The
stdlib is 4.1× slower than `goccy/go-json`, which means the thing that looked like a
language gap was a *library-choice* gap.

Go also gets exact decimals nearly free: `json.Number` preserves the decimal text — the
same correctness property Python needs `parse_float=Decimal` for — at ~6% cost, against
Python's much larger penalty.

And the ceiling still binds: [0008](0008-implementation-language.md#the-ceiling-that-actually-matters)
measured Postgres' own logical decoding at **~186k rows/s**, language-independent. Go and
Rust both saturate it. Throughput cannot distinguish them for Path 2.

### Argument 2: "Rust means the core is written once." — Conflated two components.

0008 claimed Go would force the decode/write core to be written twice: once for the
standalone ingest, once for the Phase 3 in-process decoder. That conflates the **decoder**
with the **writer**.

- The Phase 3 physical-WAL **decoder** is a Postgres background worker. It is C or Rust
  in *every* option, and it is **new code either way** — no Go version of it would ever
  exist to be discarded.
- The **writer** (columnar files, deletion vectors, compaction, verification) does not
  have to live in the same process. `ChangeStream` was designed as a seam, and a seam can
  be a **process boundary**: the C/Rust bgworker emits transactions over a socket into the
  writer, whatever the writer is written in.

So nothing is written twice under Go either. The argument evaporates once the two
components are separated.

## Options considered

### Option 1 — Go for plugin and data plane; C/Rust only for the Phase 3 bgworker
One toolchain, one CI, one dependency ecosystem for everything that exists today.
`cnpg-i-machinery` and `client-go` are the canonical, battle-tested path for the
Kubernetes-facing half, which is the riskiest part to do off the beaten track.
**Against:** `arrow-go` is less mature than `arrow-rs`/`parquet-rs`. Real, but the
Parquet stage measured **2.2%** of the pipeline, so the blast radius is small.

### Option 2 — Go plugin + Rust data plane (ADR-0008)
**Against:** both of its arguments are refuted above, leaving two toolchains and an FFI or
process boundary bought for no measured gain — and bought *now*, to serve a Phase 3 that
S4 has not yet shown is viable.

### Option 3 — Rust for everything, including the plugin
Technically possible: CNPG-I is a gRPC contract, and `tonic`/`prost` are strong.
**Against:** discards `cnpg-i-machinery` and `client-go` for `kube-rs` on exactly the
component where ecosystem maturity matters most, and where CNPG's own evolution is
Go-first. Trades the well-trodden path for the risky one, to save a language the project
wants anyway.

## Decision

**Option 1.**

1. **CNPG-I plugin in Go** — unchanged, forced by the ecosystem.
2. **Data plane in Go** — with a fast JSON library (`goccy/go-json` or equivalent) **if
   JSON survives at all**; see 3.
3. **`pgoutput` binary protocol** rather than wal2json. Still right, and for the reasons
   0008 was corrected to state: it removes a third-party extension from the image and the
   `output_plugin_libraries` allowlisting. It also makes the JSON benchmark above moot,
   which is the honest reason not to over-weight it.
4. **Phase 3's physical-WAL decoder in C or Rust**, as a Postgres background worker that
   emits over the `ChangeStream` boundary into the Go writer. Introduce that second
   language **when S4 proves the approach**, not before.
5. **Python stays the reference oracle** — unchanged from 0008. It encodes the
   commit-boundary, deletion-vector and DDL-barrier semantics and has found six real bugs;
   `run_slice` / `run_phase1` become differential tests against the Go port.

## Consequences

**Positive.** One toolchain for everything that exists today. Canonical Kubernetes
libraries on the Kubernetes-facing half. The second language arrives only when Phase 3 is
funded and de-risked, instead of being paid for up front against an unproven bet.

**Negative.** `arrow-go` is less mature than `arrow-rs`. If columnar write becomes a
bottleneck — it is currently 2.2% — this should be revisited. A careless Go JSON choice
(the stdlib) is *slower than Python*, so the library choice must be deliberate and
benchmarked in CI.

**Neutral.** Rust remains the right language for the Phase 3 decoder. This ADR defers that
choice rather than reversing it.

## Revisit if

- S4 passes and the process boundary between the Rust/C decoder and the Go writer proves
  to be a real bottleneck — then unifying the data plane in Rust becomes worth its cost.
- `arrow-go` proves inadequate for the columnar write path at production volumes.
- The columnar writer grows past ~10% of pipeline time, making library maturity matter
  more than toolchain count.

## Postscript — why 0008 was wrong

Worth recording, because the failure mode is more general than this decision. 0008
benchmarked *the language we were leaving* (Python) and *the language we wanted*
(Rust, via `orjson`), but **never benchmarked the alternative it was rejecting** (Go). It
cited someone else's number for that one. The measured answer inverted the conclusion.
