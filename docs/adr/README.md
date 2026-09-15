# Architecture Decision Records

Short documents capturing a decision, its context, and its consequences — so that when
someone asks "why is it like this?" two years from now, the answer is written down rather
than reconstructed.

## When to write one

Write an ADR when a decision is **costly to reverse** or **likely to be relitigated**:
choosing an ingest path, a storage format, a licence, a dependency with lock-in, or a
deliberate limitation we're accepting.

Do not write one for reversible implementation details.

## Process

1. Copy [`0000-template.md`](0000-template.md) to `NNNN-short-title.md`.
2. Open it as `Proposed`, discuss on the PR.
3. Merge as `Accepted`, or close as `Rejected` (keep rejected ones — knowing what we
   considered and declined is most of the value).
4. To reverse a decision, write a **new** ADR that supersedes the old one. Never edit an
   accepted ADR's decision in place; mark it `Superseded by NNNN`.

## Accepted

| ADR | Decision |
|---|---|
| [0008](0008-implementation-language.md) | Go for the CNPG-I plugin, Rust for the data plane — **and replace wal2json with `pgoutput` binary first, which is worth more (2.3x) than the language change** |

## Index

Decisions taken during the feasibility study are currently recorded in
[../09-risks-and-open-questions.md](../09-risks-and-open-questions.md#decisions-already-taken-in-this-study).
They should be promoted to individual ADRs as each is first challenged:

| Proposed ADR | Decision | Source |
|---|---|---|
| 0001 | Serving layer is real PostgreSQL, not a proxy or reimplementation | [04](../04-storage-and-query-engine.md) |
| 0002 | Apache-2.0; no AGPL-derived code | [02](../02-prior-art.md#licence-analysis) |
| 0003 | Logical replication first, physical WAL later | [03](../03-wal-ingestion.md#recommendation) |
| 0004 | Architecture A → C; B and D rejected | [04](../04-storage-and-query-engine.md#recommendation) |
| 0005 | Derived Postgres image, never a fork | [07](../07-image-strategy.md) |
| 0006 | `serviceMode` defaults to `off` | [05](../05-cnpg-integration.md) |
| 0007 | Mirroring is opt-in per table | [06](../06-compatibility-and-semantics.md) |
| ~~0008~~ | *Written — see Accepted above* | — |
