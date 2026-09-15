# 14 — Streaming pgoutput, snapshot bootstrap, and failover

Three Phase 1 gaps closed together, because they are one story: how the mirror
stays correct when the source moves under it. All verified by
[`go/cmd/qs-phase2`](../go/cmd/qs-phase2/) against live PostgreSQL 16 with a real
`pg_basebackup` standby.

---

## A. pgoutput over streaming replication

ADR-0009 listed "replace wal2json" and "stop polling" as two items. They are one
change: **pgoutput is consumed over the streaming replication protocol.**

| | Before | After |
|---|---|---|
| Output plugin | wal2json (third-party) | **`pgoutput`** — ships with Postgres |
| Transport | `pg_logical_slot_peek_changes` polling | **`START_REPLICATION`** — server pushes |
| Image | extra extension + `output_plugin_libraries` allowlisting | neither |
| Latency floor | the poll interval | none |

**Measured: 36–40 ms commit-to-visible**, against a polling floor that was
whatever the batch interval was set to (400 ms in the Phase 1 harness). The
gain is structural, not a constant factor — polling could never go below its
interval no matter how fast the consumer was.

Two things pgoutput gives us that wal2json did not:

- **Relation messages carry the live column list and type OIDs**, so the decoder
  learns the schema from the stream rather than querying `pg_attribute`. DDL
  still needs the barrier — a Relation message only arrives when a changed table
  is next written to — but column mapping is now self-describing.
- **Values arrive as exact text** in proto v1, so numerics never touch a float.
  The correctness tax that dominated the Python profile
  ([ADR-0008](adr/0008-implementation-language.md)) is simply absent.

### The same trap, in a different spelling

wal2json needed `nextlsn` because `upto_lsn` stops *before* the record at that
position. Streaming has the identical hazard: **confirming at the commit LSN
makes the server resend that transaction forever.** The fix is `commitLSN + 1`.
Different API, same off-by-one, same symptom — a permanently stalled slot.

Recorded because it will recur in the Phase 3 physical decoder.

---

## B. Snapshot bootstrap that joins the stream

Needed in three situations that share one implementation: a table newly matches
the `tables:` pattern, a mirror node loses its storage, or a failover invalidates
the slot.

The correctness requirement is that snapshot and stream join with **neither gap
nor overlap**. Postgres provides exactly the tool: `CREATE_REPLICATION_SLOT`
returns a *consistent point*, and the slot retains WAL from there, so:

```
create slot  ->  consistent point P
snapshot at  P
stream from  P
```

Every row is applied exactly once. **Order matters and is not symmetric**:
snapshot-then-stream can overlap, which is harmless because the mirror is an
upsert-by-key store; stream-then-snapshot can *gap*, which is not.

Verified with 5,000 rows seeded **before** the slot existed — rows the stream
would never deliver — then 1,035 streamed transactions on top. Converged
exactly.

---

## C. Failover — measured, not assumed

The [docs/05 lifecycle matrix](05-cnpg-integration.md#sequencing-against-cnpg-lifecycle-events-goal-g6)
row most likely to break in production. Built a real standby with
`pg_basebackup -R -X stream`, promoted it, and looked.

```
logical slot present on standby BEFORE promotion:      0
promoted: pg_is_in_recovery()=false
logical slot present on new primary AFTER promotion:   0
```

**The logical slot does not survive failover on PostgreSQL 16.** Logical slot
failover — the `failover` slot option plus `sync_replication_slots` — landed in
**PG 17**. On 16 the slot simply is not there, and the stream cannot be resumed.

This is the failure mode that silently kills CDC pipelines: the consumer
reconnects, finds nothing, and either stalls forever or starts from the current
LSN and **silently loses every change in between**.

### The recovery path, implemented and verified

Detect the missing slot → create a new one on the new primary → **re-snapshot at
its consistent point** → resume streaming. The snapshot machinery from B is
exactly what makes this cheap to implement.

```
re-snapshotted 5693 rows at 3/500CDC8
mirror 6129 rows / source 6129 rows -> MATCH
```

**Converged against the new primary after a real promotion.**

### What this means for the product

| Postgres | Behaviour | Cost |
|---|---|---|
| **≤ 16** | Slot lost. Full re-snapshot per mirrored table. | O(table size) after every failover |
| **≥ 17** | Slot can follow with `failover=true` + `sync_replication_slots` | resume, no re-snapshot |

Consequences for [docs/05](05-cnpg-integration.md) and
[docs/09](09-risks-and-open-questions.md):

1. **Recommend PG ≥ 17 for `ingest: logical`.** Not a hard requirement — the
   re-snapshot path works and is tested — but on a large table after a failover
   it is a long, expensive rebuild during which the mirror is behind and
   readiness-gated out of service.
2. **Slot loss must be detected, never inferred from silence.** A consumer that
   reconnects and sees an empty stream cannot distinguish "idle" from "my slot is
   gone" — which is the same class of bug as the idle-vs-stale confusion found in
   Phase 1. Absence of data is never evidence of freshness.
3. **Risk R8 is now measured rather than assumed**, and its mitigation is
   implemented.

---

## A note on test hygiene

The first failover run reported `SKIP: promote failed` and then printed **PASS**.
That is the third time in this project a test has gone green without testing
anything — after the vacuous S3 security passes and the vacuous S5 widening
checks.

Fixed: a skipped section now exits `2` with `INCOMPLETE — section skipped, which
is NOT a pass`. The general lesson is worth more than the fix:

> A test that cannot run must never report success. Green has to mean *verified*,
> not *did not fail*.
