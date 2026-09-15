# 15 — PostgreSQL 17 slot synchronisation, and the GUC that deadlocks promotion

[docs/14](14-phase2-streaming-and-failover.md) measured that a logical slot does
**not** survive a promotion on PostgreSQL 16, leaving an O(table size)
re-snapshot as the only recovery. PostgreSQL 17 added logical slot
synchronisation. This document is what happened when we actually ran it.

Verified by [`go/cmd/qs-slotsync`](../go/cmd/qs-slotsync/) against PostgreSQL
17.11 with a real `pg_basebackup` standby and a real `pg_ctl promote`. Raw
output: [`bench/results/slotsync_pg17.txt`](../bench/results/slotsync_pg17.txt).

**Headline: it works, and it converts R8 from "expensive rebuild" to "resume" —
but only if promotion also clears `synchronized_standby_slots`, which nothing
does for you, and whose failure mode is an indefinite silent hang.**

---

## What the feature actually requires

Four things, and three of them are easy to get wrong in a way that shows no
symptom until a failover:

| Where | Setting | What breaks without it |
|---|---|---|
| Slot | created with `FAILOVER true` | slot is simply not synced |
| Standby | `sync_replication_slots = on` | no sync worker |
| Standby | `primary_conninfo` carries **`dbname`** | sync worker has no database to connect to; does nothing, quietly |
| **Primary** | **`synchronized_standby_slots = '<standby physical slot>'`** | the consumer can get *ahead* of the standby, so the synced slot is behind at promotion and rows are **lost** |

The last one is the half that's easy to skip because everything appears to work
without it. It makes logical decoding on the primary **wait** for the standby, so
a logical consumer can never be ahead of the node that will replace the primary.

`pglogrepl` still emits the pre-v15 positional `CREATE_REPLICATION_SLOT` grammar,
which has no room for `FAILOVER`. We build the parenthesised form ourselves
(`internal/changestream/pgoutput.go`) and let `pglogrepl` parse the result set,
which is unchanged.

---

## Measured

```
slot "qs_sync_slot" created at 0/30D1278, failover=true
snapshot: 3000 rows at the consistent point
applied 400 transactions; slot confirmed through 0/501A419
synced copy present on standby, confirmed_flush_lsn=0/501A419
```

Then 500 rows were committed on the primary and deliberately **not** confirmed —
the in-flight window a failover has to protect — and the standby was promoted.

```
logical slot present on new primary: 1
confirmed_flush_lsn=0/501A419  synced=true
our last confirm was  0/501A419
...
resumed without re-snapshot: 500 transactions, mirror 3400 -> 3900 rows
mirror 3900 rows / source 3900 rows -> MATCH
all 500 in-flight rows recovered from the synced slot
```

The slot survives, points exactly at our last confirmed LSN, and replays the
in-flight window. **No re-snapshot.**

The third assertion is the one that matters. A slot that exists but has been
fast-forwarded to the current LSN is *worse* than no slot: the consumer
reconnects happily and silently loses everything in between. Checking only for
the slot's presence would have passed that case.

---

## The finding: promotion must clear `synchronized_standby_slots`

The first run failed, and failed in the most dangerous possible way.

```
resumed without re-snapshot: 0 transactions, mirror 3400 -> 3400 rows
mirror 3400 rows / source 3900 rows -> DIVERGED
```

`START_REPLICATION` **succeeded**. The server log even shows it opening at the
right position:

```
LOG:  starting logical decoding for slot "qs_sync_slot"
DETAIL:  Streaming transactions committing after 0/3018521, reading WAL from 0/2000080.
WARNING:  replication slot "qs_standby_slot" specified in parameter
          "synchronized_standby_slots" does not exist
DETAIL:  Logical replication is waiting on the standby associated with
         replication slot "qs_standby_slot".
```

`synchronized_standby_slots` is written to `postgresql.auto.conf`, so
`pg_basebackup` copies it to the standby. After promotion the new primary still
names the **physical** slot the *old* primary held for it. Physical slots are not
synced, so that slot does not exist here. Logical decoding then waits — with a
warning in the log, and **no error to the client**.

> The GUC that makes slot synchronisation safe *before* a failover is the GUC
> that deadlocks the mirror *after* one.

The client's view is an open, healthy stream that returns nothing: indistinguishable
from an idle database. That is the same shape as the two bugs already recorded in
this project — Phase 1's idle-vs-stale readiness confusion, and docs/14's
slot-loss-vs-silence. Three times now:

> **Absence of data is never evidence of freshness.** Every "nothing is arriving"
> state must be positively distinguished from "nothing is happening".

Clearing the GUC makes the run converge, which is the proof that this — and not
slot sync itself — was the failure.

### Product consequences

1. **Promotion must reset `synchronized_standby_slots` on the new primary** to
   the physical slots of *its* standbys, or empty if it has none. This is
   Quicksilver plugin work, not something CNPG does today — it belongs in the
   post-promotion reconcile (docs/05 lifecycle matrix).
2. **The mirror must alarm on a stream that is open but silent** past the
   freshness SLO, rather than treating silence as caught-up. `health.RecordCaughtUp`
   already distinguishes idle from stale; that signal must now also be driven by
   "the server acknowledged our start position but has sent nothing", which is
   what this failure looks like.
3. `Streaming.SlotExists` exists so slot loss is **detected, never inferred from
   silence** — but as this run shows, presence alone is not sufficient evidence
   that the stream is live.

---

## Revised guidance

Supersedes the table in [docs/14 section C](14-phase2-streaming-and-failover.md#what-this-means-for-the-product):

| Postgres | Behaviour after failover | Cost |
|---|---|---|
| **≤ 16** | Slot lost. Full re-snapshot per mirrored table. | O(table size), mirror out of service |
| **≥ 17** | Slot synced and resumable, in-flight rows recovered — **provided** the four settings above are in place and promotion clears `synchronized_standby_slots` | resume, no re-snapshot |

**Recommendation: require PostgreSQL ≥ 17 for `ingest: logical`.** Not merely
prefer it. The PG 16 path is implemented and tested and stays in the codebase as
the recovery path for a slot that is genuinely gone, but a product whose
freshness SLO collapses for the duration of a full table rebuild after every
failover is not one to ship deliberately.

The reason this is now a comfortable requirement rather than an awkward one:
CloudNativePG supports 13–18, so 17+ is well inside its supported range, and
`ingest: logical` is a Phase 1/2 concern that Phase 3's physical-WAL decoder
replaces entirely.
