# 03 — WAL ingestion

This is the core feasibility document. Everything else is packaging.

The question: **how do we turn a stream of Postgres WAL into a stream of row-level changes
we can write into a column store?**

There are three candidate paths. The first is easy and limited, the second is standard and
expensive, the third is fast and *not possible in isolation* — for reasons that turn out to
determine the whole architecture.

---

## Path 1 — Tee the WAL archive (CNPG-I `WAL.Archive`)

CNPG-I exposes a **WAL service** whose `Archive` RPC hands a plugin every WAL segment as the
instance archives it (plus `Restore`, `Status`, `SetFirstRequired`). This is intended for
backup plugins like `cnpg-i-barman-cloud`, but nothing stops a plugin from *also* forwarding
each segment to an ingest pipeline.

```
primary ──archive_command──> CNPG instance manager ──gRPC──> quicksilver plugin
                                                              ├──> object store (normal backup)
                                                              └──> quicksilver ingest
```

**Why this is genuinely attractive:**

- **Zero new load on the primary.** Archiving already happens. We are a second consumer of a
  byte stream that is already being produced and shipped.
- **Zero new state on the primary.** No replication slot, so no slot-lag-fills-the-disk
  failure mode, and nothing to break on failover.
- **Trivially resumable.** Segments are immutable, named and ordered. Crash recovery is
  "start from the last segment you finished."
- **Free backfill/gap-fill channel** for whatever the low-latency path misses.

**Why it cannot be the only path:**

- **Latency floor is `archive_timeout`**, and archiving is segment-granular (16 MB default).
  A quiet database archives on the timeout; a busy one archives every few seconds. Either
  way you are in the tens-of-seconds-to-minutes range, not the 1–10 s target from
  [01](01-problem-and-goals.md#freshness-budget).
- It delivers **physical WAL**, so it inherits every decoding problem in [Path 3](#path-3--physical-wal).

**Verdict: build it, but as the *backfill and repair* channel, not the primary one.** It is
cheap, it hardens every other path, and it is the only mechanism that lets a Quicksilver node
bootstrap or catch up from cold without touching the primary at all.

---

## Path 2 — Logical replication

The standard answer. Set `wal_level = logical`, create a publication on the source, a
replication slot, and consume `pgoutput` (or a custom output plugin). This is what
pg_mooncake, Debezium, PeerDB and ClickHouse's `MaterializedPostgreSQL` all do.

**What you get:** fully-formed logical row events. `INSERT` with all column values. `UPDATE`
with new values and — per `REPLICA IDENTITY` — old key or old row. `DELETE` with the key.
Postgres has already done the hard work of resolving the catalog, detoasting, and mapping
physical tuples to logical rows. Type mapping is well-trodden. Every driver ecosystem has a
library (`pglogrepl` in Go, `postgres-protocol` in Rust).

**What it costs:**

### Cost on the primary

- **Decoding is single-threaded per slot and runs *on the source*.** The walreceiver→
  reorder-buffer→output-plugin pipeline is one process. On a write-heavy primary this is a
  real CPU cost and a real throughput ceiling — it is precisely why walshadow exists and why
  its benchmarks show 289 k rows/s against PeerDB's ~120 k.
- **Reorder buffering.** Changes are buffered until commit, so a long transaction that
  touches many rows spills to disk on the *source*. A single bulk `UPDATE` can produce a
  multi-GB reorder buffer.
- **`wal_level = logical` increases WAL volume for everyone**, including the physical
  standbys, whether or not they care.
- **The slot is a foot-gun.** If the consumer stalls, the primary cannot recycle WAL, and
  `pg_wal` grows until the volume fills and the primary goes down. `max_slot_wal_keep_size`
  bounds this by *invalidating the slot* — which converts an outage into a silent full
  re-sync. Neither branch is good.

### Cost on failover

This is the one that actually bites in Kubernetes. A logical slot lives on one instance.
When CNPG promotes a standby, the slot is **not** there by default and the consumer has lost
its position. Postgres 17 added `synchronize_replication_slots` / failover slots, and CNPG
supports slot synchronisation — but this is version-gated (PG ≥ 17 for the good version) and
is an additional thing to get right, test and monitor. On PG 16 and below, a failover means
a full re-snapshot of every mirrored table.

### Semantic gotchas

- **`REPLICA IDENTITY`.** With `DEFAULT`, a `DELETE` carries only the primary key. For a
  table with no PK you get *nothing* unless you set `REPLICA IDENTITY FULL`, which writes
  every column of every old row into WAL on every update and delete — a large, permanent WAL
  amplification.
- **DDL is not replicated.** `pgoutput` has no DDL events. Every logical CDC pipeline in
  existence has a hand-rolled schema-drift mechanism (event triggers, polling
  `pg_attribute`, or parsing), and it is where they all leak.
- **TOAST.** Unchanged TOASTed values are sent as a placeholder, not a value. The consumer
  must carry the previous value forward or re-read it. Forgetting this corrupts wide tables
  in a way that only shows up months later.
- **Sequences, `TRUNCATE`, partition routing, and generated columns** all have their own
  special handling.

**Verdict: this is the correct v1 ingest path.** Not because it is good, but because it is
*known* — every failure mode above is documented, has a standard mitigation, and can be
implemented by one engineer in weeks rather than quarters. It gets us to a shippable product
that proves the value proposition, which is what buys the right to attempt Path 3.

---

## Path 3 — Physical WAL

The walshadow approach, and the one that motivates this whole project. Consume the same byte
stream a physical standby consumes, decode it outside the source, and never create a logical
slot at all.

The appeal is exactly as advertised: the source's load profile becomes that of a physical
standby, latency approaches replay latency (~50–200 ms), and decode parallelises across
cores because you are not bottlenecked on one reorder buffer.

### The problem: physical WAL is not self-describing

Physical WAL records describe **byte changes to pages**, not row changes to tables. Two
distinct gaps follow, and they are different in kind.

**Gap A — catalog.** A heap record identifies its target by
`(spcNode, dbNode, relNumber, forkNum, blockNum)` and gives you raw tuple bytes. To know
that relfilenode 16418 is `public.orders`, that column 3 is `amount numeric(12,2)`, and how
to walk the null bitmap and alignment padding, you need the catalog **as of that LSN**.

*This gap is solved,* and walshadow shows how: filter catalog WAL records into a schema-only
shadow Postgres and let Postgres itself maintain the catalog. Heap records for user tables
then decode against an in-memory snapshot of that catalog without touching the shadow per
record. Elegant and parallel-friendly. We can rebuild this from the published description.

**Gap B — the tuples themselves.** This is the one that decides the architecture. Reading
`src/include/access/heapam_xlog.h` (PG 17):

```c
typedef struct xl_heap_delete
{
    TransactionId xmax;          /* xmax of the deleted tuple */
    OffsetNumber  offnum;        /* deleted tuple's offset     */
    uint8         infobits_set;
    uint8         flags;
} xl_heap_delete;
```

That is the **entire** record. A `DELETE` under `wal_level = replica` tells you *a tuple at
block B offset N is now dead*. It does not tell you which row that was. The column values —
including the primary key — are simply absent. They are only present when
`XLH_DELETE_CONTAINS_OLD_TUPLE` or `XLH_DELETE_CONTAINS_OLD_KEY` is set, and **those flags
are set by logical decoding support**, i.e. only under `wal_level = logical` with an
appropriate `REPLICA IDENTITY`. Requiring them would put us straight back in Path 2's cost
model while keeping all of Path 3's complexity.

`UPDATE` is worse, because it fails even for the *new* row:

```c
#define XLH_UPDATE_PREFIX_FROM_OLD   (1<<5)
#define XLH_UPDATE_SUFFIX_FROM_OLD   (1<<6)
```

When old and new tuples land on the same page, Postgres prefix/suffix-compresses the new
tuple **against the old one**. The WAL carries only the differing middle bytes plus two
lengths. Reconstructing the new row requires the old row's bytes.

And a third wrinkle: **full-page images suppress per-tuple data.** Data registered with
`XLogRegisterBufData()` is omitted from the record when an FPI of that block is taken, unless
the caller passed `REGBUF_KEEP_DATA` (`0x10`, "include data even if a full-page image is
taken"). So the first modification to a page after a checkpoint delivers a whole page image
and *no* tuple payload — the decoder must parse the page image and extract the tuple at
`offnum` itself.

### The consequence

> **A physical-WAL decoder cannot be stateless. It must maintain its own TID-addressed copy
> of the heap** — enough state to answer "what row currently lives at
> `(relfilenode, block, offset)`?" — or it cannot process deletes or same-page updates at
> all.

This is not a detail to be engineered around. It is the defining constraint of the approach,
and it is presumably why the project is called *wal**shadow***.

### Three ways to satisfy it

**3a — Key the destination on TID.** Make the mirror's dedup key
`(relfilenode, block, offset)` rather than the logical primary key. Then a `DELETE` needs no
row contents: you emit a tombstone at that TID. An `UPDATE` becomes tombstone-at-old-TID plus
insert-at-new-TID.

Clean, and it removes the delete problem entirely — but it does **not** solve prefix/suffix
compression (you still need the old bytes to rebuild the new tuple), and it exports Postgres
physical addresses into the query layer. Vacuum, `HOT` pruning and page compaction all move
or recycle TIDs, so the mirror must also track `XLOG_HEAP2_PRUNE*` and
`XLOG_HEAP2_VACUUM` records or it will resurrect dead rows. Real, but partial.

**3b — Keep a decoder-side page cache.** Maintain in-process copies of the pages of mirrored
tables, seeded by FPIs and updated by every applied record. Then TID lookups and prefix/
suffix reconstruction are local reads.

This works, and it is almost certainly close to what walshadow does. But note what it is:
you have built a partial, special-purpose Postgres storage engine — with its own recovery,
its own memory management, its own correctness bugs, and a hard requirement that you never
miss a record. The cache must be durable across restarts or every restart is a full re-sync.

**3c — Put the decoder on a real hot standby.** A CNPG standby is *already* maintaining a
byte-exact, crash-safe, TID-addressed copy of the heap. That is what replay *is*. Run the
decoder as a background worker **inside** that standby: read the WAL it is already receiving,
and for each heap record resolve the TID by reading the local buffer/page.

`DELETE` → read the tuple at that TID from the local heap before replay removes it.
`UPDATE` with prefix/suffix compression → read the old tuple locally.
FPI-suppressed data → the page is right there.
Catalog → the local `pg_catalog` *is* the shadow catalog; Gap A dissolves too.

**This is the recommendation.** It converts the hardest problem in the physical-WAL approach
from "build a Postgres storage engine" into "read a page you already have." The cost is that
the decoder must live inside a Postgres process on a node that holds a full row-store copy —
which, per [04](04-storage-and-query-engine.md) and G3, is something we want anyway.

### What 3c still has to solve

Honest list. None of these look fatal; all need a spike (S4 in [08](08-roadmap-and-spikes.md)).

| Problem | Notes |
|---|---|
| **Racing replay** | The worker must read the old tuple *before* replay overwrites or prunes it. Options: run ahead of replay by decoding WAL before it is applied; or hold back replay via a recovery-conflict-style mechanism; or rely on the fact that dead tuples survive until vacuum and use an MVCC snapshot with `hot_standby_feedback`. Needs measurement. |
| **Timeline switches** | Promotion/failover bumps the timeline ID. The worker must follow `.history` files and handle the LSN rewind at the switchpoint without double-applying. |
| **Restart and gap recovery** | The mirror's applied-LSN must be durable and checked against the standby's `pg_last_wal_replay_lsn()`. Gaps are filled from the Path 1 archive tee. |
| **Vacuum / HOT pruning** | Must consume `XLOG_HEAP2_*` prune and vacuum records to retire mirror rows whose TIDs are recycled. |
| **Version coupling** | Record layouts change between major versions. The decoder is pinned to a major version — acceptable, since the node is a same-version standby by construction. |
| **TOAST** | TOASTed values live in a separate relation with its own WAL records. Must be reassembled, or detoasted via the local heap. |
| **Extension API surface** | A `shared_preload_libraries` background worker with `XLogReader` access (as `pg_walinspect` uses) — plus custom rmgr callbacks on PG ≥ 15 if useful. Well-trodden ground. |

---

## Recommendation

**Build all three, in this order, as one pipeline with pluggable front-ends.**

```
┌─────────────────────────────────────────────┐
│         change stream (internal API)        │
│   {table, op, lsn, old_tid, new_tid, row}   │
└─────────────────────────────────────────────┘
        ▲             ▲                ▲
   ┌────┴────┐   ┌────┴─────┐   ┌──────┴──────┐
   │ Path 2  │   │  Path 1  │   │   Path 3c   │
   │ logical │   │  archive │   │  physical,  │
   │  (v1)   │   │ (backfill│   │  on-standby │
   │         │   │ & repair)│   │  (target)   │
   └─────────┘   └──────────┘   └─────────────┘
```

Define the internal change-stream interface **first**, in phase 1, and make the logical
front-end the first implementation of it. Then Path 3c is a front-end swap rather than a
rewrite, and the whole downstream — batching, columnar write, compaction, checksum
verification, metrics — is built once and reused.

| Phase | Path | Freshness | Primary load | Why |
|---|---|---|---|---|
| 1 | Logical (2) | 1–10 s | Slot + decode | Known quantity. Ships. Proves the value proposition. |
| 1 | Archive tee (1) | minutes | None | Backfill, repair, cold bootstrap. Cheap to add alongside. |
| 3 | Physical on-standby (3c) | < 1 s | None beyond physical streaming | The differentiated end state. Only attempt once phase 1 has proven anyone wants this. |

The sequencing matters more than the destination. Path 3c is the interesting architecture,
but it is also the one where a failed spike costs a quarter. Path 2 is boring and lets us
answer the only question that actually determines whether this product should exist: *does a
columnar mirror behind the `-ro` endpoint make real workloads faster?*
