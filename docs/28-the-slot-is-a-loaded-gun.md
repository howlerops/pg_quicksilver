# 28 — The slot is a loaded gun

The large-scale test was built to find the failures that only appear when a
structure outgrows a cache. It found something better on its first complete run:
**it took the database down.**

```
=== a workload on top, for 60s ===
  11700585 row-changes in 60s (196632/s into PostgreSQL)

=== drain ===
psql: error: connection to server on socket "/tmp/.s.PGSQL.5443" failed:
  FATAL: the database system is not yet accepting connections
  DETAIL: Consistent recovery state has not been yet reached.

/dev/vda  252G  38G  272K  100% /
```

Six point six gigabytes of free space became 272 kilobytes, and PostgreSQL
stopped.

---

## This was predicted, in writing, and not implemented

[docs/09](09-risks-and-open-questions.md) has carried it since the risk register
was written:

> **R7 — Logical slot fills `pg_wal` and takes down the primary.** Likelihood
> Med, Impact High. Mitigation: `max_slot_wal_keep_size` + alerting and
> readiness gating.

The risk was identified. The mitigation was named. **Nothing anywhere in the
repository set `max_slot_wal_keep_size`** — not the plugin, not the chart, not
the example cluster. A named mitigation that nobody implements is a comment.

## The mechanism, which is the whole product in one paragraph

A logical replication slot pins every WAL segment its consumer has not
confirmed. That is not a flaw; it is the property the mirror is built on. It is
why a restarted sidecar resumes from `applied_lsn` instead of re-reading the
table — the E2E suite has a section asserting exactly that.

The same property means the slot is a debt the primary carries on the mirror's
behalf. While the mirror keeps up, the debt is a few megabytes. The moment it
cannot, the debt grows at the rate of the shortfall:

```
into PostgreSQL     196,632 row-changes/s
out through the mirror  ~95,000 row-changes/s
shortfall               ~100,000/s of WAL that no checkpoint may reclaim
```

Nothing in that arrangement applies backpressure. PostgreSQL does not slow down
because a logical consumer is behind; it writes WAL and keeps it. The mirror
does not shed load, because it has no load to shed — it is being handed changes.
The disk is the only thing that says no, and it says it by stopping the
database.

**The failure lands on the primary, which is the one thing the mirror exists to
protect.** A replica that falls behind hurts the replica. A logical consumer
that falls behind hurts the source.

---

## Two defences, in the order they should fire

### `max_slot_wal_keep_size`, which is PostgreSQL's job

```yaml
spec:
  postgresql:
    parameters:
      max_slot_wal_keep_size: 4GB
```

Past that bound PostgreSQL **invalidates the slot** rather than keeping the WAL.
The mirror then cannot resume and must rebuild from a fresh snapshot — which is
already the documented lifecycle for an invalidated slot
([docs/05](05-cnpg-integration.md)), already implemented, and already tested.

Losing a mirror costs a re-snapshot. Losing the primary costs the cluster. The
trade is not close, and it is now in the chart README and the example Cluster,
stated as not optional.

### The sidecar bounding its own slot, which is a second line

Every fifteen seconds the sidecar asks how much WAL its slot is holding
(`QS_MAX_SLOT_WAL_BYTES`, 4 GB default). Past the limit it logs what is
happening, returns, and the supervisor drops the slot before retrying — because
a restart into the same runaway slot resumes the same wedge.

The retention is exported continuously as `quicksilver_slot_retained_bytes`,
which is the only metric here describing damage the mirror is doing to something
else. An operator wants to watch it climb, not find out at the threshold.

**This does not replace the PostgreSQL parameter and the chart says so.** A
guard that runs inside the process it guards cannot be trusted alone: a wedged
sidecar is precisely the case where the slot grows fastest and precisely the
case where the guard does not run. The threshold is only possible because the
two states are so far apart — a mirror keeping up held single-digit megabytes; a
mirror that was not passed four gigabytes inside a minute.

---

## What the harness got wrong, twice

The first attempt sized the run at 41 million rows from **89 B/row**, taken from
`pg_total_relation_size / rows`. That is what the heap and its index weigh, and
it is not what the run consumes:

```
heap + pk index        ~89 B/row
WAL for the inserts    ~85 B/row
the standby's copy     the whole thing again
the mirror             ~25 B/row
```

An estimate that leaves out WAL is not conservative, it is wrong by a factor of
two. It also created the physical replication slot **before** seeding, so a slot
with no consumer pinned 4.6 GB of seed WAL that no checkpoint could recycle.

Both were fixed, and the seed then ran 22.8 million rows in 32 seconds with free
space almost flat. Then the workload phase filled the disk anyway, because the
guard had been put on the seed — **the phase that was already safe.** The
dangerous phase was the one generating changes faster than the mirror could
confirm them, which is the same mechanism this whole document is about, arriving
from the other direction.

---

## What did work, at 22.8 million rows

Worth recording, because the run got far enough to measure it:

| | |
|---|---|
| seed | 22,795,490 rows, 1.9 GB, 32s |
| **bootstrap** | **59.8s → 381,381 rows/s → 33.9 MB/s** |
| RSS after bootstrap | **113 MB** — about 5 bytes per live row |
| workload | 11.7M row-changes in 60s (196,632/s into PostgreSQL) |

The bootstrap rate is the interesting one: **381,381 rows/s at 22.8 million rows
against 347,484 rows/s at 400,000** — faster per row at fifty-seven times the
size, so nothing about it degrades with scale. And 113 MB of resident memory for
a 22.8-million-row index is the pointer-free index of
[docs/19](19-workload-matrix.md) doing exactly what it was built for.

## What to do next

1. **Finish the large run.** It has never reached the verification step, which
   is the only part that is not optional. The workload needs to be paced to
   what the mirror can drain, not to what PostgreSQL can accept.
2. **Test the guard by tripping it**, rather than by having tripped over it.
   `QS_MAX_SLOT_WAL_BYTES` set low, a workload the mirror cannot match, and an
   assertion that the primary survives and the mirror rebuilds.
3. **Ask whether the mirror should shed load before it gets here.** Dropping the
   slot is the right emergency stop and a poor steady state; a mirror that knows
   it is falling behind could stop mirroring the table it cannot keep up with
   rather than the whole database's WAL.
