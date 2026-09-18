# 30 — Dropping a database kills the standby

Found by accident, reproduced deliberately, and it is not a benchmark problem.

A measurement script dropped and recreated its test database between runs, as it
had a dozen times before. This time the sidecar came up and never became ready:

```
level=WARN msg="cannot determine local role"
  err="failed to connect to ... /tmp/.s.PGSQL.5444: connect: no such file or directory"
```

The standby was gone. Its log says why:

```
FATAL:  replication slot "rc_slot" is active for PID 4122
CONTEXT:  WAL redo at 0/2A24BB20 for Database/DROP: dir 1663/16384
LOG:  startup process (PID 4120) exited with exit code 1
LOG:  terminating any other active server processes
LOG:  shutting down due to startup process failure
LOG:  database system is shut down
```

The standby could not **replay** the `DROP DATABASE` because its own
synchronised copy of that database's logical slot was in use by the slot-sync
worker. Replay is not optional, so the startup process died, and a standby whose
startup process dies shuts the whole server down.

---

## Why this is Quicksilver's problem specifically

Slot synchronisation is not a default. It requires three things together:

- a logical slot created with `failover = true`,
- `sync_replication_slots = on` on the standby,
- `hot_standby_feedback = on`.

That is not an unusual configuration someone might stumble into. **It is the
configuration this project requires.** Failover slots plus slot synchronisation
are how a logical slot survives a promotion on PostgreSQL 17, and that is the
entire reason the plugin refuses to run `ingest: logical` below 17
([docs/14](14-phase2-streaming-and-failover.md), [docs/15](15-pg17-slot-failover.md)).
Every Quicksilver deployment has exactly the arrangement above.

So: **any Quicksilver user who drops the mirrored database loses their
standby** — not the primary, the standby, which is the thing that was supposed
to be the safety net. The failure is silent until something needs the replica.

## Reproduced deliberately

```
create database, create a logical slot with failover => true
wait for the standby to synchronise it      31s
  dt_slot | active=t | synced=t
DROP DATABASE on the primary                DROP DATABASE
20 seconds later                            *** STANDBY IS DOWN ***
```

The first attempt at this reproduction did **not** fail, and the reason is worth
recording: the slot was created with `pg_create_logical_replication_slot(name,
'pgoutput')`, which defaults `failover` to false, so the standby never
synchronised it and there was nothing to conflict with. A slot that is not a
failover slot is not affected. That is also why this had never been seen before
on a dozen prior runs of the same scripts — it needs a *synced* slot present at
the moment of the drop.

## The mitigation, also measured

Drop the slot first, wait for the standby to forget its synced copy, then drop
the database:

```
drop the SLOT on the primary
wait for the standby to forget its copy     2s
DROP DATABASE                               DROP DATABASE
15 seconds later                            STANDBY ALIVE
```

Two seconds, measured. `bench/scripts/lib_dropdb.sh` does this, and the seven
scripts that drop a database now go through it rather than each getting the
ordering slightly differently:

```bash
source bench/scripts/lib_dropdb.sh
qs_drop_database 5443 mydb
```

It terminates any walsender still holding a slot on that database, drops every
slot the database owns, waits up to thirty seconds for the standby's synced
copies to disappear, and only then drops the database.

## What this is NOT

This is not something the mirror can fix. The sidecar drops its own slot
correctly — `dropSlot` in `cmd/qs-mirror/slotguard.go` terminates the walsender
and drops the slot, never the database. The hazard is in the *order an operator
does two ordinary things*, and the only defence available to this project is to
say so loudly and to get its own scripts right.

It also looks like a PostgreSQL defect rather than intended behaviour: replaying
a `DROP DATABASE` should not be able to take a standby down, and the standby
holds the only thing blocking itself. This document records what was measured on
**PostgreSQL 17.11**; it does not claim the finding is novel, and anyone hitting
it should check whether a later 17.x has changed the behaviour before working
around it.

## What to do about it

For an operator:

1. **Never drop a database that a Quicksilver slot follows.** Set
   `mode: off` and let the sidecar's slot be dropped first, or drop the slot by
   hand, and confirm the standby has forgotten it:
   ```sql
   -- on the standby
   SELECT slot_name, synced FROM pg_replication_slots WHERE database = 'app';
   ```
   Only when that returns nothing is the drop safe.
2. If the standby is already down, it restarts cleanly once the conflicting slot
   is gone — the data is intact, it is the replay that was stuck.

For this project, the open question is whether the plugin should refuse to
deregister while a mirrored database still exists, or whether that is
overreach for a failure an operator can only hit by doing something they were
already told not to do. It is not implemented, and the argument against
implementing it is that a plugin which blocks `DROP DATABASE` is a plugin that
has to be uninstalled before anyone can clean up.
