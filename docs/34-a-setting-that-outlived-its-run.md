# 34 — A setting that outlived its run

The first end-to-end run of the whole suite came back **13 passed, 1 failed**.
The failure was `rendering_fidelity.sh`:

```
mirror 100 rows (checksum 7a9ad592630131e)
source 201 rows (checksum 696cae4d7601fe19)
DIVERGED
  rows only in source: 101, only in mirror: 0
```

The mirror held exactly its bootstrap rows and nothing after them. The stream
had applied nothing at all.

---

## It was not new

The first thing worth knowing was whether the change under test had caused it.
A worktree at the commit from before the day's work reproduced it exactly —
same counts, same two checksums. So the bug predated everything recent, and the
reason nobody had seen it is that the suite had never been run end to end on
this machine before.

---

## What it was

The sidecar's log said everything and looked like nothing:

```
new replication slot; bootstrapping by snapshot  at=0/DB036440
snapshot complete  table=public.t rows=100
key index built    table=public.t keys=100
```

…and then silence. No error, no retry, no disconnect. The slot was `active`,
the process was healthy, `/readyz` passed.

`pg_recvlogical` against the same server, same publication, same protocol
version, received the inserts immediately. So the server was decoding fine and
the fault appeared to be in the sidecar.

It was not. The probe slot and the sidecar's slot differ in one column:

```
rp_slot | logical | failover=t | ...
probe   | logical | failover=f | ...
```

And on the primary:

```
SHOW synchronized_standby_slots  ->  e2e_standby
SELECT * FROM pg_replication_slots WHERE slot_name='e2e_standby'  ->  (0 rows)
```

A logical slot with `failover=true` does not advance until every slot named in
`synchronized_standby_slots` has confirmed. A slot that does not exist never
confirms. So decoding stops, permanently, and PostgreSQL reports this by
writing a warning in the primary's log and returning **no error to any client**.

This is the exact mechanism [docs/15](15-pg17-slot-failover.md) documents and
that `clearStaleSyncSlots` exists to repair. That repair runs when the sidecar
finds itself on a **primary**. Here the sidecar was on a standby and the stale
setting was on the primary, where no sidecar was running.

Clearing the setting, with nothing else changed, turned the same run into
`MATCH`, 200 rows.

---

## Where the setting came from

`e2e_mirror.sh` sets it, legitimately, to exercise failover slots:

```
ALTER SYSTEM SET synchronized_standby_slots = 'e2e_standby'
```

`ALTER SYSTEM` writes `postgresql.auto.conf`, which outlives the process. The
`e2e_standby` slot does not — it is created and dropped inside that script. So
every later run against the same primary inherited a promise to wait for a slot
that had ceased to exist.

`e2e_mirror.sh` already knew this was dangerous, but only for itself: it opens
by deleting `postgresql.auto.conf` with the comment *"ALTER SYSTEM from an
earlier run must not leak into this one."* It protected itself from its
predecessors and poisoned its successors. Three other scripts —
`large_scale.sh`, `perf_mirror.sh`, `workload_matrix.sh` — set the same GUC and
also never cleared it.

The ordering hid it further. `e2e` runs **last**, because it promotes the
standby; so the damage always landed on the *next* invocation of the suite,
where nothing had obviously changed.

---

## Three fixes, at three different distances from the cause

**The product.** The sidecar now checks the primary's
`synchronized_standby_slots` on connect and says so:

```
WARN  the primary holds logical decoding back for a slot that does not exist;
      this mirror will connect, pass readiness and then never advance
      synchronized_standby_slots=ghost_slot missing=[ghost_slot] slot=rp_slot
      fix="on the primary: ALTER SYSTEM SET synchronized_standby_slots = ''
           (or to slots that exist), then SELECT pg_reload_conf()"
```

This is the one that matters outside this repository. An operator meeting this
in production sees a mirror that bootstraps, goes ready and then quietly stops
gaining rows — with a freshness SLO that will eventually fail readiness and take
the Pod out of service for a reason that has nothing to do with the Pod. The
warning names the cause and the fix.

**The harness.** `lib_syncslots.sh` provides `qs_set_sync_slots` and
`qs_clear_sync_slots`, and all four scripts that set the GUC now clear it from
their `EXIT` trap.

**The defence.** Scripts that use a failover slot without setting the GUC call
`qs_assert_sync_slots_sane`, which fails immediately with the diagnosis rather
than timing out on an empty mirror:

```
synchronized_standby_slots on :5443 names a slot that does not exist: e2e_standby
A failover logical slot will connect, pass readiness and never advance.
Left behind by an earlier benchmark run.
```

---

## A second bug, hiding behind the first

The same section's checker crashed rather than reporting:

```
TypeError: 'NoneType' object is not iterable
  FAIL: the mirror holds the same instant under more than one spelling
```

`st.get("delta_files", [])` returns `None`, not `[]`, when the key is **present
and null** — which is what `state.json` writes when there are no deltas. The
default never fires. So a Python `TypeError` was being reported as a finding
about the mirror's contents.

Two wrong answers stacked: a stalled stream, and a checker that would have
mis-described the result even if the stream had worked.

---

## What it says about the suite

With the stall gone, the section passes and finally demonstrates the thing it
was written to demonstrate:

```
timestamptz  1 distinct spelling(s):  2026-01-15 12:00:00+00:00  x201
date         1 distinct spelling(s):  2026-01-15                 x201
MATCH (mirror 201 rows)
```

That assertion — one spelling per instant, across a bootstrap and a stream taken
under two different server timezones — had **never once been observed passing**.
The stall meant only the 100 bootstrap rows were ever examined, which is the
half where a rendering bug cannot show up.

This is the fourth time in this project that the measurement was broken rather
than the code, and the first time a broken measurement was *caused* by another
part of the harness. The pattern is now specific enough to name: state that
outlives a run — `postgresql.auto.conf`, a replication slot, a mirror directory
— is shared mutable state between tests, and the suite's ordering decides who
pays for it.
