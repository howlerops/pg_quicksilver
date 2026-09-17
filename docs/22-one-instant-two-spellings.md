# 22 — One instant, two spellings

Found by chasing a harness detail. [docs/20](20-serving-the-mirror.md) recorded
that the mirror's timestamps come back as `2026-01-15 12:00:00+00` while psycopg
hands back `…+00:00`, called it a difference between two client libraries, and
moved on. It is. The question underneath is not.

**The mirror stores the text PostgreSQL renders, and that text is a property of
the session, not of the value.**

```
TimeZone=UTC               2026-01-15 12:00:00+00
TimeZone=America/New_York  2026-01-15 07:00:00-05     the same instant
TimeZone=Asia/Kolkata      2026-01-15 17:30:00+05:30  the same instant
DateStyle=German,DMY       15.01.2026                 the same date
```

Every type the writer does not map to a native Arrow type — timestamps, dates,
intervals, jsonb, bytea, arrays — arrives as text. **Three different sessions
render into this mirror**: the bootstrap snapshot, the walsender behind the
replication stream, and the verifier comparing the two. Nothing made them agree.

They agree today because a default PostgreSQL is `Etc/UTC, ISO, MDY` and every
session in every benchmark inherits it. That is not a design; it is a default.

---

## The bug, reproduced

`bench/scripts/rendering_fidelity.sh` makes the sessions disagree on purpose. A
hundred rows carrying one instant are bootstrapped with the server on
`America/New_York`; the server is moved to `Asia/Kolkata`, which is a thing an
operator does; a hundred more rows carrying **the same instant** are streamed.
Then it asks the only question with one right answer: how many spellings does
the mirror hold?

```
=== how many spellings does the mirror hold for one instant? ===
  timestamptz  2 distinct spelling(s):
      2026-01-15 17:30:00+05:30        x101
      2026-01-15 07:00:00-05           x100
  FAIL: the mirror holds the same instant under more than one spelling

=== and the verifier agrees with the source ===
  FAIL: qs-verify reports DIVERGED
    mirror 201 rows (checksum 573da1c2f2a7a67d)
    source 201 rows (checksum d5866c2dd70eb953)
    DIVERGED
      rows only in source: 0, only in mirror: 0
```

Read the verifier's output again. **Identical row counts. No rows missing from
either side. No row present in only one.** Every row is there and every row is
wrong, which is the signature this project has learned to recognise: the failure
that looks like health.

What it costs downstream is worse than a checksum. `GROUP BY at` returns **two
groups for one instant**. A join against a timestamp misses. `WHERE at = $1`
finds the half of the table rendered the way the querying session happens to
render, and silently omits the rest.

---

## The fix

`internal/pgtext` pins the rendering on **every** connection the mirror opens —
the snapshot, the replication stream, the verifier, and the view generator:

```
TimeZone=UTC  DateStyle=ISO,MDY  IntervalStyle=postgres
extra_float_digits=3  bytea_output=hex  client_encoding=UTF8
```

UTC because it is the only zone that means the same thing everywhere; ISO
because it is the only `DateStyle` that sorts; `extra_float_digits=3` because
anything less loses bits of a `float8` on the way through text.

**The point is not which spelling. It is that there is exactly one.** A value in
that list may never change once mirrors exist: an existing mirror holds text
rendered under the old setting, and the stream would start appending text
rendered under the new one — this bug, caused by the fix for it.

The replication connection is pinned separately and deliberately. A logical
replication connection is a backend of its own and pgoutput renders values using
*its* session settings, not the snapshot's, and it is the session that renders
most of the mirror's lifetime.

### It is demonstrated, not asserted

`QS_PIN_RENDERING=0` turns the fix off, and the script then **requires** the
failure:

```
bash bench/scripts/rendering_fidelity.sh                     -> PASS
QS_PIN_RENDERING=0 bash bench/scripts/rendering_fidelity.sh  -> EXPECTED FAIL
```

A pass with the fix off is reported as `UNEXPECTED PASS` and exits non-zero,
because a test that cannot detect the bug it was written for is worse than a
failing one. Same discipline as docs/12: *on a security test, a clean sweep is
a reason to check the error messages, not to celebrate.*

---

## Two bugs in the fix, both caught by tests

Worth recording because both were silent and both are generic.

**libpq does not decode `+` as a space.** Go's `url.Values.Encode` spells a
space as `+`; libpq's URI parser only percent-decodes. So `-c+TimeZone=UTC`
reached the server as a configuration parameter literally named `+TimeZone`, and
every connection was refused:

```
FATAL: unrecognized configuration parameter "+TimeZone"
```

Spaces are now `%20`. Since a literal plus in a value would have been encoded as
`%2B`, every bare `+` in the encoded output is a space, so the substitution is
safe.

**The idempotence guard looked in the wrong place.** Pinning twice is by design
— the replication dialler pins defensively on a DSN the caller has usually
pinned already — and the guard checked for `TimeZone=UTC` in the raw DSN. In a
URI that is spelled `TimeZone%3DUTC`, so the guard never matched and every
setting was appended twice. It now looks in the *decoded* options.

Neither would have shown up in a benchmark. Both took one unit test each.

---

## What this does not fix

A query engine still sees `VARCHAR`. Pinning makes the text **consistent**; it
does not make it a **timestamp**. `WHERE at > now() - interval '1 day'` still
needs a cast a query written against PostgreSQL will not have, and row-group
statistics still describe strings rather than instants.

That is the next item, and it is now a smaller one: with the rendering pinned,
every temporal value in a mirror is ISO-8601 in UTC, which is exactly the
precondition for parsing it into an Arrow timestamp without guessing.

**Done in [docs/23](23-storing-an-instant.md).**
