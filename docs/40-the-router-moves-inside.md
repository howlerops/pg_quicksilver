# 40 — The router moves inside

[38](38-a-router-in-front.md) ended on a problem with no fix in a proxy: the
router could choose a side, but the side it chose was a second SQL dialect, and
it could only refuse the differences somebody had already written down.
[39](39-how-exact-is-the-mirror.md) then measured the dialect: 17% of 728
statements came back with a different value and no error.

This is the same router as a PostgreSQL extension,
[`ext/pg_quicksilver`](../ext/pg_quicksilver). It decides inside the server,
with the typed query tree in its hands, and the rule is inverted: nothing goes to
the mirror unless every construct in the statement is on a list of things
verified to come back identical. The heap is always right, so everything else
stays there.

On 1,142 statements, each run from the heap and through the extension on the
same server:

- **361 were answered by the mirror. All 361 returned byte-identical rows and
  an identical RowDescription** (column names, type OIDs, typmods).
- 781 stayed on the heap, and those are identical because they are the heap.
- With one opt-in setting 409 are routed; all 409 are identical.

**Since then the extension has been wired to the sidecar and the mirror is
read through DuckLake** ([Wired to the sidecar](#wired-to-the-sidecar), at the
end). On the hot standby, with mirror relations the sidecar made and an
allowlist widened by the same harness: **438 of the 1,142 statements routed in
the ICU `en-US` database and 467 in the C locale one, 0 different in either**
(486 and 515 with the opt-in setting, 0 different). Freshness is strict by
default now, and three things in the sections below are no longer true; each
says so where it stands.

**Since 2026-11-26** ([Measured again](#measured-again-2026-11-26)): the
freshness wait is taken, inside a bound; a READ COMMITTED transaction block that
has written nothing is routed by default (`quicksilver.route_in_transaction =
safe`); and the routed numbers under concurrent writes to the mirrored table are
no longer the zeros that the sections below report as history.

**Ordinary roles** (2026-11-26, [Ordinary roles](#ordinary-roles)): on the
patched pg_duckdb 1.2.2 build, a role that is not a superuser is routed, and it
is not a member of `duckdb.postgres_role`: the mirror side runs as
`quicksilver_mirror_reader`, the one role that is. Every other rule still
applies to the role. On stock pg_duckdb nothing changes for them.

Getting there removed things from the list that the task's own sketch put on
it. They are in [What the harness took off the list](#what-the-harness-took-off-the-list),
and they are the most useful part of this document.

Everything below is PostgreSQL 17.12, the stock pg_duckdb 1.2.0 package, an ICU
`en-US` database, a 1,000,000-row `orders` table, and a real mirror built by the
production sidecar on a hot standby. One host, 4 vCPU. The scripts are in
[`ext/pg_quicksilver/validate`](../ext/pg_quicksilver/validate).

## What the proxy could not do

| the proxy | the extension |
|---|---|
| No typed view of the statement. `7/2`, `avg(numeric)`, text ordering were refused by spelling, after somebody found them. | Walks the analysed, rewritten query tree. Every operator, function, aggregate, cast and type is a catalog object; it is routed only if that object is on the allowlist. |
| Planned every candidate twice: its own `EXPLAIN`, then the real thing. | The heap plan it judges by is the plan PostgreSQL makes anyway. A cheap statement is planned once and costs nothing more. |
| A transaction block was always the heap's: the proxy's own probe broke pg_duckdb's state. | Routed inside a block that is READ COMMITTED, has written nothing and has no savepoint open (`quicksilver.route_in_transaction = safe`, the default since 2026-11-26; `off` refuses every block). |
| 40 to 90 µs added to a 15 to 30 µs lookup, warm. | Within this host's noise on a prepared lookup; about 3 µs on the simple protocol. |
| TLS, authentication, pipelining, cancel, COPY re-implemented or refused. | PostgreSQL's own. There is no second listener. |
| The mirror's types reached the client (`decimal(38,2)`, `hugeint`). | The client sees the heap's RowDescription or the statement is not routed. |
| Permissions were the mirror view's. | Permissions are the heap table's, column privileges included. |
| A stale view cost one failed attempt, then the heap until republished. | The same, plus: a mirror that fails while executing, before its rows have left the server, is answered from the heap. |

## What it does

It is loaded with `shared_preload_libraries = 'pg_duckdb,pg_quicksilver'` and
does nothing until `quicksilver.route` says so (`off | shadow | auto | heap |
mirror`). For a `SELECT` that reads at least one registered table:

1. **PostgreSQL plans it for the heap.** This always happens, and it is the
   plan that is executed unless everything below says otherwise.
2. **The session.** Inside a function, a `DO` block, `CREATE TABLE AS` or
   `COPY`; in a transaction block that has written, or has a savepoint open,
   or is not READ COMMITTED (see below); a scrollable cursor, and a cursor
   declared with neither option whose heap plan can scan backward (a sort, an
   ordered index scan): heap.
3. **The cost gate.** Heap unless the plan's total cost is at least
   `quicksilver.min_cost` (10,000) or it reads at least `quicksilver.min_rows`
   (50,000) rows from mirrored tables. A scan under a `Limit` counts for the
   fraction the `Limit` lets through, unless a sort, aggregate, hash or
   materialize stands between them; an inner scan of a nested loop counts once
   per outer row. A point lookup stops here.
4. **The safety walk.** Every node of the tree. Each operator, function,
   aggregate, window function, cast, type, expression node and statement
   feature must have a row in `quicksilver.allowlist`; the collation of every
   comparison, ordering, grouping and min/max must satisfy that row's rule.
   What fails is collected, not just the first, and is the statement's
   machine-readable "why not".
5. **Parameters.** A custom plan is planned for its parameter values, so they
   are written into the mirror's statement as constants. A generic plan has no
   values: heap.
6. **Privileges.** If the executor would refuse the heap statement, the mirror
   is not even asked.
7. **Substitution.** Each heap table's range-table entry becomes a subquery
   over its mirror relation, columns matched by name, cast back to the heap's
   declared types.
8. **pg_duckdb plans it.** It prepares the statement in DuckDB at this point,
   so a missing file or a function DuckDB lacks is an error here, caught, and
   the answer is "heap".
9. **The result types.** Each column pg_duckdb will return must be the heap's
   type, or a numeric (whose precision is only a label), or a numeric where the
   heap says bigint. Otherwise: heap.
10. **One plan, two children.** The statement gets a `Custom Scan
    (QuicksilverRoute)` over pg_duckdb's plan and the heap plan. When it starts
    it checks the session again and **the mirror's freshness**; then it runs the
    mirror and holds its rows back while they fit in `work_mem`. If the mirror
    fails in that stretch, the heap child answers and the client sees nothing.
    (Freshness is now decided before step 8 for a plan that is about to run,
    and at this point only for a kept plan; and "fails" is now a narrower
    word. See [Wired to the sidecar](#wired-to-the-sidecar).)

```
=# EXPLAIN (COSTS OFF) SELECT * FROM orders WHERE id = 4242;
 Index Scan using orders_pkey on orders
   Index Cond: (id = 4242)
 Quicksilver: heap (cheap on the heap)

=# EXPLAIN (VERBOSE, COSTS OFF) SELECT status, count(*), sum(amount) FROM orders GROUP BY status;
 Custom Scan (QuicksilverRoute)
   Output: status, count, sum
   Quicksilver: mirror
   Quicksilver Reason: 416667 rows from mirrored tables, over 50000
   ->  Custom Scan (DuckDBScan)
         ...
   ->  Finalize GroupAggregate          <- the heap plan, kept in case
         ...
```

### The two tables

**`quicksilver.mirrors`**: heap table, mirror relation, `applied_lsn`,
`max_lag_bytes`, `enabled`, `finite_columns`. One row per mirrored table.

**`quicksilver.allowlist`**: `kind`, `ident`, `collation_rule`, `volatility`,
`options`, `verified_by`. The allowlist is data. An `ident` is what
`regoperator`, `regprocedure` or `regtype` would print, and it is resolved to an
OID with only `pg_catalog` on the path, so a row means one catalog object: it
survives dump and restore, and an operator named `+` in another schema is a
different operator (there is a regression test that makes one). A row that does
not resolve, or that calls a stable function immutable, is not loaded and
`quicksilver.allowlist_problems()` says why. An expression node this version of
the code has never heard of cannot be allowlisted at all.

The seed is 152 rows: 10 types, 103 operators, 21 aggregates, 5 functions, 5
node types, 8 statement features. Every one of them was needed by at least one
statement that was routed and came back identical
(`validate/coverage.py`; an entry nothing exercised was taken out or given a
statement).

### What it tells you

| | |
|---|---|
| `quicksilver.unsafe_constructs(text)` | The constructs that keep a statement on the heap, one row each `(kind, ident, detail)`. Nothing is planned or run. This is what a larger allowlist is to be generated from. |
| `quicksilver.last_decision()` | Side, reason, detail, the same constructs as `jsonb`, heap cost, rows. |
| `quicksilver.route = shadow` | Decide, report, never act. |
| `quicksilver.decision_stats`, `quicksilver.reason_stats` | Per statement (queryId) and per reason, in shared memory. |
| `quicksilver.mirror_status()` | Usable? Which columns are not, and why? Applied LSN, current LSN, fresh? |
| `quicksilver.log_decisions` | One LOG line per decision with the constructs as JSON. |
| `EXPLAIN` | Which side and why, as above. |

`validate/constructs.py` is the worklist for whoever grows the allowlist: for
each construct not on it, how many corpus statements contain it and how many it
*alone* keeps on the heap.

## Where the sketch was wrong

**"Hook after parse analysis."** The decision is made in the planner hook. The
tree there is still fully typed, and three things are only available there: the
rewritten tree (a view over a mirrored table is routed without a line of code
for views), the heap plan's cost (which is the cost gate, for free), and the
bound parameter values. The cheap statements the sketch worried about planning
twice are planned once.

**"Cache decisions per queryId."** There is little left to cache. A cheap
statement is decided by one comparison against a plan that exists anyway. The
walk is microseconds on a statement that passed a 10,000-cost gate, and its
output (the list of constructs) is the product, so it is not cached. What is
remembered per queryId is the expensive failure: "the mirror could not plan
this", for `quicksilver.negative_cache_ttl` (60 s) or until anything it depends
on changes. Only negative answers are remembered, so a queryId collision can
cost a routing, never a wrong one.

**"Substitution: so that the rewriter and pg_duckdb take over."** Substituting
before the rewriter would throw the heap plan away. The extension keeps both:
the routed plan carries the heap statement's range table and permission entries
unchanged, which is what makes the next three things true with no code of their
own:

- *Privileges.* The executor checks the heap table's ACL, column by column, at
  every execution, as it would have. The mirror subquery runs with the mirror
  owner's rights, like a view: users need, and should have, **no** privilege on
  the mirror relation or its schema.
- *Invalidation.* The plan depends on the heap table, on each mirror view
  (`relationOids`, added by `qs_decide`), and on the registry tables. Change any
  of them and PostgreSQL's own plan cache discards it, on the primary and on
  every standby.
- *Fallback.* The heap plan is already there when the mirror fails.

**"The sidecar will update the registry's applied LSN."** Not on a standby, it
will not: a standby cannot write a table, and the standby is where the mirror
lives. `quicksilver.report_applied_lsn(table, lsn)` writes shared memory
instead (no WAL, no bloat, works in recovery). The column in the table is the
durable copy, written on the primary and replicated; the newer of the two is
believed, and an `UPDATE` of that column alone does not disturb anybody's
plans.

**Freshness at planning.** A prepared statement without parameters is planned
once and its plan is kept. Freshness is a fact about the moment of execution,
so it is checked there. (True of a kept plan. For a statement planned and run
in one go this turned out to be too late: pg_duckdb fixes the lake snapshot
when it plans. See [a snapshot older than the freshness check had just
approved](#a-snapshot-older-than-the-freshness-check-had-just-approved).) So is the session's state: a plan made outside a
transaction and executed inside one goes to its heap child.

**The plan cache.** There is no hook where PostgreSQL chooses between a custom
and a generic plan; the only lever is the generic plan's cost. For a statement
that could be routed, the generic plan is given a prohibitive cost
(`quicksilver.prefer_custom_plans`), so the cache keeps planning it with its
values and each execution gets its own decision. A statement whose every scan
of a mirrored table is a one-row lookup is left alone, and settles on its
generic plan as it always did. The cost is visible in `EXPLAIN (GENERIC_PLAN)`.

**The registry is read as the bootstrap superuser**, with `search_path =
pg_catalog`. Otherwise a user with no rights on the mirror's schema (the
intended configuration) could not load it, and a user with a crafted
`search_path` could change what `=(text,text)` resolves to. Only the two tables
that `CREATE EXTENSION` created are read; a look-alike schema is ignored.

## What the harness took off the list

`validate/equality.py` runs every statement with `quicksilver.route = heap` and
with `auto`, and compares the bytes the server sent. The first run routed 141
statements and 17 of them differed (one was a statement of mine whose `ORDER
BY` left a tie). Every construct below was "on the seed list".

**Integer to numeric.** `n * 2` on a `numeric(12,2)` came back `-9841.62000`;
PostgreSQL says `-9841.62`. PostgreSQL's numeric carries its scale in each
value; DuckDB's decimal carries it in the column, and it gives a bare integer
three decimals when it has to make one a decimal. So:

- `numeric(integer)` is exact only as the direct operand of a comparison, where
  scale cannot be seen. It is on the list with that restriction (`cmp_only`).
- A numeric literal PostgreSQL prints without a decimal point (`2`, `-0.5`,
  `NULL`) is refused outside a comparison. `n * 2.0` is routed, `n * 2` is not.
- `CASE WHEN ... THEN n ELSE 0 END`, and `COALESCE` over numerics of different
  declared scales, are refused: one column, two scales.
- A column declared `numeric` without precision is refused. The mirror holds it
  as `DECIMAL(38,9)`.

**Infinity.** `WHERE ts = '-infinity'` found the row on the heap and not in the
mirror, and reading an infinite date fails in pg_duckdb with *value should be
between min and max*. The mirror does not keep infinity. Nothing in a query
tree says whether a column holds one, so date and timestamp columns are
routable only if the registry lists them in `finite_columns`. That list is the
sidecar's to maintain, because it sees every value it writes;
`validate/finite.sql` fills it by scanning the heap, which is a stand-in.
(No longer: the mirror keeps infinity now, the harness reads it back identical,
and `finite_columns` is retired. See [below](#infinity-is-kept-now-and-finite_columns-is-retired).)

**Integer overflow.** Found late, by a statement written to test something
else:

```
=# SELECT count(*) FROM t WHERE bi * 9223372036854775807 > 0;   -- heap
ERROR:  bigint out of range
=# SET quicksilver.route = mirror;  SELECT ...                  -- mirror
 count
-------
  9519
```

DuckDB's optimizer rewrites `x * c > 0` to `x > 0` and never multiplies, and
it does not add inside `sum(x + c)` either. The *values* of integer `+ - *`
are exact in all 48 statements that exercise them; the *error* is not always
raised (`sum(i + 1)` over a column holding the largest integer: `integer out of
range` from the heap, `18778` from the mirror). The 12 operators stay on the list, marked `errors`, and a statement
that uses one is routed only under `quicksilver.tolerate_lost_errors = on`.
That is the difference between 361 and 409. When DuckDB does evaluate the
overflow it raises an error of its own; that one is caught and the heap
answers with PostgreSQL's (7 statements in the corpus do this).

**Text order** was never on the list: byte order is the only order the mirror
has. `<` on text, `ORDER BY`, `min`/`max` are routed only when the collation is
`C` (a column declared `COLLATE "C"`). Equality, `GROUP BY` and `DISTINCT` need
only a deterministic collation. An explicit `COLLATE` clause is an unknown node
and stays home.

**And one crash.** A parameterless prepared statement keeps its routed plan.
Dropping the mirror view and executing it again killed the backend: a
segmentation fault inside pg_duckdb, which walks its statement's range table
again at execution and met a relation that was gone. Nothing had told the plan
cache that the plan depended on the view. It does now: the view is a dependency
of the plan (`relationOids`, see [Lock order](#lock-order)), and the statement
carries no view OID, so the plan cache takes no lock on the view when it reuses
the plan. The regression test that found it is `invalidation.sql`. The check of
the replacement, as it is now, is `invalidation.sql` and `prepared.sql`; the
parameterless case (a generic plan run ten times, its mirror view replaced with
`CREATE OR REPLACE VIEW`, then run again) answered with the new definition
(validated by hand on the cluster of `validate/real/setup.sh`).

## The measurement

`validate/equality.py`, three corpora, the same server for both sides, sessions
in `America/New_York`:

| corpus | statements | mirror | heap |
|---|---|---|---|
| `bench/exactness` (docs/39), unchanged | 728 | 52 | 676 |
| `corpus_own.sql`: the router's statements and their unsafe neighbours | 121 | 60 | 61 |
| `corpus_seed.sql`: generated, one or more per seed entry | 293 | 249 | 44 |
| **total** | **1,142** | **361** | **781** |

Routed and identical: 361. Routed and different: **0**. Heap and different: 0.
With `quicksilver.tolerate_lost_errors = on`: 409 routed, 409 identical, 0
different, and 8 more where the mirror overflowed and the heap answered (7 at
execution, 1 remembered from the statement before it).

The exactness tables hold 2,000 rows each, so for them the cost gate is opened
(`min_cost = min_rows = 0`); statements on the 1M-row table run with the
defaults.

| why it stayed on the heap | statements |
|---|---|
| a construct is not on the allowlist | 620 |
| reads no mirrored table (`SELECT 7/2`) | 143 |
| cheap on the heap | 17 |
| reads a relation with no usable mirror | 1 |

| the first construct that stopped it | statements |
|---|---|
| window functions | 38 |
| `<` on text under the database's collation | 34 |
| integer `+` (`errors`, see above) | 28 |
| `t_time.d`, `.tz`, `.ts`: may hold infinity | 62 |
| `LIKE` | 12 |
| `t_str.e`: an enum, text in the mirror | 10 |
| `t_num.nu`: numeric without precision | 9 |
| `array_agg`, `~`, integer `-`, `count(DISTINCT)`, `varchar`, `CASE` over two scales, `IN (subquery)` | 7 or 8 each |

52 of 728 is 7% of the exactness corpus, against the 23.5% that
[39](39-how-exact-is-the-mirror.md) found exact with DuckDB alone. The seed is
small on purpose, and that corpus was written to find differences. A batch of
15 more statement features (`OFFSET`, `DISTINCT`, CTEs, set operations,
`EXISTS`, `count(DISTINCT)`, `FILTER`, right and full joins, `VALUES`) was tried
on top: 146 routed where the seed alone routed 114 at the time, none different.
They are **not** in the seed. Each was exercised by between 0 and 7 statements,
and that is not verification.

## Crash safety

A PostgreSQL 17.12 built with `--enable-cassert` (`./configure --enable-cassert
--enable-debug --with-icu --with-openssl --with-lz4 --with-zstd`), the stock
pg_duckdb package copied into it, the extension built against it.
`validate/real/cassert.sh` runs all of it (the stand-in `validate/cassert.sh` was
removed); `debug_assertions` is `on`.

| on the cassert server | |
|---|---|
| pg_regress, 9 files | all pass |
| equality harness | 1,142 statements, 361 routed, 0 different; 409 and 0 with `tolerate_lost_errors` |
| psycopg check | 16 of 16 |
| standby check | 6 of 6 |
| fuzz, seed 3 | 10,273 statements sent, 3,208 of them errors, 100 answered by the mirror, 0 connections lost |
| primary and standby logs | 0 `TRAP`, 0 signals, 0 `PANIC`; no `WARNING` but the fuzz's own stray `COMMIT`s; 7,369 `ERROR` lines |

Two longer fuzz runs (seeds 1 and 2: 15,529 and 15,561 statements, 72 and 248
answered by the mirror) and the pgx tests ran on the same server against the
build before the last change to the extension (the `errors` option): no crash
line, no lost connection there either. Of the mutated statements the mirror
answered across the three runs, 74 were plain enough to compare with the heap;
none differed.

The non-cassert server's log has two `signal 11` lines. They are the pg_duckdb
crash above and its reproduction under gdb, both before the fix; there has been
none since.

`validate/fuzz.py` mutates the corpora (tokens dropped, doubled, swapped,
replaced by odd values and parameters; statements cut short), wraps them
(`EXPLAIN`, `EXPLAIN ANALYZE`, `PREPARE`/`EXECUTE` seven times, cursors in
transactions, savepoints, a `DO` block, `CREATE TABLE AS`, `COPY`, a CTE, a
`UNION` with itself) and changes settings at random, including every routing
mode and the unsafe override, while a second connection disables mirrors,
reports stale LSNs, deletes allowlist rows, adds and drops a column and runs
`ANALYZE`.

No valgrind run was made.

## What it costs

`validate/latency.sh`: a primary-key lookup on `orders`, which is a mirrored
table, so the extension looks at every one. One pgbench client pinned to CPU 2
(`taskset -c 2`), 20 s per cell, three interleaved rounds, the server restarted
to change `shared_preload_libraries`. Microseconds, from pgbench's
per-transaction log.

| | prepared: median | p95 | simple: median | p95 |
|---|---|---|---|---|
| not loaded | 13 | 48 to 56 | 23 | 41 to 68 |
| loaded, `route = off` | 13 to 14 | 20 to 57 | 23 to 24 | 45 to 59 |
| loaded, `route = auto` | 13 | 19 to 55 | 26 | 47 to 63 |

On a prepared lookup there is nothing to see: after five executions the
statement runs its generic plan and the planner hook is not called again. On
the simple protocol `auto` costs about 3 µs: a copy of the query tree, a walk
to find which relations it reads, and one counter. The p95 is the host's (it
moves as much between rounds of one configuration as between configurations;
another session's benchmark was running on the machine during this run).
A lookup on a table that is *not* mirrored skips the copy; that was not
measured separately. An earlier run of this script, on the build before the
last change, had one cell (`auto`, prepared, round 1) at 51 µs; six immediate
repetitions of that cell gave 13 each, and it did not recur in the run above.

For a statement the extension does route, `validate/routed_timing.py`: nine
alternating runs of each, median, client-side wall clock, rows fetched. The
heap side has two parallel workers.

| statement | side | heap ms | routed ms | |
|---|---|---|---|---|
| `SELECT count(*) FROM orders` | mirror | 44 | 8.4 | 5.2x |
| `SELECT status, count(*), sum(amount) FROM orders GROUP BY status` | mirror | 169 | 50 | 3.4x |
| `SELECT sum(amount) FROM orders WHERE created_at >= ... AND created_at < ...` | mirror | 28 | 21 | 1.4x |
| `SELECT c.region, sum(o.amount) FROM orders o JOIN customers c ON ... GROUP BY c.region` | mirror | 186 | 52 | 3.6x |
| `SELECT id, amount FROM orders WHERE amount > 990.00` (10,000 rows) | mirror | 84 | 58 | 1.4x |
| `SELECT id, amount FROM orders ORDER BY amount DESC, id LIMIT 10` | mirror | 106 | 59 | 1.8x |
| `SELECT count(*), sum(amount) FROM orders WHERE customer_id = 1` (300,000 rows) | mirror | 62 | 38 | 1.6x |
| `SELECT min(id), max(id), min(amount), max(amount), min(created_at), max(created_at) FROM orders` | mirror | 142 | 80 | 1.8x |
| `SELECT * FROM orders WHERE id = 4242` | heap | 0.06 | 0.07 | |
| `SELECT count(*), sum(amount) FROM orders WHERE customer_id = 5` (70 rows) | heap | 0.12 | 0.10 | |

The last two are what [38](38-a-router-in-front.md) measured at 24 and 26 ms
through the mirror: the statements the gate exists to keep off it. A range
aggregate and a 10,000-row result gain little, and an earlier run of this
script had `min`/`max` level at 128 ms on both sides: the thresholds are where
the mirror starts to win on this host at 1M rows, and they are settings, not
constants.

### Memory per routed backend

Each routed backend keeps a DuckDB instance, and that instance grows with the lake
statements it runs (pg_duckdb 1.2.2, DuckLake reads; the router does not cause the
growth, and an unrouted read grows the same way). The extension cannot recycle it, since
pg_duckdb accepts `CALL duckdb.recycle_ddb()` only at top level. The numbers, the frames
and the sizing advice are in docs/16, "Memory in each routed backend".

## Clients

| | |
|---|---|
| `psql` 17.12 | The nine pg_regress files: simple queries, `\bind` (extended protocol), `PREPARE`/`EXECUTE`, cursors, transactions, `COPY`. |
| pgx 5.11 (`go/cmd/qs-extcheck`, 4 tests) | All five exec modes, seven rounds each: the unselective parameter is answered by the mirror, the selective one, the lookup and the division by the heap, all equal to a heap-only connection. RowDescription identical field by field in every mode, table OID and attribute number included, text and binary. A transaction with a nested (savepoint) transaction. A batch. |
| psycopg 3.3.9, libpq 18 (`validate/psycopg_check.py`, 16 checks) | Autocommit: simple and extended protocol routed; a named prepared statement alternating two values twelve times goes mirror, heap, mirror, heap; binary results; `cursor.description`; pipeline mode; `COPY`; cancel mid-statement. Not autocommit: see below. |

**Transaction blocks: routed by default, when they are safe (2026-11-26).**
A client that does not autocommit (psycopg's default, most ORMs) is routed
inside a block that is READ COMMITTED, has no transaction id (it has written
nothing) and has no savepoint open: `quicksilver.route_in_transaction = safe`.
The three conditions are checked for every setting except `off` (in
`qs_state_blocks`); a block that writes is the heap's from its first write
on, because the mirror cannot see its own uncommitted changes.

The text of this section before 2026-11-26 said that a
non-autocommit client was not routed by default, because pg_duckdb aborts a
transaction that takes a `SAVEPOINT` after it has run a query. That is no
longer the case for the block as a whole: a `SAVEPOINT`, `RELEASE`, `EXPLAIN`,
`EXPLAIN ANALYZE` or `INSERT` after a routed read works (`sql/transactions.sql`
and the psycopg check both do it). The mirror's answer is taken whole when the
statement starts and DuckDB's transaction is ended with it. What a routed
statement does not see is the transaction's own writes, which is why a
transaction that has written is never routed.

Three things the transaction fuzzer found on the way, all fixed:
a cursor declared with neither `SCROLL` nor `NO SCROLL`, over a plan that can
scan backward, is scrollable on the heap and was not on a routed plan, so
`FETCH BACKWARD` was refused (fixed: the heap keeps it); a statement cancelled
by `statement_timeout` while routed reaches the client as `XX000` "Query
cancelled" from pg_duckdb, not `57014` (not changed here, noted in the check);
and `EXPLAIN (FORMAT JSON)` of a heap decision wrote its `Quicksilver`
property outside the plan's object, which made the document invalid (fixed:
JSON, XML and YAML carry no decision; text does).

The other two things [38](38-a-router-in-front.md) found pg_duckdb doing in a
transaction are gone as problems. After an `EXPLAIN` in the same transaction it
refuses to execute (*not supported inside functions*); that refusal now arrives
before any row and the heap answers. Inside an open savepoint the mirror is not
asked.

## Permissions

`permissions.sql`, with real non-superuser logins and a mirror whose rows say
`mirror` and include a row the heap does not have:

- `SELECT` on the heap table, **nothing** on the mirror or its schema: routed.
- No privilege on the heap table: `permission denied for table h`, the heap's
  own error. The mirror is not asked.
- `SELECT (id, a, b)` only: those columns are routed; `secret`, `*`, a
  predicate on `secret`, `max(secret)` are all denied.
- `REVOKE` between two executions of a prepared statement: denied on the
  second.
- Row-level security enabled on a table: never routed, for anyone, the owner
  included. Disabled again: routed again.
- The registry, the allowlist, `register_mirror`, `report_applied_lsn` and
  `quicksilver.allow_unsafe` are refused to an ordinary role.

**Roles that are not superusers are routed only on the confined build.** The
rules, and what changed on 2026-11-26, are in [Ordinary roles](#ordinary-roles).
On stock pg_duckdb 1.2.x such a role is answered from the heap, as it always
was, and the reasons are the ones measured on this cluster (`sql/unprivileged.sql`):
a role outside `duckdb.postgres_role` is refused DuckDB outright; a member
gets a DuckDB with no local file system; and `pg_read_server_files` plus
`pg_write_server_files` would give it the file system back, with `COPY` to and
from any file the server can reach.


## Ordinary roles

A role that is not a superuser is routed only when all of these hold. Otherwise
it is answered from the heap, with the heap's rows, error and plan, and the
reason says which condition failed.

1. **The confined build.** `SHOW duckdb.confined_file_access` is `on`. The
   marker is defined by `bench/patches/pg_duckdb-1.2.2-allowed-directories.patch`
   (`PGC_INTERNAL`, always `on`). Stock pg_duckdb 1.2.2 does not define it, so on
   the stock build no ordinary role is ever routed. The extension reads the
   marker with `GetConfigOption`, not by searching the binary.
2. **The lake is confined.** `duckdb.allowed_directories` is set, and the
   catalog file of every lake in `quicksilver.lakes` resolves (`realpath`) to a
   path inside one of its directories. A catalog that is not on this node is not
   read, so that lake is not attached. (The lake's data files are DuckDB's to
   confine: DuckDB refuses any file it may not read, and the statement is then
   answered from the heap.)
3. **The mirror role.** `quicksilver_mirror_reader` exists, is a member, with
   inheritance, of `duckdb.postgres_role`, is not a superuser, and is a member of
   none of `pg_read_server_files`, `pg_write_server_files` and
   `pg_execute_server_program`. `CREATE EXTENSION pg_quicksilver` creates it
   (`NOLOGIN`; it owns the mirror relations and nothing else, and holds no
   privilege on a heap table). The operator makes it a member (below).

The application role needs no membership at all. Its own memberships are not
part of this test, and it is not made a member of `duckdb.postgres_role` by any
step of the extension. A role that is a member anyway is routed exactly the same
way; see the warning below for what that membership gives it.

The test is made for each statement: it is a few catalog lookups, so a `GRANT`,
a `REVOKE` or a change of a superuser flag is seen by the next statement. A kept
plan is checked again when it starts, so a role whose mirror role has lost its
membership since the plan was made is answered from the heap.

**What runs as whom.** The checks that concern the role run as the role, before
anything is switched: the executor's check of `SELECT` on the heap table and on
every column the statement reads, the row-level security test, and the allowlist
walk (every operator, function, aggregate, cast, type and feature must be on it).
Then the mirror side runs as `quicksilver_mirror_reader`: the planning of the
mirror statement, its execution, the attachment of the lake, and the conversion
of its rows, which uses built-in functions only. The switch is made with
`SetUserIdAndSecContext` and `SECURITY_RESTRICTED_OPERATION`, so the switched code
cannot `SET ROLE` or change the session, and it is undone on every path: each
`PG_CATCH` restores it, and a transaction or subtransaction abort restores it if
a switch was still open. The mirror statement asks for no privilege of its own
(the heap statement was checked above), so no check on the role's behalf is made
as the mirror role. What the role controls cannot be evaluated under the switch:
a user function or operator is refused by the walk, and a view, a row-level
policy or a default is either expanded before the walk or not part of a `SELECT`
of the heap table. The heap plan, and any heap execution, runs as the calling
role, as before.

The mirror relations are views in the schema `quicksilver_mirror`, owned by
`quicksilver_mirror_reader`; PUBLIC has no privilege on the schema or on the
views. The sidecar makes them so (`go/cmd/qs-mirror/extension.go`): it creates
each view, then hands it to the mirror role in the same transaction, because a
DDL trigger on the primary runs as the role doing the DDL.

**Operator configuration**, the whole of it:

```
shared_preload_libraries = 'pg_duckdb,pg_quicksilver'
duckdb.enable_external_access = off           # the patch turns it off for the role's DuckDB anyway
duckdb.allowed_directories = '<the lake directory>/'   # superuser-only
duckdb.postgres_role = '<the duckdb role>'    # postmaster context: a restart to change it
```

```
-- once, in the database that has the lake, after CREATE EXTENSION pg_quicksilver
GRANT <duckdb.postgres_role> TO quicksilver_mirror_reader;
```

and nothing else. No application role is a member of `duckdb.postgres_role`, and
`quicksilver_mirror_reader` is made a member of nothing but that role. The sidecar
connects as a superuser (it creates the mirror relations and hands them over).
Routing is then switched on per role or per database with `quicksilver.route`, as
above.

**Read this before granting a role membership of `duckdb.postgres_role`.** A
member runs DuckDB statements itself (`duckdb.query`, `duckdb.raw_query`), and on
the confined build DuckDB may read any file under `duckdb.allowed_directories`.
The lake directory holds the files of every mirrored table, so a member can read
every mirrored table through DuckDB, whatever its SELECT privileges are, including
rows the heap has deleted for as long as the lake keeps them. This was measured:
a member with no privilege on any table read the mirror file of `customers` with
`read_parquet` and counted its 10,000 rows. The routing rules do not prevent it,
because they cover the mirror path only. **Making an application role a member of
`duckdb.postgres_role` re-opens that bypass.** Routing does not need the
membership, and nothing in this extension gives it.

**What remains true.**

- A superuser reads every mirrored table directly. DuckDB is its own, and the
  mirror path is open to it as before.
- A role an operator adds to `duckdb.postgres_role` can read every mirrored table
  through DuckDB. So can a role an operator adds to `quicksilver_mirror_reader`,
  which can run the mirror's statements itself: that grant is not to be made to
  an application role.
- A role with read access to the lake directory at the operating-system level
  reads its files, as it always could.
- The extension's functions that PUBLIC may run (`last_decision`, `decisions`,
  `reasons`, `mirror_status`, `unsafe_constructs`, `allowlist_problems`) return
  statement texts and metadata, not rows. The functions that reach the mirror side
  (`register_mirror`, `report_applied_lsn`, `attach_lakes`, `reset`, `prune`) are
  revoked from PUBLIC, as are the registry tables.

**On the stock build** nothing changes for ordinary roles: condition 1 fails, and
they are answered from the heap. `sql/unprivileged.sql` and `sql/permissions.sql`
have their heap-only expected output (`expected/unprivileged.out`,
`expected/permissions.out`); on the confined build they have their own
(`expected/unprivileged_1.out`, `expected/permissions_1.out`), where a privileged
role is routed. `sql/unprivileged_confined.sql` runs on the confined build only:
on stock it prints one line, which is its expected output
(`expected/unprivileged_confined_1.out`).

## What is still not routed, and why

- **Most SQL.** Division, `avg`, float and double, `varchar(n)`, `char`,
  `interval`, `time`, json, arrays, enums, `bytea`, `inet`; every function
  call; `LIKE` and regular expressions; window functions; `DISTINCT`, `OFFSET`,
  CTEs, set operations, sublinks, right and full joins; `LIMIT` without `ORDER
  BY`. None of it is known to be wrong. None of it has been shown to be right.
- **Integer `+ - *`** unless the session accepts a lost overflow error.
- **Text order** under any collation but `C`.
- **Date and timestamp columns** that nobody has asserted to be finite.
- **Transaction blocks** that have written, or have a savepoint open;
  REPEATABLE READ and SERIALIZABLE always. (A READ COMMITTED block that has not
  written is routed by default; see [Transaction blocks](#what-it-does).)
- **Anything inside a function**, a `DO` block, `CREATE TABLE AS`, `COPY
  (SELECT ...)`.
- **Scrollable and holdable cursors.**
- **Generic plans.** `plan_cache_mode = force_generic_plan` means heap.
- **Partitioned tables, inheritance parents, tables with row-level security,
  temporary and unlogged tables, generated columns, system columns, whole-row
  references, `TABLESAMPLE`.**
- **A statement that also reads an unmirrored table.**
- **A table whose mirror has gone stale**, until its applied LSN is reported
  again. After a restart that is every table: the reports live in shared
  memory, and the durable copy is only as recent as its last write.

And where it is not what the heap would do:

- **A result larger than `work_mem` is streamed** once the buffer is full. If
  the mirror fails after that, the client gets the error.
- **`EXPLAIN ANALYZE` of a routed statement is pg_duckdb's.** It runs the
  statement inside DuckDB's own `EXPLAIN ANALYZE`; an error there is not
  caught, and the row counts on the nodes are not the statement's.
- **The heap child of a routed plan runs without parallel workers** when it has
  to answer.
- **`sum(integer)`** is exact and typed bigint; if its true value passed
  2^63 PostgreSQL would fail and so does the routed plan, but only when the sum
  is an output column.
- **`ORDER BY ... LIMIT` over ties** returns some of the tied rows on either
  side, not necessarily the same ones. That is true of the heap against itself.
- **A parameterless prepared statement first planned inside a transaction
  block** keeps its heap plan until something invalidates it.
- **`quicksilver.last_decision()`** is the last statement that was *planned*.
  A reused generic plan makes no new decision.
- **Estimates.** The cost gate is the planner's guess.

## What was not validated

(As of the stand-in runs. The sidecar now does its part, the equality harness
runs on the standby, and there is a 16-client run; the list that stands is at
the end of [Wired to the sidecar](#wired-to-the-sidecar).)

- **A non-superuser reading a real mirror.** Stock pg_duckdb lets only a
  superuser read local files. The permission tests use mirrors that are
  DuckDB `VALUES`, which need no file; the equality, timing and client runs are
  all as `postgres`. The confined build in `bench/patches` was not installed.
- **The sidecar does none of this yet.** `validate/publish.sh` creates the
  views, registers them, reports the LSN and fills `finite_columns`. A view
  still goes stale at the next compaction (docs/38); here that costs one failed
  plan and 60 s on the heap, and nothing republishes.
- **pg_duckdb itself under assertions.** The package is not a cassert build.
- **Concurrency.** One pgbench client, one fuzz connection plus one making
  changes. The shared-memory statistics have not been contended.
- **The standby**, beyond `validate/standby_check.sh` (6 checks: routed there,
  `report_applied_lsn` in recovery, a kept plan following an allowlist change
  made on the primary). The equality harness ran on the primary.
- **An error after streaming has begun**, and memory use over a long session.
- **Anything but PostgreSQL 17.12, pg_duckdb 1.2.0, x86_64**, and tables larger
  than 1M rows.
- **The applied LSN the validation uses is the LSN at publication**, since a
  view names the files as they were then.

## What an image needs

(Items 4 and 5 are superseded: see [What an image needs, now](#what-an-image-needs-now).)

1. `pg_quicksilver.so`, `pg_quicksilver.control`, `pg_quicksilver--0.1.sql`
   (`make install`; PGXS, `postgresql-server-dev-17`; no other dependency).
2. `shared_preload_libraries = 'pg_duckdb,pg_quicksilver'`, in that order.
3. `CREATE EXTENSION pg_quicksilver` in each database, on the primary.
4. pg_duckdb in a build that lets the application's role read the mirror
   directory and nothing else (`bench/patches`), and `duckdb.postgres_role`.
   Without it only superusers are routed; everybody else is answered from the
   heap, correctly.
5. From the sidecar, per table: publish the view when the files change; then
   `quicksilver.register_mirror(heap, view)` on the primary, once;
   `quicksilver.report_applied_lsn(heap, lsn)` on the instance it sits next to,
   as often as it likes; `finite_columns` kept true.
6. `quicksilver.route = auto`, per role or per database. `shadow` first:
   `quicksilver.reason_stats` then says what would have moved.

The mirror relation's path is local to the instance that has the files. On an
instance that does not, the mirror cannot plan and the heap answers.

## At 10M rows

`validate/real/scale.py` on a 10,000,000-row `orders` and 100,000-row
`customers` (`bench/engine_compare/four_engines/scripts/load.sql`; 3 min 52 s to
load, so not the 3M fallback), a hot standby with the sidecar, ICU `en-US`,
4 vCPU, `work_mem` 4 MB. Median of five runs. Heap and routed are the same
statement, on the same server, with `quicksilver.route` set to `heap` and `auto`.

| statement (`bench.py`) | heap ms | routed ms | answered by |
|---|---|---|---|
| `count(*)` | 527.6 | 20.2 | mirror |
| `sum(amount)` | 1,109.9 | 160.7 | mirror |
| `count(*), sum(amount)` over one month | 189.9 | 82.3 | mirror |
| `status, count(*)` ordered by `status` | 848.5 | 853.2 | heap: `<` on text under ICU |
| revenue by month, joined to `customers` | 11,260.4 | 11,699.1 | heap: `date_trunc(text, timestamptz)` |
| primary-key lookup | 0.2 | 0.2 | heap: cheap on the heap |

Every routed answer equals the heap's (`scale.py`'s check). Under ICU, the two
aggregates the text order and `date_trunc` keep on the heap are the ones the
C locale would route; that run was not made here. The 10M-row stream of all
rows to the client was not faster routed: 96.1 s heap, 92.1 s routed, the same
checksum. Cut the lake's file short while its rows were arriving, the client got
the error after 2,326,528 rows, none twice; inside a transaction block the heap
answered the whole 10M rows (75.6 s) and the block went on.

## Not done: top-N seeding

`ORDER BY col DESC LIMIT n` on the mirror is not seeded with a bound from the
row groups. A seed would be `col >= T` with T taken from the row-group minimums,
so that DuckDB reads only the groups that can hold the top rows. It is not
implemented, because T must account for deleted rows (a group's minimum may be
one that a delete has since removed, so T must be lower than any live row would
need) and for NULL ordering (`DESC` puts NULLs first in PostgreSQL, so a bound on
the non-null values alone would drop them).

Measured on the 10M-row cluster above, with `amount` (numeric), `ORDER BY amount
DESC LIMIT 10`, one session each: the mirror 352 ms the first time and 62 to
75 ms after that; the heap 969 to 987 ms. The 1,100 ms figure this section was
meant to record is not what this run shows, so it is not repeated here: a
seed would be worth less than the sketch assumed, and it should be measured
again before anybody builds it. The 13 to 20 ms for a bound taken from the
row-group minimums is not re-measured here either.

## Running it

The stand-in path, as it was run for the sections above. Its views are
per-file and its LSN is the one at publication, so `validate/setup.sh` now
sets `quicksilver.max_lag = '16MB'` for its cluster; it has not been run again
since the extension changed. The regression tests now run against
`validate/real` (the commands are at the end of the next section).

```
cd ext/pg_quicksilver && make && sudo make install
validate/setup.sh                       # primary 5541, standby 5542, sidecar, mirror, views, registry
make installcheck                       # as a user who can read this directory
python3 validate/equality.py && python3 validate/coverage.py
python3 validate/equality.py --set quicksilver.tolerate_lost_errors=on --out validate/results/equality_lost_errors.json
python3 validate/constructs.py          # what one more allowlist entry would buy
python3 validate/psycopg_check.py && validate/standby_check.sh
(cd ../../go && QS_EXT_DSN='host=/tmp port=5541 dbname=app user=postgres' go test ./cmd/qs-extcheck/)
validate/latency.sh && python3 validate/routed_timing.py
CA=/path/to/cassert/prefix validate/real/cassert.sh
validate/teardown.sh
```

## Wired to the sidecar

Everything above this section was measured with stand-ins: a script created
the mirror relations, registered them, scanned the heap for infinities and
reported an LSN once. The three pieces are now one system, and this section
is what changed, what it found, and the numbers that replace the ones above.
The scripts are in [`validate/real`](../ext/pg_quicksilver/validate/real): a
primary, a streaming hot standby, the production sidecar next to the standby
with `QS_DUCKLAKE=on`, and nothing else configured. Results are in
[`validate/results`](../ext/pg_quicksilver/validate/results).

### What the sidecar does for the extension

`go/cmd/qs-mirror/extension.go`. If `pg_quicksilver` is installed in the
mirrored database (it looks, says once if not, and looks again every minute):

- **On the primary**, because the registry is a table: each mirrored table gets
  a mirror relation in the schema `quicksilver_mirror`, registered in
  `quicksilver.mirrors`. It is a pg_duckdb view over the table's **DuckLake**
  table, with the heap's column names, order and types:

  ```
  CREATE VIEW "quicksilver_mirror"."public.orders" AS
  SELECT (r['id'])::bigint AS "id", (r['amount'])::numeric(12,2) AS "amount", ...
  FROM duckdb.query($qs$ SELECT * FROM lake."public"."orders" $qs$) r
  ```

  It names the lake table, no file and no path, so it is the same text on
  every instance and is never generated again when the mirror compacts. It is
  made again only when the heap's columns change at a DDL barrier (the view's
  comment carries a hash of its definition). It is dropped and unregistered
  only when the table's heap relation is gone. A halt, a rebuild, or a table
  that leaves one sidecar's `QS_TABLES` does not touch it: the registry is one
  for every sidecar, and whether a node reads a table is that node's report
  (see *The registry is configuration; a node's state is its report* below). A type pg_duckdb
  cannot cast a row field to (an enum, `inet`, `jsonb`, an array) is published
  as text, and the extension then keeps statements that read that column on
  the heap. `max_lag_bytes` and `enabled` are the operator's and survive.
- **The lake's catalog path** goes in a new table, `quicksilver.lakes`. The
  extension attaches it in a session itself, the first time that session is
  about to route, and only where the file exists. There is no login trigger,
  so there is nothing that can stop a login on a node without a mirror; and a
  session that was open before any of this existed is routed like a new one.
- **On its own node**: `quicksilver.report_applied_lsn` for every table, in one
  statement on a kept connection, from a goroutine of its own (the apply loop
  never waits for the local server; a restart of it is reconnected to and the
  reports, which live in its shared memory, are sent again). What it reports
  is what a reader of the lake is guaranteed to see: the publisher's last
  published LSN, not the manifest's, and `0/0` for a table that is halted,
  being rebuilt or refused in the lake. `0/0` now means "do not read it on
  this node" and overrules the registry's own `applied_lsn` column, which is
  the same on every node and which the sidecar leaves NULL.
- **Nothing about infinity.** See below.

### The registry is configuration; a node's state is its report

`quicksilver.mirrors`, its mirror relations, `quicksilver.lakes` and
`quicksilver.join_domains` are ONE copy for every sidecar of the cluster,
replicated to every standby. They describe what is mirrored, not what a node
can serve. So:

- **A sidecar writes the rows of the tables it is configured for**, halted or
  not. A halt, a rebuild, a re-snapshot or a lagging table changes no row and
  no view (`registryWanted`, `joinsWanted`; the sync's decision for a halt is
  the same as for a healthy table).
- **A row is removed only when its heap relation is gone** (`registryUnregistrations`,
  decided over the sidecar's own configured tables). A sidecar never removes
  another sidecar's row, nor a row of a table it is not configured for. A
  statement that changes nothing is not run: the registry's trigger fires for
  it and every backend re-reads the registry.
- **What a node reads is its report** (`quicksilver.report_applied_lsn`, this
  node's shared memory). A halted or rebuilding table is reported `0/0`: the
  extension answers it from this node's heap, with the reason *its mirror on
  this node is reported unreadable (halted, rebuilding or not published)*, and
  that holds whatever `quicksilver.max_lag` says, because `0/0` is refused before
  the lag is compared (`qs_applied_lsn`, `qs_fresh`). Other nodes read the same
  row from their own reports.
- **Two sidecars whose table lists differ** share the registry and never fight
  over it: each writes its own tables, and removes nothing it does not own.

A table that **leaves every sidecar's `QS_TABLES`** is the one removal that is not
the sidecar's own. Each sidecar keeps a claim row on the primary,
`quicksilver.sidecar_claims` (one per pod: the tables it is configured for, and
when it last heartbeated, every 30s, under the registry's advisory lock). The
janitor on the primary removes a registry row, its join rows and its mirror view
only when no claim seen within `QS_CLAIM_GRACE` (default 1h) names the table, and
only once the table has stayed unclaimed for the grace by the janitor's own clock.
So a table dropped from one sidecar's list stays while another sidecar claims it,
and a sidecar that is down longer than the grace loses its claim and re-registers
its tables when it returns. Until the row goes, a node that still has a report for
the table may answer from it under a relaxed `max_lag`; once the row is gone the
node reads the heap. The code is `go/cmd/qs-mirror/claims.go`; the decision
is `registryOrphans` and `janitor.settle`.

The sidecar runs the registry statements of its own for a table whose heap
relation is gone (above). The statements an operator would run by hand, for a
database whose sidecars predate the claims table, are:

```sql
DELETE FROM quicksilver.mirrors WHERE mirror = 'quicksilver_mirror."public.orders"'::regclass;
DROP VIEW quicksilver_mirror."public.orders";
DELETE FROM quicksilver.join_domains WHERE heap = 'public.orders'::regclass;  -- if it had join domain columns
```

Each sidecar also reports `0/0` on its own node for every registered table it is
not configured for, at start, at the start of every attempt, and when its node is
promoted (`go/cmd/qs-mirror/blank.go`). So a table dropped from a sidecar's list
is not served by that node from the moment the sidecar restarts, whatever row
remains.

(under the default strict freshness a table whose lake stops advancing is refused
as the WAL moves on, so the statement is needed for a relaxed `max_lag` only.)

`QS_EXTENSION=off` turns all of it off. It needs `QS_DUCKLAKE=on`; without it
the sidecar says so once and registers nothing.

### Infinity is kept now, and `finite_columns` is retired

The mirror used to lose `±infinity`; it now stores DuckDB's own infinity. With
the `finite` option taken off the three types, the equality harness routes the
statements over `t_time` (which holds `infinity` and `-infinity` in a date, a
timestamp and a timestamptz): `SELECT max(d), min(d), max(ts), min(tz) FROM
t_time` and 20 more, all identical. So the seed no longer carries `finite`,
`finite_columns` is unused by any shipped row, and the sidecar does not
maintain it. (The option still exists in the extension for a list that wants
it.) That alone moved the seed from 361 routed statements to 382.

### Freshness is strict by default

`quicksilver.max_lag` defaulted to 16MB, which let a routed read be older than
what the heap on the same node would have answered: not "identical". It now
defaults to **0**: a statement is routed only if its mirror has reached
everything the statement can see on this node when it starts (the replay
position on a standby; the insert position on a primary, which bounds every
commit a snapshot can see). A mirror that has not is answered by the heap at once. The router has a
wait for it, `quicksilver.freshness_wait_fraction` (10%) of the heap plan's
estimated time (total cost times `quicksilver.ms_per_cost_unit`, 0.005 ms), capped at
`quicksilver.freshness_wait_max` (500 ms), with `quicksilver.freshness_wait` as a
floor under it (0 by default). **The wait is taken (2026-11-26)**, in the planner
hook and nowhere else:

1. The node's position R is read once, before the mirror side is planned: the
   replay position on a standby, the insert position on a primary (the same
   function the freshness comparison uses).
2. The mirror's reported LSN X is compared with R, and there are two bounds.
   The statement is routed only when `R - bound <= X <= R`: at or past the lower
   bound (`R` less `quicksilver.max_lag`, zero by default), and not past `R`.
   Behind the lower bound, and the bound has not run out, the planner sleeps on
   the report condition variable (`ConditionVariableTimedSleep`, which waits
   with `WL_EXIT_ON_PM_DEATH`) in steps of at most 5 ms, and checks interrupts on
   each step, so a cancel or `statement_timeout` ends it. Each report wakes it
   early. Past `R` the mirror has committed something this statement cannot see
   (the sidecar holds commits back until the standby has replayed them, but a
   statement that started before a replay step may still find the mirror one),
   so waiting cannot help: the statement goes to the heap at once.
3. The bound is `max(quicksilver.freshness_wait, min(cost x ms_per_cost_unit x
   freshness_wait_fraction, freshness_wait_max))`, where cost is the heap plan's
   total cost. With the defaults a heap plan of cost 10,000 waits at most 5 ms,
   one of 100,000 at most 50 ms, and one of 1,000,000 or more at most 500 ms.
4. It ends at the first of: the mirror reaching R (the statement is routed); the
   bound (the heap answers); a standby whose startup process is waiting on a
   lock, a buffer pin or a snapshot conflict (the heap answers, so that replay
   is not held up); a cancel. R is fixed, so the wait always ends once the
   mirror has reached R or the bound has passed.

The snapshot is **not** given up. The planner runs inside the portal's snapshot,
and the portal keeps a pointer to it (the assertion that failed when the earlier
build swapped it is `portal->portalSnapshot == GetActiveSnapshot()`, see [What the
wait cost](#what-the-wait-costs)). The statement reads the snapshot it has, and
the mirror catching up to R covers it, since R is no earlier than what the
snapshot can see. The wait is not taken inside a subtransaction, nor when the
transaction has already fixed its lake snapshot (qs_pin). The execution-time
check on a kept plan does not wait, as before: a kept plan that is behind goes to
the heap. If the wait gives up, the statement runs on the heap, as it always did.
Bounded staleness is something a session asks for: `max_lag > 0` means "a
routed read may be this much WAL behind the heap on this node". The bounds apply
only to a report made in this node's current role and timeline: a report from
another role (a standby's report on the promoted node, a primary's on a standby)
is not vouched for whatever the bound is (see [Promotion](#promotion)). Separately, a
transaction that has an xid is never routed, whatever
`quicksilver.route_in_transaction` says: the mirror cannot show a transaction
its own uncommitted writes.

Strict only works if the report keeps up while the primary writes WAL that is
not for the mirrored tables. The sidecar now reports, every 25 ms
(`QS_EXTENSION_REPORT_INTERVAL`), the later of the lake's published LSN and a
position it can vouch for with nothing persisted: the end of the last
transaction the apply loop applied, or the walsender's idle position, which it
asks for (a status update with reply requested) instead of waiting for a
keepalive. Only while the lake stands at the newest manifest.

`validate/real/strict.py`: `SELECT count(*), sum(amount) FROM public.orders`
on the standby every 20 ms for 20 s per cell, 200 small transactions a second
on the primary. **History: measured before 2026-11-26, when the wait was not
taken** (the "wait 20 ms" and "wait 200 ms" rows were then the planner's
floor-only wait, which the snapshot check kept at zero). They are kept as
measured; the current measurements are in [Measured again](#measured-again-2026-11-26).

| load on the primary | setting | routed | median, routed | median, heap |
|---|---|---|---|---|
| none | strict, wait 0 | 419 of 422 (99.3%) | 23.0 ms | 68.4 ms |
| none | strict, wait 20 ms (default) | 348 of 348 | 32.5 ms | |
| none | `max_lag` 1MB | 383 of 383 | 28.3 ms | |
| an **unmirrored** table | strict, wait 0 | 37 of 223 (16.6%) | 34.8 ms | 67.9 ms |
| | strict, wait 20 ms (default) | **333 of 346 (96.2%)** | 30.6 ms | 84.9 ms |
| | strict, wait 200 ms | 294 of 294 | 45.7 ms | |
| | `max_lag` 1MB | 331 of 331 | 34.6 ms | |
| the **mirrored** table | strict, wait 0 | 0 of 225 | | 65.8 ms |
| | strict, wait 20 ms (default) | **0 of 178** | | 88.5 ms |
| | strict, wait 200 ms | 31 of 73 (42.5%) | 215.7 ms | 275.9 ms |
| | `max_lag` 1MB | 290 of 290 | 39.2 ms | |

Read plainly (history, as measured before the wait was taken): with writes that do not touch the mirrored table, the strict
default routes 96% and the wait is not visible in the median (within this
host's noise against the `max_lag` row). **With steady writes to the mirrored
table itself, the strict default routes nothing.** The mirror applies on a
200 ms tick and publishes asynchronously, so it is always a transaction
behind a standby that replays in milliseconds; a 200 ms wait gets 42% at a
cost of 200 ms each. A workload like that either accepts a bound
(`max_lag`) or is answered by the heap. The default was not loosened to hide
this.

### What the wait cost (the earlier build, and why the snapshot stays)

The wait was written first to give up the statement's snapshot while it slept,
and to take a new one after it. It did wait (a 1 s floor waited about a second
before the heap answered), and an assertion-enabled build aborted: `TRAP:
failed Assert("portal->portalSnapshot == GetActiveSnapshot()"), File: pquery.c,
Line: 1180`, because the portal of the statement keeps a pointer to the
snapshot it was given, and a replaced snapshot is not that pointer. Keeping the
pointer keeps the snapshot's xmin, which is the thing a wait must not hold. So
that build did not take the wait: it held no wait while a snapshot was held,
which is always, and the settings did nothing. What fixed it (2026-11-26) was
not to release the snapshot at all: the planner sleeps holding it, and the
executor's start, which runs a kept plan's check, does not sleep (see above).

**History (before 2026-11-26; the wait was not taken then).**
`validate/real/waitcost.py` on the 10M-row bench cluster (ICU `en-US`,
standby), the shipped build, while another session commits single-row `UPDATE`s
of `public.orders`: 10 and 100 commits a second. The "new default" row is the
10%, 500 ms setting; it waits zero time here, as does the "old default" row.
Latency is the client's median in milliseconds.

| commits/s | statement | heap | old default (20 ms floor): routed, median | new default (10%, cap 500 ms): routed, median |
|---|---|---|---|---|
| 10 | ~50 ms | 47.3 | 0 of 60, 47.2 | 0 of 60, 51.5 |
| 10 | ~500 ms | 385.7 | 0 of 30, 677.7 | 0 of 30, 575.3 |
| 10 | seconds | 2833.9 | 2 of 10, 1650.4 | 0 of 10, 2615.2 |
| 100 | ~50 ms | 52.7 | 0 of 60, 52.0 | 0 of 60, 54.8 |
| 100 | ~500 ms | 391.1 | 0 of 30, 321.1 | 0 of 30, 335.9 |
| 100 | seconds | 2849.7 | 1 of 10, 2689.4 | 0 of 10, 2668.3 |

Routing is where the mirror happened to be fresh when the statement was
planned: 0 to 2 of 10 for the statements that take seconds, none for the rest.
The latency differences are not waits, and they are of the size the heap rows move by
between runs. The
mirror was 30 to 80 KB of WAL behind on the statements that were not routed,
which is a publish cycle. With a 1 s floor the statements waited about a
second and then the heap answered; with the write load they did not catch up
(0 of 20 routed; 3 of 20 without the floor, in another run), so a wait would not
change the steady-write case that `strict.py` described above.

### A snapshot older than the freshness check had just approved

Found by `validate/real/freshness.py`, in the build that first had the wait: a
read-your-writes session waited 0.6 s for the mirror, was told it had caught
up, was routed, and returned the answer from before its write.

pg_duckdb runs one DuckDB transaction per PostgreSQL transaction, and a
DuckLake transaction reads the snapshot that was current when it first touched
the lake. For a statement that is planned and then run, that is when pg_duckdb
**plans** it. The freshness check ran when the plan started executing, and
believed a report that had arrived after the snapshot was already fixed.

So the check moved to before pg_duckdb is asked anything, and what a
transaction believes about every mirror is recorded at that moment (`qs_pin`)
and not updated for the rest of the transaction; waiting is only possible
before it. A plan kept from an earlier transaction is checked when it starts,
as before. In a transaction block the record is made at the block's first
planned statement, so with `route_in_transaction` a later statement is routed
only if nothing it can see is newer than that. `validate/real/concurrency.py`
checks the whole chain against the heap at a known LSN (below).

Two more things the same runs found, both in the sidecar's favour of being
wrong: the manifest sequence number counts files, so two manifests that differ
only in which rows are dead share one, and "published seq = submitted seq" did
not mean the lake was current (the publisher now says so by identity,
`Publisher.Current`); and a restarted sidecar reported `0/0` for every table
until its first publish, taking every table off the mirror for a moment.

### The allowlist, widened by evidence

`validate/widen.py` is re-runnable. It takes each statement's missing
constructs from `quicksilver.unsafe_constructs()`, proposes the ones that are
simply not on the list (first those that are the only thing a statement lacks,
so a difference names its culprit; then the rest together), runs all 1,142
statements on the **standby** with `route = heap` and `route = auto`, removes
the newest entries of every routed statement that differs, and repeats until
no routed statement differs. An entry is kept only if at least three routed
statements that exercise it came back identical, and those statements are
recorded per entry (`results/widen-*.json`). It ran in the ICU `en-US`
database; its result was then applied in the C locale database, prune only;
what survived both is in the extension's SQL between two markers, which
`widen.py --splice` writes.

| 1,142 statements, routed / different | default | `tolerate_lost_errors` |
|---|---|---|
| seed, stand-in views (the numbers above) | 361 / 0 | 409 / 0 |
| seed, sidecar + DuckLake, `finite` retired | 382 / 0 | 430 / 0 |
| **widened, ICU en-US, standby** | **438 / 0** | **486 / 0** |
| **widened, C locale, standby** | **467 / 0** | **515 / 0** |
| widened, ICU en-US, primary (LSN vouched for by a script) | 438 / 0 | |

Of `bench/exactness`'s 728 statements, 120 are routed in the ICU database
(52 above) and 146 in the C one; 69 of `corpus_own`'s 121; 249 of
`corpus_seed`'s 293.

17 entries were added: the features `offset`, `union`, `union_all`,
`sublink_any`, `sublink_exists`, `sublink_expr`, `agg_filter`,
`agg_order_by`, `window`; `row_number()` and `rank()`; `LIKE`
(`~~(text,text)`, deterministic collations); `<(boolean,boolean)`;
`array_agg(anynonarray)`; the types `real`, `double precision` and
`integer[]`.

**Rejected, and why.** These are the documented exclusions.

| construct | the statement | heap | mirror |
|---|---|---|---|
| `lower(text)`, `ILIKE` | cmp-016, str-017 | 40 rows match | 80: DuckDB folds `İ` to `i` |
| `upper(text)` | str-027 | `STRASSE` | `STRAẞE` |
| `~(text,text)` | str-022 | 988 | 648: POSIX classes differ |
| `=(character,character)`, `text(character)`, `\|\|` | cmp-031, str-062 | `'banana\|'`, 41 rows | `'banana  \|'`, 0 rows: `char(n)` padding |
| `/(bigint,integer)` | own-100 | 2 | 1: not integer division |
| `count(DISTINCT ...)` (`agg_distinct`) | agg-029 | 41 | 40 |
| `timestamptz - timestamptz` | dt-038 | `71 days 11:47:00` | `71 days 12:47:00`: a DST hour |
| `timestamptz::text` | cast-011 | `-04:56:02` | `-04:56`: seconds of a historical offset |
| `float8::text`, `real::text` | cast-005, cast-006 | `0`, `-1401` | `0.0`, `-1401.0` |
| `text::numeric`, `text::float8` | cast-066 | `1.5` | `1.500` |
| `bytea::text` | cast-017 | `\xc4` | `\xC4` |
| `stddev`, `var_pop`, `var_samp`, `corr`, `covar_pop`, `regr_slope`, `float8(integer)` | agg-025, agg-059 | `8750636.925781539` | `8750636.92578154`: the last digits |

- **39 constructs that are not immutable** were never proposed (`now()`,
  `date_trunc(text, timestamptz)`, `timestamptz + interval`, `to_char`,
  `concat`, `extract` from a timestamptz, ...). Their result depends on session
  settings, and the corpus runs in one time zone.
- **326 were exercised by fewer than three routed statements** and were taken
  out again (184 by none once their neighbours were rejected, 106 by one, 36 by
  two: `avg`, `sum(double precision)`, `EXCEPT`, `INTERSECT`, `round`,
  `varchar`, `json`, `bytea`, window frames, ...). None of them is known to be
  wrong. They are the worklist: a corpus that exercises one three times gets it
  verified by running the script again.
- Unchanged from above: integer `+ - *` only with `tolerate_lost_errors`, text
  order only under `C`, `numeric` without precision, `numeric(integer)` only
  under a comparison.

What keeps the 704 on the heap in the ICU database: a construct not on the
list 542, reads no mirrored table 143, cheap 17, no usable mirror 1, and one
where the mirror failed and the heap answered. The first construct that
stopped a statement: integer `+ - *` 56; text order under the database's
collation 52; `t_doc.jb` (jsonb, text in the mirror) 31; `numeric(integer)`
outside a comparison 19; `t_str.e` (enum) 14; `t_doc.ia` (array) 12;
`t_num.nu` (numeric without precision) 10; `count(DISTINCT)` 8;
`varchar::text` 8; window frames 7; `avg(integer)`, `avg(numeric)`, `ILIKE`,
`~`, `date(timestamptz)`, `varchar` 6 each.

The regression tests' "must stay on the heap" statements were written against
the seed, so `failclosed`, `prepared` and `robustness` set the widened entries
aside while they run and put them back.

### Errors, and what may be answered from the heap instead

There is still no subtransaction around the mirror: pg_duckdb raises from its
subtransaction callback when one starts while its DuckDB transaction is open.
So the catch was narrowed to where it is safe without one. A mirror-side error
is answered from the heap only if it is a plain `ERROR` that pg_duckdb raised
at its own C++ boundary (its messages begin `(PGDuckDB/`; DuckDB reads the
lake's files and takes no buffer pin, relcache reference or lock on the way,
and its C++ stack has unwound), or pg_duckdb's own refusal at the door.
Everything else is rethrown: any error of PostgreSQL's own, a cancel or a
shutdown however it is spelled (`statement_timeout` on a routed statement
reaches the client as an error; there is a test), deadlock and serialization
failures, out-of-memory, anything above `ERROR`. A mirror node that threw is
not ended with `ExecEndNode`; it goes with the query's memory context.

`sql/errors.sql` forces it: a kept plan whose Parquet file is replaced by
garbage, and a mirror whose own cast fails at execution, each outside a
transaction block, inside one, under a cursor, and with `fallback = off`. On
the assertion-enabled build the whole suite passes and neither server log has
a leaked-resource warning.

The shared-memory table of applied LSNs (1,024 entries) never gave one back.
It now holds entries only for registered heap tables and sweeps when the
registry changes or it fills; `errors.sql` registers, reports and drops 1,100
tables in a row.

### The validation

All on the standby unless it says otherwise. PostgreSQL 17.12, pg_duckdb
1.2.0, one host, 4 vCPU.

**From an empty cluster, across everything the mirror goes through**
(`validate/real/lifecycle.py`). At each step, in a session open since the
start, one opened just before the step, and a new one: `SELECT status,
count(*), sum(amount) FROM public.orders GROUP BY status` is answered by the
mirror and equals the heap; a statement prepared once at the start (its plan is
kept) is the heap's answer too; `EXPLAIN` says `Quicksilver: mirror` and why; a
primary-key lookup says `Quicksilver: heap (cheap on the heap)`.

| step | routed again after | mirror relation |
|---|---|---|
| start: 1,000,000 orders, configuration only | 0.5 s | created by the sidecar |
| write burst: 50,000 inserts, 20,000 updates, 5,000 deletes | 1.6 s | the same |
| delta merge (seen in the sidecar's log) | 0.3 s | the same |
| idle base rewrite (seen in the sidecar's log) | 0.1 s | the same |
| `TRUNCATE` + reload of 300,000 rows | 3.6 s, the heap answering until then | the same |
| `ADD COLUMN` (a DDL barrier) | 1.4 s; the new column is routed | re-created, as it should be |
| a date the mirror cannot hold: `qs_types` halts | that table: the heap, at once; the others routed | kept, registered; that node's report is 0/0 |
| `REBUILD` of that table | 0.3 s after the sidecar was ready | registered again |
| sidecar `kill -9`, a write, restart | the heap while it is down; 0.2 s after | the same |
| standby restart | 0.7 s (the reports in its shared memory are sent again) | the same |

The two sessions that were open across the standby restart were closed by it,
as a restart does; every other step kept its sessions.

**Freshness** (`validate/real/freshness.py`). Sidecar stopped, one write:
strict goes to the heap ("760 WAL bytes behind"). A `max_lag = 1MB` session
keeps the mirror's older answer, which is what it asked for, until the primary
has written 9.7 MB, then the heap. `min_applied_lsn` set to the LSN of a
write: with `freshness_wait = 0` the heap answers; with 500 ms it waits 567 ms
and the heap answers; with 60 s and the sidecar started at the same moment it
waits 1.1 s and the mirror answers, with the write. Strict routing resumes as
soon as the sidecar is ready.

**Concurrency** (`validate/real/concurrency.py`, on the assertion-enabled
build): 16 clients on the standby for 300 s (6 aggregates with `max_lag`
64MB, 4 strict, 3 lookups, 3 prepared statements), 12,819 transfer
transactions on the primary, 70 delta merges and one base rewrite in the
sidecar. Every answer is checked against three invariants computed from the
heap before the writes (total, customer 1, customer 5: a transfer moves an
amount between two rows of one customer), and every 50 s the writer pauses
and a checksum the writes do change is computed from the primary's heap at a
known LSN and asked of the standby with `min_applied_lsn` set to it.

| | |
|---|---|
| statements answered by the mirror | 5,722 with `max_lag`, 884 strict, 537 prepared |
| statements answered by the heap | 148,997 lookups, 1,325 strict, 4,638 prepared |
| errors | 0 |
| wrong answers | 0 |
| checksum at a known LSN, routed and equal | 6 of 6 |
| `TRAP`, signal, `PANIC`, leaked-resource warning, either log | 0 |

**On the assertion-enabled build** (`validate/real/cassert.sh`): the ten
regression files pass, and the run above.

### What an image needs, now

1. pg_duckdb 1.2.2 (the pin; the run above was 1.2.0) and `pg_quicksilver` in the PostgreSQL image
   (`deploy/Dockerfile`, target `postgres`), and
   `shared_preload_libraries = 'pg_duckdb,pg_quicksilver'`, in that order.
2. `CREATE EXTENSION pg_duckdb; CREATE EXTENSION pg_quicksilver;` in the
   database, on the primary.
3. The plugin parameter `ducklake: "true"`. The sidecar does the rest,
   including `duckdb.install_extension('ducklake')` and `('sqlite')`, which
   every instance then loads: an instance that cannot reach DuckDB's extension
   repository and does not have them under `$PGDATA/pg_duckdb/extensions`
   cannot plan a mirror relation, and answers from the heap.
4. `quicksilver.route = auto`, per role or per database; `shadow` first.

Stock pg_duckdb still gives only a superuser a DuckDB that can read local
files, so only superusers are routed; for anybody else the extension does not
try to attach (the attempt fails, slowly) unless
`quicksilver.attach_lakes_for_all` says the build allows it.

### Not validated

- A real Kubernetes Pod: the plugin's new parameters are unit-tested, and the
  `postgres` image target was written and not built (no Docker daemon here).
- More than one standby, each with its own sidecar, reconciling one registry;
  a failover. (A non-superuser reading a real mirror is validated on the
  confined build only; see [Ordinary roles](#ordinary-roles).)
- An instance with no route to DuckDB's extension repository.
- Strict freshness on a primary beyond the regression tests: the primary has
  no mirror in production, and the runs that route there vouch for the LSN by
  script.
- A lake reader that outlives `QS_DUCKLAKE_RETENTION` (60 s in these runs, 5 m
  by default) inside one statement.
- `route_in_transaction` under concurrency; pg_duckdb itself under assertions;
  valgrind; anything but PostgreSQL 17.12, pg_duckdb 1.2.0, x86_64, 1M rows.
- The catch in `qs_build_mirror` (analysis of a statement the extension wrote
  over the mirror relation) is still a `PG_TRY` with no subtransaction. It is
  not expected to throw, and it rethrows cancel, shutdown and resource errors.
- After a failed attach, or when pg_duckdb's DuckDB was first started under a
  non-superuser role and the session then became a superuser, the session
  stays on the heap for five minutes before trying again.

```
cd ext/pg_quicksilver && make && sudo make install
validate/real/setup.sh                                  # LOCALE=C for the other database
python3 validate/widen.py --out validate/results/widen-icu-en-US.json
python3 validate/widen.py --apply validate/results/widen-icu-en-US.json --out validate/results/widen-C.json
python3 validate/widen.py --splice validate/results/widen-C.json && sudo make install
python3 validate/equality.py --port 5581 --set quicksilver.freshness_wait=5s
python3 validate/real/strict.py; python3 validate/real/freshness.py
python3 validate/real/concurrency.py; python3 validate/real/lifecycle.py
DB_ROUTE=none validate/real/setup.sh && validate/real/primary_report.sh && make installcheck PPORT=5580
CA=/path/to/cassert/prefix validate/real/cassert.sh
validate/real/teardown.sh
```

## Measured again (2026-11-26)

Four things in the extension changed on this date, and the two builds it is
tested on were run again: PostgreSQL 17.12 from source, once with
`--enable-cassert` and once without, pg_duckdb 1.2.2 copied into each, one
host with 4 vCPU.

- **The freshness wait is taken**, in the planner, within a bound
  ([Freshness is strict by default](#freshness-is-strict-by-default)).
- **`quicksilver.route_in_transaction` is `off | safe | on`, and `safe` is the
  default.** A non-autocommit client is routed inside a block that is READ
  COMMITTED, has written nothing and has no savepoint open. The three
  conditions were already checked for every setting but `off`. At the commit
  this section starts from, the source's boot value was `on` while the text
  said `off`.
- **A `DECLARE` whose heap plan can scan backward goes to the heap.** A routed
  cursor cannot be scrolled, and the heap's can. The transaction fuzzer found
  it with the default settings: 5 of 2,000 scripts differed (seed 11), and none
  did after the fix (seeds 11 and 12).
- **`EXPLAIN (FORMAT JSON)` of a heap decision is valid JSON.** The decision's
  property had been written after the plan's object closed.

### Validation

| check | result |
|---|---|
| regression, assertion build, `validate/real/cassert.sh 300` | 12 of 12 pass; `debug_assertions = on` on both servers; 0 `TRAP`, `PANIC`, signal or leak lines in either log |
| known-LSN checksums during 300 s of concurrent writes | 6 of 6 routed and equal; 0 wrong answers, 0 errors |
| regression, normal build (`validate/real/setup.sh`, `make installcheck`) | 12 of 12 pass; `debug_assertions = off` |
| `validate/real/txfuzz.py`, default settings, seed 11, 2,000 scripts | 27,603 statements, 7,272 answered by the mirror, 0 scripts differed, 0 connections lost |
| `validate/real/txfuzz.py`, default settings, seed 12, 2,000 scripts | 27,241 statements, 7,025 answered by the mirror, 0 scripts differed, 0 connections lost |
| `validate/equality.py --port <standby> --set quicksilver.freshness_wait=5s` | 1,142 statements: 438 routed, 438 identical, **routed_different 0**, heap_different 0 |
| `validate/psycopg_check.py` | 16 of 16 |

In one 300 s run before the last one, a checksum went to the heap with a
reason that began "no": the text the wait appends when the standby's startup
process is waiting on a lock, a buffer pin or a snapshot conflict starts that
way. The harness cut the reason at 160 characters, so the whole text was not
kept, and the cause is not confirmed. The run kept here has six of six.

### The wait in the router's own terms

`validate/real/waitcost.py` (3,000,000-row `public.orders`, bench dataset,
normal build, the sidecar built from this worktree, default settings: database
`quicksilver.route = auto`), while another session commits single-row `UPDATE`s
of `public.orders` with `synchronous_commit = off`. Rates are the writer's
achieved rates. Each row is one statement, issued 60, 30 or 10 times:

| writes/s | class (heap, routing off, median) | routed / issued (default) | median, routed | median, heap (routing off) |
|---|---|---|---|---|
| 10 | "~50 ms" (15.9 ms) | 57 / 60 | 23.6 ms | 15.9 ms |
| 10 | "~500 ms" (89.8 ms) | 29 / 30 | 17.8 ms | 89.8 ms |
| 10 | "seconds" (878.5 ms) | 10 / 10 | 37.9 ms | 878.5 ms |
| 100 | "~50 ms" (15.1 ms) | 54 / 60 | 27.8 ms | 15.1 ms |
| 100 | "~500 ms" (87.7 ms) | 30 / 30 | 17.2 ms | 87.7 ms |
| 100 | "seconds" (889.7 ms) | 10 / 10 | 36.9 ms | 889.7 ms |
| 1,000 | "~50 ms" (15.0 ms) | 37 / 60 | 28.9 ms | 15.0 ms |
| 1,000 | "~500 ms" (94.4 ms) | 26 / 30 | 18.7 ms | 94.4 ms |
| 1,000 | "seconds" (912.1 ms) | 10 / 10 | 50.0 ms | 912.1 ms |

The writer reached 10, 100 and 1,000 commits a second (437, 4,446 and 50,008
rows over 44, 44 and 50 s). The three classes are labels from the script; the
heap times measured here are 15 ms, 90 ms and 0.9 s, not the 50 ms and 500 ms
the labels name. The statements not routed gave "mirror is not fresh enough":
3 of 60, 1 of 30, 6 of 60, 23 of 60 and 4 of 30 in the rows above, and none in
the others. The default wait could last at most 2 ms (the "~50 ms" class), 32 ms
("~500 ms") or 93 ms ("seconds"). Whether the mirror reached the position in
that time decided each statement, and the mirror's publish cycle is longer than
the bound. Nothing here compares results: the correctness under writes is
`concurrency.py`'s, in the run above.

**The earlier 0% figures are history.** The table above replaces the "0 of 60
at 10 and 100 commits a second" rows of [the section on the wait](#freshness-is-strict-by-default)
and the strict-freshness table, which were measured before this date and kept
as measured. A first run of this script on this date reported 0 routed in every
row, while its median latencies for the same statements were far below the
heap's. The script's connection let psycopg prepare each statement after its
fifth run; a prepared statement is not planned again, so the router made no new
decision and `quicksilver.last_decision()` reported an older one. With
`prepare_threshold=None` each execution is planned, and the table is that run.
The script now prints the routed and the heap medians separately.

### Running it, now

```
cd ext/pg_quicksilver
CA=/path/to/cassert/prefix BASE=/var/lib/postgresql/<dir> PPORT=5680 SPORT=5681 HPORT=5682 validate/real/cassert.sh 300
DB_ROUTE=none BASE=... validate/real/setup.sh            # the regression cluster; make installcheck PPORT=...
python3 validate/real/txfuzz.py --port <primary> --scripts 2000 --seed 11   # after primary_report.sh
python3 validate/equality.py --port <standby> --set quicksilver.freshness_wait=5s
python3 validate/psycopg_check.py --port <primary>
DATASET=bench ROWS=3000000 DB_ROUTE=auto validate/real/setup.sh
python3 validate/real/waitcost.py --pport <primary> --sport <standby> --rates 10,100,1000
```

`validate/real/cassert.sh` now builds its copy of the extension beside its
cluster (`$BASE-cabuild`), not in a fixed path, and `validate/cassert.sh`, the
stand-in path, is gone.

## pg_duckdb 1.2.2

The pin moved from 1.2.0 (DuckDB 2.0.0) to 1.2.2 (DuckDB 2.0.1), because the
apt repository no longer serves 1.2.0. This section is the first run of the
extension against 1.2.2.

**Server.** PostgreSQL 17.12, source build with `--enable-cassert`, in a private
prefix. The 1.2.2 `pg_duckdb.so`, `libduckdb.so` and the extension's SQL and
control files were copied into that prefix. The package's `pg_duckdb.so` has
`RUNPATH=/usr/lib/postgresql/17/lib`, so the server must start with
`LD_LIBRARY_PATH` pointing at the prefix's `lib`, or it loads the system's
`libduckdb.so` (1.2.0) instead. `/proc/<pid>/maps` showed the prefix's copy.

| check | output |
|---|---|
| `extversion` of `pg_duckdb` | `1.2.2` |
| `SELECT version()` through `duckdb.query` | `v2.0.1` |

`duckdb.*` settings in 1.2.2 (`SELECT name FROM pg_settings WHERE name LIKE 'duckdb.%'`):

    duckdb.allow_community_extensions    duckdb.allow_unredacted_secrets
    duckdb.allow_unsigned_extensions     duckdb.allowed_directories
    duckdb.autoinstall_known_extensions  duckdb.autoload_known_extensions
    duckdb.azure_transport_option_type   duckdb.convert_unsupported_numeric_to_double
    duckdb.custom_user_agent             duckdb.default_collation
    duckdb.disabled_filesystems          duckdb.enable_external_access
    duckdb.extension_directory           duckdb.force_execution
    duckdb.force_motherduck_views        duckdb.log_pg_explain
    duckdb.max_memory                    duckdb.max_temp_directory_size
    duckdb.max_workers_per_postgres_scan duckdb.memory_limit
    duckdb.motherduck_session_hint       duckdb.postgres_role
    duckdb.temporary_directory           duckdb.threads
    duckdb.threads_for_postgres_scan     duckdb.unsafe_allow_execution_inside_functions
    duckdb.unsafe_allow_mixed_transactions duckdb.worker_threads

**The extension on 1.2.2.** `validate/real/setup.sh` with `DB_ROUTE=none` on
ports 5650-5652, then `make installcheck PPORT=5650`:

    ok 1 - routing   ok 2 - failclosed   ok 3 - permissions   ok 4 - prepared
    ok 5 - transactions   ok 6 - invalidation   ok 7 - shadow   ok 8 - freshness
    ok 9 - robustness   ok 10 - errors
    # All 10 tests passed.

`validate/equality.py --port 5650`: 1142 statements, 438 routed to the mirror,
0 routed and different, 0 heap-different. The 1.2.0 cassert run above
routed 361 of 1142; the count moved and nothing else did. The mirror and the
DuckLake attachment were built by the sidecar on the standby, with
`QS_DUCKLAKE=on`, and the DuckDB extensions were the 2.0.1 builds that
`deploy/duckdb-extensions.sh install` writes into `v2.0.1/linux_amd64/`.

**Offline, DuckDB extensions.** `deploy/duckdb-extensions.sh install` and then
`verify` under `unshare -n`, as the postgres user, against the 1.2.2 server:
`verified: ducklake sqlite load from ... with no network and no write access,
and a ducklake:sqlite: lake reads back`. The script needed no change: it takes
the DuckDB version and platform from the loaded `libduckdb.so`, not from a
pin.

**Serving path on 1.2.2 (`bench/scripts/serving_pg17.sh`).** The job cannot move
to the stock package. The 1.2.2 binary does contain `duckdb.allowed_directories`,
and it is documented as "always allowed for file access, even when
enable_external_access is false". Its effect on an unprivileged role was
measured on a scratch 1.2.2 server with `duckdb.allowed_directories` set to a
directory holding a parquet file, and `duckdb.enable_external_access = off`:

| role | `duckdb.query('... read_parquet(...)')` |
|---|---|
| superuser | 5 rows |
| member of `duckdb_users` only | `Permission Error: File system LocalFileSystem has been disabled by configuration` |
| plus `pg_read_server_files` | same error |
| plus `pg_read_server_files` and `pg_write_server_files` | 5 rows |

The last row is the stock gate (`AllowRawFileAccess`, both roles together), and
it is the escalation the patch exists to avoid. So on 1.2.2 the setting does
not give an unprivileged role read access to the mirror alone. That is the stock
package. With `bench/patches/pg_duckdb-1.2.2-allowed-directories.patch` on 1.2.2,
both properties hold: the mirror reads through PostgreSQL for an ordinary role,
and its denial cases are refused (`bench/results/pg_duckdb_1.2.2_confinement.txt`,
`bench/scripts/serving_pg17.sh`). The serving job builds that patched 1.2.2.

## Time zones in date_trunc

`date_trunc(text, timestamptz)` reads the session `TimeZone`, so it is routed only
under three conditions, each checked when the statement is planned and again when
the mirror runs:

- the unit is a string constant in the proven set (`microseconds` ... `decade`,
  compared case-insensitively; `hour` only where the zone's entry says so);
- the session zone is in the zone table, which `validate/tz_check.py` generates;
- DuckDB's `TimeZone` is that zone when the mirror runs. pg_duckdb's GUC hook keeps
  it equal to the session's (the confined build lets the hook through the lock, see
  `bench/patches`); the extension reads it back and sends the statement to the heap if
  it differs.

**The rule.** `tz_check.py` runs every zone in `pg_timezone_names` against the
primary: PostgreSQL's and DuckDB's `date_trunc` and wall clock, for 13 units, at
the transitions of the zone (1900-2100, to the minute), at 15-minute steps around
each, and at 2000 seeded random instants, plus infinity, -infinity and NULL. A zone
is admitted for the proven units only if every row agrees on the truncated value
and on the wall clock. `hour` is admitted too only if the zone has no offset
decrease in 1900-2100 (a repeated wall-clock hour needs one) and every hour row
agrees. `century` and `millennium` disagree in every zone and are never admitted.
Results: `validate/results/tz_check.tsv` (one row per zone, with the reason),
`validate/results/tz_check.txt`.

| zones | count |
|---|---|
| in `pg_timezone_names` | 499 |
| admitted for the proven units | 240 (of which 40 also for `hour`) |
| denied: the proven units differ | 257 (130 of them with wall-clock drift against ICU) |
| denied: a side refuses the zone | 2 (`localtime`, `posixrules`) |

No admitted zone shows wall-clock drift. POSIX-style zones (`+03`) are not in
`pg_timezone_names`, so they are not admitted.

**Measured** (one primary, 1,000,000 orders, 10,000 customers):
equality, superuser and ordinary role under UTC and New York, 1,166 statements
(the exactness corpus, the own and seed corpora, and `validate/corpus_tz.sql`):
routed 467 (UTC) and 464 (New York) of 1,166, all identical, 0 different. The existing
corpus alone routes 449 under UTC, 3 of them date_trunc. Fuzz, 2,000 scripts:
5,122 statements sent, 22 answered by the mirror, 4 of those compared, 0 different,
0 lost, 0 crash lines (not a cassert build). Headline (revenue by month over the join,
client-side median of five runs): UTC heap 400 ms, routed 66 ms; New York heap 397 ms,
routed 62 ms; the answers are identical under each zone, and the two zones differ from
each other as they should.

**A known pg_duckdb limitation, not introduced here.** A POSIX-style zone such as
`+03` cannot be used once DuckDB is up in the backend:

    SET TimeZone = '+03';                      -- fine while DuckDB is not yet initialised
    SELECT * FROM duckdb.query('SELECT 1');    -- ERROR: (PGDuckDB/CreatePlan) Not implemented Error: Unknown TimeZone '+03'!
    SET TimeZone = '+03';                      -- ERROR: (PGDuckDB/DuckAssignTimezone_Cpp) Not implemented Error: Unknown TimeZone '<+03>-03'!

The same sequence fails in the same way on the stock 1.2.2 package (extracted, with
the system PostgreSQL 17.12) and on the patched build. The hook that sends the zone to
DuckDB is the stock one. The extension never routes such a zone: `+03` is not in the
table, so a statement that depends on the zone stays on the heap with the reason
`time zone +03 is not in the proven table`.

## Join keys

A join on two uuid columns is compared on the mirror as the sidecar's integer
companions when both columns are in one join domain (docs/16, "Join domains"):
`quicksilver.join_domains` holds the configured columns and their companions,
written by the sidecar once per configuration, not per file. A companion is exposed by a
mirror relation after the heap's columns, and only for a statement that can
use it; a whole-row reference is never routed, so a companion cannot appear
in a record. Only the join comparison changes. The uuid columns are still
projected, grouped and filtered as written.

Rewritten: `A.x = B.y` with the default uuid equality, both sides plain
columns of two different mirrored relations of the same query level, both in
one domain, in a join condition or as an AND-ed WHERE conjunct, including
inside a FROM subquery (each level has its own range table). Not rewritten:
sublinks (IN, EXISTS), a cast on either side, any other operator, a column
outside the domain, a column this node does not report complete. The GUC
`quicksilver.join_domains = off` disables the rewrite; EXPLAIN shows
`Join keys: N rewritten to companions` when N > 0.

Measured on this host (2,000,000 orders, 100,000 customers, uuid keys, standby;
median of 7 interleaved runs, client-side): heap 314 ms; routed without the
rewrite 97 ms; routed with it 47 ms. Both routed answers equal the heap's.

The equality corpus has 75 join statements over the domain
(`validate/corpus_join.sql`). 60 are routed, 40 of those have a join rewritten,
and all 75 are identical to the heap; `routed_different` is 0 over the whole
corpus (1,217 statements, 498 routed).

Under writes that change the join keys, the answer stayed equal to the heap at
every known-LSN check (`validate/real/joinchurn.py`). When the domain passes its
key cap it becomes incomplete on that node: the node reports the column
incomplete before the manifest that carries the NULL companions is published,
its registry row stays, and the statements go on being routed without the
rewrite.

Completeness is per node, and the report lives in that node's shared memory
(`quicksilver.report_join_complete(heap, column, complete)`, granted as
`report_applied_lsn` is). It is not persisted: after a restart, or with no
report, a column is not complete. A report of complete is only made for a
published manifest that is complete for the column, and an incomplete manifest
is preceded by its incomplete report (sidecar, `beforePublish`). A scan that
compared a column checks, before it hands on each row and at the end of its
rows, that the column's report has not changed since it began; if it has, the
statement fails with "a join domain column changed while this statement ran",
and it can be run again. Rows already returned were read from complete
companions. Measured (`validate/real/joinnodes.py`, one standby, 120 s, four
routed workers while a superuser flipped the report 793 times): 17,284 routed
answers equal to the heap, 1,240 of them ended with that error, and no
registry row was inserted or deleted.

Not done: a jsonb column in any table a join reads keeps the join on the heap.
The walk visits the join's alias variables, and a jsonb one among them refuses
the statement even when it is not referenced. Relaxing that needs care: a
COALESCE from FULL JOIN USING lives in the same alias variables. RIGHT and
FULL joins are not on the allowlist, so they are answered from the heap.

## A crash in the registry load

On a hot standby, `ALTER TABLE public.orders ADD COLUMN` run again and again on the primary, with a client counting the table and the sidecar reporting applied LSNs every 25 ms, killed backends with SIGSEGV. The backends died in their first planning statement (`quicksilver.report_applied_lsn(...)`, `quicksilver.last_decision()`). The crash is the same on the assertion build and on the normal one (`validate/results/crash_backtrace.txt`).

The cause is a check and a use with a call in between. The registry load checked that the mirror relation exists (a syscache lookup), then looked its namespace and name up again for the `FROM` clause. A standby replays the sidecar's `DROP VIEW` of a mirror it is rebuilding; the invalidation is accepted in that window, the second lookups return nothing, and `quote_qualified_identifier` dereferences NULL. `validate/real/hardening/race_mirror.sh` reproduces it deterministically: gdb stops a backend between the two lookups, another session drops the view, and the invalidation is forced.

The same load also replaced the registry by `MemoryContextReset` of its own context. No crash came from that path (the decision code checks the registry generation after the calls that can process invalidations, and no load runs inside a decision), but nothing enforced it, so it was not safe by inspection.

The lifetime rule now in force:

1. A load builds the registry in a new memory context and retires the old one. A retired context is deleted only when no hook or SQL-callable function of the extension is on the stack (`qs_in_ext`, counted on every entry and decremented on every exit, error exits included), no load runs and no decision is in progress (`qs_busy`). The sweep runs at the start of a top-level planning and at the end of a transaction.
2. A load is not re-entered (`qs_loading`). Planning passes through while it runs, as if the extension were not installed; a read of the registry raises an ERROR.
3. Nothing that outlives a statement points into a registry context: the last-decision record holds a fixed buffer and top-context strings, plans carry OIDs and copied strings, and executor state looks a mirror up again by OID, answering from the heap if it is gone.
4. A decision takes the registry digest before anything that can process invalidations, and makes its decision again only if what it depends on moved (see "The registry digest" below). A decision is made at most `QS_DECIDE_ATTEMPTS` (4) times.
5. A mirror's catalog facts are read once, while the entry is held. A lookup that can find the relation gone marks the mirror unusable with a reason, and the statement goes to the heap.

The regression test `sql/reload.sql` runs a routed statement, changes the registry in the same session, and asks `EXPLAIN` and a prepared routed statement again across the changes. Its output does not depend on which side answered, since the sidecar may write the registry while it runs.

### The registry digest, and kept mirror entries

A cache invalidation says that the registry may have changed, not that it did.
The registry is loaded again on every invalidation of a registry table or a
watched relation (`qs_relcache_cb`), but a decision is moved only when what it
depends on is different:

- `qs_registry_digest` is a sum of per-row hashes over the mirror key (the
  allowlist with what its names resolve to, the features and time zones, the
  join domains, the registry tables' OIDs) and the attached lakes. The applied
  LSN is not in it, so the trigger on `quicksilver.mirrors` does not move a
  decision. `qs_mirrors_digest` is the same over the mirror entries a decision
  read. The sums do not depend on the order the rows come in.
- A decision takes both digests before its walk and compares them after the
  calls that can process invalidations (the lakes' attachment, the substitution).
  A moved digest is made again, from the registry as it is now, at most
  `QS_DECIDE_ATTEMPTS` (4) times; an unmoved one is not made again, and its
  mirror entries are looked up in the generation in force.
- A mirror entry of the generation being replaced is kept when nothing it was
  built from changed (`qs_reuse_mirror`): it was built under the same
  `qs_mirror_key_cur`, the relcache of its heap and mirror relations has not
  changed since (`qs_relcache_epoch`, bumped for every watched relation and for
  a full reset), and the mirror relation, the lag bound and the finite columns
  are the same. A kept entry is copied into the new generation (`qs_copy_mirror`,
  deep, with `copyObject` for the subquery), so nothing used after the load
  points into a retired context. An entry that is not usable, or whose epoch
  moved, is built again: that is the only place the catalogs are read for it.
- A registry load that could not lock its tables (above) is not kept: the
  registry is "not installed" for that statement, and the next one loads again.

The validation of the digest is the DDL storm (`validate/real/hardening/storm_ddl.py`,
routed readers on the standby: no 40P01, no wrong answer) and the churn of
unrelated DDL (`validate/real/hardening/churn_reads.py`), on the same build.

## Promotion

A promoted standby is not fed. Its sidecar stands down and drops its replication
slot, so the mirror stops at the position it had reached, and the node's own
position moves on with every write. Routing on that node must stop for
freshness, and it does, whatever `quicksilver.max_lag` says. A report
**vouches for a mirror only for the role and timeline the node had when it was
made**: each report is stamped (`QsLsnEntry.in_recovery`, `.tli`,
`qs_report_vouches`) and every read compares the stamp with the node now. A
promotion changes both (it also starts a new timeline), so the standby's last
report is void on the promoted node at the first read after `RecoveryInProgress()`
turns false, in every backend, including sessions opened before the promotion and
transactions that pinned their mirrors before it (`qs_pin_recovery`,
`qs_pin_tli`). The same holds the other way: a primary's report (the regression
harness, `validate/real/primary_report.sh`) is not honoured on a standby that a
rewind has made of the old primary; it is not vouched for until the sidecar
reports again. Before this, the check compared the last applied LSN with "now"
only against the byte bound, so a 1GB bound kept a promoted node answering from
its mirror until 1GB of WAL had passed (a measured 130 s, 1,237 of 1,246 polls;
`bench/owed_live_2026-11-28`, `c_promote2.py`). A mirror that was fed and is no
longer vouched for on a node that is not a standby is answered by the heap with
the reason "this node is not a standby: its mirror is no longer fed", followed by
the detail (it was "mirror is not fresh enough" before). A mirror nothing ever
reported on keeps the generic reason.

The sidecar's stand-down (`blank.go`, which reports 0/0) is no longer what stops
the routing: the extension does not depend on it. It is still done, so that
`quicksilver.mirror_status()` shows the mirror as unreadable, and `/readyz`
says "stood down" (see [docs/16](16-deploying.md)).

**One accessor.** `quicksilver.mirror_status()` used to read the applied LSN twice,
once for its `applied_lsn` column and once inside the freshness check, each under
its own lock, and a report that landed between the two could show NULL and fresh
(or a value and not fresh). It now calls `qs_applied_at` once, and the freshness
check (`qs_fresh_at`) takes that LSN and state. The read path, the planner's wait,
the execution-time check and the status function all use `qs_applied_at`, so an
applied LSN is shown exactly when the mirror may be read. A transaction that has
pinned its mirrors reads the pins, as before. The NULL the earlier note recorded
on A was not reproduced in the two recorded runs (`c2_run1.json`, `c2_run2.json`
show a value with `fresh` false); the cause is the one above, and the one
accessor removes the second read.

What could go wrong instead is the lake. The node's `duckdb.allowed_directories`
is copied from the node it came from, and the lake's catalog is the one the
registry names. A catalog outside the directories DuckDB may read here cannot be
attached, and before this change the failure was silent: `attach_lakes()`
returned -1, the mirror plan then failed with "could not plan it", and the
attach error was thrown away. Now the attach records which lake failed and why
(the catalog is not on this node, it is outside `duckdb.allowed_directories`, or
DuckDB refused it, with DuckDB's message). A decision that cannot attach falls
back with the reason "lake is not attached on this node", and the detail names
the lake. The first failure per backend is a WARNING, and only when the catalog
is there: a standby with no lake yet, or a primary, is not a mistake.

The operator sets `duckdb.allowed_directories` on each node to the directory that
holds that node's mirror. The extension does not widen it, because it is the only
thing that keeps an ordinary role out of other mirrors.

`sql/lakeattach.sql` checks the behaviour: routed with the lake readable, refused
(-1, the heap with the reason) with a directory that does not hold it.

## Keyword names

pg_duckdb writes a mirror subquery's names in DuckDB's SQL, and it quotes them with
PostgreSQL's `quote_identifier`. That leaves bare the words PostgreSQL does not reserve
and DuckDB does: AT, PIVOT, QUALIFY, ASOF, ANTI, SEMI, POSITIONAL, LAMBDA, DESCRIBE,
SUMMARIZE, SHOW and about a hundred more, so `SELECT at, pivot FROM (...)` reaches DuckDB,
which fails on it or reads it as something else. The list is measured, not guessed: each
of DuckDB 2.0.1's keywords (502 words in all categories) was tried as a bare name in the
three shapes pg_duckdb writes (a select list with an alias, a cast with an alias, a
qualified reference). 105 fail in one of them. COLUMNS, EXCLUDE and REPLACE are added,
because DuckDB reads them as star modifiers. The same 108 words are in the extension
(`qs_duck_keywords`) and in the patch (`pgduckdb_duckdb_keywords`).

There are two builds, and they differ here.

**Stock pg_duckdb 1.2.2.** The extension does not mirror a table with such a name: a
table named so, or one with a column named so, is answered by the heap, and its reason
names the word. The mirror relation is a view of the whole table, and its definition is
what pg_duckdb writes; one such column makes the view unplannable.

**The patched build** (`bench/patches/pg_duckdb-1.2.2-duckdb-keywords.patch`, applied after
the allowed-directories patch). Every name pg_duckdb writes for DuckDB is quoted when it is
one of the 108 words: the vendored deparser's `quote_identifier` (select-list and alias
names, CTE and table aliases, column references, the `r.name` form of a `duckdb.row`
subscript) and `pgduckdb_quote_identifier` (the names the DDL and FROM paths write:
function names, schema and relation names, `duckdb.row` reference names). The build
defines the read-only GUC `duckdb.quotes_duckdb_keywords` (`on`), and the extension reads
it with `GetConfigOption()`: with the marker present a table with such a name is mirrored
and routed like any other; without it the refusal above stands. The extension's keyword
list is the same 108 words, so the two cannot drift apart without a test failing:
`sql/keywords.sql` runs with the 108-column table on both builds, against
`expected/keywords.out` (stock) and `expected/keywords_1.out` (patched).

Where names reach DuckDB unquoted in the stock build, for the record:

- `src/vendor/pg_ruleutils_17.c` (`quote_identifier`, and through it `get_rule_expr`,
  `get_target_list`, `generate_relation_name`): every identifier of a query's SQL.
- `src/pgduckdb_ruleutils.cpp` `pgduckdb_strip_first_subscript` (`extval`, the `r.name` of a
  `duckdb.row` subscript) and `pgduckdb_write_row_refname` (the `duckdb.row` reference):
  PostgreSQL's `quote_identifier`, which does not know DuckDB's words.
- `pgduckdb_relation_name`, `pgduckdb_db_and_schema_string`, `pgduckdb_function_name`
  (the same file): schema, relation and function names.
- The sidecar's view (`go/cmd/qs-mirror/extension.go`, `extViewSQL`) already quotes every
  name it writes (`extQuote`), so the mirror view is created on both builds; what the
  stock build cannot do is plan a statement over it.
- The test fixtures' own `duckdb.query` text is the test's: its names must be quoted too.

A limit that is not the keyword refusal: `quicksilver.allowlist` (the table described under the registry above) still
decides which constructs route. An operator outside the list, a CTE, and `date_trunc` on
`timestamp` are answered by the heap whatever the names are.

`sql/keywords.sql` routes a table of ordinary names with mixed case and an embedded
quote, and checks each keyword on its own, a keyword-named table, and `SELECT *`.

## Join aliases

The walk over a statement visits every expression. It used to visit the alias
list of each join too, and that list names every column of the join whether a
statement reads it or not. So a jsonb column that no expression read kept its
joins on the heap. The walk now skips the join alias lists. A column that is
read still decides: its Var is in the target list, a qual or a sort, and the
walk sees it there. `SELECT *`, a whole-row reference, and a NATURAL or USING
join that compares a jsonb column still fall back (`sql/joinalias.sql`).

## Lock order

A routed statement holds its heap relation's lock from the parser. Building the
mirror opens the mirror view, and the registry is read for every mirror, not
only the ones the statement names. A standby's startup process replays a DDL by
taking the view's lock and then the heap's, so a routed reader that waits for
the view while holding the heap, or for another table the replay holds, is one
side of a cycle. The other side is the startup process, and the cycle is
reported to the reader as "deadlock detected" (validate/real/hardening/cycle.py
reproduces it deterministically; before this change the routed reader got it).

The rule now, in the code as it is (qs_lock_query, qs_build_mirror,
qs_registry_lockable, qs_decide):

- **Registry tables.** The four tables of the registry (mirrors, allowlist,
  lakes, join domains) are locked with ConditionalLockRelationOid before any
  of them is read. A load that cannot lock them leaves the registry not
  installed (`qs_registry_blocked`) and the statement goes to the heap; the
  next statement loads again.
- **Building a mirror entry.** The build runs in a subtransaction with
  `lock_timeout` = 50 ms (`QS_BUILD_LOCK_TIMEOUT`, set with a GUC nest level
  of its own and ended with `AtEOXact_GUC`). A lock that was not available,
  a deadlock and a serialization failure roll the subtransaction back and make
  the entry transient ("mirror relation is locked by another session (not
  waited for)"); it is built again the next time it is asked for. A query
  cancel or a backend shutdown is rethrown, not classified. Any other error is
  about the mirror, as before. No subtransaction is opened inside a parallel
  operation, or while DuckDB may hold a transaction of this session's
  (`qs_duck_used`, cleared where the router ends DuckDB's transaction): pg_duckdb
  refuses a subtransaction then, and the entry is transient until a later call.
- **The mirror views in a plan.** A mirror subquery is stored with no view OID
  (`qs_build_mirror_body` clears `relid` of its RTE_SUBQUERY entries), so the
  plan-cache's range-table locks, which wait, are never taken on a view while
  the heap is held. The views are still dependencies of the plan: `qs_decide`
  adds each usable entry's view to `relationOids`, which the walk over the
  rewritten statement no longer finds.
- **Locking the view at planning.** Before the statement is substituted,
  `qs_decide` takes `ConditionalLockRelationOid(view, AccessShareLock)` on each
  usable mirror view the statement uses. The lock is held to the end of the
  transaction, so a DROP or a replacement of the view waits for a statement
  that reads it, as it did when the statement named the view. A view that
  another session holds makes the statement the heap's, with the reason
  "mirror relation is locked by another session (not waited for)"; nothing is
  remembered about the mirror.
- **Not locked at executor start, and not at a cached plan's reuse.** The
  executor takes no lock on a view when it starts: a fallback to the heap is
  not possible from there. A running routed statement does not need the view
  for its answer: the mirror's DuckDB text is built from the expanded view, so
  a replacement of the view in the middle of the statement does not change
  what it returns. A cached plan reused in a later transaction is not locked
  again: the plan is invalidated by `relationOids` (and by the invalidation
  items of the mirror's subquery) when a view changes, and planned again. The
  validation of this is in the section on the registry digest below.

A heap-only reader takes no lock on the view and makes no cycle.
