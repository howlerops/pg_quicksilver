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
| A transaction block was always the heap's: the proxy's own probe broke pg_duckdb's state. | Still the heap's by default, for one remaining reason (below), and a setting for clients it does not apply to. |
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
   `COPY`; in a transaction block (see below), a savepoint, or anything but
   READ COMMITTED; for a scrollable or holdable cursor: heap.
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
- *Invalidation.* The plan depends on the heap table, on the mirror relation,
  and on the two tables above. Change any of them and PostgreSQL's own plan
  cache discards it, on the primary and on every standby.
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
so it is checked there. So is the session's state: a plan made outside a
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
cache that the plan depended on the view. It does now (above), and the plan
holds a lock on the mirror relation whenever it runs. The regression test that
found it is `invalidation.sql`.

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
`validate/cassert.sh` runs all of it; `debug_assertions` is `on`.

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

## Clients

| | |
|---|---|
| `psql` 17.12 | The nine pg_regress files: simple queries, `\bind` (extended protocol), `PREPARE`/`EXECUTE`, cursors, transactions, `COPY`. |
| pgx 5.11 (`go/cmd/qs-extcheck`, 4 tests) | All five exec modes, seven rounds each: the unselective parameter is answered by the mirror, the selective one, the lookup and the division by the heap, all equal to a heap-only connection. RowDescription identical field by field in every mode, table OID and attribute number included, text and binary. A transaction with a nested (savepoint) transaction. A batch. |
| psycopg 3.3.9, libpq 18 (`validate/psycopg_check.py`, 16 checks) | Autocommit: simple and extended protocol routed; a named prepared statement alternating two values twelve times goes mirror, heap, mirror, heap; binary results; `cursor.description`; pipeline mode; `COPY`; cancel mid-statement. Not autocommit: see below. |

**psycopg without autocommit is not routed by default**, and neither is any
other statement in a transaction block. pg_duckdb aborts a transaction that
takes a `SAVEPOINT` after it has run a query, and a nested transaction in every
driver is a `SAVEPOINT`. The extension cannot know one is coming. So: heap,
and correct. `quicksilver.route_in_transaction = on` routes inside a READ
COMMITTED block with no savepoint open, for a client that takes none; the
psycopg check then gets the mirror's answer, a server-side cursor's rows from
the mirror, and, on purpose, the error a later savepoint gets. Such a
transaction also does not see its own writes in the mirror.

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

## What is still not routed, and why

- **Most SQL.** Division, `avg`, float and double, `varchar(n)`, `char`,
  `interval`, `time`, json, arrays, enums, `bytea`, `inet`; every function
  call; `LIKE` and regular expressions; window functions; `DISTINCT`, `OFFSET`,
  CTEs, set operations, sublinks, right and full joins; `LIMIT` without `ORDER
  BY`. None of it is known to be wrong. None of it has been shown to be right.
- **Integer `+ - *`** unless the session accepts a lost overflow error.
- **Text order** under any collation but `C`.
- **Date and timestamp columns** that nobody has asserted to be finite.
- **Transaction blocks** by default; savepoints, REPEATABLE READ and
  SERIALIZABLE always.
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

## Running it

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
CA=/path/to/cassert/prefix validate/cassert.sh
validate/teardown.sh
```
