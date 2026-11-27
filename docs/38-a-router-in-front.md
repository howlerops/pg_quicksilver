# 38 — A router in front

> **Superseded by [40 — The router moves inside](40-the-router-moves-inside.md).**
> The open problem this document ends on (a proxy cannot know which statements
> DuckDB answers as PostgreSQL does) is addressed there by a PostgreSQL
> extension that decides with the typed query tree and routes only what is on a
> verified allowlist. `go/cmd/qs-router` is kept as the record of what a proxy
> can and cannot do; what follows is unchanged.

An experiment: one endpoint, and each statement goes to the heap or to the
mirror on its own. It works, it is in [`go/cmd/qs-router`](../go/cmd/qs-router),
and **it should not be shipped as it is**. The routing is sound. What it routes
*to* is a second SQL dialect, and the router can only refuse the differences
somebody has already found.

Everything below was run against PostgreSQL 17.12 with pg_duckdb 1.2.0 (the
stock package, superuser only, not the confined build in `bench/patches`), a
1,000,000-row `orders` table, and a real mirror built by the sidecar and
published with `qs-query -engine postgres`. The scripts are in
[`go/cmd/qs-router/validate`](../go/cmd/qs-router/validate).

## What it does

`qs-router` speaks the PostgreSQL wire protocol on both sides and has no SQL
parser. The mirror's views live in a schema of their own (`qs_mirror`) under
the heap tables' names, so "send it to the mirror" is: put `qs_mirror` in front
of `search_path`, forward the client's bytes unchanged, take it out again.

For a statement that starts like a read, outside a transaction block:

1. **Ask PostgreSQL.** `EXPLAIN (VERBOSE, FORMAT JSON)` of the statement against
   the heap, with the client's own parameter values bound, in one round trip.
   Never executed.
2. **Decide** ([`decide.go`](../go/cmd/qs-router/decide.go)). Mirror only if
   every relation in the plan is a mirrored table, every plan node is on a
   whitelist of nodes that read, and either a scan of a mirrored table is
   expected to read more than `-row-threshold` rows or the total cost is over
   `-cost-threshold`.
3. **Check it there.** Before a "mirror" decision is believed, the statement is
   described and planned once with the mirror schema in front. DuckDB has to
   accept it, the plan has to be pg_duckdb's and read no heap table, and the
   result columns have to have the types the heap gives them.
4. **Remember.** By exact text and parameter values, for `-cache-ttl`; or by
   shape (literals removed) when the answer cannot depend on them: a
   unique-index lookup of one row, or a statement the mirror can never take.
5. **Gate.** The switch to the mirror returns the server's WAL position in the
   same row; if the mirror's `applied_lsn` (the sidecar's `/readyz`, or a
   `state.json`) is more than `-max-lag-bytes` behind it, or cannot be read, the
   statement goes to the heap.

Everything else is forwarded untouched. `SET qs.route = 'heap' | 'mirror' |
'auto'` overrides per session; `SHOW qs.last_route` says where the previous
statement went and why.

## Where the design was wrong

**"Sequential scan with an estimated row count above a threshold."** Taken
literally this sends `SELECT * FROM orders LIMIT 10` to the mirror: the Seq
Scan under the Limit is labelled with the whole table. The row count is scaled
by every Limit above the scan that no Sort, Aggregate or Hash stands in front
of.

**"A transaction stays on one side once it has read."** Built, then removed.
A transaction block is always the heap's, because of what pg_duckdb did under
it, each reproduced in `psql` with no router involved:

- `SAVEPOINT` in a transaction that has run a DuckDB query aborts it
  (`SAVEPOINT is not supported in DuckDB`). That is every nested transaction pgx
  and psycopg open.
- After an `EXPLAIN` of a heap query, a `set_config()`, or statements sent
  through the extended protocol, the next DuckDB query in the same transaction
  fails with `DuckDB execution is not supported inside functions`.

The router's own probe is an `EXPLAIN` through the extended protocol. It could
not look at a transaction without breaking it. The consequence is not small:
**psycopg opens a transaction for every statement unless `autocommit=True`**,
and such a client never reaches the mirror.

**"Routing without rewriting SQL."** This is the one that does not have a fix
here. pg_duckdb runs the *whole statement* in DuckDB, and DuckDB is not
PostgreSQL:

| statement | heap | mirror |
|---|---|---|
| `SELECT 7/2` | `3` | `3.5` |
| `WHERE id/2 = 5` | ids 10 and 11 | id 10 |
| `avg(amount)` over `numeric(12,2)` | `499.9950000000000000`, numeric | `499.995`, computed as a double |
| `pg_typeof(sum(amount))` | `numeric` | `decimal(38,2)` |
| `current_setting('search_path')` | `"$user", public` | empty |
| `ORDER BY x DESC` over NULLs | NULLs first | NULLs last ([docs/20](20-serving-the-mirror.md)) |

The router keeps each of these on the heap: `/`, `ORDER BY` (unless
`-allow-order-by`) and the words in `-heap-words` are refused by text, and a
statement whose result *types* differ between the sides is refused by the
check in step 3. That is a list of what was found in an afternoon. A statement
with a difference nobody has found yet is answered by DuckDB, with no error,
and the same statement inside a transaction is answered by PostgreSQL. A
cluster-wide mode flag at least gives every statement the same dialect.

**The view goes stale.** Not a routing problem, found because of routing. The
view `qs-query` publishes names the mirror's files. Thirty idle seconds after a
write (`QS_COMPACT_IDLE_SECONDS`) the sidecar compacts, the files are replaced,
and the view fails (`No files found that match the pattern
…/base/000001.parquet`) until `qs-query` is run again. One `UPDATE` in the
routing-table script was enough, and it broke three validation runs before
`validate/publish.sh` learned to wait for the compaction. The router turns it
into a fallback: a statement that fails on the mirror before any result has
been sent is run again on the heap, and its decision is cached as heap. Nothing
errors, the mirror is simply not used until someone republishes, and for up to
`-cache-ttl` after that.

## The routing table

`validate/routing_table.py`. *side* and *why* are the router's own report;
*= heap* compares its answer with the heap's; the two timings are the same
statement sent straight to PostgreSQL with each `search_path`, median of 5, to
show whether the choice was the right one. One host, 4 vCPU, everything in
memory: read the timings as ratios.

| # | statement | side | why | = heap | heap ms | mirror ms |
|---|---|---|---|---|---|---|
| 1 | pk lookup: `SELECT * FROM orders WHERE id = 4242` | heap | unique-index lookup of at most one row | yes | 0.28 | 26 |
| 2 | IN-list on the pk: `SELECT * FROM orders WHERE id IN (1, 2, 3, 500000)` | heap | Index Scan on orders reads an estimated 4 rows, cost 22: under both thresholds | yes | 0.38 | 30 |
| 3 | small range on an index: `SELECT * FROM orders WHERE created_at >= '2024-01-05' AND created_a...` | heap | Index Scan on orders reads an estimated 568 rows, cost 29: under both thresholds | yes | 1.20 | 19 |
| 4 | count(*): `SELECT count(*) FROM orders` | mirror | Index Only Scan on orders reads an estimated 416667 rows, over 50000 | yes | 31 | 9.11 |
| 5 | group by: `SELECT status, count(*), sum(amount) FROM orders GROUP BY status` | mirror | Seq Scan on orders reads an estimated 416667 rows, over 50000 | yes | 102 | 44 |
| 6 | range aggregate: `SELECT sum(amount) FROM orders WHERE created_at >= '2024-01-03' AND...` | mirror | Seq Scan on orders reads an estimated 217322 rows, over 50000 | yes | 56 | 45 |
| 7 | join + group by: `SELECT c.region, sum(o.amount) FROM orders o JOIN customers c ON c....` | mirror | Seq Scan on orders reads an estimated 416667 rows, over 50000 | yes | 128 | 37 |
| 8 | LIMIT without ORDER BY: `SELECT * FROM orders LIMIT 10` | heap | Seq Scan on orders reads an estimated 10 rows, cost 0: under both thresholds | - | - | - |
| 9 | 10k-row result: `SELECT id, amount FROM orders WHERE amount > 990` | mirror | total cost 16754, over 10000 | yes | 51 | 58 |
| 10 | prepared, selective parameter: `SELECT count(*), sum(amount) FROM orders WHERE customer_id = $1` | heap | Bitmap Heap Scan on orders reads an estimated 72 rows, cost 275: under both thresholds | yes | 0.19 | 24 |
| 11 | prepared, unselective parameter: `SELECT count(*), sum(amount) FROM orders WHERE customer_id = $1` | mirror | Bitmap Heap Scan on orders reads an estimated 125930 rows, over 50000 | yes | 55 | 40 |
| 12 | top-N with ORDER BY: `SELECT id, amount FROM orders ORDER BY amount DESC, id LIMIT 10` | heap | ORDER BY, which DuckDB sorts NULLs and text differently for | yes | 75 | 56 |
| 13 | integer division: `SELECT count(*) FROM orders WHERE id/2 = 5` | heap | division, which DuckDB does not truncate for integers | yes | 39 | 35 |
| 14 | function call: `SELECT order_total(5)` | heap | reads no table | yes | 0.06 | 0.11 |
| 15 | PostgreSQL function over the table: `SELECT order_total(5), count(*) FROM orders` | heap | the mirror cannot plan it: (PGDuckDB/CreatePlan) Prepared query returned an error: Catalog Error: Scalar Funct | yes | 40 | - (fails) |
| 16 | schema-qualified select: `SELECT count(*) FROM public.orders` | heap | names schema public itself | yes | 39 | 34 |
| 17 | join with an unmirrored table: `SELECT count(*) FROM orders o JOIN audit_log a ON a.id = o.id` | heap | reads public.audit_log, which is not mirrored | yes | 0.74 | 30 |
| 18 | view over a mirrored table: `SELECT count(*) FROM orders_v` | heap | the mirror schema did not move it off the heap (Aggregate) | yes | 31 | 37 |
| 19 | SELECT FOR UPDATE: `SELECT id FROM orders WHERE id = 7 FOR UPDATE` | heap | plan node LockRows | - | - | - |
| 20 | CTE with a write: `WITH w AS (UPDATE orders SET note = note WHERE id = 4242 RETURNING ...` | heap | plan node ModifyTable | - | - | - |
| 21 | INSERT: `INSERT INTO audit_log(what) VALUES ('rt')` | heap | not a plain read (INSERT) | - | - | - |
| 22 | UPDATE: `UPDATE orders SET note = note WHERE id = 4242` | heap | not a plain read (UPDATE) | - | - | - |
| 23 | SELECT ... INTO: `SELECT id INTO TEMP rt_tmp FROM orders WHERE id < 5` | heap | contains INTO, which may create a table | - | - | - |
| 24 | two statements in one Query: `SELECT count(*) FROM orders; SELECT 1` | heap | more than one statement | - | - | - |
| 25 | count(*) inside BEGIN ... COMMIT: `SELECT count(*) FROM orders` | heap | inside a transaction block | - | - | - |
| 26 | pk lookup under SET qs.route = 'mirror': `SELECT * FROM orders WHERE id = 4242` | mirror | qs.route=mirror | yes | - | - |
| 27 | INSERT under SET qs.route = 'mirror': `INSERT INTO audit_log(what) VALUES ('rt')` | heap | not a plain read (INSERT) | - | - | - |
| 28 | count(*) under SET qs.route = 'heap': `SELECT count(*) FROM orders` | heap | qs.route=heap | - | - | - |

`validate/side_proof.sh` proves the *side* column is true rather than
reported: with the sidecar stopped and one extra order inserted, the heap has
1,000,001 rows and the mirror 1,000,000, and every count can only have come
from one side. A single named prepared statement alternates between them with
its parameter (300,000 from the mirror for customer 1, 70 from the heap for
customer 5, three rounds), which works because PostgreSQL re-analyses a cached
statement when `search_path` changes. With the gate at 64 bytes the same
`count(*)` returns 1,000,001: `heap: mirror is 35113 WAL bytes behind, over 64`.

## What it costs

`validate/latency.sh`: a primary-key lookup, 5,000 executions per row, one
client, pgbench on CPU 2 (`taskset -c 2`), three interleaved repetitions
because this host's own median moves by a factor of three between minutes.
Microseconds.

| | median | p95 |
|---|---|---|
| direct, prepared | 15 to 38 | 22 to 64 |
| router, warm decision cache, prepared | 55 to 58 | 84 to 108 |
| router, no cache, prepared | 237 to 274 | 420 to 556 |
| direct, simple protocol | 22 to 30 | 36 to 80 |
| router, warm decision cache, simple protocol | 97 to 116 | 138 to 162 |
| router, no cache, simple protocol | 254 to 269 | 409 to 448 |

Those are with the router pinned to the same CPU as pgbench, the steadier of
the two runs. With the router left unpinned: prepared 17 to 50 direct, 89 to
102 warm, 257 to 280 without the cache (p95 54 to 69, 141 to 152, 462 to 551);
simple 25 to 70, 118 to 145, 228 to 269 (p95 49 to 105, 205 to 283, 365 to
591). So a warm router adds roughly 40 to 90 microseconds to a statement that
takes 15 to 30, and an unwarmed one adds 200 to 250.

The router's own work on a point lookup is 1.7 microseconds of tokenising
(`BenchmarkAnalysePointLookup`); the rest is two more socket hops and the
wake-ups that go with them. Why the simple protocol pays about 40 more than the
extended one through the router, when it pays about 10 more direct, was not
found.

"No cache" is `-cache-ttl 0`: every statement is planned twice, once by the
router's `EXPLAIN` and once for real. With the cache a point lookup is
recognised by its shape and costs two more socket hops and nothing else.

## Clients

| | through the router |
|---|---|
| `psql` 17.12 (`psql_check.sh`, 11 checks) | simple and extended (`\bind`) queries, SQL `PREPARE`/`EXECUTE`, a transaction, `\copy` out and in, a multi-statement query, `sslmode=prefer`, cancel by SIGINT |
| pgx 5.11 (`pgx_integration_test.go`, run with `-race`) | all five query exec modes (named statements, unnamed statement kept across two round trips, simple protocol), a transaction with a savepoint, `CopyTo`/`CopyFrom` including one that fails half-way, `CancelRequest`, a batch |
| psycopg 3.3.9 on libpq 18 (`psycopg_check.py`, 15 checks) | unprepared and named prepared statements, binary results, COPY out and in, COPY through the extended protocol, cancel, pipeline mode, the default transaction-per-statement mode |

## What it does not support

All of these fail closed: the statement goes to the heap as the client sent
it, or the connection is refused.

- **TLS.** `SSLRequest` and `GSSENCRequest` are answered `N`. `sslmode=require`
  cannot connect. Authentication is relayed in the clear and the router holds
  no credential; `trust`, SCRAM-SHA-256 and md5 were exercised, each with a right and a wrong password.
- **Transaction blocks**: heap, always (above).
- **Flush before Sync** (libpq pipeline mode, so psycopg's `pipeline()`): the
  session becomes a plain pass-through to the heap until the server has answered
  every Sync, and statements prepared during it are forgotten.
- **Not planned at all**: more than one statement in a Query; SQL `EXECUTE`,
  `DECLARE`/`FETCH`, `COPY (SELECT …) TO`, `EXPLAIN`, `CALL`, `DO`; anything
  with `INTO`; anything naming `public.` or `qs_mirror.` itself; a session with
  `standard_conforming_strings = off`.
- **Replication connections**: copied through byte for byte, never routed.
- **Function-call messages**: heap.
- **NOTIFY and parameter changes while the client is idle** arrive with the
  next statement's answer, not when they happen.
- **`SHOW qs.last_route`** is answered only as a simple Query. **`SET
  qs.route`** is observed on its way to the server, so it is not transactional:
  a rolled-back `SET` still holds in the router. An unknown value means `heap`.
- **Cancel** is forwarded to the server as received. One arriving while the
  router is planning cancels the plan and the client gets `57014` for its
  statement. A cancel during a statement on the mirror was not tested.
- **TCP only**, one server, no pooling: a client connection is a server
  connection.

## Where it routes wrongly

Slower, not wrong, unless it says otherwise:

- **Wrong answers** are possible wherever DuckDB and PostgreSQL disagree and
  nobody has listed it (above). This is the open problem.
- **Not your own writes.** The mirror is asynchronous. Any bound above zero
  lets a session insert a row and then count on the mirror without it; a bound
  of zero sends everything to the heap on a busy server.
- **Estimates.** The decision is the planner's guess. A merge join that stops
  early is counted in full.
- **Shape cache.** `WHERE id = 1` and `WHERE id = 1 OR true` have one shape; the
  second stays on the heap until the entry expires.
- **A new connection** plans its first read even if the answer is cached,
  because the router does not yet know its `search_path`.
- **A stale view** costs one failed attempt per cached statement, then the
  heap, silently, until republished.
- **A named statement created and first executed in one batch** is not retried
  if the mirror fails it; the client sees that error once.
- **The thresholds** (50,000 rows, cost 10,000) are where the mirror started to
  win on this host at 1M rows. At 10M the mirror's floor was measured at
  240–440 ms; they are not constants.

## Running it

```
validate/setup.sh          # primary 5491, standby 5492, sidecar, mirror, views
validate/router.sh start -mirror-lsn http://127.0.0.1:5493/readyz -log-routes
python3 validate/routing_table.py
QS_ROUTER_DSN='host=127.0.0.1 port=5490 dbname=app user=postgres' go test -race ./cmd/qs-router/
validate/teardown.sh
```
