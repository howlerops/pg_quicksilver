-- When the mirror fails, and statements shaped to trip the extension up.
\set last 'SELECT side, reason, detail FROM quicksilver.last_decision();'
SET client_min_messages = warning;
SET search_path = public;
-- This file is about the mechanism, and its "must stay on the heap" statements
-- were written against the seed allowlist. The entries validate/widen.py has
-- added since (LIKE, window functions, UNION ALL, ...) are set aside while it
-- runs and put back at the end, so that it goes on testing the same thing
-- however long the list grows.
CREATE TABLE public.qs_regress_widened AS
  SELECT * FROM quicksilver.allowlist WHERE verified_by LIKE 'validate/widen.py%';
DELETE FROM quicksilver.allowlist WHERE verified_by LIKE 'validate/widen.py%';
CREATE SCHEMA qt;
CREATE SCHEMA qtm;
CREATE TABLE qt.h (id int PRIMARY KEY, a int, b text);
INSERT INTO qt.h SELECT g, CASE WHEN g = 3 THEN 2147483647 ELSE g * 10 END, 'heap' FROM generate_series(1, 3) g;
CREATE VIEW qtm.h AS SELECT r['id']::int AS id, r['a']::int AS a, r['b']::text AS b
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 10, 'mirror'), (2, 20, 'mirror'), (3, 2147483647, 'mirror')) t(id, a, b) $$) r;
SELECT quicksilver.register_mirror('qt.h', 'qtm.h', 1000000000000);
SELECT quicksilver.report_applied_lsn('qt.h', pg_current_wal_lsn());
SET quicksilver.route = mirror;
SET quicksilver.negative_cache_ttl = 0;
SET quicksilver.tolerate_lost_errors = on;     -- integer + is used below to make the mirror fail

-- ---------------------------------------------------------------- the mirror cannot plan it
-- a mirror whose file is gone (what a compaction does to a published view, docs/38)
CREATE TABLE qt.stale (id int PRIMARY KEY, b text);
INSERT INTO qt.stale VALUES (1, 'heap');
CREATE VIEW qtm.stale AS SELECT r['id']::int AS id, r['b']::text AS b
  FROM duckdb.query($$ SELECT 1 AS id, 'mirror' AS b $$) r;
SELECT quicksilver.register_mirror('qt.stale', 'qtm.stale', 1000000000000);
SELECT quicksilver.report_applied_lsn('qt.stale', pg_current_wal_lsn());
SELECT b FROM qt.stale ORDER BY id;
CREATE OR REPLACE VIEW qtm.stale AS SELECT r['id']::int AS id, r['b']::text AS b
  FROM duckdb.query($$ SELECT id, b FROM read_parquet('/nonexistent/qs/000001.parquet') $$) r;
SELECT b FROM qt.stale ORDER BY id;
SELECT side, reason, left(detail, 60) AS detail FROM quicksilver.last_decision();
-- remembered for negative_cache_ttl, so the mirror is not asked again each time
SET quicksilver.negative_cache_ttl = 60;
SELECT b FROM qt.stale ORDER BY id;
SELECT b FROM qt.stale ORDER BY id;
SELECT side, reason, right(detail, 12) AS detail_ends FROM quicksilver.last_decision();
-- republishing is DDL on the mirror, which forgets it
CREATE OR REPLACE VIEW qtm.stale AS SELECT r['id']::int AS id, r['b']::text AS b
  FROM duckdb.query($$ SELECT 1 AS id, 'mirror again' AS b $$) r;
SELECT b FROM qt.stale ORDER BY id;
SET quicksilver.negative_cache_ttl = 0;

-- ---------------------------------------------------------------- the mirror fails while executing
-- integer overflow on row 3: PostgreSQL's own error, from the heap
SELECT id, a + 1 FROM qt.h ORDER BY id;
:last
-- only the mirror fails (its third row), after two good rows: none of them was
-- sent, the heap answers all three
CREATE OR REPLACE VIEW qtm.h AS SELECT r['id']::int AS id, r['a']::int AS a, r['b']::text AS b
  FROM duckdb.query($$ SELECT id, CAST(a AS INTEGER) AS a, b FROM (VALUES (1, '10', 'mirror'), (2, '20', 'mirror'), (3, 'thirty', 'mirror')) t(id, a, b) $$) r;
SELECT id, a, b FROM qt.h ORDER BY id;
SELECT side, reason, left(detail, 100) AS detail FROM quicksilver.last_decision();
SELECT id, b FROM qt.h ORDER BY id;
-- EXPLAIN ANALYZE is the exception: pg_duckdb does not run the statement in the
-- executor then but inside DuckDB's own EXPLAIN ANALYZE, where nothing can be caught
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT id, a, b FROM qt.h WHERE id > 0;
SELECT mirror, heap, fallbacks FROM quicksilver.decision_stats WHERE fallbacks > 0 ORDER BY fallbacks DESC LIMIT 1;
-- with the fallback off the client sees the mirror's error
SET quicksilver.fallback = off;
SELECT id, a, b FROM qt.h ORDER BY id;
RESET quicksilver.fallback;
-- a cursor: the failure is met at the first FETCH
SET quicksilver.route_in_transaction = on;
BEGIN;
DECLARE c CURSOR FOR SELECT id, a, b FROM qt.h ORDER BY id;
FETCH 2 FROM c;
FETCH 2 FROM c;
COMMIT;
RESET quicksilver.route_in_transaction;
-- a result larger than work_mem is streamed: the first rows are still protected
SET work_mem = '64kB';
SELECT count(*), min(id), max(id) FROM (SELECT id, amount FROM orders WHERE amount > 900.00 ORDER BY id) q;
SELECT id FROM orders WHERE amount > 990.00 ORDER BY id LIMIT 3;
SELECT side, reason FROM quicksilver.last_decision();
RESET work_mem;
-- cancellation is not a reason to fall back
SET statement_timeout = '1ms';
SELECT count(*) FROM orders o JOIN orders p ON p.customer_id = o.customer_id WHERE o.amount > 1.00;
RESET statement_timeout;
SELECT count(*) FROM orders;

CREATE OR REPLACE VIEW qtm.h AS SELECT r['id']::int AS id, r['a']::int AS a, r['b']::text AS b
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 10, 'mirror'), (2, 20, 'mirror'), (3, 30, 'mirror')) t(id, a, b) $$) r;

-- ---------------------------------------------------------------- odd statements
SELECT FROM qt.h;
SELECT FROM qt.h WHERE id = 1;
SELECT count(*) FROM qt.h WHERE false;
SELECT 1 AS one FROM qt.h ORDER BY id;
SELECT b AS "select", b AS "a b", b AS "é", b AS " " FROM qt.h WHERE id = 1;
SELECT id AS x, id AS x, a AS x FROM qt.h WHERE id = 1;
SELECT x.id, y.id, z.id FROM qt.h x, qt.h y, qt.h z WHERE x.id = y.id AND y.id = z.id AND z.id = 1;
SELECT b FROM qt.h WHERE id = 1 AND (((((((((((id = 1)))))))))));
SELECT b FROM qt.h WHERE id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30) ORDER BY id;
SELECT b FROM qt.h WHERE id = 1 AND NULL;
SELECT b FROM qt.h WHERE id = NULL;
SELECT NULL AS n, b FROM qt.h WHERE id = 1;
SELECT NULL::int AS n, b, true AS t, 'x' AS s, 1.5 AS d, 12345678901 AS big FROM qt.h WHERE id = 1;
SELECT count(*) FROM (SELECT 1 FROM (SELECT id FROM (SELECT id FROM qt.h) q1) q2) q3;
SELECT q.b FROM (SELECT b, id FROM qt.h ORDER BY id LIMIT 2) q ORDER BY q.id;
SELECT b FROM qt.h h WHERE h.id = (SELECT 1);
SELECT (SELECT b FROM qt.h WHERE id = 1);
SELECT b FROM (VALUES (1)) v(x) JOIN qt.h ON id = x;
TABLE qt.h;
SELECT * FROM qt.h NATURAL JOIN qt.h x ORDER BY id;
SELECT * FROM qt.h JOIN qt.h x USING (id) ORDER BY id;
SELECT h.* FROM qt.h h JOIN qt.h x USING (id, a, b) ORDER BY id;
SELECT b FROM qt.h GROUP BY b HAVING count(*) > 100;
SELECT b FROM qt.h GROUP BY GROUPING SETS ((b), ());
SELECT b FROM qt.h ORDER BY id OFFSET 1 ROWS FETCH FIRST 1 ROW WITH TIES;
WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT b FROM qt.h JOIN r ON r.n = id ORDER BY id;
SELECT b FROM qt.h WHERE id = ANY (ARRAY[1, 2]) ORDER BY id;
SELECT b FROM qt.h WHERE id = ANY ('{1,2}'::int[]) ORDER BY id;
SELECT b FROM qt.h WHERE (id, a) = (1, 10);
SELECT b FROM qt.h WHERE ROW(id, a) < ROW(2, 0) ORDER BY id;
SELECT b, a FROM qt.h WHERE a BETWEEN SYMMETRIC 20 AND 10;
SELECT b FROM qt.h WHERE b IS NOT DISTINCT FROM 'mirror' AND id = 1;
SELECT nullif(b, 'mirror'), greatest(id, a), least(id, a) FROM qt.h WHERE id = 1;
SELECT b FROM qt.h WHERE id = 1 AND b = ANY (SELECT b FROM qt.h);
SELECT b FROM qt.h FOR SHARE SKIP LOCKED;
SELECT b FROM qt.h WHERE id = $1 \bind 1 \g
SELECT b FROM qt.h WHERE id = $1 AND $2::int IS NOT NULL \bind 1 2 \g
EXPLAIN (COSTS OFF, FORMAT JSON) SELECT b FROM qt.h WHERE id = 1 FOR UPDATE;
EXPLAIN (COSTS OFF, GENERIC_PLAN) SELECT b FROM qt.h WHERE id = $1;

-- forced parallelism on the heap side of a routed plan
SET debug_parallel_query = on;
SELECT b, count(*) FROM qt.h GROUP BY b;
SELECT count(*) FROM orders;
RESET debug_parallel_query;
-- and with JIT, and a tiny work_mem
SET jit = on;
SET jit_above_cost = 0;
SELECT b, count(*), sum(a) FROM qt.h GROUP BY b;
RESET jit;
RESET jit_above_cost;

-- misuse of the extension's own functions
SELECT quicksilver.report_applied_lsn(NULL, NULL);
SELECT quicksilver.register_mirror('qt.h', 'qt.h', 0);
SELECT usable, reason FROM quicksilver.mirror_status() WHERE heap = 'qt.h'::regclass;
SELECT b FROM qt.h WHERE id = 1;
SELECT quicksilver.register_mirror('qt.h', 'pg_class', 0);
SELECT b FROM qt.h WHERE id = 1;
SELECT quicksilver.register_mirror('pg_class', 'qtm.h', 0);
SELECT usable, reason FROM quicksilver.mirror_status() WHERE heap = 'pg_class'::regclass;
SELECT count(*) > 0 FROM pg_class;
DELETE FROM quicksilver.mirrors WHERE heap = 'pg_class'::regclass;
SELECT * FROM quicksilver.unsafe_constructs(NULL);
SELECT * FROM quicksilver.unsafe_constructs('');
SELECT * FROM quicksilver.unsafe_constructs('SELECT * FROM no_such_table');
SELECT * FROM quicksilver.unsafe_constructs('SELEC');
SELECT * FROM quicksilver.unsafe_constructs('VACUUM');
SELECT quicksilver._changed();
SET quicksilver.route = 'sideways';
SET quicksilver.min_cost = -1;
SELECT quicksilver.register_mirror('qt.h', 'qtm.h', 1000000000000);
SELECT b FROM qt.h WHERE id = 1;
BEGIN;
TRUNCATE quicksilver.mirrors;
SELECT count(*) FROM quicksilver.mirror_status();
SELECT b FROM qt.h WHERE id = 1;
ROLLBACK;
SELECT b FROM qt.h WHERE id = 1;
SELECT count(*) > 3 FROM quicksilver.mirror_status();

DROP SCHEMA qt, qtm CASCADE;
SELECT quicksilver.prune();
INSERT INTO quicksilver.allowlist SELECT * FROM public.qs_regress_widened;
DROP TABLE public.qs_regress_widened;
