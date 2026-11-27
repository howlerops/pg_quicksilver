-- date_trunc(text, timestamptz) on the mirror (validate/tz_check.py, docs/40).
--
-- A routed date_trunc needs three things: the unit is a string constant in the
-- proven set (case-insensitively); the session's TimeZone is one the zone table
-- admits, and for hour one whose entry says so; and DuckDB's TimeZone is that zone
-- when the mirror runs, read back before it does. Every answer is the heap's.
-- Every heap decision names its reason. Each comparison runs the same statement
-- routed and on the heap, and prints the result, so the expected output holds both.
\set last 'SELECT side, reason, left(detail, 160) AS detail FROM quicksilver.last_decision();'
SET search_path = public;
SET client_min_messages = warning;
SELECT quicksilver.forget_last();
SET quicksilver.route = auto;
SET quicksilver.min_cost = 0;
SET quicksilver.min_rows = 0;

-- 0. a POSIX-style zone is not in the table, whatever its offset. (First in the
--    file: once DuckDB is up in this backend, pg_duckdb's own hook refuses a zone
--    DuckDB does not know, and the SET itself fails.)
SET TimeZone = '+03';
SELECT count(*) FROM (SELECT date_trunc('day', created_at) AS d FROM orders GROUP BY 1) x;
:last
SET TimeZone = 'UTC';

-- 1. the units, in UTC: routed, and the answer is the heap's
SELECT date_trunc('month', created_at) AS m, sum(amount) AS s FROM orders GROUP BY 1 ORDER BY 1;
:last
SET quicksilver.route = heap;
SELECT date_trunc('month', created_at) AS m, sum(amount) AS s FROM orders GROUP BY 1 ORDER BY 1;
SET quicksilver.route = auto;

-- the unit is case-insensitive, as PostgreSQL's is
SELECT date_trunc('MONTH', created_at) AS m, count(*) FROM orders GROUP BY 1 ORDER BY 1 LIMIT 2;
:last
-- a unit PostgreSQL does not accept is an error on either side, and the heap's
SELECT count(*) FROM (SELECT date_trunc(' month', created_at) AS m FROM orders GROUP BY 1) x;
:last

-- 2. the zones: a proven unit in New York is routed; hour is not proven there
SET TimeZone = 'America/New_York';
SELECT date_trunc('day', created_at) AS d, count(*) FROM orders GROUP BY 1 ORDER BY 1 LIMIT 3;
:last
SELECT date_trunc('hour', created_at) AS h, count(*) FROM orders GROUP BY 1 ORDER BY 1 LIMIT 3;
:last
SET quicksilver.route = heap;
SELECT date_trunc('hour', created_at) AS h, count(*) FROM orders GROUP BY 1 ORDER BY 1 LIMIT 3;
SET quicksilver.route = auto;

-- 3. the heap, with the reason: a century (not proven), a unit that is not a constant
SET TimeZone = 'UTC';
SELECT date_trunc('century', created_at) AS c, count(*) FROM orders GROUP BY 1 ORDER BY 1;
:last
SELECT date_trunc(CASE WHEN id % 2 = 0 THEN 'day' ELSE 'month' END, created_at) AS d, count(*)
  FROM orders GROUP BY 1 ORDER BY 1 LIMIT 2;
:last

-- 4. infinity and NULL, on the exactness table (ex.t_time: rows 9003 and 9004 are infinite)
SET search_path = ex, public;
SELECT id, date_trunc('day', tz) AS d, date_trunc('decade', tz) AS dec
  FROM t_time WHERE id IN (9003, 9004) OR tz IS NULL OR id <= 3 ORDER BY id;
:last
SELECT id, date_trunc('quarter', tz) AS q FROM t_time WHERE id IN (9003, 9004) OR tz IS NULL OR id <= 3 ORDER BY id;
:last
SET quicksilver.route = heap;
SELECT id, date_trunc('quarter', tz) AS q FROM t_time WHERE id IN (9003, 9004) OR tz IS NULL OR id <= 3 ORDER BY id;
SET quicksilver.route = auto;
SELECT date_trunc('quarter', tz) AS q, count(*) FROM t_time GROUP BY 1 ORDER BY 1 LIMIT 4;
:last
SET quicksilver.route = heap;
SELECT date_trunc('quarter', tz) AS q, count(*) FROM t_time GROUP BY 1 ORDER BY 1 LIMIT 4;
SET quicksilver.route = auto;
SET search_path = public;

-- 5. a cached plan made under UTC, run under New York: the plan is reused, so the
--    zone is checked again when it runs. No parameter: a generic plan with an unknown
--    parameter is never routed (the planner says so), a plan without one is reused
SET plan_cache_mode = force_generic_plan;
SET TimeZone = 'UTC';
SET search_path = ex, public;
-- EXPLAIN ANALYZE shows which side each execution took: a reused routed plan records
-- no decision when it runs, so the plan's own line says it
PREPARE dt_p AS SELECT date_trunc('hour', tz) AS h, count(*) FROM t_time WHERE id < 100 AND tz IS NOT NULL GROUP BY 1 ORDER BY 1 LIMIT 3;
EXECUTE dt_p;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) EXECUTE dt_p;
SET TimeZone = 'America/New_York';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) EXECUTE dt_p;
SET TimeZone = 'UTC';
EXECUTE dt_p;
DEALLOCATE dt_p;
RESET plan_cache_mode;
SET search_path = public;

-- 6. the zone is restored: a failed statement in a block leaves no zone behind, in
--    PostgreSQL or in DuckDB; a SET LOCAL is undone the same way
SET TimeZone = 'UTC';
BEGIN;
SET LOCAL TimeZone = 'America/New_York';
SELECT date_trunc('day', created_at) AS d, count(*) FROM orders GROUP BY 1 ORDER BY 1 LIMIT 2;
:last
SELECT 1 / 0;
ROLLBACK;
SHOW TimeZone;
SELECT * FROM duckdb.query('SELECT current_setting(''TimeZone'') AS z');
SET TimeZone = 'America/New_York';
SELECT date_trunc('month', created_at) AS m, count(*) FROM orders GROUP BY 1 ORDER BY 1 LIMIT 2;
:last
SELECT * FROM duckdb.query('SELECT current_setting(''TimeZone'') AS z');
SET TimeZone = 'UTC';

-- 7. an ordinary role, on the confined build (the stock build keeps it on the heap)
SELECT coalesce(current_setting('duckdb.confined_file_access', true), 'off') = 'on' AS confined \gset
\if :confined
CREATE ROLE qs_tz LOGIN;
GRANT SELECT ON public.orders, public.customers TO qs_tz;
GRANT USAGE ON SCHEMA ex TO qs_tz;
GRANT SELECT ON ex.t_time TO qs_tz;
\c app qs_tz
SET quicksilver.route = auto;
SET quicksilver.min_cost = 0;
SET quicksilver.min_rows = 0;
SET TimeZone = 'Europe/London';
SELECT date_trunc('month', created_at) AS m, sum(amount) AS s FROM orders GROUP BY 1 ORDER BY 1 LIMIT 3;
:last
SET TimeZone = 'America/New_York';
SELECT date_trunc('hour', created_at) AS h, count(*) FROM orders GROUP BY 1 ORDER BY 1 LIMIT 2;
:last
\c app postgres
DROP OWNED BY qs_tz;
DROP ROLE qs_tz;
\else
SELECT 'stock build: an ordinary role is not routed (see unprivileged_confined)' AS note;
\endif
