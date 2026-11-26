-- Prepared statements: the decision is made per execution, with the values.
-- Customer 1 owns 30% of public.orders (300,000 rows); customer 5 owns 70.
\set last 'SELECT side, reason FROM quicksilver.last_decision();'
SET search_path = public;
SET quicksilver.route = auto;

PREPARE p(int) AS SELECT count(*), sum(amount) FROM orders WHERE customer_id = $1;
EXECUTE p(1);
:last
EXECUTE p(5);
:last
-- past the fifth execution the plan cache would settle on a generic plan and
-- stop asking; the extension prices that plan out, so it keeps asking
EXECUTE p(1);
EXECUTE p(5);
EXECUTE p(1);
EXECUTE p(5);
EXECUTE p(5);
EXECUTE p(5);
:last
EXECUTE p(1);
:last
EXECUTE p(NULL);
:last
SELECT generic_plans, custom_plans FROM pg_prepared_statements WHERE name = 'p';

-- a generic plan has no values to judge by: heap, always
SET plan_cache_mode = force_generic_plan;
SELECT quicksilver.forget_last();
EXECUTE p(1);
-- (the cached generic plan is reused without planning, so there is no new decision)
:last
EXPLAIN (COSTS OFF) EXECUTE p(1);
SET plan_cache_mode = force_custom_plan;
EXECUTE p(1);
:last
RESET plan_cache_mode;

-- a one-row lookup is the same plan for every value: it is left alone
PREPARE pk(bigint) AS SELECT id, status FROM orders WHERE id = $1;
EXECUTE pk(1);
EXECUTE pk(2);
EXECUTE pk(3);
EXECUTE pk(4);
EXECUTE pk(5);
EXECUTE pk(6);
EXECUTE pk(7);
:last
SELECT generic_plans > 0 AS went_generic FROM pg_prepared_statements WHERE name = 'pk';

-- the extended protocol, unnamed statement
SELECT count(*), sum(amount) FROM orders WHERE customer_id = $1 \bind 1 \g
:last
SELECT count(*), sum(amount) FROM orders WHERE customer_id = $1 \bind 5 \g
:last
-- parameters of several types
SELECT count(*) FROM orders WHERE customer_id = $1 AND amount > $2 AND status = $3 AND created_at >= $4 \bind 1 '500.00' 'paid' '2024-01-02 00:00:00+00' \g
:last
SELECT count(*) FROM orders WHERE customer_id = $1 AND (note = $2 OR amount >= $3) \bind 1 'n-7' '999.00' \g
:last
SET quicksilver.route = heap;
SELECT count(*) FROM orders WHERE customer_id = $1 AND amount > $2 AND status = $3 AND created_at >= $4 \bind 1 '500.00' 'paid' '2024-01-02 00:00:00+00' \g
SELECT count(*) FROM orders WHERE customer_id = $1 AND (note = $2 OR amount >= $3) \bind 1 'n-7' '999.00' \g
SET quicksilver.route = auto;
-- an untyped parameter that PostgreSQL resolves to something not allowlisted
SELECT count(*) FROM orders WHERE note LIKE $1 \bind 'n-1%' \g
:last

-- a parameter's value reaches the mirror as that value (the mirror's rows say so)
SET client_min_messages = warning;
CREATE SCHEMA qt;
CREATE SCHEMA qtm;
CREATE TABLE qt.h (id int PRIMARY KEY, a int, b text);
INSERT INTO qt.h SELECT g, g * 10, 'heap' FROM generate_series(1, 4) g;
CREATE VIEW qtm.h AS SELECT r['id']::int AS id, r['a']::int AS a, r['b']::text AS b
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 10, 'mirror'), (2, 20, 'mirror'), (3, 30, 'it''s'), (4, 40, NULL)) t(id, a, b) $$) r;
SELECT quicksilver.register_mirror('qt.h', 'qtm.h', 1000000000000);
SELECT quicksilver.report_applied_lsn('qt.h', pg_current_wal_lsn());
SET quicksilver.route = mirror;
PREPARE m(int, text) AS SELECT id, a, b FROM qt.h WHERE a > $1 AND (b = $2 OR b IS NULL) ORDER BY id;
EXECUTE m(5, 'mirror');
EXECUTE m(25, 'it''s');
EXECUTE m(25, NULL);
EXECUTE m(-2147483648, 'a''; DROP TABLE qt.h; --');
:last
SELECT count(*) FROM qt.h;
DEALLOCATE ALL;
DROP SCHEMA qt, qtm CASCADE;
SELECT quicksilver.prune();
