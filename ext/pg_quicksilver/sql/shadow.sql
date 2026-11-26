-- shadow: decide, report, never act. The heap answers every statement.
\set last 'SELECT side, reason FROM quicksilver.last_decision();'
SET client_min_messages = warning;
SET search_path = public;
CREATE SCHEMA qt;
CREATE SCHEMA qtm;
CREATE TABLE qt.h (id int PRIMARY KEY, a int, b text);
INSERT INTO qt.h SELECT g, g * 10, 'heap' FROM generate_series(1, 3) g;
CREATE VIEW qtm.h AS SELECT r['id']::int AS id, r['a']::int AS a, r['b']::text AS b
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 10, 'mirror')) t(id, a, b) $$) r;
SELECT quicksilver.register_mirror('qt.h', 'qtm.h', 1000000000000);
SELECT quicksilver.report_applied_lsn('qt.h', pg_current_wal_lsn());
SELECT quicksilver.reset();

SET quicksilver.route = shadow;
SET quicksilver.min_cost = 0;
SELECT b, count(*) FROM qt.h GROUP BY b;
:last
EXPLAIN (COSTS OFF) SELECT b, count(*) FROM qt.h GROUP BY b;
SELECT a / 2 AS half, b FROM qt.h WHERE id = 1;
SELECT side, reason, detail, constructs FROM quicksilver.last_decision();
RESET quicksilver.min_cost;
SELECT b FROM qt.h WHERE id = 1;
:last
-- on the real table
SELECT count(*) FROM orders;
:last
SELECT count(*) FROM orders WHERE id / 2 = 5;
:last
-- a prepared statement is left to PostgreSQL's own plan cache in shadow mode
PREPARE p(int) AS SELECT count(*) FROM orders WHERE customer_id = $1;
EXECUTE p(1);
:last
EXECUTE p(5);
:last
DEALLOCATE p;

-- what was counted
SELECT side, reason, statements FROM quicksilver.reason_stats WHERE statements > 0 ORDER BY reason;
SELECT sum(mirror) AS routed, sum(heap) AS heap, count(*) AS distinct_statements FROM quicksilver.decision_stats;
SELECT quicksilver.reset();
SELECT count(*) FROM quicksilver.decision_stats;

-- the log line a harness can read
SET quicksilver.log_decisions = on;
SET client_min_messages = log;
SELECT a / 2 AS half, b FROM qt.h WHERE id = 1;
SET client_min_messages = warning;
RESET quicksilver.log_decisions;

DROP SCHEMA qt, qtm CASCADE;
SELECT quicksilver.prune();
