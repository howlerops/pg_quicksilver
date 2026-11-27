-- The opening of sql/robustness.sql up to its first forced-mirror SELECT, then that
-- SELECT and the decision it left. One fresh backend per iteration (race_loop.sh),
-- as each regression file gets. Cleans up after itself.
SET client_min_messages = warning;
SET search_path = public;
CREATE SCHEMA qt;
CREATE SCHEMA qtm;
CREATE TABLE qt.h (id int PRIMARY KEY, a int, b text);
INSERT INTO qt.h SELECT g, g * 10, 'heap' FROM generate_series(1, 3) g;
CREATE VIEW qtm.h AS SELECT r['id']::int AS id, r['a']::int AS a, r['b']::text AS b
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 10, 'mirror'), (2, 20, 'mirror'), (3, 30, 'mirror')) t(id, a, b) $$) r;
SELECT quicksilver.register_mirror('qt.h', 'qtm.h', 1000000000000);
SELECT quicksilver.report_applied_lsn('qt.h', pg_current_wal_lsn());
SET quicksilver.route = mirror;
SET quicksilver.negative_cache_ttl = 0;
SET quicksilver.tolerate_lost_errors = on;
CREATE TABLE qt.stale (id int PRIMARY KEY, b text);
INSERT INTO qt.stale VALUES (1, 'heap');
CREATE VIEW qtm.stale AS SELECT r['id']::int AS id, r['b']::text AS b
  FROM duckdb.query($$ SELECT 1 AS id, 'mirror' AS b $$) r;
SELECT quicksilver.register_mirror('qt.stale', 'qtm.stale', 1000000000000);
SELECT quicksilver.report_applied_lsn('qt.stale', pg_current_wal_lsn());
\echo ANSWER
SELECT b FROM qt.stale ORDER BY id;
\echo DECISION
SELECT side, reason, replace(left(coalesce(detail, ''), 200), E'\n', ' ') FROM quicksilver.last_decision();
DROP SCHEMA qt, qtm CASCADE;
SELECT quicksilver.prune();
