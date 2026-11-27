-- The registry is replaced while a session holds what it decided from: a statement is
-- routed, the registry changes in the same session, and the decision, EXPLAIN and a
-- prepared routed statement are asked again. Nothing may read memory of the registry
-- that was replaced (docs/40, "Lifetime of the registry").
--
-- The output must not depend on which side answered (the sidecar may write the registry
-- while this runs): the mirror's columns equal the heap's, so only id and a are printed, and
-- the decision is checked for being recorded, not for its side.
\set recorded 'SELECT side IN (''mirror'', ''heap'') AND reason IS NOT NULL AS recorded FROM quicksilver.last_decision();'
SET client_min_messages = warning;
CREATE SCHEMA rl;
CREATE SCHEMA rlm;
CREATE TABLE rl.h (id int PRIMARY KEY, a int, b text);
INSERT INTO rl.h SELECT g, g * 10, 'heap' FROM generate_series(1, 4) g;
CREATE TABLE rl.g (id int PRIMARY KEY, a int);
INSERT INTO rl.g SELECT g, g FROM generate_series(1, 4) g;
CREATE VIEW rlm.h AS SELECT r['id']::int AS id, r['a']::int AS a, r['b']::text AS b
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 10, 'mirror'), (2, 20, 'mirror'), (3, 30, 'mirror'), (4, 40, 'mirror')) t(id, a, b) $$) r;
CREATE VIEW rlm.g AS SELECT r['id']::int AS id, r['a']::int AS a
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 1), (2, 2), (3, 3), (4, 4)) t(id, a) $$) r;
SELECT quicksilver.register_mirror('rl.h', 'rlm.h', 1000000000000);
SELECT quicksilver.report_applied_lsn('rl.h', pg_current_wal_lsn());
SET quicksilver.route = mirror;

-- routed; then the registry gains a table in this session; the next answer comes from the new registry
SELECT id, a FROM rl.h ORDER BY id;
:recorded
SELECT quicksilver.register_mirror('rl.g', 'rlm.g', 1000000000000);
SELECT quicksilver.report_applied_lsn('rl.g', pg_current_wal_lsn());
SELECT id, a FROM rl.h ORDER BY id;
:recorded

-- EXPLAIN after a change to the registry row the plan was made from (the plan is built, and not shown)
UPDATE quicksilver.mirrors SET max_lag_bytes = 1000000000001 WHERE heap = 'rl.h'::regclass;
DO $$ DECLARE r text; BEGIN FOR r IN EXPLAIN (COSTS OFF) SELECT id, a FROM rl.h ORDER BY id LOOP END LOOP; END $$;
SELECT id, a FROM rl.h ORDER BY id;
:recorded

-- a prepared routed statement, executed across two registry changes
PREPARE rp AS SELECT id, a FROM rl.h WHERE a >= $1 ORDER BY id;
EXECUTE rp(20);
UPDATE quicksilver.mirrors SET max_lag_bytes = 1000000000002 WHERE heap = 'rl.h'::regclass;
EXECUTE rp(20);
DELETE FROM quicksilver.mirrors WHERE heap = 'rl.g'::regclass;
EXECUTE rp(20);
:recorded

-- the registry stops naming the table: the statement reads the heap, and records no decision
-- (a statement that reads no mirrored table is nobody's business)
DELETE FROM quicksilver.mirrors WHERE heap = 'rl.h'::regclass;
SELECT id, a FROM rl.h ORDER BY id;
SELECT quicksilver.register_mirror('rl.h', 'rlm.h', 1000000000000);
SELECT quicksilver.report_applied_lsn('rl.h', pg_current_wal_lsn());
SELECT id, a FROM rl.h ORDER BY id;
:recorded

RESET quicksilver.route;
DELETE FROM quicksilver.mirrors WHERE heap IN ('rl.h'::regclass, 'rl.g'::regclass);
DROP SCHEMA rl CASCADE;
DROP SCHEMA rlm CASCADE;
