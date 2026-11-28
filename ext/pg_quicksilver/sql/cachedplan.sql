-- A cached routed plan, reused in a later transaction (docs/40, "Lock order").
-- A statement with no parameters is planned once and kept by the plan cache as
-- a generic plan; a later EXECUTE in another transaction takes no lock on the
-- mirror view. What answers for it is the invalidation of the plan (its
-- relationOids include the view) and the checks qs_begin_scan makes when the
-- plan starts: the session (route, role), the freshness of the mirror.
-- A statement with parameters is never routed from a generic plan (prepared.sql),
-- so it has no cached routed plan to test here.
\set last 'SELECT side, reason FROM quicksilver.last_decision();'
SET client_min_messages = warning;
CREATE SCHEMA qcp;
CREATE SCHEMA qcpm;
CREATE TABLE qcp.h (id int PRIMARY KEY, a int, b text);
INSERT INTO qcp.h SELECT g, g * 10, 'heap' || g FROM generate_series(1, 5) g;
CREATE VIEW qcpm.h AS SELECT r['id']::int AS id, r['a']::int AS a, r['b']::text AS b
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 10, 'v1-1'), (2, 20, 'v1-2'), (3, 30, 'v1-3'), (4, 40, 'v1-4'), (5, 50, 'v1-5')) t(id, a, b) $$) r;
SELECT quicksilver.register_mirror('qcp.h', 'qcpm.h', 100000000);
SELECT quicksilver.report_applied_lsn('qcp.h', pg_current_wal_lsn());
SET quicksilver.route = mirror;
SET plan_cache_mode = force_generic_plan;
PREPARE q AS SELECT id, b FROM qcp.h ORDER BY id;
EXECUTE q;
:last
EXECUTE q;
:last
SELECT generic_plans, custom_plans FROM pg_prepared_statements WHERE name = 'q';

-- ---------------------------------------------------------------- the mirror view
-- a: replaced with the same columns: the plan is planned again, and answers from the new view
CREATE OR REPLACE VIEW qcpm.h AS SELECT r['id']::int AS id, r['a']::int AS a, r['b']::text AS b
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 10, 'v2-1'), (2, 20, 'v2-2'), (3, 30, 'v2-3'), (4, 40, 'v2-4'), (5, 50, 'v2-5')) t(id, a, b) $$) r;
EXECUTE q;
:last
-- b: replaced with a column of another type: the mirror is not the heap's shape, so the heap answers
DROP VIEW qcpm.h;
CREATE VIEW qcpm.h AS SELECT r['id']::int AS id, r['a']::int AS a, r['b']::int AS b
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 10, 111), (2, 20, 222), (3, 30, 333), (4, 40, 444), (5, 50, 555)) t(id, a, b) $$) r;
EXECUTE q;
:last
-- c: dropped: the table is no longer mirrored, the heap answers
DROP VIEW qcpm.h;
EXECUTE q;
:last
-- the mirror back (same columns as the heap): a view of the same name is a different
-- relation until it is registered again, so the heap answers until then
CREATE VIEW qcpm.h AS SELECT r['id']::int AS id, r['a']::int AS a, r['b']::text AS b
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 10, 'v3-1'), (2, 20, 'v3-2'), (3, 30, 'v3-3'), (4, 40, 'v3-4'), (5, 50, 'v3-5')) t(id, a, b) $$) r;
EXECUTE q;
:last
SELECT quicksilver.register_mirror('qcp.h', 'qcpm.h', 100000000);
SELECT quicksilver.report_applied_lsn('qcp.h', pg_current_wal_lsn());
EXECUTE q;
:last

-- ---------------------------------------------------------------- the session
-- A reuse is not planned again, so last_decision() keeps the last planning
-- decision; from here the rows say which side answered (v3 is the mirror, heapN the heap).
-- f: the mirror is behind what this statement must see: the cached plan is gated at its start.
-- (A registry change, such as register_mirror, would plan the statement again, and a plan made
-- while the mirror is behind is the heap's; so the staleness here is a setting, which does not.)
SET quicksilver.min_applied_lsn = 'FFFFFFFF/FFFFFFFF';
EXECUTE q;
:last
RESET quicksilver.min_applied_lsn;
EXECUTE q;
-- f: the routing mode no longer asks for the mirror
SET quicksilver.route = heap;
EXECUTE q;
:last
SET quicksilver.route = mirror;
EXECUTE q;
-- f: a role without privilege on the heap table must not be answered from the plan a privileged role made
CREATE ROLE qcp_nobody;
SET ROLE qcp_nobody;
EXECUTE q;
RESET ROLE;
-- f: DDL on an unrelated table changes the registry's inputs, not this mirror: the plan still answers from it
CREATE TABLE qcp.unrelated (x int);
DROP TABLE qcp.unrelated;
EXECUTE q;
RESET client_min_messages;
DROP ROLE qcp_nobody;
DEALLOCATE q;
DROP SCHEMA qcp CASCADE;
DROP SCHEMA qcpm CASCADE;
SELECT quicksilver.prune();
