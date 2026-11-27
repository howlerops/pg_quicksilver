-- The superuser part of sql/permissions.sql up to the forced-mirror SELECT on qt.rls,
-- then that SELECT and the decision it left. One fresh backend per iteration.
SET client_min_messages = warning;
SET search_path = public;
CREATE SCHEMA qt;
CREATE SCHEMA qtm;
CREATE TABLE qt.h (id int PRIMARY KEY, a int, secret text, b text);
INSERT INTO qt.h SELECT g, g * 10, 'heap secret', 'heap' FROM generate_series(1, 3) g;
CREATE VIEW qtm.h AS
  SELECT r['id']::int AS id, r['a']::int AS a, r['secret']::text AS secret, r['b']::text AS b
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 10, 'mirror secret', 'mirror'), (2, 20, 'mirror secret', 'mirror'),
                                             (3, 30, 'mirror secret', 'mirror'), (4, 40, 'mirror secret', 'mirror')) t(id, a, secret, b) $$) r;
SELECT quicksilver.register_mirror('qt.h', 'qtm.h', 1000000000000);
SELECT quicksilver.report_applied_lsn('qt.h', pg_current_wal_lsn());
CREATE TABLE qt.rls (id int PRIMARY KEY, owner_name text, b text);
INSERT INTO qt.rls VALUES (1, 'qs_t_all', 'heap'), (2, 'somebody else', 'heap');
CREATE VIEW qtm.rls AS
  SELECT r['id']::int AS id, r['owner_name']::text AS owner_name, r['b']::text AS b
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 'qs_t_all', 'mirror'), (2, 'somebody else', 'mirror')) t(id, owner_name, b) $$) r;
SELECT quicksilver.register_mirror('qt.rls', 'qtm.rls', 1000000000000);
SELECT quicksilver.report_applied_lsn('qt.rls', pg_current_wal_lsn());
SET quicksilver.route = mirror;
\echo ANSWER
SELECT b FROM qt.rls ORDER BY id;
\echo DECISION
SELECT side, reason, replace(left(coalesce(detail, ''), 200), E'\n', ' ') FROM quicksilver.last_decision();
DROP SCHEMA qt, qtm CASCADE;
SELECT quicksilver.prune();
