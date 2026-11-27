-- A lake this node cannot attach is the heap's, and the decision says why
-- (docs/40, "Promotion"). A promoted node's copied duckdb.allowed_directories
-- names the directory of the node it came from; the catalog is then outside
-- what DuckDB may read here, and the statement is answered by the heap with
-- that reason, not by a mirror plan that fails.
\set last 'SELECT side, reason, left(detail, 120) FROM quicksilver.last_decision();'
SET search_path = public;
-- the warning names this node's paths: it is checked in the log, not here
SET client_min_messages = error;
DROP SCHEMA IF EXISTS ap, apm CASCADE;
CREATE SCHEMA ap;
CREATE SCHEMA apm;
CREATE TABLE ap.h (id int PRIMARY KEY, v int);
INSERT INTO ap.h SELECT g, g * 5 FROM generate_series(1, 4) g;
CREATE VIEW apm.h AS
  SELECT r['id']::int AS id, r['v']::int AS v
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 100), (2, 200)) t(id, v) $$) r;
SELECT quicksilver.register_mirror('ap.h', 'apm.h', 1000000000000);
SELECT quicksilver.report_applied_lsn('ap.h', pg_current_wal_insert_lsn() + 1000000000);
SET quicksilver.route = mirror;

-- the lake is readable with the configuration this cluster has: routed
SELECT count(*) FROM ap.h;
:last

-- a directory the lake's catalog is not in: the attachment is refused. DuckDB
-- takes the setting only in a new instance, so the instance is replaced first.
CALL duckdb.recycle_ddb();
SET duckdb.allowed_directories = '/nonexistent/quicksilver-regress';
SELECT quicksilver.attach_lakes() AS attach;
-- and so the statement is the heap's, with the reason
SELECT count(*) FROM ap.h;
-- the detail names this cluster's paths, so it is checked, not printed
SELECT side, reason, detail LIKE 'lake "%": catalog %is outside duckdb.allowed_directories%' AS says_why
  FROM quicksilver.last_decision();

-- the same statement with the heap forced: the same answer, for the same reason it is not routed
SET quicksilver.route = heap;
SELECT count(*) FROM ap.h;

DELETE FROM quicksilver.mirrors WHERE heap = 'ap.h'::regclass;
DROP SCHEMA ap, apm CASCADE;
SELECT quicksilver.prune();
