-- A mirror that fails under a plan that was made when it worked: what is
-- answered from the heap, what is not, and that nothing is left held.
--
-- There is no subtransaction around the mirror (pg_duckdb refuses one), so the
-- only errors answered from the heap are DuckDB's own, which pg_duckdb raises
-- at its C++ boundary with nothing of PostgreSQL's held. A cancel is rethrown.
-- A leaked buffer pin, relcache reference or lock is reported at commit, and
-- would show in this file's output: there must be none.
\set last 'SELECT side, reason, left(detail, 40) AS detail FROM quicksilver.last_decision();'
SET client_min_messages = warning;
SET search_path = public;
CREATE SCHEMA qt;
CREATE SCHEMA qtm;
CREATE TABLE qt.h (id int PRIMARY KEY, a int, b text);
INSERT INTO qt.h SELECT g, g * 10, 'heap' FROM generate_series(1, 3) g;
-- the mirror is a file, as a lake's data file is
SELECT duckdb.raw_query($$ COPY (SELECT * FROM (VALUES (1, 10, 'mirror'), (2, 20, 'mirror'), (3, 30, 'mirror')) t(id, a, b)) TO '/tmp/qs_regress_errors.parquet' $$);
CREATE VIEW qtm.h AS SELECT r['id']::int AS id, r['a']::int AS a, r['b']::text AS b
  FROM duckdb.query($$ SELECT * FROM read_parquet('/tmp/qs_regress_errors.parquet') $$) r;
SELECT quicksilver.register_mirror('qt.h', 'qtm.h', 1000000000000);
SELECT quicksilver.report_applied_lsn('qt.h', pg_current_wal_lsn());
SET quicksilver.route = mirror;
SET quicksilver.negative_cache_ttl = 0;

-- ---------------------------------------------------------------- a kept plan, then the file goes
PREPARE kept AS SELECT b, count(*) FROM qt.h GROUP BY b;
EXECUTE kept;
-- the file is replaced by something that is not Parquet (no SQL unlinks a file;
-- to the reader it is the same: the file its plan names cannot be read)
SELECT lo_from_bytea(0, 'not a parquet file') AS lo \gset
SELECT lo_export(:lo, '/tmp/qs_regress_errors.parquet');
SELECT lo_unlink(:lo);
-- outside a transaction block
EXECUTE kept;
:last
EXECUTE kept;
-- inside one
SET quicksilver.route_in_transaction = on;
BEGIN;
EXECUTE kept;
:last
SELECT count(*) FROM qt.h;
COMMIT;
-- with a cursor, inside a transaction block
BEGIN;
DECLARE c NO SCROLL CURSOR FOR SELECT b, count(*) FROM qt.h GROUP BY b;
FETCH ALL FROM c;
CLOSE c;
EXECUTE kept;
COMMIT;
RESET quicksilver.route_in_transaction;

-- ---------------------------------------------------------------- DuckDB raises while it runs
-- the mirror holds a value its own cast cannot take: an error at execution, not at planning
CREATE TABLE qt.e (id int PRIMARY KEY, a int);
INSERT INTO qt.e VALUES (1, 10), (2, 20);
CREATE VIEW qtm.e AS SELECT r['id']::int AS id, r['a']::int AS a
  FROM duckdb.query($$ SELECT id, CASE WHEN id = 2 THEN a::VARCHAR || 'x' ELSE a::VARCHAR END::INTEGER AS a FROM (VALUES (1, 10), (2, 20)) t(id, a) $$) r;
SELECT quicksilver.register_mirror('qt.e', 'qtm.e', 1000000000000);
SELECT quicksilver.report_applied_lsn('qt.e', pg_current_wal_lsn());
SELECT sum(a) FROM qt.e;
SELECT side, reason FROM quicksilver.last_decision();
PREPARE bad AS SELECT sum(a) FROM qt.e;
EXECUTE bad;
EXECUTE bad;
BEGIN;
DECLARE c NO SCROLL CURSOR FOR SELECT id, a FROM qt.e ORDER BY id;
FETCH ALL FROM c;
COMMIT;
SET quicksilver.route_in_transaction = on;
BEGIN;
SELECT sum(a) FROM qt.e;
SELECT sum(a) FROM qt.e;
INSERT INTO qt.e VALUES (3, 30);
-- and once the transaction has written, the mirror is not asked at all
SELECT sum(a) FROM qt.e;
SELECT side, reason, detail FROM quicksilver.last_decision();
ROLLBACK;
RESET quicksilver.route_in_transaction;
-- without the fallback the client is told what happened
SET quicksilver.fallback = off;
SELECT sum(a) FROM qt.e;
RESET quicksilver.fallback;

-- ---------------------------------------------------------------- a cancel is not an error to answer from the heap
CREATE TABLE qt.slow (id int PRIMARY KEY, n bigint);
INSERT INTO qt.slow VALUES (1, 1);
CREATE VIEW qtm.slow AS SELECT r['id']::int AS id, r['n']::bigint AS n
  FROM duckdb.query($$ SELECT 1 AS id, count(*) AS n FROM range(200000000000) $$) r;
SELECT quicksilver.register_mirror('qt.slow', 'qtm.slow', 1000000000000);
SELECT quicksilver.report_applied_lsn('qt.slow', pg_current_wal_lsn());
SET statement_timeout = '300ms';
SELECT n FROM qt.slow;
RESET statement_timeout;
-- the session is intact, and the heap still answers when asked
SET quicksilver.route = heap;
SELECT n FROM qt.slow;
SET quicksilver.route = mirror;

-- ---------------------------------------------------------------- the applied-LSN table gives entries back
-- More registrations than it holds (1024), one after another: every other one
-- is unregistered, the rest have their table dropped under the registry row.
DO $$
BEGIN
  FOR i IN 1..1100 LOOP
    EXECUTE format('CREATE TABLE qt.many_%s (id int PRIMARY KEY)', i);
    EXECUTE format('CREATE VIEW qtm.many_%s AS SELECT r[''id'']::int AS id FROM duckdb.query($q$ SELECT 1 AS id $q$) r', i);
    EXECUTE format('SELECT quicksilver.register_mirror(''qt.many_%s'', ''qtm.many_%s'')', i, i);
    EXECUTE format('SELECT quicksilver.report_applied_lsn(''qt.many_%s'', ''0/10'')', i);
    IF i % 2 = 0 THEN
      EXECUTE format('DELETE FROM quicksilver.mirrors WHERE heap = ''qt.many_%s''::regclass', i);
    END IF;
    EXECUTE format('DROP VIEW qtm.many_%s', i);
    EXECUTE format('DROP TABLE qt.many_%s', i);
  END LOOP;
END $$;
SELECT quicksilver.prune();
-- and a table registered after all that is reported and routed
SELECT quicksilver.report_applied_lsn('qt.h', pg_current_wal_lsn());
SELECT fresh FROM quicksilver.mirror_status() WHERE heap = 'qt.h'::regclass;
-- a report for a relation that is not registered is not kept
CREATE TABLE qt.unreg (id int);
SELECT quicksilver.report_applied_lsn('qt.unreg', '0/10');

DROP SCHEMA qt, qtm CASCADE;
SELECT quicksilver.prune();
