-- A jsonb column decides a statement only if the statement reads it. The walk
-- used to visit every column of every join (the join alias lists), so an unread
-- jsonb column kept a join on the heap. Joins that do read it, by SELECT *, a
-- whole-row reference, a NATURAL or USING join on it, or an output or predicate
-- that names it, still fall back.
\set last 'SELECT side, reason, left(detail, 150) FROM quicksilver.last_decision();'
SET search_path = public;
SET client_min_messages = warning;
CREATE SCHEMA jt;
CREATE SCHEMA jtm;
CREATE TABLE jt.a (id int PRIMARY KEY, j jsonb, n int);
CREATE TABLE jt.b (id int PRIMARY KEY, a_id int, s text);
CREATE TABLE jt.c (id int PRIMARY KEY, j jsonb, x int);
INSERT INTO jt.a SELECT g, '{"k": 1}', g FROM generate_series(1, 4) g;
INSERT INTO jt.b SELECT g, g, 'b' || g FROM generate_series(1, 4) g;
INSERT INTO jt.c SELECT g, '{"k": 1}', g * 10 FROM generate_series(1, 4) g;
-- the mirrors: the jsonb columns are text in the mirror, which the walk does not accept for jsonb
CREATE VIEW jtm.a AS SELECT r['id']::int AS id, r['j']::text AS j, r['n']::int AS n
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, '{"k": 1}', 100), (2, '{"k": 1}', 200)) t(id, j, n) $$) r;
CREATE VIEW jtm.b AS SELECT r['id']::int AS id, r['a_id']::int AS a_id, r['s']::text AS s
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 1, 'mb1'), (2, 2, 'mb2')) t(id, a_id, s) $$) r;
CREATE VIEW jtm.c AS SELECT r['id']::int AS id, r['j']::text AS j, r['x']::int AS x
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, '{"k": 1}', 1000), (2, '{"k": 1}', 2000)) t(id, j, x) $$) r;
SELECT quicksilver.register_mirror('jt.a', 'jtm.a', 1000000000000);
SELECT quicksilver.register_mirror('jt.b', 'jtm.b', 1000000000000);
SELECT quicksilver.register_mirror('jt.c', 'jtm.c', 1000000000000);
SELECT quicksilver.report_applied_lsn('jt.a', pg_current_wal_insert_lsn() + 1000000000);
SELECT quicksilver.report_applied_lsn('jt.b', pg_current_wal_insert_lsn() + 1000000000);
SELECT quicksilver.report_applied_lsn('jt.c', pg_current_wal_insert_lsn() + 1000000000);
SET quicksilver.route = mirror;

-- the jsonb column is never read: the join is routed, and the mirror answers
SELECT a.n, b.s FROM jt.a a JOIN jt.b b ON b.a_id = a.id ORDER BY 1;
:last
SELECT count(*) FROM jt.a a JOIN jt.b b ON b.a_id = a.id;
:last
-- a USING join on a column that is not jsonb: routed
SELECT n, s FROM jt.a JOIN jt.b USING (id) ORDER BY 1;
:last

-- SELECT * exposes the jsonb column: heap
SELECT * FROM jt.a a JOIN jt.b b ON b.a_id = a.id ORDER BY 1;
:last
-- a whole-row reference exposes it: heap
SELECT a, a.n FROM jt.a a JOIN jt.b b ON b.a_id = a.id ORDER BY a.n;
:last
-- the jsonb column is read in an output: heap
SELECT a.j, b.s FROM jt.a a JOIN jt.b b ON b.a_id = a.id ORDER BY b.id;
:last
-- a NATURAL join on id and j compares j: heap, though no output names it
SELECT n, x FROM jt.a NATURAL JOIN jt.c ORDER BY 1;
:last
-- a USING join on j compares j: heap
SELECT a.n, c.x FROM jt.a a JOIN jt.c c USING (j) ORDER BY 1, 2;
:last
-- an ON join that compares j: heap
SELECT a.n FROM jt.a a JOIN jt.c c ON c.j = a.j ORDER BY 1;
:last

-- the heap of the routed statements, for comparison
SET quicksilver.route = heap;
SELECT a.n, b.s FROM jt.a a JOIN jt.b b ON b.a_id = a.id ORDER BY 1, 2;
SELECT n, s FROM jt.a JOIN jt.b USING (id) ORDER BY 1, 2;
SET quicksilver.route = off;

DELETE FROM quicksilver.mirrors WHERE heap IN ('jt.a'::regclass, 'jt.b'::regclass, 'jt.c'::regclass);
DROP SCHEMA jt, jtm CASCADE;
SELECT quicksilver.prune();
