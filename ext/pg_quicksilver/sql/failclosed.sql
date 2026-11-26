-- Everything that must stay on the heap. The fabricated mirror answers
-- 'mirror', so a statement that was wrongly routed shows.
\set last 'SELECT side, reason, detail FROM quicksilver.last_decision();'
SET search_path = public;
SET client_min_messages = warning;
CREATE SCHEMA qt;
CREATE SCHEMA qtm;
CREATE TABLE qt.h (id int PRIMARY KEY, a int, n numeric(10,2), nu numeric, b text, bc text COLLATE "C", j jsonb, ts timestamptz);
INSERT INTO qt.h SELECT g, g * 10, g + 0.25, g, 'heap', 'heap', '{}', '2024-01-01 00:00:00+00' FROM generate_series(1, 4) g;
CREATE VIEW qtm.h AS
  SELECT r['id']::int AS id, r['a']::int AS a, r['n']::numeric(10,2) AS n, r['nu']::numeric(38,9) AS nu,
         r['b']::text AS b, r['bc']::text AS bc, r['j']::text AS j, r['ts']::timestamptz AS ts
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 10, 1.25, 1, 'mirror', 'mirror', '{}', TIMESTAMPTZ '2024-01-01 00:00:00+00'),
                                             (2, 20, 2.25, 2, 'mirror', 'mirror', '{}', TIMESTAMPTZ '2024-01-01 00:00:00+00'))
                                     t(id, a, n, nu, b, bc, j, ts) $$) r;
SELECT quicksilver.register_mirror('qt.h', 'qtm.h', 1000000000000);
UPDATE quicksilver.mirrors SET finite_columns = '{ts}' WHERE heap = 'qt.h'::regclass;
SELECT quicksilver.report_applied_lsn('qt.h', pg_current_wal_lsn());
SELECT usable, unusable_columns FROM quicksilver.mirror_status() WHERE heap = 'qt.h'::regclass;
SET quicksilver.route = mirror;

-- the control: this one is routed
SELECT b, count(*) FROM qt.h GROUP BY b;
:last

-- not a plain SELECT
SELECT b FROM qt.h WHERE id = 1 FOR UPDATE;
:last
WITH w AS (UPDATE qt.h SET a = a WHERE id = 1 RETURNING id) SELECT b FROM qt.h WHERE id IN (SELECT id FROM w);
:last
SELECT b INTO TEMP qs_into FROM qt.h WHERE id = 1;
TABLE qs_into;
CREATE TEMP TABLE qs_ctas AS SELECT b FROM qt.h WHERE id = 1;
TABLE qs_ctas;
EXPLAIN (COSTS OFF) CREATE TEMP TABLE qs_ctas2 AS SELECT b FROM qt.h WHERE id = 1;
COPY (SELECT b FROM qt.h WHERE id = 1) TO STDOUT;

-- inside a function, whatever its language
CREATE FUNCTION qt.f_sql() RETURNS text LANGUAGE sql AS 'SELECT min(b) FROM qt.h';
CREATE FUNCTION qt.f_plpgsql() RETURNS text LANGUAGE plpgsql AS 'DECLARE r text; BEGIN SELECT b INTO r FROM qt.h WHERE id = 1; RETURN r; END';
SELECT qt.f_sql(), qt.f_plpgsql();
SET client_min_messages = notice;
DO $$ DECLARE r text; BEGIN SELECT b INTO r FROM qt.h WHERE id = 1; RAISE NOTICE 'DO block read: %', r; END $$;
SET client_min_messages = warning;
-- and a function over the table keeps the outer statement on the heap too
SELECT qt.f_plpgsql(), b FROM qt.h WHERE id = 1;
:last

-- operators, functions and aggregates DuckDB answers differently
SELECT a / 4 AS q, b FROM qt.h WHERE id = 1;
:last
SELECT avg(a), min(b) FROM qt.h;
:last
SELECT b FROM qt.h WHERE b LIKE 'h%' AND id = 1;
:last
SELECT upper(b) FROM qt.h WHERE id = 1;
:last
SELECT b, a::float8 FROM qt.h WHERE id = 1;
:last
SELECT b, id::text FROM qt.h WHERE id = 1;
:last

-- the session: clock, time zone, settings
SELECT b FROM qt.h WHERE ts < now() AND id = 1;
:last
SELECT b, current_date > '2000-01-01' FROM qt.h WHERE id = 1;
:last
SELECT b, ts::date FROM qt.h WHERE id = 1;
:last
SELECT b, current_setting('search_path') <> '' FROM qt.h WHERE id = 1;
:last
SELECT b FROM qt.h WHERE random() >= 0 AND id = 1;
:last

-- text: equality under a deterministic collation is safe, order is not unless "C"
SELECT b FROM qt.h WHERE b = 'mirror' ORDER BY id LIMIT 1;
:last
SELECT b FROM qt.h WHERE b < 'zzz' AND id = 1;
:last
SELECT b FROM qt.h ORDER BY b, id LIMIT 1;
:last
SELECT min(b) FROM qt.h;
:last
SELECT bc FROM qt.h WHERE bc < 'zzz' ORDER BY bc, id LIMIT 1;
:last
SELECT min(bc), max(bc) FROM qt.h;
:last
-- an explicit COLLATE is an unknown: heap
SELECT b FROM qt.h ORDER BY b COLLATE "C", id LIMIT 1;
:last
CREATE COLLATION qt.ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
SELECT b FROM qt.h WHERE b = 'HEAP' COLLATE qt.ci AND id = 1;
:last

-- numeric: scale lives in the value on the heap, in the column in the mirror
SELECT b, n * 2 FROM qt.h WHERE id = 1;
:last
SELECT b, n * 2.0 FROM qt.h WHERE id = 1;
:last
SELECT b FROM qt.h WHERE n > 1 AND id = 1;
:last
SELECT b, CASE WHEN a > 0 THEN n ELSE 0 END FROM qt.h WHERE id = 1;
:last
SELECT b, coalesce(n, 0.5) FROM qt.h WHERE id = 1;
:last
SELECT b, nu FROM qt.h WHERE id = 1;
:last

-- integer arithmetic: the values are exact, the overflow error is not. Row 9 holds
-- the largest integer on both sides; PostgreSQL fails on a + 1, the mirror's
-- optimizer turns (a + 1 > 0) into (a > -1) and never adds. Not routed, unless
-- the session says it can live with that.
INSERT INTO qt.h (id, a, b) VALUES (9, 2147483647, 'heap');
CREATE VIEW qtm.big AS SELECT r['id']::int AS id, r['a']::int AS a, r['b']::text AS b
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 10, 'mirror'), (9, 2147483647, 'mirror')) t(id, a, b) $$) r;
CREATE TABLE qt.big (id int PRIMARY KEY, a int, b text);
INSERT INTO qt.big VALUES (1, 10, 'heap'), (9, 2147483647, 'heap');
SELECT quicksilver.register_mirror('qt.big', 'qtm.big', 1000000000000);
SELECT quicksilver.report_applied_lsn('qt.big', pg_current_wal_lsn());
SELECT b, a + 1 FROM qt.big WHERE id = 1;
:last
SELECT count(*) FROM qt.big WHERE a + 1 > 0;
SET quicksilver.tolerate_lost_errors = on;
SELECT b, a + 1 FROM qt.big WHERE id = 1;
:last
-- the heap's error is lost here ...
SELECT count(*) FROM qt.big WHERE a + 1 > 0;
-- ... and kept here, where DuckDB does add: it fails, and the heap answers (with its error)
SELECT b, a + 1 FROM qt.big ORDER BY id;
RESET quicksilver.tolerate_lost_errors;
DELETE FROM qt.h WHERE id = 9;

-- types and columns the mirror does not hold as PostgreSQL does
SELECT b, j FROM qt.h WHERE id = 1;
:last
SELECT b, ctid FROM qt.h WHERE id = 1;
:last
SELECT b, tableoid::regclass FROM qt.h WHERE id = 1;
:last
SELECT h FROM qt.h h WHERE id = 1;
:last
SELECT b FROM qt.h TABLESAMPLE BERNOULLI (100) WHERE id = 1;
:last

-- statement shapes that are not on the seed list
SELECT b FROM qt.h LIMIT 1;
:last
SELECT b, row_number() OVER (ORDER BY id) FROM qt.h WHERE id = 1;
:last
SELECT DISTINCT b FROM qt.h;
:last
SELECT b FROM qt.h WHERE id = 1 UNION ALL SELECT b FROM qt.h WHERE id = 2;
:last
SELECT b FROM qt.h h WHERE EXISTS (SELECT 1 FROM qt.h x WHERE x.id = h.id + 1) AND id = 1;
:last
WITH c AS (SELECT b FROM qt.h WHERE id = 1) SELECT * FROM c;
:last
SELECT b, g FROM qt.h, generate_series(1, 1) g WHERE id = 1;
:last

-- the machine-readable form: one row per construct, nothing executed
SELECT * FROM quicksilver.unsafe_constructs('SELECT upper(b), a / 2, avg(n) OVER () FROM qt.h WHERE b LIKE $1 ORDER BY b');
SELECT * FROM quicksilver.unsafe_constructs('SELECT b, count(*) FROM qt.h WHERE a > $1 GROUP BY b');
SELECT * FROM quicksilver.unsafe_constructs('UPDATE qt.h SET a = 1');
SELECT * FROM quicksilver.unsafe_constructs('SELECT 1; SELECT 2');
SELECT a / 4 AS q, b FROM qt.h WHERE id = 1;
SELECT side, constructs FROM quicksilver.last_decision();

-- partitioned and inherited tables are never routed
CREATE TABLE qt.p (id int, b text) PARTITION BY RANGE (id);
CREATE TABLE qt.p1 PARTITION OF qt.p FOR VALUES FROM (0) TO (100);
INSERT INTO qt.p VALUES (1, 'heap');
CREATE VIEW qtm.p AS SELECT r['id']::int AS id, r['b']::text AS b FROM duckdb.query($$ SELECT 1 AS id, 'mirror' AS b $$) r;
SELECT quicksilver.register_mirror('qt.p', 'qtm.p', 1000000000000);
SELECT quicksilver.report_applied_lsn('qt.p', pg_current_wal_lsn());
SELECT usable, reason FROM quicksilver.mirror_status() WHERE heap = 'qt.p'::regclass;
SELECT b FROM qt.p;
:last
CREATE TABLE qt.par (id int, b text);
CREATE TABLE qt.kid () INHERITS (qt.par);
INSERT INTO qt.par VALUES (1, 'heap');
INSERT INTO qt.kid VALUES (2, 'heap child');
SELECT quicksilver.register_mirror('qt.par', 'qtm.p', 1000000000000);
SELECT quicksilver.report_applied_lsn('qt.par', pg_current_wal_lsn());
SELECT b FROM qt.par ORDER BY id;
:last
-- ONLY the parent is a plain table again
SELECT b FROM ONLY qt.par ORDER BY id;
:last

-- a temporary table that shadows a mirrored name is a different relation
SET search_path = qt, public;
SELECT b, count(*) FROM h GROUP BY b;
CREATE TEMP TABLE h (id int, b text);
INSERT INTO h VALUES (1, 'temp');
SELECT b, count(*) FROM h GROUP BY b;
SELECT quicksilver.forget_last();
SELECT b, count(*) FROM pg_temp.h GROUP BY b;
SELECT side FROM quicksilver.last_decision();
DROP TABLE pg_temp.h;
-- and so is a same-named table reached through search_path
CREATE SCHEMA qt2;
CREATE TABLE qt2.h (id int, b text);
INSERT INTO qt2.h VALUES (1, 'the other h');
SET search_path = qt2, qt, public;
SELECT b, count(*) FROM h GROUP BY b;
SET search_path = public;

-- mirror mode still refuses what is not allowlisted; the override is superuser-only
SELECT a / 4 AS q, b FROM qt.h WHERE id = 1;
SET quicksilver.allow_unsafe = on;
SELECT a / 4 AS q, b FROM qt.h WHERE id = 1;
:last
RESET quicksilver.allow_unsafe;
CREATE ROLE qs_t_plain;
SET ROLE qs_t_plain;
SET quicksilver.allow_unsafe = on;
RESET ROLE;
DROP ROLE qs_t_plain;

-- an allowlist row cannot admit what it should not
INSERT INTO quicksilver.allowlist (kind, ident, verified_by) VALUES
  ('function', 'now()', 'nobody'),                 -- stable, entry says immutable
  ('function', 'random()', 'nobody'),              -- volatile
  ('function', 'no_such_function(integer)', 'nobody'),
  ('node', 'tag999', 'nobody');
SELECT * FROM quicksilver.allowlist_problems() ORDER BY 1;
SELECT b FROM qt.h WHERE ts < now() AND id = 1;
DELETE FROM quicksilver.allowlist WHERE verified_by = 'nobody';
-- a stable function is admitted only by an entry that says it is stable
INSERT INTO quicksilver.allowlist (kind, ident, volatility, verified_by) VALUES ('function', 'now()', 'stable', 'regression test');
INSERT INTO quicksilver.allowlist (kind, ident, options, verified_by) VALUES ('operator', '<(timestamp with time zone,timestamp with time zone)', '{cmp}', 'regression test')
  ON CONFLICT (kind, ident) DO NOTHING;
SELECT * FROM quicksilver.allowlist_problems();
SELECT b FROM qt.h WHERE ts < now() AND id = 1;
DELETE FROM quicksilver.allowlist WHERE verified_by = 'regression test';

DROP SCHEMA qt, qtm, qt2 CASCADE;
SELECT quicksilver.prune();
