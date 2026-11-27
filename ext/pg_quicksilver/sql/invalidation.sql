-- A decision never outlives what it was based on: DDL on the heap table or the
-- mirror, a change to the allowlist, a change to the registry.
\set last 'SELECT side, reason, detail FROM quicksilver.last_decision();'
SET client_min_messages = warning;
CREATE SCHEMA qt;
CREATE SCHEMA qtm;
CREATE TABLE qt.h (id int PRIMARY KEY, a int, b text, c int);
INSERT INTO qt.h SELECT g, g * 10, 'heap', g FROM generate_series(1, 3) g;
CREATE VIEW qtm.h AS SELECT r['c']::int AS c, r['id']::int AS id, r['a']::int AS a, r['b']::text AS b
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 10, 'mirror', 100), (2, 20, 'mirror', 200), (3, 30, 'mirror', 300)) t(id, a, b, c) $$) r;
SELECT quicksilver.register_mirror('qt.h', 'qtm.h', 1000000000000);
SELECT quicksilver.report_applied_lsn('qt.h', pg_current_wal_lsn());
SET quicksilver.route = mirror;
SET quicksilver.tolerate_lost_errors = on;     -- the allowlist tests below use integer +

PREPARE ab AS SELECT id, a, b FROM qt.h ORDER BY id;
PREPARE bc AS SELECT id, b, c FROM qt.h ORDER BY id;
PREPARE idb AS SELECT id, b FROM qt.h ORDER BY id;
EXECUTE ab;
EXECUTE bc;
SELECT * FROM qt.h ORDER BY id;

-- ---------------------------------------------------------------- DDL on the heap table
-- a new column the mirror does not have: SELECT * goes home, the rest does not
ALTER TABLE qt.h ADD COLUMN d int DEFAULT 7;
SELECT * FROM qt.h ORDER BY id;
:last
EXECUTE ab;
:last
SELECT unusable_columns FROM quicksilver.mirror_status() WHERE heap = 'qt.h'::regclass;
-- a dropped column: attribute numbers shift on the heap; the mirror is matched by name
ALTER TABLE qt.h DROP COLUMN a;
EXECUTE ab;
EXECUTE bc;
:last
-- a renamed column is a column the mirror does not have
ALTER TABLE qt.h RENAME COLUMN c TO c2;
SELECT id, b, c2 FROM qt.h ORDER BY id;
:last
EXECUTE idb;
ALTER TABLE qt.h RENAME COLUMN c2 TO c;
EXECUTE bc;
-- a column whose type changed
ALTER TABLE qt.h ALTER COLUMN c TYPE bigint;
SELECT id, b, c FROM qt.h ORDER BY id;
:last
EXECUTE idb;
-- row-level security switched on
ALTER TABLE qt.h ENABLE ROW LEVEL SECURITY;
EXECUTE idb;
ALTER TABLE qt.h DISABLE ROW LEVEL SECURITY;
EXECUTE idb;
-- a child table appears, and goes
CREATE TABLE qt.kid () INHERITS (qt.h);
EXECUTE idb;
:last
DROP TABLE qt.kid;
EXECUTE idb;

-- ---------------------------------------------------------------- DDL on the mirror
CREATE OR REPLACE VIEW qtm.h AS SELECT r['c']::int AS c, r['id']::int AS id, r['a']::int AS a, r['b']::text AS b
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 10, 'mirror, republished', 100)) t(id, a, b, c) $$) r;
EXECUTE idb;
DROP VIEW qtm.h;
EXECUTE idb;
:last
SELECT usable, reason FROM quicksilver.mirror_status() WHERE NOT usable;
SELECT quicksilver.prune();
CREATE VIEW qtm.h AS SELECT r['id']::int AS id, r['b']::text AS b
  FROM duckdb.query($$ SELECT 1 AS id, 'mirror, recreated' AS b $$) r;
-- a view of the same name is a different relation until it is registered
EXECUTE idb;
SELECT quicksilver.register_mirror('qt.h', 'qtm.h', 1000000000000);
EXECUTE idb;

-- ---------------------------------------------------------------- the allowlist
ALTER TABLE qt.h ADD COLUMN a int DEFAULT 1;
CREATE OR REPLACE VIEW qtm.h AS SELECT r['id']::int AS id, r['b']::text AS b, r['a']::int AS a
  FROM duckdb.query($$ SELECT 1 AS id, 'mirror' AS b, 1 AS a $$) r;
PREPARE plus AS SELECT id, a + id AS x, b FROM qt.h ORDER BY id;
EXECUTE plus;
SET quicksilver.route_in_transaction = on;
BEGIN;
DELETE FROM quicksilver.allowlist WHERE kind = 'operator' AND ident = '+(integer,integer)';
-- visible to this transaction at once
EXECUTE plus;
SELECT side, reason, detail FROM quicksilver.last_decision();
ROLLBACK;
RESET quicksilver.route_in_transaction;
-- and gone with it
EXECUTE plus;
DELETE FROM quicksilver.allowlist WHERE kind = 'operator' AND ident = '+(integer,integer)';
EXECUTE plus;
:last
SELECT id, a + id AS x, b FROM qt.h ORDER BY id;
INSERT INTO quicksilver.allowlist (kind, ident, options, verified_by) VALUES ('operator', '+(integer,integer)', '{errors}', 'docs/40 equality harness, pg_duckdb 1.2.0');
EXECUTE plus;
SELECT id, a + id AS x, b FROM qt.h ORDER BY id;
-- an entry is the object it names: recreating a look-alike elsewhere changes nothing
CREATE FUNCTION qt.myplus(int, int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1 - $2';
CREATE OPERATOR qt.+ (LEFTARG = int, RIGHTARG = int, FUNCTION = qt.myplus);
SET search_path = qt, pg_catalog, public;
SELECT id, a + id AS x, b FROM qt.h ORDER BY id;
SELECT side, reason, detail FROM quicksilver.last_decision();
SET search_path = public;
SELECT id, a + id AS x, b FROM qt.h ORDER BY id;

-- ---------------------------------------------------------------- the mode
SET quicksilver.route = heap;
EXECUTE plus;
SET quicksilver.route = mirror;
EXECUTE plus;

-- ---------------------------------------------------------------- the registry
UPDATE quicksilver.mirrors SET enabled = false WHERE heap = 'qt.h'::regclass;
EXECUTE plus;
:last
UPDATE quicksilver.mirrors SET enabled = true WHERE heap = 'qt.h'::regclass;
EXECUTE plus;
DELETE FROM quicksilver.mirrors WHERE heap = 'qt.h'::regclass;
EXECUTE plus;
SELECT quicksilver.forget_last();
SELECT id, b FROM qt.h ORDER BY id;
SELECT side FROM quicksilver.last_decision();
-- the heap table itself goes away and comes back under the same name: a different relation
SELECT quicksilver.register_mirror('qt.h', 'qtm.h', 1000000000000);
SELECT id, b FROM qt.h ORDER BY id;
DROP TABLE qt.h;
CREATE TABLE qt.h (id int PRIMARY KEY, b text);
INSERT INTO qt.h VALUES (1, 'a new heap');
SELECT id, b FROM qt.h ORDER BY id;

DEALLOCATE ALL;
DROP SCHEMA qt, qtm CASCADE;
SELECT quicksilver.prune();
