-- What a user may read through the mirror is decided by the heap table's
-- privileges and nothing else. Users hold no privilege on the mirror relation
-- or its schema. The mirror's rows say 'mirror' (and include a row the heap
-- does not have), so a leak would show.
--
-- The roles are members of qs_duck (duckdb.postgres_role in validate/setup.sh):
-- pg_duckdb refuses to run for anyone else. The fabricated mirror reads no
-- file, which stock pg_duckdb would refuse a non-superuser.
\set last 'SELECT side, reason FROM quicksilver.last_decision();'
SET client_min_messages = warning;
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

CREATE ROLE qs_t_all LOGIN IN ROLE qs_duck;
CREATE ROLE qs_t_none LOGIN IN ROLE qs_duck;
CREATE ROLE qs_t_col LOGIN IN ROLE qs_duck;
GRANT USAGE ON SCHEMA qt TO qs_t_all, qs_t_none, qs_t_col;
GRANT SELECT ON qt.h, qt.rls TO qs_t_all;
GRANT SELECT (id, a, b) ON qt.h TO qs_t_col;

-- SELECT on the heap table: routed, with no privilege on the mirror at all
\c - qs_t_all
\set last 'SELECT side, reason FROM quicksilver.last_decision();'
SET quicksilver.route = mirror;
SELECT b, count(*) FROM qt.h GROUP BY b;
:last
SELECT count(*) FROM qtm.h;
-- the registry and the allowlist are not theirs to change
INSERT INTO quicksilver.mirrors (heap, mirror) VALUES ('qt.h', 'qt.h');
DELETE FROM quicksilver.allowlist;
SELECT quicksilver.register_mirror('qt.h', 'qt.h');
SELECT quicksilver.report_applied_lsn('qt.h', '0/0');
SET quicksilver.allow_unsafe = on;

-- no privilege on the heap table: the heap's own error, and nothing from the mirror
\c - qs_t_none
\set last 'SELECT side, reason FROM quicksilver.last_decision();'
SET quicksilver.route = mirror;
SELECT b, count(*) FROM qt.h GROUP BY b;
:last
SELECT count(*) FROM qt.h;
SELECT id FROM qt.h WHERE id = 4;

-- column privileges
\c - qs_t_col
\set last 'SELECT side, reason FROM quicksilver.last_decision();'
SET quicksilver.route = mirror;
SELECT id, a, b FROM qt.h ORDER BY id;
:last
SELECT id, secret FROM qt.h ORDER BY id;
:last
SELECT * FROM qt.h ORDER BY id;
SELECT count(*) FROM qt.h WHERE secret = 'mirror secret';
SELECT max(secret) FROM qt.h;

-- privileges are checked when the statement runs, not when it was prepared
\c - postgres
SET client_min_messages = warning;
SET quicksilver.route = mirror;
SET ROLE qs_t_all;
PREPARE p AS SELECT b, count(*) FROM qt.h GROUP BY b;
EXECUTE p;
RESET ROLE;
REVOKE SELECT ON qt.h FROM qs_t_all;
SET ROLE qs_t_all;
EXECUTE p;
SELECT b, count(*) FROM qt.h GROUP BY b;
RESET ROLE;
GRANT SELECT ON qt.h TO qs_t_all;
SET ROLE qs_t_all;
EXECUTE p;
RESET ROLE;
DEALLOCATE p;

-- row-level security: a table that has it is never routed, for anyone
ALTER TABLE qt.rls ENABLE ROW LEVEL SECURITY;
CREATE POLICY mine ON qt.rls USING (owner_name = current_user);
SELECT b, count(*) FROM qt.rls GROUP BY b;
SELECT side, reason, detail FROM quicksilver.last_decision();
\c - qs_t_all
SET quicksilver.route = mirror;
SELECT id, b FROM qt.rls ORDER BY id;
SELECT side, reason, detail FROM quicksilver.last_decision();
\c - postgres
SET client_min_messages = warning;
SET quicksilver.route = mirror;
ALTER TABLE qt.rls DISABLE ROW LEVEL SECURITY;
SELECT b, count(*) FROM qt.rls GROUP BY b;

-- a registry outside the extension is not obeyed: only the tables CREATE
-- EXTENSION made are read, so nobody can point a table at a "mirror" of their choice
SELECT count(*) = 2 AS only_the_extensions_tables FROM pg_depend d
  WHERE d.refclassid = 'pg_extension'::regclass AND d.deptype = 'e' AND d.classid = 'pg_class'::regclass
    AND d.objid IN ('quicksilver.mirrors'::regclass, 'quicksilver.allowlist'::regclass);

DROP SCHEMA qt, qtm CASCADE;
DROP OWNED BY qs_t_all, qs_t_none, qs_t_col;
DROP ROLE qs_t_all, qs_t_none, qs_t_col;
SELECT quicksilver.prune();
