-- The freshness gate. The mirror's rows say 'mirror'.
\set last 'SELECT side, reason, regexp_replace(detail, ''[0-9A-F]+/[0-9A-F]+|[0-9]+'', ''N'', ''g'') AS detail FROM quicksilver.last_decision();'
SET client_min_messages = warning;
CREATE SCHEMA qt;
CREATE SCHEMA qtm;
CREATE TABLE qt.h (id int PRIMARY KEY, a int, b text);
INSERT INTO qt.h SELECT g, g * 10, 'heap' FROM generate_series(1, 3) g;
CREATE VIEW qtm.h AS SELECT r['id']::int AS id, r['a']::int AS a, r['b']::text AS b
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 10, 'mirror')) t(id, a, b) $$) r;
CREATE TABLE qt.filler (x text);
SET quicksilver.route = mirror;
-- The default is strict (max_lag = 0) and waits; the first half of this file
-- is about the bound, so it sets one and does not wait.
SHOW quicksilver.max_lag;
SHOW quicksilver.freshness_wait;
SET quicksilver.max_lag = '16MB';
SET quicksilver.freshness_wait = 0;

-- registered, but nobody has said what it has applied: not routed
SELECT quicksilver.register_mirror('qt.h', 'qtm.h');
SELECT b, count(*) FROM qt.h GROUP BY b;
:last
SELECT usable, applied_lsn IS NULL AS no_lsn, fresh FROM quicksilver.mirror_status() WHERE heap = 'qt.h'::regclass;

-- reported at the current position: fresh
SELECT quicksilver.report_applied_lsn('qt.h', pg_current_wal_lsn());
SELECT b, count(*) FROM qt.h GROUP BY b;

-- the server moves on by more than the bound: stale
SET quicksilver.max_lag = '64kB';
INSERT INTO qt.filler SELECT repeat(md5(g::text), 20) FROM generate_series(1, 2000) g;
SELECT b, count(*) FROM qt.h GROUP BY b;
:last
SELECT fresh FROM quicksilver.mirror_status() WHERE heap = 'qt.h'::regclass;
-- a wider bound for the session
SET quicksilver.max_lag = '1GB';
SELECT b, count(*) FROM qt.h GROUP BY b;
-- the registry row's own bound wins over the setting
UPDATE quicksilver.mirrors SET max_lag_bytes = 1000 WHERE heap = 'qt.h'::regclass;
SELECT b, count(*) FROM qt.h GROUP BY b;
UPDATE quicksilver.mirrors SET max_lag_bytes = NULL WHERE heap = 'qt.h'::regclass;
SELECT b, count(*) FROM qt.h GROUP BY b;
-- the mirror catches up
SET quicksilver.max_lag = '64kB';
SELECT quicksilver.report_applied_lsn('qt.h', pg_current_wal_lsn());
SELECT b, count(*) FROM qt.h GROUP BY b;
SET quicksilver.max_lag = '16MB';

-- the durable copy in the registry counts too; the newer of the two is believed
UPDATE quicksilver.mirrors SET applied_lsn = '0/1' WHERE heap = 'qt.h'::regclass;
SELECT b, count(*) FROM qt.h GROUP BY b;

-- read your writes: "the mirror must have applied at least what I just wrote"
INSERT INTO qt.h VALUES (4, 40, 'heap');
SELECT pg_current_wal_lsn() AS mine \gset
SET quicksilver.min_applied_lsn = :'mine';
SELECT b, count(*) FROM qt.h GROUP BY b;
:last
SELECT quicksilver.report_applied_lsn('qt.h', :'mine');
SELECT b, count(*) FROM qt.h GROUP BY b;
SET quicksilver.min_applied_lsn = 'not an lsn';
SET quicksilver.min_applied_lsn = '';
SELECT b, count(*) FROM qt.h GROUP BY b;

-- a join is as fresh as its stalest table
CREATE TABLE qt.g (id int PRIMARY KEY, b text);
INSERT INTO qt.g VALUES (1, 'heap');
CREATE VIEW qtm.g AS SELECT r['id']::int AS id, r['b']::text AS b FROM duckdb.query($$ SELECT 1 AS id, 'mirror' AS b $$) r;
SELECT quicksilver.register_mirror('qt.g', 'qtm.g', 0);
SELECT quicksilver.report_applied_lsn('qt.g', '0/10');
SELECT h.b, g.b FROM qt.h h JOIN qt.g g ON g.id = h.id ORDER BY h.id;
:last
SELECT quicksilver.report_applied_lsn('qt.g', pg_current_wal_insert_lsn() + 100000000);
SELECT h.b, g.b FROM qt.h h JOIN qt.g g ON g.id = h.id ORDER BY h.id;

-- ---------------------------------------------------------------- strict, the default
-- Routed only if the mirror has reached everything this statement can see.
RESET quicksilver.max_lag;
SELECT quicksilver.report_applied_lsn('qt.g', pg_current_wal_insert_lsn() + 100000000);
SELECT quicksilver.report_applied_lsn('qt.h', pg_current_wal_insert_lsn());
INSERT INTO qt.filler VALUES ('one more record');     -- the node moves on, by a few bytes
SELECT b, count(*) FROM qt.h GROUP BY b;
:last
-- a mirror that is ahead of the node is not behind it
SELECT quicksilver.report_applied_lsn('qt.h', pg_current_wal_insert_lsn() + 100000000);
SELECT b, count(*) FROM qt.h GROUP BY b;
-- behind again; it is given freshness_wait to catch up, and does not
SELECT quicksilver.report_applied_lsn('qt.h', pg_current_wal_insert_lsn());
INSERT INTO qt.filler VALUES ('and another');
SET quicksilver.freshness_wait = '200ms';
SELECT b, count(*) FROM qt.h GROUP BY b;
:last
SET quicksilver.freshness_wait = 0;
-- bounded staleness is something a session asks for
SET quicksilver.max_lag = '1MB';
SELECT b, count(*) FROM qt.h GROUP BY b;
RESET quicksilver.max_lag;
-- 0/0 is "do not read it here", and the registry's durable copy does not overrule it
UPDATE quicksilver.mirrors SET applied_lsn = pg_current_wal_insert_lsn() + 100000000 WHERE heap = 'qt.h'::regclass;
SELECT quicksilver.report_applied_lsn('qt.h', '0/0');
SELECT b, count(*) FROM qt.h GROUP BY b;
:last
SELECT fresh, regexp_replace(stale_because, 'qt\.h: ', '') AS stale_because FROM quicksilver.mirror_status() WHERE heap = 'qt.h'::regclass;
UPDATE quicksilver.mirrors SET applied_lsn = NULL WHERE heap = 'qt.h'::regclass;

-- ---------------------------------------------------------------- a transaction that has written
SELECT quicksilver.report_applied_lsn('qt.h', pg_current_wal_insert_lsn() + 100000000);
SET quicksilver.route_in_transaction = on;
BEGIN;
SELECT b, count(*) FROM qt.h GROUP BY b;              -- nothing written yet: the mirror
INSERT INTO qt.h VALUES (5, 50, 'mine');
SELECT b, count(*) FROM qt.h GROUP BY b ORDER BY b;   -- the mirror cannot show it its own row
:last
ROLLBACK;
RESET quicksilver.route_in_transaction;

DROP SCHEMA qt, qtm CASCADE;
SELECT quicksilver.prune();
