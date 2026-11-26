-- The routing rules, on the real mirror (public.orders, 1M rows) and on a
-- fabricated one whose rows differ from the heap's, so that every answer says
-- which side it came from.
\set last 'SELECT side, reason FROM quicksilver.last_decision();'
SET search_path = public;
SELECT quicksilver.forget_last();

-- off (the default): the extension does nothing and records nothing
SHOW quicksilver.route;
SELECT count(*) FROM orders;
:last

SET quicksilver.route = auto;

-- expensive on the heap, every construct allowlisted: the mirror
SELECT count(*) FROM orders;
:last
SELECT status, count(*), sum(amount) FROM orders GROUP BY status ORDER BY count(*), sum(amount);
:last
SELECT c.region, sum(o.amount) FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY c.region ORDER BY 2;
:last

-- cheap on the heap: stays, and EXPLAIN says why
SELECT * FROM orders WHERE id = 4242;
:last
EXPLAIN (COSTS OFF) SELECT * FROM orders WHERE id = 4242;
SELECT id FROM orders WHERE id IN (1, 2, 3, 500000) ORDER BY id;
:last

-- a scan under a LIMIT is labelled with the whole table but reads ten rows
EXPLAIN (COSTS OFF) SELECT id FROM orders ORDER BY id LIMIT 10;
SELECT id FROM orders ORDER BY id LIMIT 3;
:last
-- ... unless a sort stands between them
SELECT id, amount FROM orders ORDER BY amount DESC, id LIMIT 3;
:last

-- the thresholds are settings
SET quicksilver.min_rows = 2000000;
SET quicksilver.min_cost = 1e9;
SELECT count(*) FROM orders;
:last
SET quicksilver.min_cost = 5;
SELECT * FROM orders WHERE id = 4242;
:last
RESET quicksilver.min_rows;
RESET quicksilver.min_cost;

-- heap: never; mirror: whatever the cost
SET quicksilver.route = heap;
SELECT count(*) FROM orders;
:last
SET quicksilver.route = mirror;
SELECT * FROM orders WHERE id = 4242;
:last
SET quicksilver.route = auto;

-- a join with a table that has no mirror
SELECT count(*) FROM orders o JOIN audit_log a ON a.id = o.id;
:last
EXPLAIN (COSTS OFF) SELECT count(*) FROM orders o JOIN audit_log a ON a.id = o.id;

-- a statement that reads no mirrored table is not the extension's business
SELECT quicksilver.forget_last();
SELECT count(*) FROM audit_log;
:last

-- ---------------------------------------------------------------- which side?
-- heap rows say 'heap', the mirror's say 'mirror'. The mirror's columns are in
-- another order, and the heap has a dropped column in the middle.
SET client_min_messages = warning;
CREATE SCHEMA qt;
CREATE SCHEMA qtm;
CREATE TABLE qt.h (id int PRIMARY KEY, gone int, a int, n numeric(10,2), b text);
INSERT INTO qt.h SELECT g, 0, g * 10, g + 0.25, 'heap' FROM generate_series(1, 4) g;
ALTER TABLE qt.h DROP COLUMN gone;
CREATE VIEW qtm.h AS
  SELECT r['b']::text AS b, r['n']::numeric(10,2) AS n, r['id']::int AS id, r['a']::int AS a
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 10, 1.25, 'mirror'), (2, 20, 2.25, 'mirror'),
                                             (3, 30, 3.25, 'mirror'), (4, 40, 4.25, 'mirror')) t(id, a, n, b) $$) r;
SELECT quicksilver.register_mirror('qt.h', 'qtm.h', 1000000000000);
SELECT quicksilver.report_applied_lsn('qt.h', pg_current_wal_lsn());
SELECT heap, mirror, usable, reason, unusable_columns, fresh FROM quicksilver.mirror_status() WHERE heap = 'qt.h'::regclass;

SET quicksilver.min_cost = 0;
SELECT id, a, n, b FROM qt.h ORDER BY id;
:last
SELECT * FROM qt.h ORDER BY id;
SELECT b, count(*), sum(a), sum(n), min(n), max(id) FROM qt.h GROUP BY b;
-- the same statement from each side (validate/equality.py compares the RowDescription)
SET quicksilver.route = heap;
SELECT b, count(*) AS c, sum(a) AS s, sum(n) AS sn, sum(id::bigint) AS sb, min(n) AS mn FROM qt.h GROUP BY b;
SET quicksilver.route = auto;
SELECT b, count(*) AS c, sum(a) AS s, sum(n) AS sn, sum(id::bigint) AS sb, min(n) AS mn FROM qt.h GROUP BY b;

-- a view over a mirrored table, a subquery, an IN list
CREATE VIEW qt.v AS SELECT id, a, b FROM qt.h WHERE a >= 20;
SELECT * FROM qt.v ORDER BY id;
:last
SELECT q.b, q.s FROM (SELECT b, sum(a) AS s FROM qt.h GROUP BY b) q WHERE q.s > 5;
:last
SELECT id, b FROM qt.h WHERE id IN (1, 3) ORDER BY id;
:last
-- a self-join reads the mirror twice
SELECT x.id, y.b FROM qt.h x JOIN qt.h y ON y.a = x.a AND y.id > 1 ORDER BY x.id;
:last
SELECT x.id, y.b FROM qt.h x LEFT JOIN qt.h y ON y.a = x.id ORDER BY x.id;
:last

-- observability
SELECT reason, statements > 0 AS seen FROM quicksilver.reason_stats WHERE reason IN ('routed', 'cheap on the heap') ORDER BY 1;
SELECT count(*) > 0 AS has_rows FROM quicksilver.decision_stats WHERE mirror > 0;

DROP SCHEMA qt, qtm CASCADE;
SELECT quicksilver.prune();
