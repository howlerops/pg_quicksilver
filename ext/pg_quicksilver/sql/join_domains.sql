-- Join domains (docs/40, "Join keys"): an equality of two uuid columns of mirrored
-- tables, both in one domain of quicksilver.join_domains, is compared on the
-- mirror as the two companion columns. The mirrors here are stand-ins built from
-- VALUES, with the companions the sidecar would write; the rows of
-- quicksilver.join_domains are set by hand, as the sidecar sets them.
\set last 'SELECT side, reason, detail FROM quicksilver.last_decision();'
SET client_min_messages = warning;
SET search_path = public;
SET quicksilver.route = mirror;
SET quicksilver.negative_cache_ttl = 0;

CREATE SCHEMA jd;
CREATE SCHEMA jdm;
CREATE TABLE jd.cust (id uuid PRIMARY KEY, region text COLLATE "C" NOT NULL);
CREATE TABLE jd.ord (id int PRIMARY KEY, cust uuid, amount int);
CREATE TABLE jd.note (id int PRIMARY KEY, cust uuid);
INSERT INTO jd.cust VALUES
  ('00000000-0000-0000-0000-000000000001', 'east'),
  ('00000000-0000-0000-0000-000000000002', 'west'),
  ('00000000-0000-0000-0000-000000000003', 'east'),
  ('00000000-0000-0000-0000-000000000004', 'west');
-- 3 has no orders; 9 is in no customer; 4 has one order; one order has a NULL key
INSERT INTO jd.ord VALUES
  (1, '00000000-0000-0000-0000-000000000001', 10),
  (2, '00000000-0000-0000-0000-000000000001', 20),
  (3, '00000000-0000-0000-0000-000000000002', 30),
  (4, NULL, 40),
  (5, '00000000-0000-0000-0000-000000000009', 50),
  (6, '00000000-0000-0000-0000-000000000004', 60);
INSERT INTO jd.note VALUES
  (1, '00000000-0000-0000-0000-000000000003'),
  (2, NULL),
  (3, '00000000-0000-0000-0000-000000000002');

-- The stand-in mirrors: the same rows, with the companion each domain gives
-- (customers 1..4 are 1..4; 9 is the next id the domain hands out, 5).
CREATE VIEW jdm.cust AS
  SELECT r['id']::uuid AS id, (r['region'])::text COLLATE "C" AS region, r['__qs_d_id']::bigint AS "__qs_d_id"
  FROM duckdb.query($$ SELECT * FROM (VALUES
    (CAST('00000000-0000-0000-0000-000000000001' AS UUID), 'east', CAST(1 AS BIGINT)),
    (CAST('00000000-0000-0000-0000-000000000002' AS UUID), 'west', CAST(2 AS BIGINT)),
    (CAST('00000000-0000-0000-0000-000000000003' AS UUID), 'east', CAST(3 AS BIGINT)),
    (CAST('00000000-0000-0000-0000-000000000004' AS UUID), 'west', CAST(4 AS BIGINT))
  ) t(id, region, __qs_d_id) $$) r;
CREATE VIEW jdm.ord AS
  SELECT r['id']::int AS id, r['cust']::uuid AS cust, r['amount']::int AS amount,
         r['__qs_d_cust']::bigint AS "__qs_d_cust"
  FROM duckdb.query($$ SELECT * FROM (VALUES
    (1, CAST('00000000-0000-0000-0000-000000000001' AS UUID), 10, CAST(1 AS BIGINT)),
    (2, CAST('00000000-0000-0000-0000-000000000001' AS UUID), 20, CAST(1 AS BIGINT)),
    (3, CAST('00000000-0000-0000-0000-000000000002' AS UUID), 30, CAST(2 AS BIGINT)),
    (4, CAST(NULL AS UUID), 40, CAST(NULL AS BIGINT)),
    (5, CAST('00000000-0000-0000-0000-000000000009' AS UUID), 50, CAST(5 AS BIGINT)),
    (6, CAST('00000000-0000-0000-0000-000000000004' AS UUID), 60, CAST(4 AS BIGINT))
  ) t(id, cust, amount, __qs_d_cust) $$) r;
CREATE VIEW jdm.note AS
  SELECT r['id']::int AS id, r['cust']::uuid AS cust, r['__qs_o_cust']::bigint AS "__qs_o_cust"
  FROM duckdb.query($$ SELECT * FROM (VALUES
    (1, CAST('00000000-0000-0000-0000-000000000003' AS UUID), CAST(3 AS BIGINT)),
    (2, CAST(NULL AS UUID), CAST(NULL AS BIGINT)),
    (3, CAST('00000000-0000-0000-0000-000000000002' AS UUID), CAST(2 AS BIGINT))
  ) t(id, cust, __qs_o_cust) $$) r;

SELECT quicksilver.register_mirror('jd.cust', 'jdm.cust', 1000000000000);
SELECT quicksilver.register_mirror('jd.ord', 'jdm.ord', 1000000000000);
SELECT quicksilver.register_mirror('jd.note', 'jdm.note', 1000000000000);
-- domain "cust": customers.id and orders.cust. note.cust is in domain "other".
INSERT INTO quicksilver.join_domains VALUES
  ('jd.cust'::regclass, 'id', 'cust', '__qs_d_id'),
  ('jd.ord'::regclass, 'cust', 'cust', '__qs_d_cust'),
  ('jd.note'::regclass, 'cust', 'other', '__qs_o_cust');
-- last: the mirrors are fresh as of the registry as it is now
SELECT quicksilver.report_applied_lsn('jd.cust', pg_current_wal_lsn());
SELECT quicksilver.report_applied_lsn('jd.ord', pg_current_wal_lsn());
SELECT quicksilver.report_applied_lsn('jd.note', pg_current_wal_lsn());
-- and this node reads each domain column complete. The registry rows say which
-- columns are members; only the node's report lets a join compare their companions.
SELECT quicksilver.report_join_complete('jd.cust', 'id', true);
SELECT quicksilver.report_join_complete('jd.ord', 'cust', true);
SELECT quicksilver.report_join_complete('jd.note', 'cust', true);

-- ---------------------------------------------------------------- the rewrite
-- inner join: rewritten, and the same answer as the heap
EXPLAIN (COSTS OFF) SELECT c.region, sum(o.amount) FROM jd.cust c JOIN jd.ord o ON o.cust = c.id GROUP BY c.region ORDER BY 1;
SELECT c.region, sum(o.amount) FROM jd.cust c JOIN jd.ord o ON o.cust = c.id GROUP BY c.region ORDER BY 1;
:last
SET quicksilver.route = heap;
SELECT c.region, sum(o.amount) FROM jd.cust c JOIN jd.ord o ON o.cust = c.id GROUP BY c.region ORDER BY 1;
SET quicksilver.route = mirror;

-- the same, as an implicit join in WHERE
EXPLAIN (COSTS OFF) SELECT c.region, o.amount FROM jd.cust c, jd.ord o WHERE c.id = o.cust AND o.amount > 15 ORDER BY 2;
SELECT c.region, o.amount FROM jd.cust c, jd.ord o WHERE c.id = o.cust AND o.amount > 15 ORDER BY 2;
:last

-- a join of the orders to themselves: both sides in the domain
SELECT count(*) FROM jd.ord a JOIN jd.ord b ON a.cust = b.cust;
:last

-- ---------------------------------------------------------------- not rewritten
-- a column this node does not read complete: the registry row is there, the join is
-- compared on the uuid columns, and the answer is the heap's
SELECT quicksilver.report_join_complete('jd.ord', 'cust', false);
EXPLAIN (COSTS OFF) SELECT c.region, sum(o.amount) FROM jd.cust c JOIN jd.ord o ON o.cust = c.id GROUP BY c.region ORDER BY 1;
SELECT c.region, sum(o.amount) FROM jd.cust c JOIN jd.ord o ON o.cust = c.id GROUP BY c.region ORDER BY 1;
:last
-- reported again: rewritten again
SELECT quicksilver.report_join_complete('jd.ord', 'cust', true);
EXPLAIN (COSTS OFF) SELECT c.region, sum(o.amount) FROM jd.cust c JOIN jd.ord o ON o.cust = c.id GROUP BY c.region ORDER BY 1;

-- one side not in the domain (the registry row for orders.cust is gone)
DELETE FROM quicksilver.join_domains WHERE heap = 'jd.ord'::regclass;
EXPLAIN (COSTS OFF) SELECT c.region, sum(o.amount) FROM jd.cust c JOIN jd.ord o ON o.cust = c.id GROUP BY c.region ORDER BY 1;
SELECT c.region, sum(o.amount) FROM jd.cust c JOIN jd.ord o ON o.cust = c.id GROUP BY c.region ORDER BY 1;
:last
INSERT INTO quicksilver.join_domains VALUES ('jd.ord'::regclass, 'cust', 'cust', '__qs_d_cust');

-- the GUC is off: the uuid columns are compared, as before
SET quicksilver.join_domains = off;
EXPLAIN (COSTS OFF) SELECT c.region, sum(o.amount) FROM jd.cust c JOIN jd.ord o ON o.cust = c.id GROUP BY c.region ORDER BY 1;
SELECT c.region, sum(o.amount) FROM jd.cust c JOIN jd.ord o ON o.cust = c.id GROUP BY c.region ORDER BY 1;
:last
SET quicksilver.join_domains = on;

-- the operator differs: a cast to text on one side is not a uuid equality
EXPLAIN (COSTS OFF) SELECT count(*) FROM jd.cust c JOIN jd.ord o ON o.cust::text = c.id::text;
SELECT count(*) FROM jd.cust c JOIN jd.ord o ON o.cust::text = c.id::text;
:last

-- two domains: note.cust is in "other", customers.id in "cust"
EXPLAIN (COSTS OFF) SELECT count(*) FROM jd.cust c JOIN jd.note n ON n.cust = c.id;
SELECT count(*) FROM jd.cust c JOIN jd.note n ON n.cust = c.id;
:last

-- ---------------------------------------------------------------- outer joins
-- NULL keys and unmatched rows on both sides come out as the heap has them
SELECT o.id, c.region FROM jd.ord o LEFT JOIN jd.cust c ON c.id = o.cust ORDER BY o.id;
:last
SET quicksilver.route = heap;
SELECT o.id, c.region FROM jd.ord o LEFT JOIN jd.cust c ON c.id = o.cust ORDER BY o.id;
SET quicksilver.route = mirror;
EXPLAIN (COSTS OFF) SELECT o.id, c.region FROM jd.ord o LEFT JOIN jd.cust c ON c.id = o.cust ORDER BY o.id;
SELECT c.id IS NULL AS no_customer, count(*), sum(o.amount) FROM jd.cust c FULL JOIN jd.ord o ON o.cust = c.id GROUP BY 1 ORDER BY 1;
:last
SET quicksilver.route = heap;
SELECT c.id IS NULL AS no_customer, count(*), sum(o.amount) FROM jd.cust c FULL JOIN jd.ord o ON o.cust = c.id GROUP BY 1 ORDER BY 1;
SET quicksilver.route = mirror;
SELECT o.id, c.region FROM jd.cust c RIGHT JOIN jd.ord o ON c.id = o.cust ORDER BY o.id;
:last

-- ---------------------------------------------------------------- three tables
-- only one of the two pairs is in a domain
EXPLAIN (COSTS OFF) SELECT c.region, o.amount, n.id FROM jd.cust c JOIN jd.ord o ON o.cust = c.id JOIN jd.note n ON n.cust = c.id ORDER BY 1, 2, 3;
SELECT c.region, o.amount, n.id FROM jd.cust c JOIN jd.ord o ON o.cust = c.id JOIN jd.note n ON n.cust = c.id ORDER BY 1, 2, 3;
:last

-- ---------------------------------------------------------------- result shape
-- SELECT *: the output is the heap's columns, and no companion
SELECT * FROM jd.ord o JOIN jd.cust c ON c.id = o.cust ORDER BY o.id;
:last
-- a whole-row reference is not routed, so no companion can show in a record
SELECT o FROM jd.ord o JOIN jd.cust c ON c.id = o.cust ORDER BY o.id;
:last
SELECT count(*) FROM jd.ord o JOIN jd.cust c ON c.id = o.cust;
:last

-- a join in a FROM subquery is rewritten at its own level
EXPLAIN (COSTS OFF) SELECT region, sum(amount) FROM (SELECT c.region, o.amount FROM jd.cust c JOIN jd.ord o ON o.cust = c.id) s GROUP BY region ORDER BY 1;
SELECT region, sum(amount) FROM (SELECT c.region, o.amount FROM jd.cust c JOIN jd.ord o ON o.cust = c.id) s GROUP BY region ORDER BY 1;

-- ---------------------------------------------------------------- prepared
PREPARE jdq (text) AS SELECT count(*), sum(o.amount) FROM jd.ord o JOIN jd.cust c ON c.id = o.cust WHERE c.region = $1;
EXECUTE jdq('east');
EXECUTE jdq('west');
:last
DEALLOCATE jdq;

-- ---------------------------------------------------------------- cleanup
SET quicksilver.route = off;
DELETE FROM quicksilver.join_domains WHERE heap IN ('jd.cust'::regclass, 'jd.ord'::regclass, 'jd.note'::regclass);
DELETE FROM quicksilver.mirrors WHERE heap IN ('jd.cust'::regclass, 'jd.ord'::regclass, 'jd.note'::regclass);
DROP SCHEMA jd CASCADE;
DROP SCHEMA jdm CASCADE;
RESET quicksilver.route;
RESET quicksilver.negative_cache_ttl;
