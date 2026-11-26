-- Subqueries and joins, including the NULL semantics of NOT IN.

-- @id join-001
SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.cust_id WHERE o.id <= 60 ORDER BY o.id;
-- @id join-002
SELECT o.id, c.name FROM orders o LEFT JOIN customers c ON c.id = o.cust_id WHERE o.id <= 60 ORDER BY o.id;
-- @id join-003
SELECT count(*) AS n, count(c.id) AS matched FROM orders o LEFT JOIN customers c ON c.id = o.cust_id;
-- @id join-004
SELECT count(*) AS n FROM orders o RIGHT JOIN customers c ON c.id = o.cust_id WHERE o.id IS NULL;
-- @id join-005
SELECT count(*) AS n, count(o.id) AS o_side, count(c.id) AS c_side FROM orders o FULL JOIN customers c ON c.id = o.cust_id;
-- @id join-006
SELECT c.region, count(*) AS n, sum(o.amount) AS total FROM orders o JOIN customers c ON c.id = o.cust_id GROUP BY c.region ORDER BY total DESC NULLS LAST, c.region;
-- @id join-007
SELECT c.id, c.name, count(o.id) AS n_orders, coalesce(sum(o.amount), 0) AS total FROM customers c LEFT JOIN orders o ON o.cust_id = c.id GROUP BY c.id, c.name ORDER BY c.id LIMIT 40;
-- @id join-008
SELECT c.tier, round(avg(o.amount), 2) AS avg_amount FROM customers c JOIN orders o ON o.cust_id = c.id GROUP BY c.tier ORDER BY c.tier;
-- @id join-009
SELECT a.id, b.id AS other FROM t_num a JOIN t_num b ON a.i = b.i AND a.id < b.id ORDER BY a.id, b.id LIMIT 30;
-- @id join-010
SELECT count(*) AS n FROM t_num a JOIN t_num b ON a.i = b.si;
-- @id join-011
SELECT count(*) AS n FROM t_num a JOIN t_num b ON a.n = b.i;
-- @id join-012
SELECT count(*) AS n FROM t_num a JOIN t_num b ON a.d = b.r WHERE a.id >= 9000 AND b.id >= 9000;
-- @id join-013
SELECT count(*) AS n FROM t_num a JOIN t_num b ON a.r = b.r WHERE a.id >= 9000 AND b.id >= 9000;
-- @id join-014
SELECT count(*) AS n FROM t_str a JOIN t_str b ON a.g = b.t;
-- @id join-015
SELECT count(*) AS n FROM t_str a JOIN t_str b ON a.ch = b.g WHERE a.id <= 200;
-- @id join-016
SELECT count(*) AS n FROM t_str a JOIN t_str b ON lower(a.g) = lower(b.g) WHERE a.id <= 50 AND b.id <= 50;
-- @id join-017
SELECT count(*) AS n FROM t_str a JOIN t_str b USING (u) WHERE a.id < b.id;
-- @id join-018
SELECT count(*) AS n FROM t_num a NATURAL JOIN t_num b;
-- @id join-019
SELECT count(*) AS n FROM t_num a CROSS JOIN (SELECT 1 AS k UNION ALL SELECT 2) x WHERE a.id <= 10;
-- @id join-020
SELECT s.id, n.g FROM t_str s JOIN t_num n USING (id) WHERE s.id <= 30 ORDER BY s.id;
-- @id join-021
SELECT count(*) AS n FROM customers WHERE id IN (SELECT cust_id FROM orders);
-- @id join-022
SELECT count(*) AS n FROM customers WHERE id NOT IN (SELECT cust_id FROM orders);
-- @id join-023
SELECT count(*) AS n FROM customers WHERE id NOT IN (SELECT cust_id FROM orders WHERE cust_id IS NOT NULL);
-- @id join-024
SELECT count(*) AS n FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.cust_id = c.id);
-- @id join-025
SELECT count(*) AS n FROM orders WHERE cust_id NOT IN (SELECT id FROM customers);
-- @id join-026
SELECT count(*) AS n FROM orders o WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.id = o.cust_id);
-- @id join-027
SELECT count(*) AS n FROM t_num WHERE i NOT IN (SELECT si FROM t_num);
-- @id join-028
SELECT count(*) AS n FROM t_num WHERE i NOT IN (SELECT si FROM t_num WHERE si IS NOT NULL);
-- @id join-029
SELECT count(*) AS n FROM t_num WHERE g = ANY (SELECT g FROM t_num WHERE id >= 9000);
-- @id join-030
SELECT count(*) AS n FROM t_num WHERE i > ALL (SELECT si FROM t_num WHERE id <= 100);
-- @id join-031
SELECT count(*) AS n FROM t_num WHERE i > ALL (SELECT si FROM t_num WHERE si IS NOT NULL AND id <= 100);
-- @id join-032
SELECT count(*) AS n FROM t_num WHERE i > (SELECT avg(i) FROM t_num WHERE id < 9000);
-- @id join-033
SELECT id, (SELECT count(*) FROM orders o WHERE o.cust_id = c.id) AS n_orders, (SELECT max(amount) FROM orders o WHERE o.cust_id = c.id) AS biggest FROM customers c WHERE c.id <= 30 ORDER BY id;
-- @id join-034
SELECT id, amount FROM orders o WHERE amount > (SELECT avg(amount) FROM orders o2 WHERE o2.cust_id = o.cust_id) AND cust_id <= 10 ORDER BY id;
-- @id join-035
SELECT c.id, last.id AS last_order, last.amount FROM customers c LEFT JOIN LATERAL (SELECT id, amount FROM orders o WHERE o.cust_id = c.id ORDER BY placed DESC NULLS LAST, id DESC LIMIT 1) last ON true WHERE c.id <= 30 ORDER BY c.id;
-- @id join-036
WITH totals AS (SELECT cust_id, sum(amount) AS total FROM orders GROUP BY cust_id) SELECT c.name, t.total FROM totals t JOIN customers c ON c.id = t.cust_id ORDER BY t.total DESC NULLS LAST, c.name LIMIT 15;
-- @id join-037
WITH ranked AS (SELECT cust_id, amount, rank() OVER (PARTITION BY cust_id ORDER BY amount DESC NULLS LAST) AS rk FROM orders WHERE cust_id <= 15) SELECT cust_id, amount FROM ranked WHERE rk = 1 ORDER BY cust_id, amount;
-- @id join-038
WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) SELECT n, n * n AS sq FROM r ORDER BY n;
-- @id join-039
WITH RECURSIVE fib(a, b) AS (SELECT 0::bigint, 1::bigint UNION ALL SELECT b, a + b FROM fib WHERE b < 1000) SELECT a FROM fib ORDER BY a;
-- @id join-040
SELECT (SELECT max(i) FROM t_num WHERE id < 9000) AS hi, (SELECT min(n) FROM t_num) AS lo, (SELECT g FROM t_str WHERE id = 1) AS g1, (SELECT i FROM t_num WHERE id = -1) AS missing;
-- @id join-041
SELECT (SELECT i FROM t_num WHERE id <= 2) AS too_many;
-- @id join-042
SELECT g, n FROM (SELECT g, count(*) AS n FROM t_str GROUP BY g) s WHERE n > 40 ORDER BY n DESC, g;
-- @id join-043
SELECT o.status, c.region, count(*) AS n FROM orders o JOIN customers c ON c.id = o.cust_id WHERE o.status IS NOT NULL AND c.region IS NOT NULL GROUP BY o.status, c.region ORDER BY n DESC, o.status, c.region LIMIT 12;
-- @id join-044
SELECT count(*) AS n FROM customers c JOIN orders o ON o.cust_id = c.id AND o.placed::date = c.created;
-- @id join-045
SELECT count(*) AS n FROM t_time a JOIN t_time b ON a.d = b.ts::date WHERE a.id <= 300 AND b.id <= 300;
-- @id join-046
SELECT count(*) AS n FROM t_time a JOIN t_time b ON a.d = b.tz::date WHERE a.id <= 300 AND b.id <= 300;
-- @id join-047
SELECT count(*) AS n FROM t_str s JOIN customers c ON s.e = c.tier WHERE s.id <= 50;
-- @id join-048
SELECT EXISTS (SELECT 1 FROM orders WHERE amount IS NULL) AS has_null_amount, EXISTS (SELECT 1 FROM orders WHERE amount < 0) AS has_negative, (SELECT count(*) FROM orders) > 0 AS nonempty;
