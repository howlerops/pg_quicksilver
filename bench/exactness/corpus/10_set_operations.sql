-- Set operations.

-- @id set-001
SELECT g FROM t_num WHERE id <= 30 UNION SELECT g FROM t_num WHERE id > 1990 AND id < 9000;
-- @id set-002
SELECT g FROM t_num WHERE id <= 5 UNION ALL SELECT g FROM t_num WHERE id <= 5;
-- @id set-003
SELECT g FROM t_str WHERE id <= 30 UNION SELECT g FROM t_str WHERE id BETWEEN 20 AND 60 ORDER BY g;
-- @id set-004
SELECT lower(g) AS g FROM t_str WHERE id <= 60 INTERSECT SELECT g FROM t_str WHERE id <= 60;
-- @id set-005
SELECT g FROM t_str WHERE id <= 60 EXCEPT SELECT lower(g) FROM t_str WHERE id <= 60;
-- @id set-006
SELECT i FROM t_num WHERE id <= 100 INTERSECT SELECT i FROM t_num WHERE id BETWEEN 50 AND 150 ORDER BY i;
-- @id set-007
SELECT id FROM t_num WHERE id <= 20 EXCEPT SELECT id FROM t_num WHERE id % 2 = 0 ORDER BY id;
-- @id set-008
SELECT g FROM t_num WHERE id <= 12 EXCEPT ALL SELECT g FROM t_num WHERE id <= 3;
-- @id set-009
SELECT g FROM t_num WHERE id <= 12 INTERSECT ALL SELECT g FROM t_num WHERE id BETWEEN 8 AND 24;
-- @id set-010
SELECT i AS v FROM t_num WHERE id <= 5 UNION SELECT n FROM t_num WHERE id <= 5;
-- @id set-011
SELECT i AS v FROM t_num WHERE id <= 5 UNION ALL SELECT d FROM t_num WHERE id <= 5;
-- @id set-012
SELECT si AS v FROM t_num WHERE id <= 5 UNION ALL SELECT bi FROM t_num WHERE id <= 5;
-- @id set-013
SELECT 1 AS v UNION SELECT 1.0 UNION SELECT 1.00;
-- @id set-014
SELECT 1 AS v UNION SELECT NULL UNION SELECT NULL;
-- @id set-015
SELECT 'a' AS v UNION SELECT 'A' UNION SELECT 'a ' ORDER BY v;
-- @id set-016
SELECT n AS v FROM t_num WHERE id IN (9004, 9005) UNION SELECT nu FROM t_num WHERE id IN (9004, 9005);
-- @id set-017
SELECT ch AS v FROM t_str WHERE id <= 40 INTERSECT SELECT g FROM t_str;
-- @id set-018
SELECT ch AS v FROM t_str WHERE id <= 40 UNION SELECT vc FROM t_str WHERE id <= 5;
-- @id set-019
SELECT e::text AS v FROM t_str UNION SELECT tier::text FROM customers ORDER BY v;
-- @id set-020
SELECT e AS v FROM t_str UNION SELECT tier FROM customers ORDER BY v;
-- @id set-021
SELECT region FROM customers EXCEPT SELECT 'north' ORDER BY region NULLS FIRST;
-- @id set-022
SELECT cust_id FROM orders WHERE status = 'paid' INTERSECT SELECT cust_id FROM orders WHERE status = 'cancelled' ORDER BY cust_id;
-- @id set-023
SELECT id FROM customers EXCEPT SELECT cust_id FROM orders ORDER BY id;
-- @id set-024
SELECT cust_id FROM orders EXCEPT SELECT id FROM customers ORDER BY cust_id;
-- @id set-025
(SELECT id, 'low' AS band FROM orders WHERE amount < 1 ORDER BY id LIMIT 3) UNION ALL (SELECT id, 'high' FROM orders WHERE amount > 999 ORDER BY id LIMIT 3) ORDER BY band, id;
-- @id set-026
SELECT d::timestamp AS v FROM t_time WHERE id <= 3 UNION ALL SELECT ts FROM t_time WHERE id <= 3;
-- @id set-027
SELECT d AS v FROM t_time WHERE id <= 3 UNION ALL SELECT tz FROM t_time WHERE id <= 3;
-- @id set-028
SELECT count(*) AS n FROM (SELECT u FROM t_str UNION SELECT u FROM t_str WHERE id > 1000) s;
-- @id set-029
SELECT b FROM t_num UNION SELECT NULL ORDER BY b;
-- @id set-030
VALUES (1, 'one'), (2, 'two'), (3, NULL) ORDER BY 1 DESC;
