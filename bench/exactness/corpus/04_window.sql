-- Window functions.

-- @id win-001
SELECT id, row_number() OVER (ORDER BY id) AS rn FROM t_num WHERE id <= 50 ORDER BY id;
-- @id win-002
SELECT id, g, row_number() OVER (PARTITION BY g ORDER BY id) AS rn FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-003
SELECT id, g, rank() OVER (ORDER BY g) AS rk, dense_rank() OVER (ORDER BY g) AS drk FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-004
SELECT id, i, rank() OVER (ORDER BY i) AS rk FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-005
SELECT id, i, rank() OVER (ORDER BY i DESC) AS rk FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-006
SELECT id, g, rank() OVER (ORDER BY g) AS rk FROM t_str WHERE id <= 100 ORDER BY id;
-- @id win-007
SELECT id, sum(i) OVER (ORDER BY id) AS running FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-008
SELECT id, sum(n) OVER (ORDER BY id) AS running FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-009
SELECT id, sum(n) OVER (PARTITION BY g ORDER BY id) AS running FROM t_num WHERE id <= 200 ORDER BY id;
-- @id win-010
SELECT id, avg(i) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-011
SELECT id, avg(n) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS ma FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-012
SELECT id, avg(d) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ma FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-013
SELECT id, lag(i) OVER (ORDER BY id) AS prev, lead(i) OVER (ORDER BY id) AS next FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-014
SELECT id, lag(i, 2, 0) OVER (ORDER BY id) AS prev2, i - lag(i) OVER (ORDER BY id) AS delta FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-015
SELECT id, lag(g) OVER (ORDER BY id) AS prev FROM t_str WHERE id <= 100 ORDER BY id;
-- @id win-016
SELECT id, first_value(i) OVER (PARTITION BY g ORDER BY id) AS fv, last_value(i) OVER (PARTITION BY g ORDER BY id) AS lv FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-017
SELECT id, last_value(i) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-018
SELECT id, nth_value(i, 2) OVER (PARTITION BY g ORDER BY id) AS second FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-019
SELECT id, ntile(4) OVER (ORDER BY id) AS q FROM t_num WHERE id <= 101 ORDER BY id;
-- @id win-020
SELECT id, percent_rank() OVER (ORDER BY id) AS pr, cume_dist() OVER (ORDER BY id) AS cd FROM t_num WHERE id <= 40 ORDER BY id;
-- @id win-021
SELECT id, count(*) OVER () AS total, count(i) OVER (PARTITION BY g) AS in_group FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-022
SELECT id, max(n) OVER (PARTITION BY g) AS gmax, min(n) OVER (PARTITION BY g) AS gmin FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-023
SELECT id, n, n / sum(n) OVER (PARTITION BY g) AS share FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-024
SELECT id, round(100.0 * n / sum(n) OVER (), 2) AS pct FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-025
SELECT id, i, rank() OVER (ORDER BY i NULLS FIRST) AS rk FROM t_num WHERE id <= 60 ORDER BY id;
-- @id win-026
SELECT id, i, row_number() OVER (ORDER BY i DESC, id) AS rn FROM t_num WHERE id <= 60 ORDER BY id;
-- @id win-027
SELECT id, sum(i) OVER (ORDER BY g RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-028
SELECT id, sum(i) OVER w AS s, count(*) OVER w AS c FROM t_num WHERE id <= 100 WINDOW w AS (PARTITION BY g ORDER BY id) ORDER BY id;
-- @id win-029
SELECT id, sum(qty) OVER (PARTITION BY cust_id ORDER BY placed, id) AS running_qty FROM orders WHERE cust_id <= 5 ORDER BY id;
-- @id win-030
SELECT cust_id, id, amount FROM (SELECT cust_id, id, amount, row_number() OVER (PARTITION BY cust_id ORDER BY amount DESC NULLS LAST, id) AS rn FROM orders WHERE cust_id <= 20) s WHERE rn = 1 ORDER BY cust_id;
-- @id win-031
SELECT id, placed, placed - lag(placed) OVER (ORDER BY placed, id) AS gap FROM orders WHERE cust_id = 7 ORDER BY placed, id;
-- @id win-032
SELECT id, sum(amount) OVER (ORDER BY placed RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW) AS week_total FROM orders WHERE cust_id <= 3 AND placed IS NOT NULL ORDER BY id;
-- @id win-033
SELECT id, string_agg(g, '' ) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS pair FROM t_str WHERE id <= 30 ORDER BY id;
-- @id win-034
SELECT id, d, d - first_value(d) OVER (ORDER BY id) AS days_since_first FROM t_time WHERE id <= 30 ORDER BY id;
-- @id win-035
SELECT id, avg(i) OVER (PARTITION BY g) AS gavg FROM t_num WHERE id <= 100 ORDER BY id;
-- @id win-036
SELECT g, n FROM (SELECT g, count(*) AS n, rank() OVER (ORDER BY count(*) DESC, g) AS rk FROM t_num GROUP BY g) s WHERE rk <= 3 ORDER BY rk;
