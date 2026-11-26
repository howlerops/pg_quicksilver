-- Aggregates: result types and scale of avg/sum/stddev, count distinct,
-- bool_and, string_agg, array_agg with order.

-- @id agg-001
SELECT count(*) AS n FROM t_num;
-- @id agg-002
SELECT count(i) AS n, count(DISTINCT i) AS nd, count(*) - count(i) AS nulls FROM t_num;
-- @id agg-003
SELECT sum(i) AS s FROM t_num WHERE id < 9000;
-- @id agg-004
SELECT sum(si) AS s FROM t_num;
-- @id agg-005
SELECT sum(bi) AS s FROM t_num WHERE id < 9000;
-- @id agg-006
SELECT sum(n) AS s FROM t_num WHERE id < 9000;
-- @id agg-007
SELECT sum(nu) AS s FROM t_num WHERE id < 9000;
-- @id agg-008
SELECT sum(d) AS s FROM t_num WHERE id < 9000;
-- @id agg-009
SELECT sum(r) AS s FROM t_num WHERE id < 9000;
-- @id agg-010
SELECT avg(i) AS a FROM t_num WHERE id < 9000;
-- @id agg-011
SELECT avg(si) AS a FROM t_num WHERE id < 9000;
-- @id agg-012
SELECT avg(bi) AS a FROM t_num WHERE id < 9000;
-- @id agg-013
SELECT avg(n) AS a FROM t_num WHERE id < 9000;
-- @id agg-014
SELECT avg(d) AS a FROM t_num WHERE id < 9000;
-- @id agg-015
SELECT avg(r) AS a FROM t_num WHERE id < 9000;
-- @id agg-016
SELECT g, avg(i) AS a FROM t_num WHERE id < 9000 GROUP BY g ORDER BY g;
-- @id agg-017
SELECT g, avg(n) AS a, sum(n) AS s FROM t_num WHERE id < 9000 GROUP BY g ORDER BY g;
-- @id agg-018
SELECT g, round(avg(n), 2) AS a FROM t_num WHERE id < 9000 GROUP BY g ORDER BY g;
-- @id agg-019
SELECT g, avg(i)::numeric(12,2) AS a FROM t_num WHERE id < 9000 GROUP BY g ORDER BY g;
-- @id agg-020
SELECT min(i) AS lo, max(i) AS hi, min(n) AS nlo, max(n) AS nhi, min(d) AS dlo, max(d) AS dhi FROM t_num WHERE id < 9000;
-- @id agg-021
SELECT min(d) AS lo, max(d) AS hi, max(r) AS rhi FROM t_num;
-- @id agg-022
SELECT sum(d) AS s, avg(d) AS a FROM t_num WHERE id IN (9004, 9005, 9006);
-- @id agg-023
SELECT stddev(i) AS sd, variance(i) AS var FROM t_num WHERE id < 9000;
-- @id agg-024
SELECT stddev(n) AS sd, variance(n) AS var FROM t_num WHERE id < 9000;
-- @id agg-025
SELECT stddev(d) AS sd, var_pop(d) AS vp, stddev_pop(d) AS sp, var_samp(d) AS vs FROM t_num WHERE id < 9000;
-- @id agg-026
SELECT stddev_samp(n) AS sd, stddev_pop(n) AS sp FROM t_num WHERE id < 9000;
-- @id agg-027
SELECT g, count(DISTINCT i % 10) AS nd FROM t_num WHERE id < 9000 GROUP BY g ORDER BY g;
-- @id agg-028
SELECT count(DISTINCT g) AS n FROM t_str;
-- @id agg-029
SELECT count(DISTINCT lower(g)) AS n FROM t_str;
-- @id agg-030
SELECT count(DISTINCT ch) AS n, count(DISTINCT vc) AS nv FROM t_str;
-- @id agg-031
SELECT bool_and(b) AS all_true, bool_or(b) AS any_true, every(b) AS ev FROM t_num;
-- @id agg-032
SELECT g, bool_and(b) AS all_true, bool_or(b) AS any_true FROM t_num WHERE id < 9000 GROUP BY g ORDER BY g;
-- @id agg-033
SELECT g, count(*) FILTER (WHERE b) AS yes, count(*) FILTER (WHERE NOT b) AS no, count(*) FILTER (WHERE b IS NULL) AS unk FROM t_num GROUP BY g ORDER BY g;
-- @id agg-034
SELECT sum(i) FILTER (WHERE i > 0) AS pos, sum(i) FILTER (WHERE i < 0) AS neg FROM t_num WHERE id < 9000;
-- @id agg-035
SELECT string_agg(g, ',' ORDER BY id) AS s FROM t_str WHERE id <= 20;
-- @id agg-036
SELECT string_agg(g, ', ' ORDER BY g, id) AS s FROM t_str WHERE id <= 50;
-- @id agg-037
SELECT string_agg(DISTINCT g, '|' ORDER BY g) AS s FROM t_str WHERE id <= 60;
-- @id agg-038
SELECT string_agg(id::text, '-' ORDER BY id DESC) AS s FROM t_num WHERE id <= 12;
-- @id agg-039
SELECT e, string_agg(id::text, ',' ORDER BY id) AS ids FROM t_str WHERE id <= 30 GROUP BY e ORDER BY e;
-- @id agg-040
SELECT array_agg(id ORDER BY id) AS a FROM t_num WHERE id <= 10;
-- @id agg-041
SELECT array_agg(i ORDER BY id) AS a FROM t_num WHERE id BETWEEN 15 AND 25;
-- @id agg-042
SELECT array_agg(g ORDER BY id) AS a FROM t_str WHERE id BETWEEN 26 AND 45;
-- @id agg-043
SELECT array_agg(n ORDER BY n DESC NULLS LAST, id) AS a FROM t_num WHERE id <= 15;
-- @id agg-044
SELECT array_agg(DISTINCT g ORDER BY g) AS a FROM t_num WHERE id < 9000;
-- @id agg-045
SELECT g, array_agg(id ORDER BY id) AS ids FROM t_num WHERE id <= 30 GROUP BY g ORDER BY g;
-- @id agg-046
SELECT array_agg(b ORDER BY id) AS a FROM t_num WHERE id <= 10;
-- @id agg-047
SELECT array_agg(d ORDER BY id) AS a FROM t_num WHERE id BETWEEN 9003 AND 9009;
-- @id agg-048
SELECT array_agg(ts ORDER BY id) AS a FROM t_time WHERE id <= 4;
-- @id agg-049
SELECT status, count(*) AS n, sum(amount) AS total, avg(amount) AS mean, min(amount) AS lo, max(amount) AS hi FROM orders GROUP BY status ORDER BY status NULLS FIRST;
-- @id agg-050
SELECT cust_id, sum(amount) AS total FROM orders GROUP BY cust_id HAVING sum(amount) > 5000 ORDER BY total DESC, cust_id;
-- @id agg-051
SELECT cust_id, count(*) AS n FROM orders GROUP BY cust_id HAVING count(*) >= 10 ORDER BY n DESC, cust_id NULLS FIRST;
-- @id agg-052
SELECT sum(qty) AS q, avg(qty) AS aq, sum(amount * qty) AS rev FROM orders WHERE status = 'paid';
-- @id agg-053
SELECT status, round(avg(amount), 2) AS mean, round(sum(amount) / nullif(sum(qty), 0), 4) AS per_unit FROM orders GROUP BY status ORDER BY status;
-- @id agg-054
SELECT region, tier, count(*) AS n FROM customers GROUP BY ROLLUP (region, tier) ORDER BY region NULLS LAST, tier NULLS LAST, n;
-- @id agg-055
SELECT region, tier, count(*) AS n FROM customers GROUP BY GROUPING SETS ((region), (tier), ()) ORDER BY region NULLS LAST, tier NULLS LAST, n;
-- @id agg-056
SELECT g, count(*) AS n FROM t_num GROUP BY CUBE (g, b) HAVING g IS NULL ORDER BY n;
-- @id agg-057
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY n) AS median, percentile_disc(0.5) WITHIN GROUP (ORDER BY n) AS median_d FROM t_num WHERE id < 9000;
-- @id agg-058
SELECT percentile_cont(0.9) WITHIN GROUP (ORDER BY d) AS p90, mode() WITHIN GROUP (ORDER BY g) AS m FROM t_num WHERE id < 9000;
-- @id agg-059
SELECT corr(i, d) AS c, covar_pop(i, d) AS cp, regr_slope(d, i) AS slope FROM t_num WHERE id < 9000;
-- @id agg-060
SELECT bit_and(i) AS a, bit_or(i) AS o FROM t_num WHERE id < 50;
-- @id agg-061
SELECT max(t) AS hi, min(t) AS lo FROM t_str WHERE id <= 200;
-- @id agg-062
SELECT max(d) AS d, min(ts) AS ts, max(tz) AS tz FROM t_time WHERE id < 9000;
-- @id agg-063
SELECT max(d) AS d, min(d) AS dlo, max(ts) AS ts, min(tz) AS tz FROM t_time;
-- @id agg-064
SELECT sum(iv) AS total, avg(iv) AS mean, max(iv) AS longest FROM t_time WHERE id < 100;
-- @id agg-065
SELECT count(*) AS n, sum(i) AS s, avg(i) AS a, max(i) AS m FROM t_num WHERE false;
-- @id agg-066
SELECT json_agg(id ORDER BY id) AS j FROM t_num WHERE id <= 5;
-- @id agg-067
SELECT jsonb_object_agg(id, g ORDER BY id) AS j FROM t_num WHERE id <= 4;
-- @id agg-068
SELECT sum(n)::float8 AS f, sum(i)::numeric AS nn, avg(i)::int AS ai FROM t_num WHERE id < 9000;
-- @id agg-069
SELECT g, sum(n) / count(n) AS manual_avg, avg(n) AS a FROM t_num WHERE id < 9000 GROUP BY g ORDER BY g;
-- @id agg-070
SELECT count(*) AS n FROM (SELECT DISTINCT g, b FROM t_num) s;
