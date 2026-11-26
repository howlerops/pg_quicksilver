-- Arithmetic: integer division, overflow, numeric scale propagation, rounding,
-- mod of negatives.

-- @id ari-001
SELECT id, i / 7 AS q FROM t_num WHERE id < 9000 ORDER BY id;
-- @id ari-002
SELECT id, i / 2 AS q, -i / 2 AS nq FROM t_num WHERE id BETWEEN 9003 AND 9009 ORDER BY id;
-- @id ari-003
SELECT 7 / 2 AS a, -7 / 2 AS b, 7 / -2 AS c, 1 / 3 AS d;
-- @id ari-004
SELECT 7 % 3 AS a, -7 % 3 AS b, 7 % -3 AS c, -7 % -3 AS d;
-- @id ari-005
SELECT id, i % 7 AS m, mod(i, 7) AS m2 FROM t_num WHERE id < 200 ORDER BY id;
-- @id ari-006
SELECT id, si / 3 AS q, si % 3 AS m FROM t_num WHERE id < 200 ORDER BY id;
-- @id ari-007
SELECT id, bi / 1000 AS q FROM t_num WHERE id < 200 ORDER BY id;
-- @id ari-008
SELECT 7 / 2.0 AS a, 7::numeric / 2 AS b, 1::numeric / 3 AS c, 10 / 4::numeric AS d;
-- @id ari-009
SELECT 7 / 2::float8 AS a, 1 / 3::float8 AS b;
-- @id ari-010
SELECT id, i + 1 AS p FROM t_num WHERE id IN (9001, 9002) ORDER BY id;
-- @id ari-011
SELECT id, i * i AS sq FROM t_num WHERE id < 50 ORDER BY id;
-- @id ari-012
SELECT id, i * 100000 AS big FROM t_num WHERE id < 50 ORDER BY id;
-- @id ari-013
SELECT id, si * si AS sq FROM t_num WHERE id < 50 ORDER BY id;
-- @id ari-014
SELECT id, bi + 1 AS p FROM t_num WHERE id = 9001;
-- @id ari-015
SELECT id, bi * 2 AS p FROM t_num WHERE id < 20 ORDER BY id;
-- @id ari-016
SELECT 2147483647 + 1 AS a;
-- @id ari-017
SELECT 2147483647::bigint + 1 AS a;
-- @id ari-018
SELECT 9223372036854775807 + 1 AS a;
-- @id ari-019
SELECT id, i / 0 AS z FROM t_num WHERE id = 1;
-- @id ari-020
SELECT 1 / 0 AS z;
-- @id ari-021
SELECT 1.0 / 0 AS z;
-- @id ari-022
SELECT 1::float8 / 0 AS z;
-- @id ari-023
SELECT 5 % 0 AS z;
-- @id ari-024
SELECT id, n * 2 AS dbl, n + 1 AS inc, n - 0.001 AS dec3 FROM t_num WHERE id < 200 ORDER BY id;
-- @id ari-025
SELECT id, n * n AS sq FROM t_num WHERE id < 200 ORDER BY id;
-- @id ari-026
SELECT id, n / 3 AS third FROM t_num WHERE id < 200 ORDER BY id;
-- @id ari-027
SELECT id, n / 100 AS pct FROM t_num WHERE id BETWEEN 9003 AND 9015 ORDER BY id;
-- @id ari-028
SELECT id, n * 1.1 AS taxed FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-029
SELECT id, n0 / 7 AS q FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-030
SELECT id, nu * 2 AS dbl, nu + 0.5 AS half FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-031
SELECT id, nu / 3 AS third FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-032
SELECT id, n * i AS mixed FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-033
SELECT id, n + d AS num_plus_double FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-034
SELECT id, round(n) AS r0, round(n, 1) AS r1, round(n, -2) AS rm2 FROM t_num WHERE id < 200 ORDER BY id;
-- @id ari-035
SELECT id, n, round(n) AS r0, round(n, 1) AS r1 FROM t_num WHERE id BETWEEN 9003 AND 9015 ORDER BY id;
-- @id ari-036
SELECT round(0.5) AS a, round(1.5) AS b, round(2.5) AS c, round(-0.5) AS d, round(-2.5) AS e;
-- @id ari-037
SELECT round(0.5::float8) AS a, round(1.5::float8) AS b, round(2.5::float8) AS c, round(-2.5::float8) AS e;
-- @id ari-038
SELECT id, round(d) AS r FROM t_num WHERE id BETWEEN 9003 AND 9009 ORDER BY id;
-- @id ari-039
SELECT id, round(d::numeric, 2) AS r2 FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-040
SELECT id, trunc(n) AS t0, trunc(n, 1) AS t1, ceil(n) AS c, floor(n) AS f FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-041
SELECT id, ceil(d) AS c, floor(d) AS f, trunc(d) AS t FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-042
SELECT id, abs(i) AS a, sign(i) AS s, -i AS neg FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-043
SELECT id, abs(n) AS a, sign(n) AS s FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-044
SELECT id, i::float8 / 3 AS f, i / 3.0 AS n FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-045
SELECT id, r * 3 AS r3, r + 1 AS r1, r / 3 AS rd FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-046
SELECT id, r * 3 AS r3 FROM t_num WHERE id BETWEEN 9004 AND 9009 ORDER BY id;
-- @id ari-047
SELECT id, r::float8 AS widened FROM t_num WHERE id BETWEEN 9004 AND 9015 ORDER BY id;
-- @id ari-048
SELECT id, d * 3 AS d3, d / 3 AS dd, d - 0.1 AS dm FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-049
SELECT 0.1 + 0.2 AS a, 0.1::float8 + 0.2::float8 AS b, 0.1::real + 0.2::real AS c;
-- @id ari-050
SELECT power(2, 10) AS a, power(2.0, 0.5) AS b, power(10::numeric, 20) AS c, 2 ^ 3 AS d;
-- @id ari-051
SELECT sqrt(2) AS a, sqrt(16::numeric) AS b, cbrt(27) AS c, exp(1) AS d, ln(10) AS e, log(100) AS f;
-- @id ari-052
SELECT id, sqrt(abs(d)) AS s FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-053
SELECT 5 & 3 AS band, 5 | 3 AS bor, 5 # 3 AS bxor, ~5 AS bnot, 1 << 4 AS shl, 256 >> 2 AS shr;
-- @id ari-054
SELECT id, i & 255 AS low, i >> 4 AS shifted FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-055
SELECT id, (n * 100)::int AS cents, (n * 100)::bigint AS cents8 FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-056
SELECT 10 / 4 * 4 AS a, 10 * 4 / 4 AS b, (10 / 4)::numeric AS c, 3 * (1 / 3) AS d;
-- @id ari-057
SELECT id, (i + si) / 2 AS avg2, (i + bi) / 2 AS avg8 FROM t_num WHERE id < 100 ORDER BY id;
-- @id ari-058
SELECT id, i / n AS int_over_num, n / i AS num_over_int FROM t_num WHERE id < 100 AND i <> 0 AND n <> 0 ORDER BY id;
-- @id ari-059
SELECT div(7, 2) AS a, div(-7, 2) AS b, div(7.5, 2) AS c, mod(7.5, 2) AS d, mod(-7.5, 2) AS e;
-- @id ari-060
SELECT 1e3::int AS a, 2.5::int AS b, 3.5::int AS c, -2.5::int AS d, 2.4::int AS e, 2.6::int AS f;
-- @id ari-061
SELECT @ -5 AS a, |/ 16 AS c, ||/ 27 AS d;
-- @id ari-062
SELECT width_bucket(5.35, 0.024, 10.06, 5) AS a, gcd(12, 18) AS b, lcm(4, 6) AS c, factorial(5) AS d;
-- @id ari-063
SELECT pi() AS pi, degrees(pi()) AS deg, radians(180) AS rad, sin(0) AS s, cos(0) AS c;
-- @id ari-064
SELECT id, n * qty_like AS total FROM (SELECT id, n, i % 5 AS qty_like FROM t_num WHERE id < 100) s ORDER BY id;
-- @id ari-065
SELECT sum(amount * qty) AS revenue, sum(amount) / count(*) AS avg_manual FROM orders;
