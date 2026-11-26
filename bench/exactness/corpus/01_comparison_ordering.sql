-- Comparison and ordering: ORDER BY on text and on NULLs, DISTINCT, GROUP BY on text.
-- Every ORDER BY here is total (it ends in a unique key) unless the point of the
-- statement is the order of the text itself, in which case ties are impossible
-- or the statement is DISTINCT.

-- @id cmp-001
SELECT id, i FROM t_num WHERE id < 9000 ORDER BY i, id;
-- @id cmp-002
SELECT id, i FROM t_num WHERE id < 9000 ORDER BY i DESC, id;
-- @id cmp-003
SELECT id, i FROM t_num WHERE id < 9000 ORDER BY i NULLS FIRST, id;
-- @id cmp-004
SELECT id, i FROM t_num WHERE id < 9000 ORDER BY i DESC NULLS LAST, id;
-- @id cmp-005
SELECT id, n FROM t_num WHERE id < 9000 ORDER BY n DESC, id LIMIT 50;
-- @id cmp-006
SELECT id, d FROM t_num ORDER BY d, id;
-- @id cmp-007
SELECT id, r FROM t_num ORDER BY r DESC, id;
-- @id cmp-008
SELECT DISTINCT g FROM t_str ORDER BY g;
-- @id cmp-009
SELECT DISTINCT g FROM t_str ORDER BY g DESC;
-- @id cmp-010
SELECT id, t FROM t_str ORDER BY t, id;
-- @id cmp-011
SELECT id, t FROM t_str ORDER BY t DESC, id LIMIT 100;
-- @id cmp-012
SELECT id, t FROM t_str ORDER BY lower(t), id;
-- @id cmp-013
SELECT id, t FROM t_str ORDER BY t COLLATE "C", id;
-- @id cmp-014
SELECT g, count(*) AS n FROM t_str GROUP BY g ORDER BY g;
-- @id cmp-015
SELECT g, count(*) AS n FROM t_str GROUP BY g;
-- @id cmp-016
SELECT lower(g) AS lg, count(*) AS n FROM t_str GROUP BY lower(g);
-- @id cmp-017
SELECT name FROM customers ORDER BY name;
-- @id cmp-018
SELECT region, count(*) AS n FROM customers GROUP BY region ORDER BY region;
-- @id cmp-019
SELECT region, count(*) AS n FROM customers GROUP BY region ORDER BY region DESC NULLS LAST;
-- @id cmp-020
SELECT min(t) AS lo, max(t) AS hi FROM t_str;
-- @id cmp-021
SELECT min(g) AS lo, max(g) AS hi FROM t_str WHERE g <> '';
-- @id cmp-022
SELECT count(*) AS n FROM t_str WHERE t < 'b';
-- @id cmp-023
SELECT count(*) AS n FROM t_str WHERE t >= 'a' AND t < 'z';
-- @id cmp-024
SELECT count(*) AS n FROM t_str WHERE t BETWEEN 'A' AND 'Z';
-- @id cmp-025
SELECT count(*) AS n FROM t_str WHERE g = 'apple';
-- @id cmp-026
SELECT count(*) AS n FROM t_str WHERE g IN ('apple', 'Apple', 'APPLE');
-- @id cmp-027
SELECT count(*) AS n FROM t_str WHERE lower(g) = 'apple';
-- @id cmp-028
SELECT 'a' < 'B' AS a_lt_upper_b, 'a' < 'b' AS a_lt_b, 'Z' < 'a' AS upper_z_lt_a, 'é' < 'f' AS eacute_lt_f;
-- @id cmp-029
SELECT 'abc' = 'abc ' AS trailing_space, 'abc' = 'ABC' AS case_fold, '' = ' ' AS empty_vs_space;
-- @id cmp-030
SELECT id, ch FROM t_str WHERE ch = 'apple' ORDER BY id;
-- @id cmp-031
SELECT count(*) AS n FROM t_str WHERE ch = 'ab';
-- @id cmp-032
SELECT count(*) AS n FROM t_str WHERE ch = 'ab      ';
-- @id cmp-033
SELECT DISTINCT ch FROM t_str;
-- @id cmp-034
SELECT id, e FROM t_str WHERE id <= 40 ORDER BY e, id;
-- @id cmp-035
SELECT e, count(*) AS n FROM t_str GROUP BY e ORDER BY e;
-- @id cmp-036
SELECT count(*) AS n FROM t_str WHERE e > 'ok';
-- @id cmp-037
SELECT min(e) AS lo, max(e) AS hi FROM t_str;
-- @id cmp-038
SELECT id, u FROM t_str WHERE u IS NOT NULL ORDER BY u, id LIMIT 30;
-- @id cmp-039
SELECT count(DISTINCT u) AS n FROM t_str;
-- @id cmp-040
SELECT id FROM t_str WHERE u = 'C4CA4238-A0B9-2382-0DCC-509A6F75849B' ORDER BY id;
-- @id cmp-041
SELECT id, b FROM t_num WHERE id < 9000 ORDER BY b, id LIMIT 20;
-- @id cmp-042
SELECT b, count(*) AS n FROM t_num GROUP BY b ORDER BY b DESC;
-- @id cmp-043
SELECT count(*) AS n FROM t_num WHERE d = 'NaN';
-- @id cmp-044
SELECT count(*) AS n FROM t_num WHERE d > 1e300;
-- @id cmp-045
SELECT id FROM t_num WHERE r = 0.1 ORDER BY id;
-- @id cmp-046
SELECT id FROM t_num WHERE r = 0.1::real ORDER BY id;
-- @id cmp-047
SELECT id FROM t_num WHERE d = 0.1 + 0.2 ORDER BY id;
-- @id cmp-048
SELECT id FROM t_num WHERE n = 0.5 ORDER BY id;
-- @id cmp-049
SELECT id, i FROM t_num WHERE i IS DISTINCT FROM 1 AND id >= 9000 ORDER BY id;
-- @id cmp-050
SELECT id FROM t_num WHERE (i, bi) = (1, 1) ORDER BY id;
-- @id cmp-051
SELECT id FROM t_num WHERE (si, i) < (0, 0) AND id < 100 ORDER BY id;
-- @id cmp-052
SELECT id, ip FROM t_str WHERE ip IS NOT NULL ORDER BY ip, id LIMIT 40;
-- @id cmp-053
SELECT id, raw FROM t_str WHERE id < 60 ORDER BY raw, id;
-- @id cmp-054
SELECT DISTINCT ON (g) g, id, t FROM t_str ORDER BY g, id;
-- @id cmp-055
SELECT g, t FROM t_str WHERE id <= 60 ORDER BY g, t, id;
-- @id cmp-056
SELECT greatest('apple', 'Banana', 'cherry') AS g, least('apple', 'Banana', 'cherry') AS l;
-- @id cmp-057
SELECT name, region FROM customers ORDER BY region NULLS FIRST, name DESC LIMIT 25;
-- @id cmp-058
SELECT id, vc FROM t_str WHERE vc = 'User 4' ORDER BY id;
-- @id cmp-059
SELECT id, vc FROM t_str WHERE vc = 'User 4  ' ORDER BY id;
-- @id cmp-060
SELECT status, count(*) AS n FROM orders GROUP BY status ORDER BY status;
