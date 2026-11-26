-- generate_series and other common functions.

-- @id fn-001
SELECT generate_series(1, 5) AS n;
-- @id fn-002
SELECT n FROM generate_series(1, 10, 3) AS n ORDER BY n;
-- @id fn-003
SELECT n FROM generate_series(5, 1, -2) AS n ORDER BY n;
-- @id fn-004
SELECT n FROM generate_series(1, 0) AS n;
-- @id fn-005
SELECT g FROM generate_series(1, 3) AS g;
-- @id fn-006
SELECT * FROM generate_series(1, 3);
-- @id fn-007
SELECT x FROM generate_series(0.5, 2.5, 0.5) AS x ORDER BY x;
-- @id fn-008
SELECT d FROM generate_series(DATE '2024-02-27', DATE '2024-03-02', INTERVAL '1 day') AS d ORDER BY d;
-- @id fn-009
SELECT d::date AS d FROM generate_series(DATE '2024-02-27', DATE '2024-03-02', INTERVAL '1 day') AS d ORDER BY d;
-- @id fn-010
SELECT ts FROM generate_series(TIMESTAMP '2024-03-09 22:00', TIMESTAMP '2024-03-10 04:00', INTERVAL '2 hours') AS ts ORDER BY ts;
-- @id fn-011
SELECT ts FROM generate_series(TIMESTAMPTZ '2024-03-09 00:00:00-05', TIMESTAMPTZ '2024-03-12 00:00:00-04', INTERVAL '1 day') AS ts ORDER BY ts;
-- @id fn-012
SELECT m FROM generate_series(DATE '2024-01-31', DATE '2024-06-30', INTERVAL '1 month') AS m ORDER BY m;
-- @id fn-013
SELECT d.day::date AS day, count(o.id) AS n FROM generate_series(DATE '2023-03-10', DATE '2023-03-14', INTERVAL '1 day') AS d(day) LEFT JOIN orders o ON o.placed::date = d.day::date GROUP BY 1 ORDER BY 1;
-- @id fn-014
SELECT n, n % 3 AS m FROM generate_series(1, 6) AS n WHERE n % 2 = 0 ORDER BY n;
-- @id fn-015
SELECT count(*) AS n, sum(x) AS s, avg(x) AS a FROM generate_series(1, 100) AS x;
-- @id fn-016
SELECT a, b FROM generate_series(1, 2) AS a, generate_series(1, 2) AS b ORDER BY a, b;
-- @id fn-017
SELECT x, ordinality FROM unnest(ARRAY['a', 'b', 'c']) WITH ORDINALITY AS t(x, ordinality) ORDER BY ordinality;
-- @id fn-018
SELECT unnest(ARRAY[1, 2, 3]) AS x;
-- @id fn-019
SELECT x FROM unnest(string_to_array('b,a,c', ',')) AS x ORDER BY x;
-- @id fn-020
SELECT regexp_split_to_table('one two  three', '\s+') AS w;
-- @id fn-021
SELECT abs(-5) AS a, abs(-5.5) AS b, sign(-3) AS c, ceil(4.1) AS d, ceiling(4.1) AS e, floor(-4.1) AS f, trunc(4.99) AS g, trunc(-4.99) AS h;
-- @id fn-022
SELECT round(42.4382, 2) AS a, round(42.5) AS b, round(1234.5678, -2) AS c, trunc(42.4382, 2) AS d, round(5::numeric / 3, 4) AS e;
-- @id fn-023
SELECT mod(10, 3) AS a, mod(-10, 3) AS b, mod(10.5, 3) AS c, power(2, 0.5) AS d, exp(0) AS e, ln(1) AS f, log(2, 8) AS g, log10(1000) AS h;
-- @id fn-024
SELECT length('héllo') AS a, char_length('日本語') AS b, octet_length('héllo') AS c, octet_length('日本語') AS d, bit_length('a') AS e;
-- @id fn-025
SELECT coalesce(NULL, NULL, 3) AS a, nullif(1, 1) AS b, nullif(1, 2) AS c, greatest(1, 5, 3) AS d, least('b', 'a') AS e;
-- @id fn-026
SELECT num_nulls(1, NULL, 3, NULL) AS n, num_nonnulls(1, NULL, 3) AS nn;
-- @id fn-027
SELECT array_length(ARRAY[1, 2, 3], 1) AS a, cardinality(ARRAY[1, 2]) AS b, array_upper(ARRAY[1, 2, 3], 1) AS c, array_position(ARRAY['a', 'b'], 'b') AS d, array_remove(ARRAY[1, 2, 1], 1) AS e, array_replace(ARRAY[1, 2], 2, 9) AS f;
-- @id fn-028
SELECT gen_random_uuid() IS NOT NULL AS has_uuid, length(gen_random_uuid()::text) AS len, random() < 1 AS r, now() IS NOT NULL AS n, current_date IS NOT NULL AS cd;
-- @id fn-029
SELECT current_database() AS db, current_schema() AS sch, version() LIKE 'PostgreSQL%' AS is_pg;
-- @id fn-030
SELECT current_user AS u, session_user AS su;
-- @id fn-031
SELECT current_setting('TimeZone') AS tz, current_setting('server_version_num')::int >= 170000 AS pg17;
-- @id fn-032
SELECT md5('hello') AS a, sha256('hello'::bytea) AS b, encode(sha256('hello'::bytea), 'hex') AS c, hashtext('hello') AS d;
-- @id fn-033
SELECT string_agg(x, ',' ORDER BY x) AS s, array_agg(x ORDER BY x DESC) AS a, count(*) AS n FROM unnest(ARRAY['b', 'a', 'C', 'é']) AS x;
-- @id fn-034
SELECT x FROM unnest(ARRAY['b', 'a', 'C', 'é', 'B', 'z', 'Z', '_', '1', ' ']) AS x ORDER BY x;
-- @id fn-035
SELECT x FROM (VALUES (3), (1), (NULL), (2)) AS v(x) ORDER BY x DESC;
-- @id fn-036
SELECT x, y FROM (VALUES (1, 'a'), (1, NULL), (NULL, 'b')) AS v(x, y) ORDER BY x NULLS FIRST, y NULLS FIRST;
-- @id fn-037
SELECT row_number() OVER () AS rn, n FROM generate_series(10, 12) AS n ORDER BY n;
-- @id fn-038
SELECT to_char(n, 'FM000') AS code, lpad(n::text, 4, '0') AS padded, n::text || '%' AS pct FROM generate_series(8, 11) AS n ORDER BY n;
-- @id fn-039
SELECT txid_current() > 0 AS has_xid;
-- @id fn-040
SELECT pg_size_pretty(1234567::bigint) AS a, pg_column_size(1::int) AS b;
-- @id fn-041
SELECT isfinite(DATE '2024-01-01') AS a, date_part('dow', DATE '2024-02-29') AS b, extract(century FROM DATE '2001-01-01') AS c, to_char(DATE '2024-02-29', 'Dy') AS d;
-- @id fn-042
SELECT bool_and(x > 0) AS all_pos, bool_or(x > 2) AS any_big, count(*) FILTER (WHERE x % 2 = 0) AS evens FROM generate_series(1, 4) AS x;
-- @id fn-043
SELECT i, CASE WHEN i % 15 = 0 THEN 'FizzBuzz' WHEN i % 3 = 0 THEN 'Fizz' WHEN i % 5 = 0 THEN 'Buzz' ELSE i::text END AS fb FROM generate_series(1, 15) AS i ORDER BY i;
-- @id fn-044
SELECT sum(i) AS s, sum(i::bigint) AS sb, sum(i::numeric) AS sn, sum(i::float8) AS sf, avg(i) AS a, avg(i::float8) AS af FROM generate_series(1, 10) AS i;
