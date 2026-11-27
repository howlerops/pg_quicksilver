-- Casts and their text output.

-- @id cast-001
SELECT id, i::text AS t FROM t_num WHERE id <= 30 ORDER BY id;
-- @id cast-002
SELECT id, n::text AS t FROM t_num WHERE id <= 30 OR id >= 9000 ORDER BY id;
-- @id cast-003
SELECT id, nu::text AS t FROM t_num WHERE id <= 30 OR id >= 9000 ORDER BY id;
-- @id cast-004
SELECT id, n0::text AS t FROM t_num WHERE id <= 30 ORDER BY id;
-- @id cast-005
SELECT id, d::text AS t FROM t_num WHERE id <= 30 OR id >= 9000 ORDER BY id;
-- @id cast-006
SELECT id, r::text AS t FROM t_num WHERE id <= 30 OR id >= 9000 ORDER BY id;
-- @id cast-007
SELECT id, b::text AS t FROM t_num WHERE id <= 30 ORDER BY id;
-- @id cast-008
SELECT id, b::int AS bi FROM t_num WHERE id <= 30 ORDER BY id;
-- @id cast-009
SELECT id, d::text AS t FROM t_time WHERE id <= 20 OR id >= 9000 ORDER BY id;
-- @id cast-010
SELECT id, ts::text AS t FROM t_time WHERE id <= 20 OR id >= 9000 ORDER BY id;
-- @id cast-011
SELECT id, tz::text AS t FROM t_time WHERE id <= 20 OR id >= 9000 ORDER BY id;
-- @id cast-012
SELECT id, tm::text AS t FROM t_time WHERE id <= 20 OR id >= 9000 ORDER BY id;
-- @id cast-013
SELECT id, iv::text AS t FROM t_time WHERE id <= 20 OR id >= 9000 ORDER BY id;
-- @id cast-014
SELECT id, u::text AS t FROM t_str WHERE id <= 20 OR id >= 9000 ORDER BY id;
-- @id cast-015
SELECT id, e::text AS t FROM t_str WHERE id <= 20 ORDER BY id;
-- @id cast-016
SELECT id, ip::text AS t FROM t_str WHERE id <= 20 OR id >= 9000 ORDER BY id;
-- @id cast-017
SELECT id, raw::text AS t FROM t_str WHERE id <= 20 OR id >= 9000 ORDER BY id;
-- @id cast-018
SELECT id, ia::text AS t, ta::text AS tt FROM t_doc WHERE id <= 20 OR id >= 9000 ORDER BY id;
-- @id cast-019
SELECT id, jb::text AS t FROM t_doc WHERE id <= 10 OR id >= 9000 ORDER BY id;
-- @id cast-020
SELECT id, j::text AS t FROM t_doc WHERE id <= 10 OR id >= 9000 ORDER BY id;
-- @id cast-021
SELECT id, i::smallint AS s FROM t_num WHERE id BETWEEN 9003 AND 9013 ORDER BY id;
-- @id cast-022
SELECT id, bi::int AS narrowed FROM t_num WHERE id BETWEEN 9003 AND 9013 ORDER BY id;
-- @id cast-023
SELECT id, bi::int AS narrowed FROM t_num WHERE id = 9014;
-- @id cast-024
SELECT id, i::numeric(6,2) AS nn FROM t_num WHERE id BETWEEN 9003 AND 9013 ORDER BY id;
-- @id cast-025
SELECT id, n::int AS as_int, n::bigint AS as_big FROM t_num WHERE id <= 30 OR id BETWEEN 9003 AND 9008 ORDER BY id;
-- @id cast-026
SELECT id, n::numeric(12,1) AS n1, n::numeric(12,0) AS n0, n::numeric(14,4) AS n4 FROM t_num WHERE id <= 30 OR id BETWEEN 9003 AND 9008 ORDER BY id;
-- @id cast-027
SELECT id, n::float8 AS f, n::real AS r FROM t_num WHERE id <= 30 ORDER BY id;
-- @id cast-028
SELECT id, d::numeric AS dn FROM t_num WHERE id <= 30 ORDER BY id;
-- @id cast-029
SELECT id, d::numeric(20,4) AS dn FROM t_num WHERE id <= 30 ORDER BY id;
-- @id cast-030
SELECT id, d::int AS di FROM t_num WHERE id <= 30 ORDER BY id;
-- @id cast-031
SELECT id, r::int AS ri, r::numeric(12,3) AS rn FROM t_num WHERE id <= 30 ORDER BY id;
-- @id cast-032
SELECT id, d::real AS narrowed FROM t_num WHERE id <= 30 ORDER BY id;
-- @id cast-033
SELECT '42'::int AS a, ' 42 '::int AS b, '4.2e1'::numeric AS c, '1e2'::float8 AS d, '0x1F'::int AS e, '1_000'::int AS f;
-- @id cast-034
SELECT 'abc'::int AS a;
-- @id cast-035
SELECT ''::int AS a;
-- @id cast-036
SELECT '1.5'::int AS a;
-- @id cast-037
SELECT 'true'::boolean AS a, 'yes'::boolean AS b, 'f'::boolean AS c, '0'::boolean AS d, 'on'::boolean AS e, 1::boolean AS f;
-- @id cast-038
SELECT '2024-02-29'::date AS a, '20240229'::date AS b, 'Feb 29, 2024'::date AS c, '2024-02-29 13:45:00'::date AS d;
-- @id cast-039
SELECT '02/03/2024'::date AS mdy;
-- @id cast-040
SELECT '2024-02-30'::date AS bad;
-- @id cast-041
SELECT '2024-03-10 02:30:00'::timestamptz AS spring_gap, '2024-11-03 01:30:00'::timestamptz AS fall_overlap;
-- @id cast-042
SELECT '2024-06-01 12:00:00+02'::timestamptz AS a, '2024-06-01T12:00:00Z'::timestamptz AS b, '2024-06-01 12:00:00 Europe/Paris'::timestamptz AS c;
-- @id cast-043
SELECT '2024-06-01 12:00:00'::timestamp::timestamptz AS a, '2024-06-01 12:00:00+00'::timestamptz::timestamp AS b, '2024-06-01 23:30:00+00'::timestamptz::date AS c;
-- @id cast-044
SELECT id, tz::date AS local_date, tz::timestamp AS local_ts, tz::time AS local_time FROM t_time WHERE id <= 30 OR id BETWEEN 9007 AND 9014 ORDER BY id;
-- @id cast-045
SELECT id, ts::date AS d, ts::time AS t, d::timestamp AS dts, d::timestamptz AS dtz FROM t_time WHERE id <= 30 OR id BETWEEN 9007 AND 9014 ORDER BY id;
-- @id cast-046
SELECT id, ts::timestamptz AS as_tz FROM t_time WHERE id BETWEEN 9007 AND 9014 ORDER BY id;
-- @id cast-047
SELECT '1 day'::interval AS a, '1 day 2 hours'::interval AS b, '90 minutes'::interval AS c, '1-2'::interval AS d, 'P1Y2M3DT4H'::interval AS e, '1.5 days'::interval AS f;
-- @id cast-048
SELECT '{1,2,3}'::int[] AS a, '{a,"b c",NULL}'::text[] AS b, ARRAY[1,2]::text[] AS c, '{}'::int[] AS d;
-- @id cast-049
SELECT '{"a":1}'::json AS j, '{"a":1,  "b":[1,2]}'::jsonb AS jb, '[1,2]'::jsonb AS arr, 'null'::jsonb AS n;
-- @id cast-050
SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid AS a, 'A0EEBC999C0B4EF8BB6D6BB9BD380A11'::uuid AS b, '{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}'::uuid AS c;
-- @id cast-051
SELECT 'not-a-uuid'::uuid AS a;
-- @id cast-052
SELECT '192.168.1.5/24'::inet AS a, '192.168.1.0/24'::cidr AS b, host('192.168.1.5/24'::inet) AS h, '::1'::inet AS v6;
-- @id cast-053
SELECT 'happy'::mood AS a, 'ok'::mood < 'happy'::mood AS in_enum_order;
-- @id cast-054
SELECT 123.456::numeric(5,2) AS a, 123.455::numeric(5,2) AS b, 0.005::numeric(5,2) AS c, -0.005::numeric(5,2) AS d;
-- @id cast-055
SELECT 99999.999::numeric(5,2) AS overflow;
-- @id cast-056
SELECT 3000000000::int AS overflow;
-- @id cast-057
SELECT 32768::smallint AS overflow;
-- @id cast-058
SELECT 1e40::real AS overflow;
-- @id cast-059
SELECT 'abcdef'::varchar(3) AS a, 'abcdef'::char(3) AS b, 'ab'::char(5) AS c, 'ab'::char(5) || '|' AS d, 'abc  '::varchar(4) AS e;
-- @id cast-060
SELECT CAST(1 AS text) AS a, CAST('1' AS integer) AS b, CAST(1.9 AS integer) AS c, CAST(-1.9 AS integer) AS d, CAST(1.5 AS bigint) AS e, CAST(2.5 AS bigint) AS f;
-- @id cast-061
SELECT 1.5::float8::int AS a, 2.5::float8::int AS b, 3.5::float8::int AS c, -2.5::float8::int AS d, 0.5::float8::int AS e;
-- @id cast-062
SELECT 1.0::float8::text AS a, 1.10::numeric::text AS b, 1e20::float8::text AS c, 1e-7::float8::text AS d, 100000000000000000000::numeric::text AS e, 0.1::real::text AS f;
-- @id cast-063
SELECT true::text AS a, false::text AS b, NULL::boolean::text AS c, (1 = 1)::text AS d, true::int AS e;
-- @id cast-064
SELECT 1::float8 AS a, 1.0::float8 AS b, 100::float8 AS c, 1e15::float8 AS d, 1e16::float8 AS e, 1.5e-5::float8 AS f, 123456789012345678::float8 AS g;
-- @id cast-065
SELECT id, id::float8 AS f, id::real AS r, id::numeric(8,2) AS n FROM t_num WHERE id <= 10 ORDER BY id;
-- @id cast-066
SELECT id, (id::text || '.5')::numeric AS n, (id::text || '.5')::float8 AS f FROM t_num WHERE id <= 10 ORDER BY id;
-- @id cast-067
SELECT id, t::int AS as_int FROM (SELECT id, g AS t FROM t_str WHERE g IN ('100', '20', '3') AND id <= 400) s ORDER BY id;
-- @id cast-068
SELECT id, amount::text || ' USD' AS label, amount::int AS rounded, (amount * 100)::bigint AS cents FROM orders WHERE id <= 30 ORDER BY id;
-- @id cast-069
SELECT id, placed::date AS day, placed::text AS txt FROM orders WHERE id <= 30 ORDER BY id;
-- @id cast-070
SELECT pg_typeof(1) AS a, pg_typeof(1.5) AS b, pg_typeof('x') AS c, pg_typeof(1 / 2) AS d, pg_typeof(1 + 1.0) AS e, pg_typeof(now()) AS f;
-- @id cast-071
SELECT pg_typeof(i) AS i, pg_typeof(si) AS si, pg_typeof(n) AS n, pg_typeof(r) AS r, pg_typeof(i + si) AS sum_t, pg_typeof(avg(i) OVER ()) AS avg_t FROM t_num WHERE id = 1;
-- @id cast-072
SELECT pg_typeof(u) AS u, pg_typeof(e) AS e, pg_typeof(ip) AS ip, pg_typeof(raw) AS raw, pg_typeof(ch) AS ch FROM t_str WHERE id = 1;
