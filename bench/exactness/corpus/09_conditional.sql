-- CASE / COALESCE / NULLIF and three-valued logic.

-- @id cond-001
SELECT id, CASE WHEN i > 0 THEN 'pos' WHEN i < 0 THEN 'neg' WHEN i = 0 THEN 'zero' ELSE 'unknown' END AS sgn FROM t_num WHERE id <= 60 OR id >= 9000 ORDER BY id;
-- @id cond-002
SELECT id, CASE g WHEN 0 THEN 'zero' WHEN 1 THEN 'one' ELSE 'many' END AS label FROM t_num WHERE id <= 30 ORDER BY id;
-- @id cond-003
SELECT id, CASE WHEN n > 0 THEN n ELSE 0 END AS clipped, CASE WHEN n > 0 THEN n END AS no_else FROM t_num WHERE id <= 60 ORDER BY id;
-- @id cond-004
SELECT id, CASE WHEN b THEN 1 ELSE 0 END AS flag, CASE WHEN b IS NULL THEN 'unknown' WHEN b THEN 'yes' ELSE 'no' END AS txt FROM t_num WHERE id <= 60 ORDER BY id;
-- @id cond-005
SELECT id, CASE WHEN i > 0 THEN i ELSE n END AS mixed_int_numeric FROM t_num WHERE id <= 60 ORDER BY id;
-- @id cond-006
SELECT id, CASE WHEN i > 0 THEN i ELSE 0.5 END AS mixed_int_literal, CASE WHEN i > 0 THEN 1 ELSE 2.0::float8 END AS mixed_int_float FROM t_num WHERE id <= 60 ORDER BY id;
-- @id cond-007
SELECT id, coalesce(i, 0) AS i0, coalesce(n, 0) AS n0, coalesce(i, si, -1) AS first_int FROM t_num WHERE id <= 60 ORDER BY id;
-- @id cond-008
SELECT id, coalesce(t, vc, '(none)') AS label, coalesce(g, '') = '' AS blank FROM t_str WHERE id <= 60 ORDER BY id;
-- @id cond-009
SELECT id, coalesce(i, n) AS int_or_numeric, coalesce(n, d) AS numeric_or_double, coalesce(si, bi) AS small_or_big FROM t_num WHERE id <= 60 ORDER BY id;
-- @id cond-010
SELECT id, coalesce(i::text, 'n/a') AS shown, coalesce(n::text, 'n/a') AS nshown FROM t_num WHERE id <= 60 ORDER BY id;
-- @id cond-011
SELECT id, nullif(g, 0) AS g_or_null, nullif(b, false) AS true_or_null FROM t_num WHERE id <= 60 ORDER BY id;
-- @id cond-012
SELECT id, nullif(g, '') AS nonblank, nullif(trim(vc), '') AS vc_nonblank FROM t_str WHERE id <= 60 ORDER BY id;
-- @id cond-013
SELECT id, i / nullif(g, 0) AS safe_div, n / nullif(g, 0) AS nsafe_div FROM t_num WHERE id <= 60 ORDER BY id;
-- @id cond-014
SELECT id, greatest(i, si, 0) AS hi, least(i, si, 0) AS lo, greatest(n, 0) AS nhi FROM t_num WHERE id <= 60 ORDER BY id;
-- @id cond-015
SELECT greatest(1, NULL, 3) AS g, least(1, NULL, 3) AS l, greatest(NULL, NULL) AS all_null, greatest(1, 2.5) AS mixed;
-- @id cond-016
SELECT NULL = NULL AS eq, NULL <> 1 AS ne, NULL IS NULL AS isn, NULL IS DISTINCT FROM NULL AS idf, 1 IS NOT DISTINCT FROM 1 AS indf, NULL AND false AS and_f, NULL OR true AS or_t, NOT NULL AS not_n;
-- @id cond-017
SELECT count(*) AS n FROM t_num WHERE b;
-- @id cond-018
SELECT count(*) AS n FROM t_num WHERE NOT b;
-- @id cond-019
SELECT count(*) AS n FROM t_num WHERE b IS NOT TRUE;
-- @id cond-020
SELECT count(*) AS n FROM t_num WHERE b IS UNKNOWN;
-- @id cond-021
SELECT count(*) AS n FROM t_num WHERE i <> 5 OR i = 5;
-- @id cond-022
SELECT count(*) AS n FROM t_num WHERE NOT (i > 0 AND b);
-- @id cond-023
SELECT count(*) AS n FROM t_num WHERE i IN (1, 2, NULL);
-- @id cond-024
SELECT count(*) AS n FROM t_num WHERE i NOT IN (1, 2, NULL);
-- @id cond-025
SELECT count(*) AS n FROM t_num WHERE i BETWEEN -10 AND 10;
-- @id cond-026
SELECT count(*) AS n FROM t_num WHERE i NOT BETWEEN -50000 AND 50000;
-- @id cond-027
SELECT id, i = ANY (ARRAY[1, 2, 3]) AS any_match, i <> ALL (ARRAY[1, 2, 3]) AS none_match FROM t_num WHERE id >= 9000 ORDER BY id;
-- @id cond-028
SELECT id, CASE WHEN g = 0 THEN NULL ELSE 100 / g END AS guarded FROM t_num WHERE id <= 30 ORDER BY id;
-- @id cond-029
SELECT id, CASE WHEN status = 'paid' OR status = 'Paid' THEN amount ELSE 0 END AS paid_amount, coalesce(note, '') AS note FROM orders WHERE id <= 40 ORDER BY id;
-- @id cond-030
SELECT sum(CASE WHEN status = 'paid' THEN amount END) AS paid, sum(CASE WHEN status = 'cancelled' THEN amount ELSE 0 END) AS cancelled, count(CASE WHEN note = '' THEN 1 END) AS empty_notes, count(nullif(note, '')) AS real_notes FROM orders;
-- @id cond-031
SELECT id, coalesce(d, DATE '1970-01-01') AS d0, coalesce(tz, TIMESTAMPTZ '2000-01-01 00:00:00+00') AS tz0 FROM t_time WHERE id <= 30 ORDER BY id;
-- @id cond-032
SELECT id, coalesce(e::text, 'none') AS mood, CASE e WHEN 'happy' THEN 1 WHEN 'sad' THEN -1 ELSE 0 END AS score FROM t_str WHERE id <= 40 ORDER BY id;
-- @id cond-033
SELECT id, CASE WHEN e >= 'happy' THEN 'good' ELSE 'not good' END AS bucket FROM t_str WHERE id <= 40 ORDER BY id;
-- @id cond-034
SELECT 1 WHERE NULL;
-- @id cond-035
SELECT id, (i > 0) AS pos, (i > 0)::int AS pos_int, i > 0 AND b AS both, i > 0 OR b AS either FROM t_num WHERE id <= 40 ORDER BY id;
