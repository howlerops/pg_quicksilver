-- date_trunc(text, timestamptz) statements, for equality.py --extra (docs/40 date_trunc).
-- Run under several session zones with equality.py --zone. Same format as the other
-- corpora: an id line, then one statement. search_path = ex, public.

-- @id tz-001
SELECT date_trunc('microseconds', tz) AS d, count(*) FROM t_time GROUP BY 1 ORDER BY 1;
-- @id tz-002
SELECT date_trunc('milliseconds', tz) AS d, count(*) FROM t_time GROUP BY 1 ORDER BY 1;
-- @id tz-003
SELECT date_trunc('second', tz) AS d, count(*) FROM t_time GROUP BY 1 ORDER BY 1;
-- @id tz-004
SELECT date_trunc('minute', tz) AS d, count(*) FROM t_time GROUP BY 1 ORDER BY 1;
-- @id tz-005
SELECT date_trunc('hour', tz) AS d, count(*) FROM t_time GROUP BY 1 ORDER BY 1;
-- @id tz-006
SELECT date_trunc('day', tz) AS d, count(*) FROM t_time GROUP BY 1 ORDER BY 1;
-- @id tz-007
SELECT date_trunc('week', tz) AS d, count(*) FROM t_time GROUP BY 1 ORDER BY 1;
-- @id tz-008
SELECT date_trunc('month', tz) AS d, count(*) FROM t_time GROUP BY 1 ORDER BY 1;
-- @id tz-009
SELECT date_trunc('quarter', tz) AS d, count(*) FROM t_time GROUP BY 1 ORDER BY 1;
-- @id tz-010
SELECT date_trunc('year', tz) AS d, count(*) FROM t_time GROUP BY 1 ORDER BY 1;
-- @id tz-011
SELECT date_trunc('decade', tz) AS d, count(*) FROM t_time GROUP BY 1 ORDER BY 1;
-- @id tz-012
SELECT date_trunc('century', tz) AS d, count(*) FROM t_time GROUP BY 1 ORDER BY 1;
-- @id tz-013
SELECT date_trunc('millennium', tz) AS d, count(*) FROM t_time GROUP BY 1 ORDER BY 1;
-- @id tz-014
SELECT id, date_trunc('hour', tz) AS h, date_trunc('day', tz) AS d FROM t_time WHERE id <= 60 OR id BETWEEN 9001 AND 9016 ORDER BY id;
-- @id tz-015
SELECT id, date_trunc('MONTH', tz) AS m, date_trunc('Quarter', tz) AS q FROM t_time WHERE id <= 60 OR id BETWEEN 9001 AND 9016 ORDER BY id;
-- @id tz-016
SELECT id FROM t_time WHERE date_trunc('month', tz) >= '2015-01-01 00:00:00+00' AND id <= 2000 ORDER BY id;
-- @id tz-017
SELECT id, date_trunc(CASE WHEN id % 2 = 0 THEN 'day' ELSE 'hour' END, tz) AS x FROM t_time WHERE id <= 60 ORDER BY id;
-- @id tz-018
SELECT id, date_trunc('day', tz, 'Asia/Kolkata') AS x FROM t_time WHERE id <= 60 ORDER BY id;
-- @id tz-019
SELECT id, date_trunc('week', tz) AS w, count(*) OVER (PARTITION BY date_trunc('week', tz)) AS n FROM t_time WHERE id <= 300 ORDER BY id;
-- @id tz-020
SELECT date_trunc('month', created_at) AS m, sum(amount) AS s FROM public.orders GROUP BY 1 ORDER BY 1;
-- @id tz-021
SELECT date_trunc('hour', created_at) AS h, count(*) FROM public.orders GROUP BY 1 ORDER BY 1 LIMIT 40;
-- @id tz-022
SELECT date_trunc('year', c.created_at) AS y, date_trunc('month', o.created_at) AS m, sum(o.amount) AS s
  FROM public.orders o JOIN public.customers c ON c.id = o.customer_id GROUP BY 1, 2 ORDER BY 1, 2;
-- @id tz-023
SELECT date_trunc('day', created_at) AS d, count(*) FROM public.orders WHERE created_at >= '2024-01-03 00:00:00+00' AND created_at < '2024-01-06 00:00:00+00' GROUP BY 1 ORDER BY 1;
-- @id tz-024
SELECT date_trunc('quarter', tz) AS q, min(tz), max(tz) FROM t_time GROUP BY 1 ORDER BY 1;
