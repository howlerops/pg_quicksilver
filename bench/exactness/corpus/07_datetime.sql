-- Date/time: extract, date_trunc, age, AT TIME ZONE, interval arithmetic, to_char.
-- Both sessions run in America/New_York. No statement depends on the wall clock.

-- @id dt-001
SELECT id, extract(year FROM d) AS y, extract(month FROM d) AS m, extract(day FROM d) AS dd FROM t_time WHERE id <= 50 ORDER BY id;
-- @id dt-002
SELECT id, extract(dow FROM d) AS dow, extract(isodow FROM d) AS isodow, extract(doy FROM d) AS doy, extract(week FROM d) AS wk, extract(quarter FROM d) AS q FROM t_time WHERE id <= 50 ORDER BY id;
-- @id dt-003
SELECT id, extract(isoyear FROM d) AS iy, extract(week FROM d) AS wk FROM t_time WHERE id BETWEEN 9011 AND 9015 ORDER BY id;
-- @id dt-004
SELECT id, extract(epoch FROM ts) AS ep FROM t_time WHERE id <= 50 ORDER BY id;
-- @id dt-005
SELECT id, extract(epoch FROM tz) AS ep FROM t_time WHERE id <= 50 OR id BETWEEN 9005 AND 9014 ORDER BY id;
-- @id dt-006
SELECT id, extract(hour FROM tz) AS h, extract(minute FROM tz) AS mi, extract(second FROM ts) AS s FROM t_time WHERE id <= 50 OR id BETWEEN 9007 AND 9014 ORDER BY id;
-- @id dt-007
SELECT id, extract(hour FROM tm) AS h, extract(second FROM tm) AS s, extract(microseconds FROM tm) AS us FROM t_time WHERE id <= 50 ORDER BY id;
-- @id dt-008
SELECT id, extract(epoch FROM iv) AS secs, extract(day FROM iv) AS days, extract(month FROM iv) AS mons, extract(year FROM iv) AS yrs FROM t_time WHERE id <= 50 OR id >= 9000 ORDER BY id;
-- @id dt-009
SELECT id, date_part('year', ts) AS y, date_part('hour', ts) AS h, date_part('epoch', d) AS ep FROM t_time WHERE id <= 50 ORDER BY id;
-- @id dt-010
SELECT extract(year FROM DATE '2024-02-29') AS y, pg_typeof(extract(year FROM DATE '2024-02-29')) AS t, extract(epoch FROM TIMESTAMPTZ '2024-01-01 00:00:00+00') AS ep, extract(timezone_hour FROM TIMESTAMPTZ '2024-07-01 00:00:00+00') AS tzh;
-- @id dt-011
SELECT id, date_trunc('month', ts) AS mo, date_trunc('year', ts) AS yr, date_trunc('hour', ts) AS hr FROM t_time WHERE id <= 50 ORDER BY id;
-- @id dt-012
SELECT id, date_trunc('day', tz) AS local_midnight FROM t_time WHERE id <= 50 OR id BETWEEN 9007 AND 9014 ORDER BY id;
-- @id dt-013
SELECT id, date_trunc('month', tz) AS mo, date_trunc('week', tz) AS wk, date_trunc('quarter', tz) AS q FROM t_time WHERE id <= 50 ORDER BY id;
-- @id dt-014
SELECT id, date_trunc('month', d) AS mo, date_trunc('week', d) AS wk FROM t_time WHERE id <= 50 ORDER BY id;
-- @id dt-015
SELECT id, date_trunc('day', tz, 'UTC') AS utc_midnight FROM t_time WHERE id <= 30 ORDER BY id;
-- @id dt-016
SELECT id, date_trunc('hour', iv) AS h, date_trunc('day', iv) AS dd FROM t_time WHERE id <= 30 ORDER BY id;
-- @id dt-017
SELECT date_trunc('month', tz)::date AS mo, count(*) AS n FROM t_time WHERE id < 9000 GROUP BY 1 ORDER BY 1 LIMIT 40;
-- @id dt-018
SELECT date_trunc('day', placed) AS day, count(*) AS n, sum(amount) AS total FROM orders WHERE placed >= '2023-03-10' AND placed < '2023-03-15' GROUP BY 1 ORDER BY 1;
-- @id dt-019
SELECT placed::date AS day, count(*) AS n FROM orders WHERE placed >= '2023-11-04' AND placed < '2023-11-07' GROUP BY 1 ORDER BY 1;
-- @id dt-020
SELECT count(*) AS n FROM orders WHERE placed >= '2023-03-12' AND placed < '2023-03-13';
-- @id dt-021
SELECT count(*) AS n FROM orders WHERE placed::date = '2023-03-12';
-- @id dt-022
SELECT count(*) AS n FROM orders WHERE placed BETWEEN '2023-06-01' AND '2023-06-30';
-- @id dt-023
SELECT extract(hour FROM placed) AS local_hour, count(*) AS n FROM orders GROUP BY 1 ORDER BY 1;
-- @id dt-024
SELECT extract(dow FROM placed) AS dow, count(*) AS n FROM orders GROUP BY 1 ORDER BY 1;
-- @id dt-025
SELECT id, tz AT TIME ZONE 'UTC' AS utc, tz AT TIME ZONE 'Asia/Kolkata' AS ist FROM t_time WHERE id <= 30 OR id BETWEEN 9007 AND 9014 ORDER BY id;
-- @id dt-026
SELECT id, ts AT TIME ZONE 'UTC' AS assume_utc, ts AT TIME ZONE 'America/New_York' AS assume_ny FROM t_time WHERE id <= 30 OR id BETWEEN 9007 AND 9014 ORDER BY id;
-- @id dt-027
SELECT id, tz AT TIME ZONE 'Europe/London' AS london, (tz AT TIME ZONE 'Europe/London')::date AS london_day FROM t_time WHERE id <= 30 ORDER BY id;
-- @id dt-028
SELECT TIMESTAMPTZ '2024-03-10 06:30:00+00' AT TIME ZONE 'America/New_York' AS before_gap, TIMESTAMPTZ '2024-03-10 07:30:00+00' AT TIME ZONE 'America/New_York' AS after_gap, TIMESTAMP '2024-03-10 02:30:00' AT TIME ZONE 'America/New_York' AS in_gap, TIMESTAMP '2024-11-03 01:30:00' AT TIME ZONE 'America/New_York' AS ambiguous;
-- @id dt-029
SELECT TIMESTAMPTZ '2024-06-01 12:00:00+00' AT TIME ZONE 'EST' AS abbrev, TIMESTAMPTZ '2024-06-01 12:00:00+00' AT TIME ZONE '+05' AS posix_sign, TIMESTAMPTZ '2024-06-01 12:00:00+00' AT TIME ZONE INTERVAL '-05:00' AS by_interval;
-- @id dt-030
SELECT id, d + 1 AS tomorrow, d - 7 AS last_week, d + INTERVAL '1 month' AS next_month FROM t_time WHERE id <= 30 OR id BETWEEN 9007 AND 9015 ORDER BY id;
-- @id dt-031
SELECT DATE '2024-01-31' + INTERVAL '1 month' AS a, DATE '2024-03-31' - INTERVAL '1 month' AS b, DATE '2023-01-31' + INTERVAL '1 month' AS c, DATE '2024-02-29' + INTERVAL '1 year' AS d, DATE '2024-02-29' - INTERVAL '4 years' AS e;
-- @id dt-032
SELECT id, ts + INTERVAL '1 day' AS a, ts - INTERVAL '90 minutes' AS b, ts + INTERVAL '1 month 2 days' AS c FROM t_time WHERE id <= 30 ORDER BY id;
-- @id dt-033
SELECT id, tz + INTERVAL '1 day' AS plus_day, tz + INTERVAL '24 hours' AS plus_24h FROM t_time WHERE id BETWEEN 9007 AND 9010 ORDER BY id;
-- @id dt-034
SELECT id, tz + INTERVAL '1 month' AS a, tz - INTERVAL '1 year' AS b FROM t_time WHERE id <= 30 ORDER BY id;
-- @id dt-035
SELECT id, ts + iv AS shifted FROM t_time WHERE id <= 40 ORDER BY id;
-- @id dt-036
SELECT id, d - DATE '2000-01-01' AS days_since, pg_typeof(d - DATE '2000-01-01') AS t FROM t_time WHERE id <= 30 ORDER BY id;
-- @id dt-037
SELECT id, ts - TIMESTAMP '2000-01-01' AS since FROM t_time WHERE id <= 30 ORDER BY id;
-- @id dt-038
SELECT id, tz - TIMESTAMPTZ '2015-01-01 00:00:00+00' AS since FROM t_time WHERE id <= 30 OR id BETWEEN 9007 AND 9010 ORDER BY id;
-- @id dt-039
SELECT id, iv * 2 AS dbl, iv / 2 AS half, iv + INTERVAL '1 hour' AS plus, -iv AS neg FROM t_time WHERE id <= 30 ORDER BY id;
-- @id dt-040
SELECT id, justify_days(iv) AS jd, justify_hours(iv) AS jh, justify_interval(iv) AS ji FROM t_time WHERE id >= 9000 ORDER BY id;
-- @id dt-041
SELECT id FROM t_time WHERE iv > INTERVAL '1 month' AND id <= 60 ORDER BY id;
-- @id dt-042
SELECT INTERVAL '1 month' = INTERVAL '30 days' AS a, INTERVAL '1 day' = INTERVAL '24 hours' AS b, INTERVAL '1 year' > INTERVAL '360 days' AS c;
-- @id dt-043
SELECT id, iv FROM t_time WHERE id <= 60 ORDER BY iv, id;
-- @id dt-044
SELECT id, age(TIMESTAMP '2025-01-01', ts) AS a FROM t_time WHERE id <= 40 ORDER BY id;
-- @id dt-045
SELECT age(DATE '2024-03-31', DATE '2024-02-29') AS a, age(TIMESTAMP '2024-03-01', TIMESTAMP '2023-01-31') AS b, age(DATE '2000-01-01', DATE '2024-06-15') AS neg;
-- @id dt-046
SELECT id, age(DATE '2025-06-15', d) AS a, extract(year FROM age(DATE '2025-06-15', d)) AS years FROM t_time WHERE id <= 40 ORDER BY id;
-- @id dt-047
SELECT id, to_char(ts, 'YYYY-MM-DD HH24:MI:SS') AS iso, to_char(ts, 'DD Mon YYYY') AS pretty, to_char(ts, 'Day, DDth Month') AS wordy FROM t_time WHERE id <= 30 ORDER BY id;
-- @id dt-048
SELECT id, to_char(tz, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS with_off, to_char(tz, 'HH12:MI AM TZ') AS clock FROM t_time WHERE id <= 30 OR id BETWEEN 9007 AND 9010 ORDER BY id;
-- @id dt-049
SELECT id, to_char(d, 'IYYY-"W"IW-ID') AS isoweek, to_char(d, 'Q') AS q, to_char(d, 'DDD') AS doy, to_char(d, 'FMMonth FMDD, YYYY') AS fm FROM t_time WHERE id <= 30 ORDER BY id;
-- @id dt-050
SELECT id, to_char(n, '999,999,990.00') AS money, to_char(n, 'FM999999990.00') AS fm, to_char(i, '0000000') AS padded FROM t_num WHERE id <= 30 ORDER BY id;
-- @id dt-051
SELECT to_char(1234.5, '9,999.99') AS a, to_char(-1234.5, '9999.99MI') AS b, to_char(0.5, '0.00') AS c, to_char(12, 'RN') AS d, to_char(1234567.891, '9.99EEEE') AS e;
-- @id dt-052
SELECT to_date('2024-02-29', 'YYYY-MM-DD') AS a, to_date('29/02/2024', 'DD/MM/YYYY') AS b, to_timestamp('2024-02-29 13:45:10', 'YYYY-MM-DD HH24:MI:SS') AS c, to_timestamp(1700000000) AS d, to_timestamp(1700000000.5) AS e;
-- @id dt-053
SELECT make_date(2024, 2, 29) AS a, make_timestamp(2024, 2, 29, 13, 45, 10.5) AS b, make_interval(days => 10, hours => 3) AS c, make_time(8, 15, 23.5) AS d, make_timestamptz(2024, 3, 10, 3, 0, 0) AS e;
-- @id dt-054
SELECT id FROM t_time WHERE d BETWEEN '2010-01-01' AND '2010-12-31' ORDER BY id;
-- @id dt-055
SELECT id FROM t_time WHERE ts >= '2010-01-01' AND ts < '2010-02-01' ORDER BY id;
-- @id dt-056
SELECT id FROM t_time WHERE tz >= '2018-03-11' AND tz < '2018-03-12' ORDER BY id;
-- @id dt-057
SELECT id FROM t_time WHERE tz >= TIMESTAMPTZ '2018-03-11 00:00:00+00' AND tz < TIMESTAMPTZ '2018-03-12 00:00:00+00' ORDER BY id;
-- @id dt-058
SELECT id, d FROM t_time WHERE d > '9000-01-01' OR d < '0002-01-01' ORDER BY d, id;
-- @id dt-059
SELECT id, ts FROM t_time WHERE ts = 'infinity' OR ts = '-infinity' ORDER BY id;
-- @id dt-060
SELECT id, isfinite(ts) AS fin, isfinite(d) AS dfin FROM t_time WHERE id BETWEEN 9001 AND 9006 ORDER BY id;
-- @id dt-061
SELECT id, tm + INTERVAL '90 minutes' AS later, tm - TIME '00:30:00' AS since_half, d + tm AS combined FROM t_time WHERE id <= 30 ORDER BY id;
-- @id dt-062
SELECT id, ttz FROM t_time WHERE id <= 20 OR id BETWEEN 9001 AND 9003 ORDER BY id;
-- @id dt-063
SELECT id, (d + 1)::text AS s, (d - 1) AS prev FROM t_time WHERE id IN (9011, 9012, 9013, 9014) ORDER BY id;
-- @id dt-064
SELECT (DATE '2024-03-01' - 1) AS leap, (DATE '2023-03-01' - 1) AS nonleap, (DATE '1900-03-01' - 1) AS century, (DATE '2000-03-01' - 1) AS y2k, DATE '2024-12-31' + 1 AS newyear;
-- @id dt-065
SELECT (TIMESTAMP '2024-01-01', TIMESTAMP '2024-02-01') OVERLAPS (TIMESTAMP '2024-01-15', TIMESTAMP '2024-03-01') AS a, (DATE '2024-01-01', INTERVAL '10 days') OVERLAPS (DATE '2024-01-11', DATE '2024-01-12') AS b;
-- @id dt-066
SELECT id, extract(epoch FROM (tz - LAG(tz) OVER (ORDER BY id))) / 3600 AS hours_gap FROM t_time WHERE id <= 20 ORDER BY id;
-- @id dt-067
SELECT to_char(placed, 'YYYY-MM') AS ym, count(*) AS n, sum(amount) AS total FROM orders GROUP BY 1 ORDER BY 1;
-- @id dt-068
SELECT extract(year FROM created) AS y, extract(month FROM created) AS m, count(*) AS n FROM customers GROUP BY 1, 2 ORDER BY 1, 2;
-- @id dt-069
SELECT date_bin('15 minutes', TIMESTAMP '2024-02-29 13:47:10', TIMESTAMP '2001-01-01') AS a, date_bin('1 hour', TIMESTAMPTZ '2024-03-10 07:29:00+00', TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b;
-- @id dt-070
SELECT '2024-03-10'::date + TIME '02:30' AS ts_plain, ('2024-03-10'::date + TIME '02:30')::timestamptz AS shifted_by_gap, '2024-03-10 00:00:00'::timestamptz + INTERVAL '3 hours' AS wall_plus_3h;
-- @id dt-071
SELECT 'epoch'::timestamp AS a, 'epoch'::timestamptz AS b, TIMESTAMP '2024-01-01' - INTERVAL '1 microsecond' AS c, TIMESTAMP '2024-01-01 00:00:00.1234565' AS rounds, TIME '23:59:59.9999995' AS trounds;
