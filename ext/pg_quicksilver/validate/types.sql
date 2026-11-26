-- public.qs_types: one table that holds every type the seed allowlist names,
-- two columns of each so that column-to-column comparisons have something to
-- say, NULLs in every nullable column, and no +-infinity (the mirror does not
-- keep it; bench/exactness's t_time covers that and stays on the heap).
-- Deterministic: arithmetic on generate_series, no random(), no now().
SET TimeZone = 'UTC';
CREATE TABLE public.qs_types (
  id   int PRIMARY KEY,
  si   smallint,
  i    int,
  bi   bigint,
  n    numeric(12,2),
  n2   numeric(12,2),
  n4   numeric(10,4),
  b    boolean,
  b2   boolean,
  t    text,
  t2   text,
  tc   text COLLATE "C",
  tc2  text COLLATE "C",
  u    uuid,
  u2   uuid,
  d    date,
  d2   date,
  ts   timestamp,
  ts2  timestamp,
  tz   timestamptz,
  tz2  timestamptz
);
INSERT INTO public.qs_types
SELECT g,
  CASE WHEN g % 17 = 0 THEN NULL ELSE ((g * 37) % 201 - 100)::smallint END,
  CASE WHEN g % 19 = 0 THEN NULL ELSE (g * 7919) % 201 - 100 END,
  CASE WHEN g % 23 = 0 THEN NULL ELSE (g::bigint * 104729) % 201 - 100 END,
  CASE WHEN g % 13 = 0 THEN NULL ELSE (((g * 7919) % 20001 - 10000) / 100.0)::numeric(12,2) END,
  CASE WHEN g % 29 = 0 THEN NULL ELSE (((g * 31) % 20001 - 10000) / 100.0)::numeric(12,2) END,
  CASE WHEN g % 31 = 0 THEN NULL ELSE (((g * 193) % 2000001 - 1000000) / 10000.0)::numeric(10,4) END,
  CASE WHEN g % 7 = 0 THEN NULL ELSE g % 3 = 0 END,
  CASE WHEN g % 11 = 0 THEN NULL ELSE g % 5 < 2 END,
  CASE WHEN g % 37 = 0 THEN NULL ELSE (ARRAY['apple','Apple','banana','Éclair','zebra','','naïve','日本','a b','A','éclair','Zebra','b','😀','ab'])[1 + g % 15] END,
  CASE WHEN g % 41 = 0 THEN NULL ELSE (ARRAY['apple','Apple','banana','Éclair','zebra','','naïve','日本','a b','A','éclair','Zebra','b','😀','ab'])[1 + (g * 7) % 15] END,
  CASE WHEN g % 37 = 0 THEN NULL ELSE (ARRAY['apple','Apple','banana','Éclair','zebra','','naïve','日本','a b','A','éclair','Zebra','b','😀','ab'])[1 + g % 15] END,
  CASE WHEN g % 41 = 0 THEN NULL ELSE (ARRAY['apple','Apple','banana','Éclair','zebra','','naïve','日本','a b','A','éclair','Zebra','b','😀','ab'])[1 + (g * 7) % 15] END,
  CASE WHEN g % 43 = 0 THEN NULL ELSE md5((g % 50)::text)::uuid END,
  CASE WHEN g % 47 = 0 THEN NULL ELSE md5(((g * 3) % 50)::text)::uuid END,
  CASE WHEN g % 53 = 0 THEN NULL ELSE date '2024-01-01' + (g % 400) END,
  CASE WHEN g % 59 = 0 THEN NULL ELSE date '2024-01-01' + ((g * 7) % 400) END,
  CASE WHEN g % 61 = 0 THEN NULL ELSE timestamp '2024-01-01 00:00:00' + make_interval(hours => g % 1000, secs => (g % 7) / 1000000.0) END,
  CASE WHEN g % 67 = 0 THEN NULL ELSE timestamp '2024-01-01 00:00:00' + make_interval(hours => (g * 7) % 1000, secs => (g % 7) / 1000000.0) END,
  CASE WHEN g % 71 = 0 THEN NULL ELSE timestamptz '2024-03-01 00:00:00+00' + make_interval(hours => g % 1000, secs => (g % 5) / 1000000.0) END,
  CASE WHEN g % 73 = 0 THEN NULL ELSE timestamptz '2024-03-01 00:00:00+00' + make_interval(hours => (g * 7) % 1000, secs => (g % 5) / 1000000.0) END
FROM generate_series(1, 20000) g;

-- The extremes each type can hold, short of infinity. ids >= 100000, so the
-- arithmetic statements (which would overflow on both sides) can leave them out.
INSERT INTO public.qs_types (id, si, i, bi, n, n2, n4, b, b2, t, t2, tc, tc2, u, u2, d, d2, ts, ts2, tz, tz2) VALUES
 (100001,  32767,  2147483647,  9223372036854775807,  9999999999.99, -9999999999.99,  999999.9999, true, false, 'ÿ', 'Z', 'ÿ', 'Z',
  'ffffffff-ffff-ffff-ffff-ffffffffffff', '00000000-0000-0000-0000-000000000000',
  '9999-12-31', '0001-01-01', '9999-12-31 23:59:59.999999', '0001-01-01 00:00:00', '9999-12-31 23:59:59.999999+00', '0001-01-01 00:00:00+00'),
 (100002, -32768, -2147483648, -9223372036854775808, -9999999999.99,  9999999999.99, -999999.9999, false, true, 'a', 'B', 'a', 'B',
  '00000000-0000-0000-0000-000000000000', 'ffffffff-ffff-ffff-ffff-ffffffffffff',
  '0001-01-01', '9999-12-31', '0001-01-01 00:00:00', '9999-12-31 23:59:59.999999', '0001-01-01 00:00:00+00', '9999-12-31 23:59:59.999999+00'),
 (100003, 0, 0, 0, 0.00, 0.00, 0.0000, NULL, NULL, '', '', '', '', NULL, NULL,
  '1970-01-01', '1969-12-31', '1970-01-01 00:00:00', '1969-12-31 23:59:59.999999', '1970-01-01 00:00:00+00', '1969-12-31 23:59:59.999999+00'),
 (100004, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 (100005, 1, 1, 1, 0.01, 0.01, 0.0001, true, true, 'A b', 'a B', 'A b', 'a B',
  'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
  '2024-02-29', '2024-02-29', '2024-03-10 02:30:00', '2024-11-03 01:30:00', '2024-03-10 06:59:59+00', '2024-11-03 05:30:00+00');
