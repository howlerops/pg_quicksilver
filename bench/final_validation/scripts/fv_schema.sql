-- Part 1 tables, created on the PRIMARY (run with the primary's port).
-- fv.events   : uuid primary key, numeric(12,2), timestamptz, text      (200,000 rows)
-- fv.accounts : text primary key, numeric(12,2), timestamptz, text      (200,000 rows)
-- fv.lookup   : small lookup table, integer key                          (500 rows)
DROP SCHEMA IF EXISTS fv CASCADE;
CREATE SCHEMA fv;
CREATE TABLE fv.events (
  id uuid PRIMARY KEY,
  amount numeric(12,2) NOT NULL,
  at timestamptz NOT NULL,
  note text
);
CREATE TABLE fv.accounts (
  code text PRIMARY KEY,
  balance numeric(12,2) NOT NULL,
  opened timestamptz NOT NULL,
  label text
);
CREATE TABLE fv.lookup (
  id integer PRIMARY KEY,
  name text NOT NULL
);
INSERT INTO fv.events
  SELECT md5('e' || i::text)::uuid,
         ((i * 7919) % 1000000)::numeric / 100 - 2000,
         timestamptz '2025-01-01 00:00:00+00' + (i * 97) * interval '1 minute' + (i % 1000) * interval '1 microsecond',
         CASE WHEN i % 11 = 0 THEN NULL ELSE 'note ' || (i % 5000) END
  FROM generate_series(1, 200000) AS i;
INSERT INTO fv.accounts
  SELECT 'AC' || lpad(i::text, 7, '0'),
         ((i::bigint * 104729) % 10000000)::numeric / 100,
         timestamptz '2024-06-01 12:00:00+00' + (i % 3650) * interval '1 day',
         CASE WHEN i % 7 = 0 THEN NULL ELSE 'label-' || (i % 997) END
  FROM generate_series(1, 200000) AS i;
INSERT INTO fv.lookup SELECT i, 'name-' || i FROM generate_series(1, 500) AS i;
ANALYZE fv.events; ANALYZE fv.accounts; ANALYZE fv.lookup;
