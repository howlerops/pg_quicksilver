-- fix_hx.sql, on the PRIMARY as superuser: the test table of setup_hx.sql as it was first created
-- had an unqualified numeric, which the allowlist refuses. Change it to numeric(12,2), point the
-- mirror view at the same type (the view is dropped and made again: CREATE OR REPLACE cannot
-- change a column's type), hand it to the mirror role, and register it again.
ALTER TABLE public.hx ALTER COLUMN amount TYPE numeric(12,2);
DROP VIEW qhxm.hx;
CREATE VIEW qhxm.hx AS SELECT r['id']::int AS id, r['amount']::numeric(12,2) AS amount, r['g']::int AS g
  FROM duckdb.query($$ SELECT * FROM read_parquet('/tmp/qs_regress/hx.parquet') $$) r;
ALTER VIEW qhxm.hx OWNER TO quicksilver_mirror_reader;
SELECT quicksilver.register_mirror('public.hx', 'qhxm.hx', 1000000000000);
