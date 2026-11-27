-- setup for experiment a (and the mirror-failure method of sql/errors.sql): a heap table of
-- its own, owned by postgres, SELECT granted to app, and a mirror view over a Parquet file
-- that DuckDB wrote, owned by quicksilver_mirror_reader (as the sidecar makes them).
--
-- The first version of this file declared amount as numeric (no precision): the allowlist
-- refused it ("column public.hx.amount declared without precision"), so the statement went to
-- the heap and the test said nothing about the mirror. The table is numeric(12,2) now (the
-- ALTER and CREATE OR REPLACE below, which fix_hx.sql ran on the already-created table).
\set VERBOSITY terse
CREATE SCHEMA qhxm;
CREATE TABLE public.hx (id int PRIMARY KEY, amount numeric(12,2), g int);
INSERT INTO public.hx SELECT g, g, g % 50 FROM generate_series(1, 2000) g;
GRANT SELECT ON public.hx TO app;
SELECT duckdb.raw_query($$ COPY (SELECT g AS id, g::double AS amount, g % 50 AS g FROM range(1, 2001) t(g)) TO '/tmp/qs_regress/hx.parquet' $$);
CREATE VIEW qhxm.hx AS SELECT r['id']::int AS id, r['amount']::numeric(12,2) AS amount, r['g']::int AS g
  FROM duckdb.query($$ SELECT * FROM read_parquet('/tmp/qs_regress/hx.parquet') $$) r;
ALTER VIEW qhxm.hx OWNER TO quicksilver_mirror_reader;
GRANT USAGE ON SCHEMA qhxm TO quicksilver_mirror_reader;
SELECT quicksilver.register_mirror('public.hx', 'qhxm.hx', 1000000000000);
