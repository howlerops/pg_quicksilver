-- as superuser, on the PRIMARY: write the test mirror's Parquet file, the same 2,000 rows as public.hx
-- (setup_hx.sql; also run after a break_hx.sql to put the file back).
SELECT duckdb.raw_query($$ COPY (SELECT g AS id, g::double AS amount, g % 50 AS g FROM range(1, 2001) t(g)) TO '/tmp/qs_regress/hx.parquet' $$);
