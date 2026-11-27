-- as superuser, on the PRIMARY (its file is the lake's file here; both servers share the host):
-- replace the test mirror's Parquet file with bytes that are not Parquet, as errors.sql does.
-- Every statement that reads public.hx through its mirror now fails on the mirror side.
SELECT lo_from_bytea(0, 'not a parquet file') AS lo \gset
SELECT lo_export(:lo, '/tmp/qs_regress/hx.parquet');
SELECT lo_unlink(:lo);
