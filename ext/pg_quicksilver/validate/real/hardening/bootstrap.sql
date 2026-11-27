-- bootstrap: the ordinary role app (primary, superuser)
CREATE ROLE app LOGIN;
GRANT SELECT ON public.orders, public.customers TO app;
SELECT rolname, rolsuper FROM pg_roles WHERE rolname IN ('app','quicksilver_mirror_reader','qs_duck');
SELECT * FROM quicksilver.mirror_status();
SHOW duckdb.postgres_role;
SHOW duckdb.confined_file_access;
SHOW duckdb.allowed_directories;
SELECT * FROM quicksilver.join_domains;
