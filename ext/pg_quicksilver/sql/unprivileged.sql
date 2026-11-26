-- Roles that are not superusers, against the lake the sidecar built
-- (public.orders, public.customers and their DuckLake tables).
--
-- The application's role holds SELECT on the heap table and nothing else: no
-- membership of duckdb.postgres_role, no privilege on a mirror relation. The
-- mirror side runs as the bootstrap superuser in a DuckDB that pg_duckdb's
-- own settings confine to the lake's directory. What a role cannot read from
-- the heap it must not be able to read through the mirror, by any route.
\set VERBOSITY terse
SHOW duckdb.enable_external_access;
SELECT count(*) AS lakes_inside_allowed_directories FROM quicksilver.lakes l
 WHERE EXISTS (SELECT 1 FROM unnest(string_to_array(current_setting('duckdb.allowed_directories'), ',')) d
                WHERE l.catalog LIKE d || '/%');

CREATE ROLE qs_u_app LOGIN;
CREATE ROLE qs_u_col LOGIN;
CREATE ROLE qs_u_none LOGIN;
CREATE ROLE qs_u_duck LOGIN IN ROLE qs_duck;      -- may run DuckDB itself: never given the lake
GRANT SELECT ON public.orders, public.customers TO qs_u_app, qs_u_duck;
GRANT SELECT (id, customer_id, amount) ON public.orders TO qs_u_col;
CREATE SCHEMA qs_u;
GRANT USAGE, CREATE ON SCHEMA qs_u TO qs_u_app, qs_u_col, qs_u_none;
-- views owned by somebody who may read the table
CREATE VIEW qs_u.plain AS SELECT id, customer_id, amount FROM public.orders WHERE customer_id = 1;
CREATE VIEW qs_u.barrier WITH (security_barrier) AS SELECT id, customer_id, amount FROM public.orders WHERE customer_id = 1;
CREATE VIEW qs_u.invoker WITH (security_invoker) AS SELECT id, customer_id, amount FROM public.orders WHERE customer_id = 1;
GRANT SELECT ON qs_u.plain, qs_u.barrier, qs_u.invoker TO qs_u_none, qs_u_app;
CREATE FUNCTION qs_u.definer_total() RETURNS numeric LANGUAGE sql SECURITY DEFINER
  AS $$ SELECT sum(amount) FROM public.orders WHERE customer_id = 1 $$;
CREATE FUNCTION qs_u.invoker_total() RETURNS numeric LANGUAGE sql SECURITY INVOKER
  AS $$ SELECT sum(amount) FROM public.orders WHERE customer_id = 1 $$;

\set last 'SELECT side, reason FROM quicksilver.last_decision();'
\set agg 'SELECT count(*), sum(amount), min(id), max(id) FROM public.orders WHERE customer_id = 1;'

-- ---------------------------------------------------------------- allowed: routed, and the heap's answer
\c - qs_u_app
\set VERBOSITY terse
SELECT current_user, pg_has_role(current_user, 'qs_duck', 'MEMBER') AS duckdb_member,
       has_schema_privilege('quicksilver_mirror', 'USAGE') AS mirror_schema;
SET quicksilver.route = auto;
:agg
:last
SET quicksilver.route = heap;
:agg
SET quicksilver.route = auto;
SELECT c.region, count(*), sum(o.amount) FROM public.orders o JOIN public.customers c ON c.id = o.customer_id
 WHERE o.customer_id = 1 GROUP BY c.region;
:last
EXPLAIN (COSTS OFF, ANALYZE, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT count(*) FROM public.orders;
-- in a transaction block, with a savepoint after it
BEGIN;
:agg
:last
SAVEPOINT s;
ROLLBACK;
-- a view owned by a role that may read the table: its owner's privileges, on either side
SELECT count(*), sum(amount) FROM qs_u.plain;
:last

-- and nothing else of DuckDB's, before or after this session's DuckDB has the lake attached
SELECT * FROM duckdb.query($$ SELECT 1 $$);
SELECT * FROM duckdb.query($$ SELECT count(*) FROM read_csv('/etc/passwd', header = false, sep = '|') $$);
SELECT * FROM duckdb.query($$ SELECT count(*) FROM lake.public.orders $$);
SELECT duckdb.raw_query($$ SELECT count(*) FROM lake.public.orders $$);
SELECT count(*) FROM read_csv('/etc/passwd') AS r;
SELECT count(*) FROM read_parquet('/tmp/qs_regress/errors.parquet') AS r;
CALL duckdb.recycle_ddb();
SELECT quicksilver.attach_lakes();
SELECT count(*) FROM quicksilver_mirror."public.orders";
SELECT quicksilver.register_mirror('public.orders', 'public.customers');
SELECT quicksilver.report_applied_lsn('public.orders', '0/0');
SET quicksilver.route_unprivileged = on;
SET quicksilver.allow_unsafe = on;
SET duckdb.enable_external_access = on;
SET duckdb.allowed_directories = '/';
COPY public.orders TO '/tmp/qs_regress/stolen.csv';
-- a view of its own over DuckDB is no way in either: pg_duckdb does not let it make one,
-- and a statement that calls a function is not one the router sends anywhere
CREATE VIEW qs_u.sneak AS SELECT r['column0']::text AS line FROM duckdb.query($$ SELECT * FROM read_csv('/etc/passwd', header = false, sep = '|') $$) r;
SET quicksilver.route = mirror;
SELECT count(*) FROM public.orders o, duckdb.query($$ SELECT 1 $$) s WHERE o.customer_id = 1;
SET quicksilver.route = auto;
-- still routed after all of that
:agg
:last

-- ---------------------------------------------------------------- no privilege on the table: the heap's error
\c - qs_u_none
\set VERBOSITY terse
SET quicksilver.route = mirror;
:agg
:last
SET quicksilver.route = heap;
:agg
SET quicksilver.route = mirror;
-- through a security_barrier view: allowed to read it, never through the mirror
SELECT count(*), sum(amount) FROM qs_u.barrier;
:last
SET quicksilver.route = heap;
SELECT count(*), sum(amount) FROM qs_u.barrier;
SET quicksilver.route = mirror;
-- a security_invoker view checks the caller: denied, as on the heap
SELECT count(*), sum(amount) FROM qs_u.invoker;
-- a plain view owned by a role that may read the table
SELECT count(*), sum(amount) FROM qs_u.plain;
:last
-- functions: their statements stay on the heap; DEFINER answers, INVOKER is denied
SELECT qs_u.definer_total();
SELECT qs_u.invoker_total();

-- ---------------------------------------------------------------- column privileges
\c - qs_u_col
\set VERBOSITY terse
SET quicksilver.route = mirror;
SELECT count(*), sum(amount) FROM public.orders WHERE customer_id = 1;
:last
SELECT count(*) FROM public.orders WHERE status = 'new';
SELECT max(created_at) FROM public.orders;
SELECT * FROM public.orders WHERE id = 1;
SELECT count(*) FROM public.orders o WHERE EXISTS (SELECT 1 FROM public.orders p WHERE p.id = o.id AND p.status = 'new');
SET quicksilver.route = heap;
SELECT count(*) FROM public.orders WHERE status = 'new';

-- ---------------------------------------------------------------- row-level security
\c - postgres
\set VERBOSITY terse
ALTER TABLE public.customers ENABLE ROW LEVEL SECURITY;
CREATE POLICY qs_u_few ON public.customers FOR SELECT TO qs_u_app USING (id <= 10);
\c - qs_u_app
\set VERBOSITY terse
SET quicksilver.route = mirror;
SELECT count(*), min(id), max(id) FROM public.customers;
:last
SELECT count(*) FROM public.orders o JOIN public.customers c ON c.id = o.customer_id WHERE o.customer_id IN (1, 500);
:last
SET quicksilver.route = heap;
SELECT count(*), min(id), max(id) FROM public.customers;
SELECT count(*) FROM public.orders o JOIN public.customers c ON c.id = o.customer_id WHERE o.customer_id IN (1, 500);
\c - postgres
\set VERBOSITY terse
DROP POLICY qs_u_few ON public.customers;
ALTER TABLE public.customers DISABLE ROW LEVEL SECURITY;
\c - qs_u_app
SET quicksilver.route = mirror;
SELECT count(*), min(id), max(id) FROM public.customers;
:last

-- ---------------------------------------------------------------- a member of duckdb.postgres_role
\c - qs_u_duck
\set VERBOSITY terse
SET quicksilver.route = mirror;
:agg
SELECT side, reason, detail FROM quicksilver.last_decision();
-- stock pg_duckdb gives such a role DuckDB without the local file system
SELECT * FROM duckdb.query($$ SELECT count(*) FROM read_csv('/etc/passwd', header = false, sep = '|') $$);

-- ---------------------------------------------------------------- an unsafe configuration is detected, and says what is missing
\c - postgres
\set VERBOSITY terse
ALTER ROLE qs_u_app SET duckdb.enable_external_access = on;
\c - qs_u_app
SET quicksilver.route = mirror;
:agg
SELECT side, reason, detail FROM quicksilver.last_decision();
\c - postgres
ALTER ROLE qs_u_app RESET duckdb.enable_external_access;
ALTER ROLE qs_u_app SET duckdb.allowed_directories = '/nowhere';
\c - qs_u_app
SET quicksilver.route = mirror;
:agg
SELECT side, reason, detail FROM quicksilver.last_decision();
\c - postgres
ALTER ROLE qs_u_app RESET duckdb.allowed_directories;
ALTER ROLE qs_u_app SET quicksilver.route_unprivileged = off;
\c - qs_u_app
SET quicksilver.route = mirror;
:agg
SELECT side, reason, detail FROM quicksilver.last_decision();
\c - postgres
ALTER ROLE qs_u_app RESET quicksilver.route_unprivileged;
-- ---------------------------------------------------------------- the superuser's DuckDB is confined too
SELECT * FROM duckdb.query($$ SELECT count(*) FROM read_csv('/etc/passwd', header = false, sep = '|') $$);
SELECT duckdb.raw_query($$ SET enable_external_access = true $$);
SELECT duckdb.raw_query($$ SET allowed_directories = ['/'] $$);

-- a superuser who becomes the role is the role: routed for it, and the role's privileges apply
SET ROLE qs_u_none;
SET quicksilver.route = mirror;
:agg
RESET ROLE;

DROP SCHEMA qs_u CASCADE;
REVOKE ALL ON public.orders, public.customers FROM qs_u_app, qs_u_duck, qs_u_col;
DROP ROLE qs_u_app, qs_u_col, qs_u_none, qs_u_duck;
