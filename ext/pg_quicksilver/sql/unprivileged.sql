-- Roles that are not superusers, against the lake the sidecar built
-- (public.orders, public.customers and their DuckLake tables).
--
-- Stock pg_duckdb gives a role that is not a superuser no local file system
-- for its DuckDB, and refuses DuckDB to any role that is not a member of
-- duckdb.postgres_role. So nothing a role of this kind runs is routed: the
-- heap answers it, with the heap's rows or the heap's error, and the mirror is
-- never asked. What a role cannot read from the heap it must not be able to
-- read through the mirror, by any route. The superuser contrast is at the top.
\set VERBOSITY terse
SHOW duckdb.enable_external_access;
SHOW duckdb.allowed_directories;

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

-- ---------------------------------------------------------------- the superuser: routed (the contrast)
SET quicksilver.route = mirror;
:agg
:last

-- ---------------------------------------------------------------- SELECT on the table, nothing else: the heap
\c - qs_u_app
\set VERBOSITY terse
SELECT current_user, pg_has_role(current_user, 'qs_duck', 'MEMBER') AS duckdb_member,
       has_schema_privilege('quicksilver_mirror', 'USAGE') AS mirror_schema;
SET quicksilver.route = mirror;
:agg
:last
SET quicksilver.route = heap;
:agg
SET quicksilver.route = mirror;
SELECT c.region, count(*), sum(o.amount) FROM public.orders o JOIN public.customers c ON c.id = o.customer_id
 WHERE o.customer_id = 1 GROUP BY c.region;
:last
EXPLAIN (COSTS OFF) SELECT count(*) FROM public.orders;
-- in a transaction block, with a savepoint after it
BEGIN;
:agg
:last
SAVEPOINT s;
ROLLBACK;
-- a view owned by a role that may read the table: its owner's privileges, on either side
SELECT count(*), sum(amount) FROM qs_u.plain;
:last
-- DuckDB itself, and the lake's files, are not the role's
SELECT * FROM duckdb.query($$ SELECT 1 $$);
SELECT * FROM duckdb.query($$ SELECT count(*) FROM read_csv('/etc/passwd', header = false, sep = '|') $$);
SELECT * FROM duckdb.query($$ SELECT count(*) FROM lake.public.orders $$);
SELECT count(*) FROM read_csv('/etc/passwd') AS r;
SELECT count(*) FROM quicksilver_mirror."public.orders";
SELECT quicksilver.attach_lakes();
SELECT quicksilver.register_mirror('public.orders', 'public.customers');
SELECT quicksilver.report_applied_lsn('public.orders', '0/0');
SET quicksilver.allow_unsafe = on;
SET duckdb.enable_external_access = on;
SET duckdb.allowed_directories = '/';
COPY public.orders TO '/tmp/qs_regress/stolen.csv';
-- still on the heap after all of that
:agg
:last
-- a set-returning call does not change where the statement goes
SET quicksilver.route = mirror;
SELECT count(*) FROM public.orders o, duckdb.query($$ SELECT 1 $$) s WHERE o.customer_id = 1;
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
-- through a security_barrier view: allowed to read it, and the heap answers
SELECT count(*), sum(amount) FROM qs_u.barrier;
:last
-- a security_invoker view checks the caller: denied, as on the heap
SELECT count(*), sum(amount) FROM qs_u.invoker;
-- a plain view owned by a role that may read the table
SELECT count(*), sum(amount) FROM qs_u.plain;
:last
-- functions: DEFINER answers, INVOKER is denied
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

-- ---------------------------------------------------------------- row-level security
\c - postgres
\set VERBOSITY terse
ALTER TABLE public.customers ENABLE ROW LEVEL SECURITY;
CREATE POLICY qs_u_few ON public.customers FOR SELECT TO qs_u_app USING (id <= 10);
SET quicksilver.route = mirror;
SELECT count(*), min(id), max(id) FROM public.customers;
:last
\c - qs_u_app
\set VERBOSITY terse
SET quicksilver.route = mirror;
SELECT count(*), min(id), max(id) FROM public.customers;
:last
SELECT count(*) FROM public.orders o JOIN public.customers c ON c.id = o.customer_id WHERE o.customer_id IN (1, 500);
:last
\c - postgres
\set VERBOSITY terse
DROP POLICY qs_u_few ON public.customers;
ALTER TABLE public.customers DISABLE ROW LEVEL SECURITY;

-- ---------------------------------------------------------------- a member of duckdb.postgres_role
\c - qs_u_duck
\set VERBOSITY terse
SET quicksilver.route = mirror;
:agg
SELECT side, reason FROM quicksilver.last_decision();
-- stock pg_duckdb gives such a role DuckDB without the local file system
SELECT * FROM duckdb.query($$ SELECT count(*) FROM read_csv('/etc/passwd', header = false, sep = '|') $$);

-- ---------------------------------------------------------------- superuser, and a superuser who becomes the role
\c - postgres
\set VERBOSITY terse
SELECT * FROM duckdb.query($$ SELECT count(*) FROM read_csv('/etc/passwd', header = false, sep = '|') $$);
SELECT duckdb.raw_query($$ SET enable_external_access = true $$);
SELECT duckdb.raw_query($$ SET allowed_directories = ['/'] $$);
SET quicksilver.route = mirror;
SET ROLE qs_u_none;
:agg
RESET ROLE;
:agg
:last

DROP SCHEMA qs_u CASCADE;
REVOKE ALL ON public.orders, public.customers FROM qs_u_app, qs_u_duck, qs_u_col;
DROP ROLE qs_u_app, qs_u_col, qs_u_none, qs_u_duck;
