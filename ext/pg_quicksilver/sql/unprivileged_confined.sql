-- Ordinary roles on the confined build (docs/40, "Ordinary roles").
--
-- A role that is not a superuser is routed only where pg_duckdb is confined:
-- the build defines duckdb.confined_file_access, the lake's directory is in
-- duckdb.allowed_directories, and the role is a member of duckdb.postgres_role
-- and of neither pg_read_server_files nor pg_write_server_files. On the stock
-- build this file does nothing but say so (expected/unprivileged_confined_1.out).
--
-- Each case: the answer is the heap's, and the decision is either "routed"
-- (the mirror answered, with the same rows) or the heap's reason.
\set VERBOSITY terse
SELECT coalesce(current_setting('duckdb.confined_file_access', true), 'off') = 'on' AS confined \gset
\if :confined
SELECT current_setting('duckdb.allowed_directories') LIKE '%/mirror,/tmp/qs_regress' AS confined_to_the_lake;

CREATE ROLE qs_c_ok LOGIN IN ROLE qs_duck;             -- member, SELECT on both tables: routed
CREATE ROLE qs_c_nosel LOGIN IN ROLE qs_duck;          -- member, no privilege on orders
CREATE ROLE qs_c_nocol LOGIN IN ROLE qs_duck;          -- member, SELECT on (id, customer_id) only
CREATE ROLE qs_c_rls LOGIN IN ROLE qs_duck;            -- member, SELECT on customers, which has row-level security
CREATE ROLE qs_c_out LOGIN;                            -- not a member: the heap
CREATE ROLE qs_c_files LOGIN IN ROLE qs_duck, pg_read_server_files;  -- member and holder of the file role: the heap
GRANT SELECT ON public.orders, public.customers TO qs_c_ok, qs_c_out, qs_c_files;
GRANT SELECT (id, customer_id) ON public.orders TO qs_c_nocol;
GRANT SELECT ON public.customers TO qs_c_rls;
CREATE SCHEMA qs_c;
GRANT USAGE ON SCHEMA qs_c TO qs_c_ok, qs_c_nosel, qs_c_nocol, qs_c_rls, qs_c_out, qs_c_files;
CREATE VIEW qs_c.plain AS SELECT id, customer_id, amount FROM public.orders WHERE customer_id = 1;
GRANT SELECT ON qs_c.plain TO qs_c_ok, qs_c_nosel, qs_c_out;

\set last 'SELECT side, reason FROM quicksilver.last_decision();'
\set agg 'SELECT count(*), sum(amount), min(id), max(id) FROM public.orders WHERE customer_id = 1;'

-- ---------------------------------------------------------------- member of duckdb.postgres_role, SELECT on the table: routed
\c - qs_c_ok
\set VERBOSITY terse
SELECT current_user, pg_has_role(current_user, 'qs_duck', 'MEMBER') AS duckdb_member,
       pg_has_role(current_user, 'pg_read_server_files', 'MEMBER') AS read_files;
SET quicksilver.route = mirror;
:agg
:last
SET quicksilver.route = heap;
:agg
SET quicksilver.route = mirror;
SELECT c.region, count(*), sum(o.amount) FROM public.orders o JOIN public.customers c ON c.id = o.customer_id
 WHERE o.customer_id = 1 GROUP BY c.region;
:last
-- in a read-only transaction block: routed as well
BEGIN;
:agg
:last
COMMIT;
-- a view owned by a role that may read the table
SELECT count(*), sum(amount) FROM qs_c.plain;
:last
-- the role's DuckDB is confined: a file outside the lake is refused (SQLSTATE only; the message differs by build)
\set VERBOSITY sqlstate
SELECT * FROM duckdb.query($$ SELECT count(*) FROM read_csv('/etc/passwd', header = false, sep = '|') $$);
\set VERBOSITY terse
:agg
:last

-- ---------------------------------------------------------------- member, no privilege on the table: the heap's error
\c - qs_c_nosel
\set VERBOSITY terse
SET quicksilver.route = mirror;
:agg
:last
SELECT count(*) FROM qs_c.plain;
:last

-- ---------------------------------------------------------------- member, SELECT on one column only
\c - qs_c_nocol
\set VERBOSITY terse
SET quicksilver.route = mirror;
SELECT count(*) FROM public.orders WHERE customer_id = 1;
:last
:agg
:last

-- ---------------------------------------------------------------- member, row-level security on the table: the heap's rows
\c - postgres
\set VERBOSITY terse
ALTER TABLE public.customers ENABLE ROW LEVEL SECURITY;
CREATE POLICY qs_c_few ON public.customers FOR SELECT TO qs_c_rls USING (id <= 10);
\c - qs_c_rls
\set VERBOSITY terse
SET quicksilver.route = mirror;
SELECT count(*), min(id), max(id) FROM public.customers;
:last
\c - postgres
\set VERBOSITY terse
DROP POLICY qs_c_few ON public.customers;
ALTER TABLE public.customers DISABLE ROW LEVEL SECURITY;

-- ---------------------------------------------------------------- not a member of duckdb.postgres_role: the heap
\c - qs_c_out
\set VERBOSITY terse
SET quicksilver.route = mirror;
:agg
:last
SELECT count(*), sum(amount) FROM qs_c.plain;
:last

-- ---------------------------------------------------------------- member, and holds pg_read_server_files: the heap
\c - qs_c_files
\set VERBOSITY terse
SET quicksilver.route = mirror;
:agg
:last

-- ---------------------------------------------------------------- back as the superuser
\c - postgres
\set VERBOSITY terse
DROP SCHEMA qs_c CASCADE;
REVOKE ALL ON public.orders, public.customers FROM qs_c_ok, qs_c_nosel, qs_c_nocol, qs_c_rls, qs_c_out, qs_c_files;
DROP ROLE qs_c_ok, qs_c_nosel, qs_c_nocol, qs_c_rls, qs_c_out, qs_c_files;
\else
\echo skipped: the DuckDB build is not the confined one (duckdb.confined_file_access is off)
\endif
