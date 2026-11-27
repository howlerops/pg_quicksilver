-- d_primary.sql, primary, superuser. Three states, chosen with -v step=...
--   grant : app may read the join tables in full (the baseline for experiment d)
--   revoke: app may read ex.jord(id, amount) and public.orders(id, amount) only, and ALL of
--           ex.jcust and public.customers: the join key columns (cust, customer_id) are not granted
--   restore: back to the grants of the regression setup (SELECT on public.orders, public.customers)
--            and no ex-schema grant at all
\if :{?step}
\else
\echo 'd_primary.sql needs -v step=grant|revoke|restore'
\quit 1
\endif
SELECT :'step' = 'grant' AS is_grant \gset
SELECT :'step' = 'revoke' AS is_revoke \gset
SELECT :'step' = 'restore' AS is_restore \gset
\if :is_grant
GRANT USAGE ON SCHEMA ex TO app;
GRANT SELECT ON ex.jcust, ex.jord TO app;
\endif
\if :is_revoke
REVOKE SELECT ON ex.jord FROM app;
GRANT SELECT (id, amount) ON ex.jord TO app;
REVOKE SELECT ON public.orders FROM app;
GRANT SELECT (id, amount) ON public.orders TO app;
\endif
\if :is_restore
REVOKE SELECT (id, amount) ON ex.jord FROM app;
REVOKE SELECT (id, amount) ON public.orders FROM app;
GRANT SELECT ON public.orders, public.customers TO app;
REVOKE SELECT ON ex.jcust, ex.jord FROM app;
REVOKE USAGE ON SCHEMA ex FROM app;
\endif
SELECT grantee, table_schema, table_name, privilege_type, column_name
  FROM information_schema.column_privileges WHERE grantee = 'app' AND table_schema IN ('public', 'ex')
  ORDER BY 2, 3, 5;
