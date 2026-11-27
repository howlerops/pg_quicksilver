-- c_primary.sql: the primary-side changes of experiment c, selected by the PSQL variable 'step'
-- (the psql client passes -v step=...; the experiment file calls it with \! ). Run as the superuser.
-- policy_on : row-level security on public.orders with a policy that hides every row from app
-- policy_off: the policy dropped, RLS disabled
-- revoke    : app loses its SELECT on the table and keeps SELECT on (id) only
-- restore   : app's SELECT on the table is restored, the column grant removed
\if :{?step}
\else
\echo 'c_primary.sql needs -v step=policy_on|policy_off|revoke|restore'
\quit 1
\endif
SELECT :'step' = 'policy_on' AS is_policy_on \gset
SELECT :'step' = 'policy_off' AS is_policy_off \gset
SELECT :'step' = 'revoke' AS is_revoke \gset
SELECT :'step' = 'restore' AS is_restore \gset
\if :is_policy_on
ALTER TABLE public.orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY p1 ON public.orders FOR SELECT TO app USING (false);
\endif
\if :is_policy_off
DROP POLICY p1 ON public.orders;
ALTER TABLE public.orders DISABLE ROW LEVEL SECURITY;
\endif
\if :is_revoke
REVOKE SELECT ON public.orders FROM app;
GRANT SELECT (id) ON public.orders TO app;
\endif
\if :is_restore
REVOKE SELECT (id) ON public.orders FROM app;
GRANT SELECT ON public.orders TO app;
\endif
SELECT relrowsecurity FROM pg_class WHERE oid = 'public.orders'::regclass;
