-- Experiment d: column privileges and the join rewrite. As app on the STANDBY (run.sh 5861 app d.sql).
-- The join domain is cust (ex.jcust.id = ex.jord.cust, uuid; public.customers.id and orders.customer_id
-- are integers, which the sidecar refuses as a join domain, so the join-domain pair is the uuid one
-- that join_tables.sql loads). The public join is run too: it has no domain, and the statement
-- from the brief is the one at the end.
-- Privilege changes are made on the primary with \! (d_primary.sql), then wait_replay.sh.
\set VERBOSITY terse
\echo == 0. baseline: app may read both tables in full; the join is routed when forced
SELECT current_user, session_user, current_setting('role') AS role;
SET quicksilver.route = mirror;
SELECT count(*) FROM ex.jord o JOIN ex.jcust c ON c.id = o.cust;
SELECT side, reason, left(detail, 90) AS detail FROM quicksilver.last_decision();
RESET quicksilver.route;
SELECT count(*) FROM ex.jord o JOIN ex.jcust c ON c.id = o.cust;

\echo == 1. primary: app loses SELECT on the join key columns (ex.jord.cust, public.orders.customer_id)
\! /var/lib/postgresql/qsexp/pg17/usr/lib/postgresql/17/bin/psql -X -h /tmp -p 5860 -U postgres -d app -v step=revoke -f /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/d_primary.sql
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/wait_replay.sh

\echo == 2. the join with the key column not granted: EXPECTED permission denied, the same on the heap
SET quicksilver.route = mirror;
SELECT count(*) FROM ex.jord o JOIN ex.jcust c ON c.id = o.cust;
SELECT side, reason, left(detail, 90) AS detail FROM quicksilver.last_decision();
SET quicksilver.route = heap;
SELECT count(*) FROM ex.jord o JOIN ex.jcust c ON c.id = o.cust;
RESET quicksilver.route;
SELECT count(*) FROM ex.jord o JOIN ex.jcust c ON c.id = o.cust;
SELECT side, reason, left(detail, 90) AS detail FROM quicksilver.last_decision();

\echo == 3. the statement from the brief: SELECT count(*) FROM public.orders o JOIN public.customers c ON c.id = o.customer_id
SET quicksilver.route = mirror;
SELECT count(*) FROM public.orders o JOIN public.customers c ON c.id = o.customer_id;
SELECT side, reason, left(detail, 90) AS detail FROM quicksilver.last_decision();
SET quicksilver.route = heap;
SELECT count(*) FROM public.orders o JOIN public.customers c ON c.id = o.customer_id;
RESET quicksilver.route;
SELECT count(*) FROM public.orders o JOIN public.customers c ON c.id = o.customer_id;
SELECT side, reason, left(detail, 90) AS detail FROM quicksilver.last_decision();

\echo == 4. the columns that were granted still route: the aggregate on orders(id, amount)
SET quicksilver.route = mirror;
SELECT count(*), sum(amount) FROM public.orders WHERE id > 0;
SELECT side, reason FROM quicksilver.last_decision();
RESET quicksilver.route;

\echo == 5. primary: the grants are put back
\! /var/lib/postgresql/qsexp/pg17/usr/lib/postgresql/17/bin/psql -X -h /tmp -p 5860 -U postgres -d app -v step=restore -f /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/d_primary.sql
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/wait_replay.sh
SELECT count(*), sum(amount) FROM public.orders WHERE id > 0;
SELECT side, reason FROM quicksilver.last_decision();
