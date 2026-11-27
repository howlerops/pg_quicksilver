-- Experiment a: identity after failure. ONE session, as app, on the STANDBY (run.sh 5861 app).
-- The mirror-side failure is made from inside this session with \! (a superuser psql on the
-- primary, i.e. another session), as break_hx.sql does; the file is restored at the end.
\set VERBOSITY terse
\echo == 0. baseline in this session: the test mirror routes before it is broken (forced: the table is too cheap to route by itself)
SET quicksilver.route = mirror;
SELECT count(*), sum(amount) FROM public.hx;
SELECT side, reason FROM quicksilver.last_decision();
SELECT current_user, session_user, current_setting('role') AS role;

\echo == 1. a routed statement that fails on the mirror side, inside a DO block that swallows the error
\! /var/lib/postgresql/qsexp/pg17/usr/lib/postgresql/17/bin/psql -X -h /tmp -p 5860 -U postgres -d app -f /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/break_hx.sql
DO $$ BEGIN PERFORM count(*), sum(amount) FROM public.hx; EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'swallowed on mirror side: %', SQLERRM; END $$;
SELECT current_user, session_user, current_setting('role') AS role;
SELECT side, reason FROM quicksilver.last_decision();
SELECT count(*), sum(amount) FROM public.hx;
SELECT side, reason, left(detail, 70) AS detail FROM quicksilver.last_decision();
SELECT current_user, session_user, current_setting('role') AS role;

\echo == 2. cursor over a routed aggregate with GROUP BY (many rows), fetch 1, close, commit
BEGIN;
DECLARE c CURSOR FOR SELECT customer_id, count(*), sum(amount) FROM public.orders GROUP BY customer_id;
FETCH 1 FROM c;
CLOSE c;
SELECT current_user, session_user, current_setting('role') AS role;
COMMIT;
SELECT current_user, session_user, current_setting('role') AS role;

\echo == 3. the same cursor, rolled back while open
BEGIN;
DECLARE c CURSOR FOR SELECT customer_id, count(*), sum(amount) FROM public.orders GROUP BY customer_id;
FETCH 1 FROM c;
ROLLBACK;
SELECT current_user, session_user, current_setting('role') AS role;

\echo == 4. statement_timeout cancels a routed statement over a large table
SET statement_timeout = '1ms';
SELECT customer_id, count(*), sum(amount) FROM public.orders GROUP BY customer_id;
RESET statement_timeout;
SELECT current_user, session_user, current_setting('role') AS role;
SELECT side, reason FROM quicksilver.last_decision();
SELECT current_user, session_user;

\echo == 5. the file is put back (as superuser, from another session); the mirror is not asked again for quicksilver.negative_cache_ttl (60 s), so wait it out
\! /var/lib/postgresql/qsexp/pg17/usr/lib/postgresql/17/bin/psql -X -h /tmp -p 5860 -U postgres -d app -f /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/restore_hx.sql
SELECT count(*), sum(amount) FROM public.hx;
SELECT side, reason FROM quicksilver.last_decision();
SELECT pg_sleep(61);
SELECT count(*), sum(amount) FROM public.hx;
SELECT side, reason FROM quicksilver.last_decision();
SELECT side, reason FROM quicksilver.last_decision();
SELECT current_user, session_user, current_setting('role') AS role;
