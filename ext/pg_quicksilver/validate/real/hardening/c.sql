-- Experiment c: privilege changes after PREPARE. ONE session, as app, on the STANDBY (run.sh 5861 app c.sql).
-- The changes are made on the primary as the owner, from this session with \! (c_primary.sql), and
-- wait_replay.sh waits for the standby and the mirrors.
\set VERBOSITY terse
\echo == 1. prepared, then executed six times (the plan may switch to a generic one)
PREPARE p AS SELECT count(*), sum(amount) FROM public.orders;
EXECUTE p;
SELECT side, reason FROM quicksilver.last_decision();
EXECUTE p;
SELECT side, reason FROM quicksilver.last_decision();
EXECUTE p;
SELECT side, reason FROM quicksilver.last_decision();
EXECUTE p;
SELECT side, reason FROM quicksilver.last_decision();
EXECUTE p;
SELECT side, reason FROM quicksilver.last_decision();
EXECUTE p;
SELECT side, reason FROM quicksilver.last_decision();
SELECT name, generic_plans, custom_plans FROM pg_prepared_statements;
SELECT current_user, session_user, current_setting('role') AS role;

\echo == 2. primary (owner): ENABLE ROW LEVEL SECURITY and a policy that hides every row from app
\! /var/lib/postgresql/qsexp/pg17/usr/lib/postgresql/17/bin/psql -X -h /tmp -p 5860 -U postgres -d app -v step=policy_on -f /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/c_primary.sql
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/wait_replay.sh
EXECUTE p;
SELECT side, reason, left(detail, 80) AS detail FROM quicksilver.last_decision();
-- forced to the mirror: the policy must still apply (the count must still be 0)
SET quicksilver.route = mirror;
EXECUTE p;
SELECT side, reason, left(detail, 80) AS detail FROM quicksilver.last_decision();
RESET quicksilver.route;

\echo == 3. primary: the policy dropped, RLS disabled; EXECUTE is routed again with the full count
\! /var/lib/postgresql/qsexp/pg17/usr/lib/postgresql/17/bin/psql -X -h /tmp -p 5860 -U postgres -d app -v step=policy_off -f /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/c_primary.sql
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/wait_replay.sh
EXECUTE p;
SELECT side, reason, left(detail, 80) AS detail FROM quicksilver.last_decision();

\echo == 4. primary: REVOKE SELECT on orders from app, GRANT SELECT (id) only; EXPECTED permission denied for table orders
\! /var/lib/postgresql/qsexp/pg17/usr/lib/postgresql/17/bin/psql -X -h /tmp -p 5860 -U postgres -d app -v step=revoke -f /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/c_primary.sql
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/wait_replay.sh
EXECUTE p;
SELECT side, reason, left(detail, 80) AS detail FROM quicksilver.last_decision();
-- forced to the mirror: still the heap's permission error
SET quicksilver.route = mirror;
EXECUTE p;
SELECT side, reason, left(detail, 80) AS detail FROM quicksilver.last_decision();
RESET quicksilver.route;
-- count(*) names no column: PostgreSQL asks for SELECT on some column, which app has (id)
SELECT count(*) FROM public.orders;
SELECT side, reason, left(detail, 80) AS detail FROM quicksilver.last_decision();

\echo == 5. primary: privileges restored; EXECUTE is routed again
\! /var/lib/postgresql/qsexp/pg17/usr/lib/postgresql/17/bin/psql -X -h /tmp -p 5860 -U postgres -d app -v step=restore -f /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/c_primary.sql
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/wait_replay.sh
EXECUTE p;
SELECT side, reason, left(detail, 80) AS detail FROM quicksilver.last_decision();
SELECT current_user, session_user, current_setting('role') AS role;
