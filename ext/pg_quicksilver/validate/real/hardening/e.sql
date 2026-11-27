-- Experiment e: kept plan versus new data. ONE session, as app, on the STANDBY (run.sh 5861 app e.sql).
-- Each round: e_round.sh (run by a psql backslash-bang command) inserts 1,000 rows on the primary and waits until
-- the standby has replayed that commit and the orders mirror has an applied LSN at or past it. Then EXECUTE q (the
-- kept plan) is compared with the heap count of the same statement in this session. 20 rounds.
\set VERBOSITY terse
PREPARE q AS SELECT count(*) FROM public.orders;
\echo == round 1
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 2
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 3
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 4
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 5
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 6
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 7
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 8
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 9
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 10
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 11
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 12
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 13
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 14
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 15
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 16
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 17
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 18
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 19
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
\echo == round 20
\! /home/user/pg_quicksilver/.claude/worktrees/agent-aad78140159c1c478/ext/pg_quicksilver/validate/real/hardening/e_round.sh
EXECUTE q \gset m_
SELECT side || ' / ' || reason AS d FROM quicksilver.last_decision() \gset
SET quicksilver.route = heap;
SELECT count(*) AS h FROM public.orders \gset
RESET quicksilver.route;
SELECT :m_count::bigint AS kept_plan_count, :h::bigint AS heap_count, :m_count::bigint = :h::bigint AS equal, :'d' AS decision_for_execute;
