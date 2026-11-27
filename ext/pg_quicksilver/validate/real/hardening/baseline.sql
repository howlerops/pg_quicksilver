-- baseline: ordinary role app on the standby
\set VERBOSITY terse
SELECT current_user, session_user, current_setting('role') AS role;
SELECT count(*), sum(amount) FROM public.orders;
SELECT side, reason FROM quicksilver.last_decision();
SET quicksilver.route = 'heap';
SELECT count(*), sum(amount) FROM public.orders;
RESET quicksilver.route;
SELECT side, reason FROM quicksilver.last_decision();
