-- Experiment b: shadowed built-ins. As app on the STANDBY (run.sh 5861 app b.sql), after setup_b.sql.
-- NEVER acceptable: a NOTICE "evil ran as quicksilver_mirror_reader" (or as a superuser).
\set VERBOSITY terse
\echo == 1. the default search_path: the aggregate is routed
SELECT sum(amount) FROM public.orders;
SELECT side, reason FROM quicksilver.last_decision();

\echo == 2. search_path = evil, public, pg_catalog: sum(amount) and count(*) now resolve to evil.*
SET search_path = evil, public, pg_catalog;
SET evil.noted = '';
SELECT current_user, session_user, current_setting('role') AS role;
SELECT sum(amount) FROM public.orders;
SELECT side, reason, left(detail, 90) AS detail FROM quicksilver.last_decision();

\echo == 3. WHERE status = 'x' resolves to evil.= (and count(*) to evil.count)
SET evil.noted = '';
SELECT count(*) FROM public.orders WHERE status = 'x';
SELECT side, reason, left(detail, 90) AS detail FROM quicksilver.last_decision();

\echo == 4. the same two statements forced to the mirror (quicksilver.route = mirror)
SET quicksilver.route = mirror;
SET evil.noted = '';
SELECT sum(amount) FROM public.orders;
SELECT side, reason, left(detail, 90) AS detail FROM quicksilver.last_decision();
SET evil.noted = '';
SELECT count(*) FROM public.orders WHERE status = 'x';
SELECT side, reason, left(detail, 90) AS detail FROM quicksilver.last_decision();
RESET quicksilver.route;

\echo == 4b. the operator alone, so that its own NOTICE is seen (the one per session above may have come from the aggregate)
SET evil.noted = '';
SELECT 'a'::text OPERATOR(evil.=) 'a'::text AS evil_eq;
SET evil.noted = '';
SELECT count(*) FROM public.orders WHERE status = 'x';
SELECT side, reason FROM quicksilver.last_decision();

\echo == 5. the answers on the heap (forced), and the session afterwards
SET quicksilver.route = heap;
SELECT sum(amount) FROM public.orders;
SELECT count(*) FROM public.orders WHERE status = 'x';
RESET quicksilver.route;
RESET search_path;
SELECT current_user, session_user, current_setting('role') AS role, current_setting('search_path') AS search_path;
