SET quicksilver.route = mirror;
SELECT count(*), sum(amount) FROM public.hx;
SELECT side, reason FROM quicksilver.last_decision();
