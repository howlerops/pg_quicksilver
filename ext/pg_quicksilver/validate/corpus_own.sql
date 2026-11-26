-- Statements over the router's tables (public.orders, 1M rows; public.customers)
-- and the exactness tables (ex.*), built from the seed allowlist's constructs,
-- plus the ones next to them that must stay on the heap. Same format as
-- bench/exactness/corpus: an id line, then one statement.
-- Run with search_path = ex, public; public tables are always qualified.

-- @id own-001
SELECT count(*) FROM public.orders;
-- @id own-002
SELECT status, count(*), sum(amount) FROM public.orders GROUP BY status;
-- @id own-003
SELECT sum(amount) FROM public.orders WHERE created_at >= '2024-01-03 00:00:00+00' AND created_at < '2024-01-05 00:00:00+00';
-- @id own-004
SELECT c.region, sum(o.amount) FROM public.orders o JOIN public.customers c ON c.id = o.customer_id GROUP BY c.region;
-- @id own-005
SELECT id, amount FROM public.orders WHERE amount > 990;
-- @id own-006
SELECT id, amount FROM public.orders ORDER BY amount DESC, id LIMIT 10;
-- @id own-007
SELECT count(*), sum(amount) FROM public.orders WHERE customer_id = 1;
-- @id own-008
SELECT count(*), sum(amount) FROM public.orders WHERE customer_id = 5;
-- @id own-009
SELECT min(id), max(id), min(amount), max(amount), min(created_at), max(created_at) FROM public.orders;
-- @id own-010
SELECT customer_id, count(*) FROM public.orders GROUP BY customer_id ORDER BY customer_id LIMIT 20;
-- @id own-011
SELECT customer_id, sum(amount) AS total FROM public.orders GROUP BY customer_id HAVING sum(amount) > 100000 ORDER BY customer_id;
-- @id own-012
SELECT count(*) FROM public.orders WHERE status = 'paid';
-- @id own-013
SELECT count(*) FROM public.orders WHERE status <> 'paid' AND amount >= 500;
-- @id own-014
SELECT count(*) FROM public.orders WHERE note IS NULL;
-- @id own-015
SELECT count(*) FROM public.orders WHERE note IS NOT NULL AND customer_id = 1;
-- @id own-016
SELECT count(note), count(*) FROM public.orders;
-- @id own-017
SELECT status, min(amount), max(amount) FROM public.orders GROUP BY status;
-- @id own-018
SELECT sum(amount * 2), sum(amount + 1), sum(amount - 1) FROM public.orders;
-- @id own-019
SELECT sum(id), sum(customer_id) FROM public.orders;
-- @id own-020
SELECT sum(id * 2), sum(customer_id + 1), sum(id - customer_id) FROM public.orders;
-- @id own-021
SELECT id, id + 1, id - 1, id * 3, customer_id * 2 FROM public.orders WHERE id <= 2000 ORDER BY id;
-- @id own-022
SELECT id, amount, amount * 2, amount + 0.5, amount - 100 FROM public.orders WHERE id > 999000 ORDER BY id;
-- @id own-023
SELECT id, amount * amount FROM public.orders WHERE id > 999900 ORDER BY id;
-- @id own-024
SELECT count(*) FROM public.orders WHERE amount = 42.42;
-- @id own-025
SELECT count(*) FROM public.orders WHERE amount BETWEEN 10 AND 20;
-- @id own-026
SELECT count(*) FROM public.orders WHERE id IN (1, 2, 3, 500000) OR customer_id = 1;
-- @id own-027
SELECT CASE WHEN amount < 100 THEN 1 WHEN amount < 500 THEN 2 ELSE 3 END AS bucket, count(*) FROM public.orders GROUP BY 1 ORDER BY 1;
-- @id own-028
SELECT coalesce(note, 'none') = 'none' AS missing, count(*) FROM public.orders GROUP BY 1;
-- @id own-029
SELECT count(*) FROM public.orders WHERE NOT (status = 'new' OR status = 'paid');
-- @id own-030
SELECT count(*) FROM public.orders WHERE (amount > 500) IS TRUE;
-- @id own-031
SELECT o.id, c.name, c.region FROM public.orders o JOIN public.customers c ON c.id = o.customer_id WHERE o.id > 999990 ORDER BY o.id;
-- @id own-032
SELECT c.id, count(o.id) FROM public.customers c LEFT JOIN public.orders o ON o.customer_id = c.id GROUP BY c.id ORDER BY c.id LIMIT 30;
-- @id own-033
SELECT c.region, count(*), min(o.amount), max(o.amount) FROM public.orders o JOIN public.customers c ON c.id = o.customer_id WHERE o.status = 'paid' GROUP BY c.region;
-- @id own-034
SELECT region, count(*) FROM public.customers GROUP BY region;
-- @id own-035
SELECT q.customer_id, q.n FROM (SELECT customer_id, count(*) AS n FROM public.orders GROUP BY customer_id) q WHERE q.n > 100 ORDER BY q.customer_id;
-- @id own-036
SELECT count(*) FROM (SELECT id FROM public.orders WHERE amount > 900) q;
-- @id own-037
SELECT created_at, id FROM public.orders WHERE created_at < '2024-01-01 00:01:00+00' ORDER BY created_at, id;
-- @id own-038
SELECT max(created_at) FROM public.orders WHERE customer_id = 1;
-- @id own-039
SELECT id FROM public.orders WHERE created_at = '2024-01-05 12:00:00+00';
-- @id own-040
SELECT id, customer_id, status, amount, created_at, note FROM public.orders WHERE id > 999950 ORDER BY id;
-- @id own-041
SELECT * FROM public.orders WHERE customer_id = 1 AND amount > 999.9 ORDER BY id;
-- @id own-042
SELECT id, note FROM public.orders WHERE note = 'n-777';
-- @id own-043
SELECT count(*) FROM public.orders o WHERE o.amount > 999 AND o.customer_id > 5000;
-- @id own-044
SELECT status, customer_id, count(*) FROM public.orders WHERE customer_id < 10 GROUP BY status, customer_id ORDER BY customer_id, count(*) DESC, max(id);
-- @id own-045
SELECT sum(CASE WHEN status = 'paid' THEN amount ELSE 0 END), sum(CASE WHEN status = 'new' THEN 1 ELSE 0 END) FROM public.orders;
-- @id own-046
SELECT id FROM public.orders ORDER BY id DESC LIMIT 5;
-- @id own-047
SELECT id, note FROM public.orders WHERE id <= 30 ORDER BY note IS NULL, id;

-- the same shapes that must stay on the heap
-- @id own-100
SELECT count(*) FROM public.orders WHERE id / 2 = 5;
-- @id own-101
SELECT avg(amount) FROM public.orders;
-- @id own-102
SELECT status, count(*) FROM public.orders GROUP BY status ORDER BY status;
-- @id own-103
SELECT min(status), max(note) FROM public.orders;
-- @id own-104
SELECT count(*) FROM public.orders WHERE status < 'paid';
-- @id own-105
SELECT count(*) FROM public.orders WHERE created_at < now();
-- @id own-106
SELECT count(*) FROM public.orders WHERE note LIKE 'n-1%';
-- @id own-107
SELECT * FROM public.orders LIMIT 10;
-- @id own-108
SELECT count(*) FROM public.orders o JOIN public.audit_log a ON a.id = o.id;
-- @id own-109
SELECT count(*), sum(amount)::float8 FROM public.orders;
-- @id own-110
SELECT upper(status), count(*) FROM public.orders GROUP BY 1;
-- @id own-111
SELECT count(*) FROM public.orders WHERE created_at::date = '2024-01-05';
-- @id own-112
SELECT count(DISTINCT customer_id) FROM public.orders;
-- @id own-113
SELECT id, row_number() OVER (ORDER BY id) FROM public.orders WHERE id < 20;
-- @id own-114
SELECT order_total(5), count(*) FROM public.orders;
-- @id own-115
SELECT count(*) FROM public.orders WHERE amount > 990 AND random() >= 0;
-- @id own-116
SELECT id FROM public.orders WHERE id < 5 UNION ALL SELECT id FROM public.orders WHERE id > 999998;
-- @id own-117
SELECT o.id FROM public.orders o WHERE EXISTS (SELECT 1 FROM public.customers c WHERE c.id = o.customer_id AND c.region = 'emea') AND o.id > 999990;
-- @id own-118
SELECT sum(amount) / count(*) FROM public.orders;
-- @id own-119
SELECT current_date, count(*) FROM public.orders;
-- @id own-120
SELECT tableoid::regclass, count(*) FROM public.orders GROUP BY 1;
-- @id own-121
SELECT o FROM public.orders o WHERE id = 3;
-- @id own-122
SELECT ctid, id FROM public.orders WHERE id = 3;

-- seed constructs over the exactness tables (NULLs, extremes, both write paths)
-- @id own-200
SELECT count(*), count(i), count(bi), count(n), count(si) FROM t_num;
-- @id own-201
SELECT sum(i), sum(bi), sum(n), sum(n0) FROM t_num WHERE id < 9000;
-- @id own-202
SELECT min(si), max(si), min(i), max(i), min(bi), max(bi), min(n), max(n), min(n0), max(n0) FROM t_num;
-- @id own-203
SELECT g, count(*), sum(i), sum(n), min(bi), max(bi) FROM t_num GROUP BY g ORDER BY g;
-- @id own-204
SELECT id, i, bi, n FROM t_num ORDER BY i DESC, id;
-- @id own-205
SELECT id, i, bi, n FROM t_num ORDER BY bi NULLS FIRST, id;
-- @id own-206
SELECT id, n FROM t_num ORDER BY n DESC NULLS LAST, id LIMIT 40;
-- @id own-207
SELECT id FROM t_num WHERE i > 0 AND bi < 0 ORDER BY id;
-- @id own-208
SELECT id FROM t_num WHERE si = i OR si < bi ORDER BY id;
-- @id own-209
SELECT id, i + 1, i - 1, i * 2 FROM t_num WHERE id < 9000 ORDER BY id;
-- @id own-210
SELECT id, bi + 1, bi - i, bi * 2 FROM t_num WHERE id < 9000 ORDER BY id;
-- @id own-211
SELECT id, n + n0, n - n0, n * n0 FROM t_num WHERE id < 9000 ORDER BY id;
-- @id own-212
SELECT id, n * 3, n + 1, n0 - 7 FROM t_num WHERE id < 9000 ORDER BY id;
-- @id own-213
SELECT id, i IS NULL, n IS NOT NULL, b IS TRUE, b IS NOT FALSE, b IS UNKNOWN FROM t_num ORDER BY id;
-- @id own-214
SELECT id, coalesce(i, 0), coalesce(n, 0.00), coalesce(bi, i, si) FROM t_num ORDER BY id;
-- @id own-215
SELECT id, CASE WHEN i > 0 THEN n WHEN i < 0 THEN n0 ELSE NULL END FROM t_num ORDER BY id;
-- @id own-216
SELECT b, count(*) FROM t_num GROUP BY b ORDER BY b;
-- @id own-217
SELECT id FROM t_num WHERE b ORDER BY id LIMIT 25;
-- @id own-218
SELECT id FROM t_num WHERE NOT b AND i <> 5 ORDER BY id LIMIT 25;
-- @id own-219
SELECT id, i FROM t_num WHERE i IN (1, 2, 3, 100, -100, 2147483647) ORDER BY id;
-- @id own-220
SELECT id, i + 1 FROM t_num WHERE id >= 9000 ORDER BY id;
-- @id own-221
SELECT id, bi * 2 FROM t_num WHERE id >= 9000 ORDER BY id;
-- @id own-222
SELECT sum(bi) FROM t_num;
-- @id own-223
SELECT id, d FROM t_time WHERE d >= '2024-03-01' AND d < '2024-04-01' ORDER BY d, id;
-- @id own-224
SELECT min(d), max(d), min(ts), max(ts), min(tz), max(tz) FROM t_time;
-- @id own-225
SELECT id, ts FROM t_time WHERE ts > '2024-06-01 12:00:00' ORDER BY ts DESC, id LIMIT 30;
-- @id own-226
SELECT id, tz FROM t_time WHERE tz <= '2024-03-10 03:00:00-04' ORDER BY tz, id;
-- @id own-227
SELECT d, count(*) FROM t_time GROUP BY d ORDER BY d LIMIT 50;
-- @id own-228
SELECT id, d, ts, tz FROM t_time ORDER BY id;
-- @id own-229
SELECT id, u FROM t_str WHERE u = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' ORDER BY id;
-- @id own-230
SELECT id, u FROM t_str ORDER BY id;
-- @id own-231
SELECT g, count(*) FROM t_str GROUP BY g;
-- @id own-232
SELECT id, t FROM t_str WHERE t = 'apple' OR t <> g ORDER BY id;
-- @id own-233
SELECT id, t, g FROM t_str ORDER BY id;
-- @id own-234
SELECT count(u), count(t), count(g) FROM t_str;
-- @id own-235
SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON c.id = o.cust_id ORDER BY o.id;
-- @id own-236
SELECT c.id, c.name, count(o.id), sum(o.amount) FROM customers c LEFT JOIN orders o ON o.cust_id = c.id GROUP BY c.id, c.name ORDER BY c.id;
-- @id own-237
SELECT o.status, count(*), sum(o.qty), sum(o.amount) FROM orders o GROUP BY o.status;
-- @id own-238
SELECT o.id, o.qty * 2, o.amount * o.qty FROM orders o ORDER BY o.id;
-- @id own-239
SELECT c.region, count(*) FROM customers c GROUP BY c.region;
-- @id own-240
SELECT id, created FROM customers WHERE created > '2023-06-01' ORDER BY created, id;
-- @id own-241
SELECT o.id, o.placed FROM orders o WHERE o.placed >= '2024-01-01 00:00:00+00' ORDER BY o.placed, o.id LIMIT 60;
-- @id own-242
SELECT q.g, q.s FROM (SELECT g, sum(i) AS s FROM t_num GROUP BY g) q WHERE q.s IS NOT NULL ORDER BY q.g;
-- @id own-243
SELECT g, sum(n) FROM t_num GROUP BY g HAVING count(*) > 10 ORDER BY g;
-- @id own-244
SELECT id, nu FROM t_num ORDER BY id;
-- @id own-245
SELECT id, vc, ch FROM t_str ORDER BY id;
-- @id own-246
SELECT id, e, ip FROM t_str ORDER BY id;
-- @id own-247
SELECT id, r, d FROM t_num ORDER BY id;
-- @id own-248
SELECT id, si + si, si * si FROM t_num ORDER BY id;
-- @id own-249
SELECT id, i * i FROM t_num WHERE id < 9000 ORDER BY id;
-- @id own-250
SELECT id, bi * bi FROM t_num WHERE id < 100 ORDER BY id;
