\timing on
SELECT setseed(0.7);
CREATE SCHEMA wl;
CREATE TABLE wl.picks AS
  SELECT id, CASE WHEN rn <= 500000 THEN 's' WHEN rn <= 600000 THEN 'p' ELSE 'd' END AS kind, (rn % 200)::int AS batch
  FROM (SELECT id, row_number() OVER (ORDER BY random()) rn FROM public.orders) s WHERE rn <= 700000;
CREATE TABLE wl.payload_new AS
  SELECT p.id, p.batch, o.payload || jsonb_build_object('edited', true, 'items', 99, 'rev', md5(p.id::text)) AS new_payload
  FROM wl.picks p JOIN public.orders o USING (id) WHERE p.kind = 'p';
CREATE TABLE wl.new_orders AS
  SELECT gen_random_uuid() AS id, 1 + c % 200 AS tenant_id, md5('c' || c)::uuid AS customer_id,
  CASE WHEN r < 0.05 THEN 'pending' WHEN r < 0.45 THEN 'paid' WHEN r < 0.75 THEN 'shipped' WHEN r < 0.93 THEN 'delivered' WHEN r < 0.98 THEN 'cancelled' ELSE 'refunded' END AS status,
  round((5 + random() * 495)::numeric, 2)::numeric(12,2) AS amount,
  timestamptz '2026-10-01 00:00:00+00' + (g::float8 / 100000) * interval '30 days' AS created_at,
  jsonb_build_object('source', (ARRAY['web','ios','android','api','pos','email'])[1 + floor(random() * 6)::int],
   'campaign', 'cmp-' || floor(random() * 40)::int,
   'device', jsonb_build_object('os', (ARRAY['ios','android','macos','windows','linux'])[1 + floor(random() * 5)::int], 'version', (10 + floor(random() * 8))::int || '.' || floor(random() * 10)::int),
   'items', 1 + floor(random() * 9)::int, 'gift', random() < 0.1, 'ref', md5('n' || g::text)) AS payload,
  CASE WHEN random() < 0.3 THEN 'customer asked for ' || (ARRAY['gift wrap','a call before delivery','delivery after 5pm','an invoice'])[1 + floor(random() * 4)::int] END AS note,
  (g % 200)::int AS batch
  FROM (SELECT g, 1 + floor(random() * 500000)::bigint AS c, random() AS r FROM generate_series(1, 100000) g) s;
CREATE INDEX ON wl.picks (kind, batch);
CREATE INDEX ON wl.payload_new (batch);
CREATE INDEX ON wl.new_orders (batch);
ANALYZE wl.picks; ANALYZE wl.payload_new; ANALYZE wl.new_orders;
SELECT kind, count(*), count(distinct batch) FROM wl.picks GROUP BY 1;
\t on
\o /tmp/qsb/results/lookup_id.txt
SELECT id FROM wl.picks WHERE kind='s' AND batch = 100 ORDER BY id LIMIT 1;
