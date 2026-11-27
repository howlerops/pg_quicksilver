\timing on
SELECT setseed(0.42);
CREATE TABLE public.customers (id uuid PRIMARY KEY, tenant_id integer NOT NULL, name text NOT NULL, email text NOT NULL, created_at timestamptz NOT NULL);
INSERT INTO public.customers SELECT md5('c' || g)::uuid, 1 + g % 200, 'Customer ' || g, 'customer' || g || '@example.com',
  timestamptz '2024-10-01 00:00:00+00' + (g::float8 / :nc) * interval '730 days' FROM generate_series(1, :nc) g;
CREATE TABLE public.orders (id uuid NOT NULL, tenant_id integer NOT NULL, customer_id uuid NOT NULL, status text NOT NULL,
  amount numeric(12,2) NOT NULL, created_at timestamptz NOT NULL, payload jsonb NOT NULL, note text);
INSERT INTO public.orders SELECT gen_random_uuid(), 1 + c % 200, md5('c' || c)::uuid,
  CASE WHEN r < 0.05 THEN 'pending' WHEN r < 0.45 THEN 'paid' WHEN r < 0.75 THEN 'shipped' WHEN r < 0.93 THEN 'delivered' WHEN r < 0.98 THEN 'cancelled' ELSE 'refunded' END,
  round((5 + random() * 495)::numeric, 2),
  timestamptz '2024-10-01 00:00:00+00' + ((g - 1)::float8 / :rows) * interval '730 days' + floor(random() * 600) * interval '1 second',
  jsonb_build_object('source', (ARRAY['web','ios','android','api','pos','email'])[1 + floor(random() * 6)::int],
   'campaign', 'cmp-' || floor(random() * 40)::int,
   'device', jsonb_build_object('os', (ARRAY['ios','android','macos','windows','linux'])[1 + floor(random() * 5)::int], 'version', (10 + floor(random() * 8))::int || '.' || floor(random() * 10)::int),
   'items', 1 + floor(random() * 9)::int, 'gift', random() < 0.1, 'ref', md5(g::text)),
  CASE WHEN random() < 0.3 THEN 'customer asked for ' || (ARRAY['gift wrap','a call before delivery','delivery after 5pm','an invoice'])[1 + floor(random() * 4)::int] END
  FROM (SELECT g, 1 + floor(random() * :nc)::bigint AS c, random() AS r FROM generate_series(1, :rows) g) s;
ALTER TABLE public.orders ADD PRIMARY KEY (id);
CREATE INDEX orders_tenant_created ON public.orders (tenant_id, created_at);
CREATE INDEX orders_created ON public.orders (created_at);
VACUUM (ANALYZE) public.customers;
VACUUM (ANALYZE) public.orders;
CHECKPOINT;
SELECT pg_size_pretty(pg_total_relation_size('orders')) orders_total, pg_size_pretty(pg_relation_size('orders')) orders_heap, pg_size_pretty(pg_total_relation_size('customers')) cust_total, pg_size_pretty(pg_database_size('app')) db;
