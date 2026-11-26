-- Item 3: public.orders on the private PostgreSQL 17 tree (pg_duckdb and pg_quicksilver preloaded).
-- The shape of the example statements in ext/pg_quicksilver/validate/real: an integer key,
-- a customer, a numeric amount, a low-cardinality status.
CREATE TABLE public.orders (
  id bigint PRIMARY KEY,
  customer_id int NOT NULL,
  amount numeric(12,2) NOT NULL,
  status text NOT NULL,
  note text);
INSERT INTO public.orders
SELECT g, (g % 500) + 1, ((g * 37) % 100000) / 100.0,
       (ARRAY['new','paid','shipped','delivered','cancelled'])[1 + (g % 5)],
       repeat(md5(g::text), 4)
FROM generate_series(1, 200000) g;
VACUUM ANALYZE public.orders;
