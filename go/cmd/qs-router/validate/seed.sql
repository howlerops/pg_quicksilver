-- psql -v rows=1000000 -f seed.sql
CREATE TABLE customers(id int PRIMARY KEY, name text NOT NULL, region text NOT NULL);
INSERT INTO customers
SELECT g, 'cust-'||g, (ARRAY['emea','amer','apac','latam'])[1+g%4]
FROM generate_series(1,10000) g;

CREATE TABLE orders(
  id bigint PRIMARY KEY,
  customer_id int NOT NULL,
  status text NOT NULL,
  amount numeric(12,2) NOT NULL,
  created_at timestamptz NOT NULL,
  note text);
INSERT INTO orders
SELECT g, CASE WHEN g%10 < 3 THEN 1 ELSE 2+(g::bigint*7919)%9999 END,  -- customer 1 owns 30%
       (ARRAY['new','paid','shipped','returned'])[1+g%4], (g%100000)/100.0,
       timestamptz '2024-01-01 00:00:00+00' + (g||' seconds')::interval,
       CASE WHEN g%10=0 THEN NULL ELSE 'n-'||g END
FROM generate_series(1,:rows) g;
CREATE INDEX orders_customer_idx ON orders(customer_id);
CREATE INDEX orders_created_idx ON orders(created_at);

-- Not mirrored, so there is a table the router has to keep on the heap.
CREATE TABLE audit_log(id bigserial PRIMARY KEY, what text, at timestamptz DEFAULT now());
INSERT INTO audit_log(what) SELECT 'seed-'||g FROM generate_series(1,1000) g;

CREATE FUNCTION order_total(bigint) RETURNS numeric LANGUAGE sql STABLE
AS 'SELECT amount FROM public.orders WHERE id = $1';

VACUUM ANALYZE;
