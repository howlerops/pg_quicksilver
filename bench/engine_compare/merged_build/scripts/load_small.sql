DROP TABLE IF EXISTS public.items; DROP TABLE IF EXISTS public.marker;
CREATE TABLE public.marker(n bigint); INSERT INTO marker VALUES (0);
CREATE TABLE public.items(
  id uuid PRIMARY KEY, seq bigint NOT NULL, tenant_id integer NOT NULL, status text NOT NULL,
  amount numeric(12,2), created_at timestamptz NOT NULL, payload jsonb, note text, qty bigint, flag boolean, d date);
INSERT INTO public.items SELECT gen_random_uuid(), g, 1 + g % 200,
  (ARRAY['pending','paid','shipped','delivered'])[1 + g % 4],
  CASE WHEN g % 53 = 0 THEN NULL WHEN g % 7 = 0 THEN -round((random()*1e6)::numeric,2) ELSE round((random()*9999999999)::numeric, 2) END,
  timestamptz '2024-01-01 00:00:00+00' + g * interval '137.123456 seconds',
  CASE WHEN g % 41 = 0 THEN NULL ELSE jsonb_build_object('k', g, 's', md5(g::text), 'nested', jsonb_build_object('a', ARRAY[1,2,g % 9], 'u', 'é☃ "q"'), 'f', g/7.0) END,
  CASE WHEN g % 3 = 0 THEN NULL WHEN g % 11 = 0 THEN '' ELSE 'note ' || g || E' ünï\tcode' END,
  CASE WHEN g % 19 = 0 THEN NULL ELSE ((g::numeric * 900719925474099) % 9223372036854775807)::bigint END,
  CASE g % 3 WHEN 0 THEN true WHEN 1 THEN false END,
  date '2020-01-01' + (g % 3000)::int
FROM generate_series(1, 200000) g;
CREATE UNIQUE INDEX items_seq ON public.items(seq);
ANALYZE public.items;
