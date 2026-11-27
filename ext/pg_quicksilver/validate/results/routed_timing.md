| statement | side | heap ms | routed ms | ratio |
|---|---|---|---|---|
| count(*): `SELECT count(*) FROM public.orders` | mirror | 44 | 8.43 | 5.2x |
| group by: `SELECT status, count(*), sum(amount) FROM public.orders GROUP BY st...` | mirror | 169 | 50 | 3.4x |
| range aggregate: `SELECT sum(amount) FROM public.orders WHERE created_at >= '2024-01-...` | mirror | 28 | 21 | 1.4x |
| join + group by: `SELECT c.region, sum(o.amount) FROM public.orders o JOIN public.cus...` | mirror | 186 | 52 | 3.6x |
| 10k-row result: `SELECT id, amount FROM public.orders WHERE amount > 990.00` | mirror | 84 | 58 | 1.4x |
| top-N: `SELECT id, amount FROM public.orders ORDER BY amount DESC, id LIMIT...` | mirror | 106 | 59 | 1.8x |
| unselective parameter: `SELECT count(*), sum(amount) FROM public.orders WHERE customer_id = 1` | mirror | 62 | 38 | 1.6x |
| min/max: `SELECT min(id), max(id), min(amount), max(amount), min(created_at),...` | mirror | 142 | 80 | 1.8x |
| pk lookup (stays on the heap): `SELECT * FROM public.orders WHERE id = 4242` | heap | 0.06 | 0.07 | 0.9x |
| selective parameter (stays): `SELECT count(*), sum(amount) FROM public.orders WHERE customer_id = 5` | heap | 0.12 | 0.10 | 1.2x |
