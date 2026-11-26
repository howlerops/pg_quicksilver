#!/usr/bin/env python3
"""Mixed writes on the primary: inserts, updates and deletes on the Part 1 tables.

  fv_mixed.py --port 5800 --rounds 5 [--seed 1]

Each round, per table, in one transaction: insert N new rows, update M random
existing rows, delete K random rows. The numbers are printed so the run's log
says what was written.
"""
import argparse, random
import psycopg


def run(port, rounds, seed):
    rnd = random.Random(seed)
    con = psycopg.connect(host="/tmp", port=port, user="postgres", dbname="app", autocommit=False)
    cur = con.cursor()
    for r in range(rounds):
        g = seed * 1000 + r          # unique per seed and round: keys do not collide between runs
        # fv.events: uuid key
        cur.execute("""INSERT INTO fv.events SELECT md5('m' || %s || '-' || i::text)::uuid, (i %% 5000)::numeric / 7,
                       now() - (i || ' seconds')::interval, 'mixed ' || i FROM generate_series(1, 1000) i""", (g,))
        cur.execute("UPDATE fv.events SET amount = amount + 1.25, note = note || ' u' WHERE id IN (SELECT id FROM fv.events ORDER BY md5(id::text || %s) LIMIT 800)", (str(r),))
        cur.execute("DELETE FROM fv.events WHERE id IN (SELECT id FROM fv.events ORDER BY md5(id::text || 'd' || %s) LIMIT 300)", (str(r),))
        # fv.accounts: text key
        cur.execute("""INSERT INTO fv.accounts SELECT 'MX' || %s || lpad(i::text, 6, '0'), (i %% 900)::numeric, now(), NULL
                       FROM generate_series(1, 1000) i""", (str(g),))
        cur.execute("UPDATE fv.accounts SET balance = balance * 0.5, label = 'hit-' || %s WHERE code IN (SELECT code FROM fv.accounts ORDER BY md5(code || %s) LIMIT 500)", (str(r), str(r)))
        cur.execute("DELETE FROM fv.accounts WHERE code IN (SELECT code FROM fv.accounts WHERE code LIKE 'MX%%' ORDER BY md5(code) LIMIT 100)")
        # fv.lookup: small table
        cur.execute("INSERT INTO fv.lookup SELECT 1000 + %s * 10 + i, 'extra-' || %s || '-' || i FROM generate_series(1, 10) i ON CONFLICT DO NOTHING", (g, g))
        cur.execute("UPDATE fv.lookup SET name = name || '*' WHERE id %% 13 = %s", (r % 13,))
        # public.orders and public.customers, the bench-shaped tables
        cur.execute("""INSERT INTO public.orders (id, customer_id, status, amount, created_at, note)
                       SELECT 900000000 + %s::bigint * 1000 + i, (i %% 10000) + 1, 'new', (i %% 999)::numeric / 4, now(), 'mx'
                       FROM generate_series(1, 1000) i""", (g,))
        cur.execute("UPDATE public.orders SET status = 'shipped' WHERE id %% 97 = %s", (r,))
        cur.execute("DELETE FROM public.orders WHERE id >= 900000000 AND id %% 53 = %s", (r,))
        cur.execute("UPDATE public.customers SET region = region || '' WHERE id %% 1000 = %s", (r,))
        # ex.t_num: a table with numeric and integer columns
        cur.execute("UPDATE ex.t_num SET n = round(n / 2, 2) WHERE id %% 17 = %s", (r,))
        con.commit()
        print(f"round {r}: committed", flush=True)
    cur.close()
    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=5800)
    ap.add_argument("--rounds", type=int, default=5)
    ap.add_argument("--seed", type=int, default=1)
    a = ap.parse_args()
    run(a.port, a.rounds, a.seed)
