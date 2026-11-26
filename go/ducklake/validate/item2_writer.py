"""Item 2 writer: one INSERT every 200 ms into public.rb_ledger, logging each committed count.

Usage: item2_writer.py SECONDS OUT_FILE

The table only grows, so the counts the source table ever had are exactly
seed, seed+1, ... up to the last committed count. Each line of OUT_FILE is
"<unix time> <committed count>", written after the commit.
"""
import sys
import time

import psycopg

import lakecheck as L

seconds, out_path = float(sys.argv[1]), sys.argv[2]
out = open(out_path, "w", buffering=1)
conn = psycopg.connect(L.PG_DSN, autocommit=True)
with conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM public.rb_ledger")
    n = cur.fetchone()[0]
out.write(f"{time.time():.3f} {n} seed\n")
next_id = 10_000_000
end = time.time() + seconds
while time.time() < end:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO public.rb_ledger (id, amount) VALUES (%s, %s)", (next_id, next_id % 1000))
        cur.execute("SELECT count(*) FROM public.rb_ledger")
        n = cur.fetchone()[0]
    next_id += 1
    out.write(f"{time.time():.3f} {n}\n")
    time.sleep(0.2)
out.close()
