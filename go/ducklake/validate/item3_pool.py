"""Item 3: qualifying aggregates through pgbouncer (transaction pooling) on the standby, while the primary writes.

The writer (primary, port 5663) commits batches to public.orders and logs, after each
commit, (pg_current_wal_lsn, count(*), sum(amount)). Because it is the only writer, that
state is exactly the heap's state at that LSN.

Each client (8 of them, through pgbouncer on 5665 to the standby on 5664) runs ITER
transactions. Each transaction, on one server backend, does:
  1. the mirror's applied LSN (quicksilver.mirror_status)
  2. the qualifying aggregate with routing as the database sets it (auto)
  3. the side it was served from (quicksilver.last_decision)
  4. the heap's answer with quicksilver.route = 'heap', in the same transaction
A routed answer R from the mirror at applied LSN A is correct if R equals the writer's
logged state at an LSN not after A (the latest such entry, or the one after it, since
a commit's end can precede its logged LSN). 'equal_now' counts R == H (same transaction).

Usage: item3_pool.py OUT_JSON
"""
import json
import random
import sys
import threading
import time

import psycopg

WRITE_DSN = "host=127.0.0.1 port=5663 dbname=app user=postgres sslmode=disable"
POOL_DSN = "host=127.0.0.1 port=5665 dbname=app user=postgres sslmode=disable"
CLIENTS = 8
ITER = int(__import__("os").environ.get("ITEM3_ITER", 200))
AGG = "SELECT count(*), sum(amount) FROM public.orders"
ROUTE_IN_TXN = __import__("os").environ.get("ITEM3_ROUTE_IN_TXN", "1") == "1"
out_path = sys.argv[1]

log_lock = threading.Lock()
states = []            # (lsn_num, count, sum) in commit order
stop = threading.Event()
writer_commits = [0]


def lsn_num(s):
    hi, lo = s.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def writer():
    conn = psycopg.connect(WRITE_DSN, autocommit=False, prepare_threshold=None)
    rng = random.Random(7)
    next_id = int(time.time()) * 1000  # unique per run: the table keeps earlier runs' rows
    while not stop.is_set():
        with conn.cursor() as cur:
            for _ in range(20):
                next_id += 1
                cur.execute("INSERT INTO public.orders VALUES (%s, %s, %s, 'new', 'w')",
                            (next_id, rng.randint(1, 500), "%d.%02d" % (rng.randint(0, 999), rng.randint(0, 99))))
            a = rng.randint(1, 190000)
            cur.execute("UPDATE public.orders SET amount = amount + 0.01 WHERE id BETWEEN %s AND %s", (a, a + 49))
            if rng.random() < 0.5:
                b = rng.randint(1, 190000)
                cur.execute("DELETE FROM public.orders WHERE id BETWEEN %s AND %s", (b, b + 2))
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT pg_current_wal_lsn()::text")
            lsn = cur.fetchone()[0]
            cur.execute(AGG)
            cnt, total = cur.fetchone()
        conn.commit()
        with log_lock:
            states.append((lsn_num(lsn), cnt, total))
        writer_commits[0] += 1
        time.sleep(0.02)
    conn.close()


results = []
errors = []


def client(k):
    conn = psycopg.connect(POOL_DSN, autocommit=False, prepare_threshold=None)
    for _ in range(ITER):
        try:
            with conn.cursor() as cur:
                # Routing is off inside an explicit transaction block unless this is set
                # (pg_duckdb aborts after a SAVEPOINT); SET LOCAL keeps it to this transaction,
                # which pgbouncer keeps on one server connection.
                if ROUTE_IN_TXN:
                    cur.execute("SET LOCAL quicksilver.route_in_transaction = on")
                cur.execute("SELECT applied_lsn::text FROM quicksilver.mirror_status() "
                            "WHERE heap = 'public.orders'::regclass")
                row = cur.fetchone()
                applied = row[0] if row else None
                cur.execute(AGG)
                routed = cur.fetchone()
                cur.execute("SELECT side, reason FROM quicksilver.last_decision()")
                d = cur.fetchone()
                side = d[0] if d else None
                cur.execute("SET LOCAL quicksilver.route = 'heap'")
                cur.execute(AGG)
                heap_now = cur.fetchone()
            conn.commit()
            results.append({"client": k, "applied": applied, "routed": [routed[0], str(routed[1])],
                            "side": side, "heap_now": [heap_now[0], str(heap_now[1])]})
        except psycopg.Error as e:
            conn.rollback()
            errors.append((k, str(e)[:200]))
    conn.close()


def logged_match(applied, routed):
    """True if routed equals the writer's state at the latest logged LSN <= applied, or the next one."""
    a = lsn_num(applied)
    with log_lock:
        st = sorted(states)
    if not st:
        return False
    prev = None
    nxt = None
    for lsn, cnt, tot in st:
        if lsn <= a:
            prev = (cnt, tot)
        else:
            nxt = (cnt, tot)
            break
    cands = [c for c in (prev, nxt) if c is not None]
    return any(c[0] == routed[0] and str(c[1]) == routed[1] for c in cands)


w = threading.Thread(target=writer, daemon=True)
w.start()
time.sleep(1.0)
t0 = time.time()
clients = [threading.Thread(target=client, args=(k,)) for k in range(CLIENTS)]
for c in clients:
    c.start()
for c in clients:
    c.join()
elapsed = time.time() - t0
stop.set()
w.join(30)

ok_stmt = len(results)
routed_mirror = sum(1 for r in results if r["side"] == "mirror")
routed_heap = sum(1 for r in results if r["side"] == "heap")
equal_now = sum(1 for r in results if r["routed"] == r["heap_now"])
logged = 0
bad_logged = []
for r in results:
    if r["side"] == "mirror":
        if logged_match(r["applied"], r["routed"]):
            logged += 1
        else:
            bad_logged.append(r)

summary = {
    "clients": CLIENTS,
    "iterations_per_client": ITER,
    "transactions_ok": ok_stmt,
    "transactions_failed": len(errors),
    "errors_sample": errors[:5],
    "served_by_mirror": routed_mirror,
    "served_by_heap": routed_heap,
    "served_by_other_or_none": ok_stmt - routed_mirror - routed_heap,
    "routed_equal_heap_routing_off_same_txn": equal_now,
    "mirror_answers_equal_logged_heap_state_at_applied_lsn": logged,
    "mirror_answers_NOT_matching_logged_state": len(bad_logged),
    "writer_commits": writer_commits[0],
    "elapsed_s": round(elapsed, 2),
    "mismatch_sample": bad_logged[:3],
}
json.dump({"summary": summary, "results": results}, open(out_path, "w"), indent=1, default=str)
print(json.dumps(summary, indent=1, default=str))
