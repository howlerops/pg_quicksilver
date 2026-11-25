#!/usr/bin/env python3
"""Runs representative statements through qs-router and prints, for each, the
side the router chose and why (its own SHOW qs.last_route), whether the answer
equals the heap's, and how long the statement takes on each side when sent
straight to PostgreSQL (median of 5) so the choice can be judged.

    python3 routing_table.py ROUTER_DSN DIRECT_DSN
"""
import statistics
import sys
import time

import psycopg

ROUTER = sys.argv[1] if len(sys.argv) > 1 else "host=127.0.0.1 port=5490 dbname=app user=postgres"
DIRECT = sys.argv[2] if len(sys.argv) > 2 else "host=127.0.0.1 port=5491 dbname=app user=postgres"

# (label, sql, params, kind)   kind: r = read to compare and time, w = write, x = special
S = [
    ("pk lookup", "SELECT * FROM orders WHERE id = 4242", None, "r"),
    ("IN-list on the pk", "SELECT * FROM orders WHERE id IN (1, 2, 3, 500000)", None, "r"),
    ("small range on an index", "SELECT * FROM orders WHERE created_at >= '2024-01-05' AND created_at < '2024-01-05 00:10'", None, "r"),
    ("count(*)", "SELECT count(*) FROM orders", None, "r"),
    ("group by", "SELECT status, count(*), sum(amount) FROM orders GROUP BY status", None, "r"),
    ("range aggregate", "SELECT sum(amount) FROM orders WHERE created_at >= '2024-01-03' AND created_at < '2024-01-09'", None, "r"),
    ("join + group by", "SELECT c.region, sum(o.amount) FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY c.region", None, "r"),
    ("LIMIT without ORDER BY", "SELECT * FROM orders LIMIT 10", None, "n"),
    ("10k-row result", "SELECT id, amount FROM orders WHERE amount > 990", None, "r"),
    ("prepared, selective parameter", "SELECT count(*), sum(amount) FROM orders WHERE customer_id = %s", (5,), "r"),
    ("prepared, unselective parameter", "SELECT count(*), sum(amount) FROM orders WHERE customer_id = %s", (1,), "r"),
    ("top-N with ORDER BY", "SELECT id, amount FROM orders ORDER BY amount DESC, id LIMIT 10", None, "r"),
    ("integer division", "SELECT count(*) FROM orders WHERE id/2 = 5", None, "r"),
    ("function call", "SELECT order_total(5)", None, "r"),
    ("PostgreSQL function over the table", "SELECT order_total(5), count(*) FROM orders", None, "r"),
    ("schema-qualified select", "SELECT count(*) FROM public.orders", None, "r"),
    ("join with an unmirrored table", "SELECT count(*) FROM orders o JOIN audit_log a ON a.id = o.id", None, "r"),
    ("view over a mirrored table", "SELECT count(*) FROM orders_v", None, "r"),
    ("SELECT FOR UPDATE", "SELECT id FROM orders WHERE id = 7 FOR UPDATE", None, "n"),
    ("CTE with a write", "WITH w AS (UPDATE orders SET note = note WHERE id = 4242 RETURNING id) SELECT count(*) FROM w", None, "w"),
    ("INSERT", "INSERT INTO audit_log(what) VALUES ('rt')", None, "w"),
    ("UPDATE", "UPDATE orders SET note = note WHERE id = 4242", None, "w"),
    ("SELECT ... INTO", "SELECT id INTO TEMP rt_tmp FROM orders WHERE id < 5", None, "w"),
    ("two statements in one Query", "SELECT count(*) FROM orders; SELECT 1", None, "w"),
]


def run(conn, sql, params):
    cur = conn.execute(sql, params, prepare=bool(params))
    return cur.fetchall() if cur.description else None


def canon(rows):
    return sorted(map(repr, rows)) if rows is not None else None


def timed(conn, sql, params, n=5):
    ts = []
    for _ in range(n):
        t = time.perf_counter()
        run(conn, sql, params)
        ts.append((time.perf_counter() - t) * 1000)
    return statistics.median(ts)


def ms(x):
    return "-" if x is None else (f"{x:.2f}" if x < 10 else f"{x:.0f}")


router = psycopg.connect(ROUTER, autocommit=True)
heap = psycopg.connect(DIRECT, autocommit=True)
mirror = psycopg.connect(DIRECT, autocommit=True)
mirror.execute("SET search_path = qs_mirror, public")
heap.execute("CREATE OR REPLACE VIEW public.orders_v AS SELECT * FROM public.orders")


def last():
    return router.execute("SHOW qs.last_route", prepare=False).fetchone()[0].replace(" [cached]", "")


print("| # | statement | side | why | = heap | heap ms | mirror ms |")
print("|---|---|---|---|---|---|---|")
wrong = 0
i = 0


def row(label, sql, params, kind, route=None, same="-"):
    global i, wrong
    i += 1
    side, _, why = route.partition(": ")
    h = m = None
    if kind == "r":
        h = timed(heap, sql, params)
        try:
            m = timed(mirror, sql, params)
        except psycopg.Error as e:
            m = None
            mirror.rollback()
    shown = sql.replace("%s", "$1").replace("|", "\\|")
    if len(shown) > 70:
        shown = shown[:67] + "..."
    print(f"| {i} | {label}: `{shown}` | {side} | {why[:110]} | {same} | {ms(h)} | {ms(m) if kind == 'r' else '-'}{' (fails)' if kind == 'r' and m is None else ''} |")


for label, sql, params, kind in S:
    try:
        got = run(router, sql, params)
    except psycopg.Error as e:
        got = "ERROR " + str(e).splitlines()[0]
    route = last()
    same = "-"
    if kind == "r":
        want = run(heap, sql, params)
        same = "yes" if canon(got) == canon(want) else "NO"
        if same == "NO":
            wrong += 1
    row(label, sql, params, kind, route, same)

# Inside a transaction block.
router.execute("BEGIN")
n = router.execute("SELECT count(*) FROM orders").fetchone()[0]
route = last()
router.execute("COMMIT")
row("count(*) inside BEGIN ... COMMIT", "SELECT count(*) FROM orders", None, "n", route)

# The escape hatch, both ways.
router.execute("SET qs.route = 'mirror'")
got = run(router, "SELECT * FROM orders WHERE id = 4242", None)
route = last()
same = "yes" if canon(got) == canon(run(heap, "SELECT * FROM orders WHERE id = 4242", None)) else "NO"
row("pk lookup under SET qs.route = 'mirror'", "SELECT * FROM orders WHERE id = 4242", None, "n", route, same)
router.execute("INSERT INTO audit_log(what) VALUES ('rt')")
row("INSERT under SET qs.route = 'mirror'", "INSERT INTO audit_log(what) VALUES ('rt')", None, "n", last())
router.execute("SET qs.route = 'heap'")
run(router, "SELECT count(*) FROM orders", None)
row("count(*) under SET qs.route = 'heap'", "SELECT count(*) FROM orders", None, "n", last())
router.execute("RESET qs.route")

heap.execute("DELETE FROM audit_log WHERE what = 'rt'")
heap.execute("DROP VIEW public.orders_v")
print(f"\nanswers that differ from the heap's: {wrong}")
sys.exit(1 if wrong else 0)
