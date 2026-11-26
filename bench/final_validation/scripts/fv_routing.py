#!/usr/bin/env python3
"""Routing check on the standby (scenarios b, h, i and the joins of l use this file's helpers).

  fv_routing.py --port 5801 [--role postgres] [--statements FILE]

For each statement: EXPLAIN (VERBOSE) on the standby with quicksilver.route
unset (the database's own setting, auto), the routed answer, then the answer with
quicksilver.route = heap in the same session. Prints the Quicksilver line of the
plan, the decision reason, and EQUAL or DIFFERENT for the two answers.
"""
import argparse, sys
import psycopg

QUALIFYING = [
    ("agg_status", "SELECT status, count(*), sum(amount) FROM public.orders GROUP BY status"),
    ("agg_events_amount", "SELECT count(*), sum(amount) FROM fv.events WHERE amount > 1000"),
    ("agg_accounts", "SELECT label, count(*), sum(balance) FROM fv.accounts GROUP BY label"),
    # a column named "at", a DuckDB keyword: the mirror's SQL does not quote it (finding)
    ("agg_events_at", "SELECT count(*), sum(amount) FROM fv.events WHERE at >= timestamptz '2025-03-01 00:00:00+00'"),
]
POINT = [
    ("pk_accounts", "SELECT * FROM fv.accounts WHERE code = 'AC0000042'"),
    ("pk_orders", "SELECT * FROM public.orders WHERE id = 4242"),
    ("pk_events", "SELECT * FROM fv.events WHERE id = md5('e42')::uuid"),
]


def connect(port, user="postgres"):
    return psycopg.connect(host="/tmp", port=port, user=user, dbname="app", autocommit=True)


def explain_lines(conn, sql):
    cur = conn.execute("EXPLAIN (VERBOSE, COSTS OFF) " + sql)
    return [r[0] for r in cur.fetchall()]


def answer(conn, sql, route=None):
    cur = conn.cursor()
    if route:
        cur.execute(f"SET quicksilver.route = '{route}'")
    cur.execute(sql)
    # sorted: an unordered GROUP BY has no order to preserve; the claim is the set of rows
    rows = sorted(tuple(map(str, r)) for r in cur.fetchall()) if cur.description else []
    if route:
        cur.execute("RESET quicksilver.route")
    return rows


def decision(conn):
    try:
        r = conn.execute("SELECT side, reason FROM quicksilver.last_decision()").fetchone()
        return f"{r[0]} / {r[1]}" if r else "none"
    except Exception as e:  # not every role may read it
        return f"unavailable ({str(e).splitlines()[0]})"


def check(conn, name, sql):
    plan = explain_lines(conn, sql)
    qs = [l.strip() for l in plan if "Quicksilver" in l]
    routed = answer(conn, sql)
    heap = answer(conn, sql, route="heap")
    same = routed == heap
    print(f"[{name}] plan: {' | '.join(qs) if qs else '(no Quicksilver line)'}")
    print(f"[{name}] last_decision: {decision(conn)}")
    print(f"[{name}] rows routed={len(routed)} heap={len(heap)} {'EQUAL' if same else 'DIFFERENT'}"
          + ("" if same else f" routed={routed[:3]} heap={heap[:3]}"))
    return same, any(l == "Quicksilver: mirror" for l in qs)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=5801)
    ap.add_argument("--user", default="postgres")
    a = ap.parse_args()
    conn = connect(a.port, a.user)
    bad = 0
    for name, sql in QUALIFYING + POINT:
        same, mirror = check(conn, name, sql)
        bad += not same
        kind = "qualifying" if (name, sql) in QUALIFYING else "point"
        print(f"[{name}] kind={kind} routed_to_mirror={mirror}")
        print()
    print(f"different {bad}")
    sys.exit(1 if bad else 0)
