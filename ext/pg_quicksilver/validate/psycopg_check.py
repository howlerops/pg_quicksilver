#!/usr/bin/env python3
"""psycopg 3 against a server with pg_quicksilver loaded.

Every check runs a statement on a routing connection, compares the answer with
a heap-only connection's, and asks the extension which side answered.

    psycopg_check.py [--port 5541]
"""
import argparse, os, sys
import psycopg

BIG = "SELECT count(*), sum(amount) FROM public.orders WHERE customer_id = %s"
BIG_NOPARAM = "SELECT status, count(*), sum(amount) FROM public.orders GROUP BY status ORDER BY count(*), sum(amount)"
LOOKUP = "SELECT id, status FROM public.orders WHERE id = %s"
UNSAFE = "SELECT count(*) FROM public.orders WHERE id / 2 = %s"
results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(f"{'ok  ' if ok else 'FAIL'} {name}{': ' + detail if detail else ''}")


def side(c):
    cur = c.cursor()
    r = cur.execute("SELECT side, reason FROM quicksilver.last_decision()").fetchone()
    return r if r else ("heap", "no new decision")


def forget(c):
    c.cursor().execute("SELECT quicksilver.forget_last()")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=int(os.environ.get("PPORT", 5541)))
    args = ap.parse_args()
    dsn = f"host=/tmp port={args.port} dbname=app user=postgres"
    print(f"psycopg {psycopg.__version__}, libpq {psycopg.pq.version()}")

    heap = psycopg.connect(dsn, autocommit=True)
    heap.execute("SET quicksilver.route = heap")

    # ---------------------------------------------------------------- autocommit
    c = psycopg.connect(dsn, autocommit=True)
    c.execute("SET quicksilver.route = auto")
    forget(c)
    got = c.execute(BIG_NOPARAM).fetchall()
    check("autocommit, no parameters (simple protocol): routed", side(c)[0] == "mirror" and got == heap.execute(BIG_NOPARAM).fetchall(), str(side(c)))
    forget(c)
    got = c.execute(BIG, (1,)).fetchall()
    check("autocommit, parameter (extended protocol): routed", side(c)[0] == "mirror" and got == heap.execute(BIG, (1,)).fetchall(), str(side(c)))
    forget(c)
    got = c.execute(BIG, (5,)).fetchall()
    check("autocommit, selective parameter: heap", side(c)[0] == "heap" and got == heap.execute(BIG, (5,)).fetchall(), str(side(c)))
    forget(c)
    got = c.execute(LOOKUP, (4242,)).fetchall()
    check("autocommit, primary-key lookup: heap", side(c)[0] == "heap" and got == heap.execute(LOOKUP, (4242,)).fetchall(), str(side(c)))
    forget(c)
    got = c.execute(UNSAFE, (5,)).fetchall()
    check("autocommit, integer division: heap", side(c)[0] == "heap" and got == heap.execute(UNSAFE, (5,)).fetchall(), str(side(c)))

    # a named prepared statement, alternating values, well past the fifth execution
    ok, sides = True, []
    for i in range(12):
        v = 1 if i % 2 == 0 else 5
        forget(c)
        got = c.execute(BIG, (v,), prepare=True).fetchall()
        sides.append(side(c)[0])
        ok = ok and got == heap.execute(BIG, (v,)).fetchall()
    check("named prepared statement, 12 executions alternating 1 and 5", ok and sides == ["mirror", "heap"] * 6, " ".join(sides))

    # binary results
    forget(c)
    cur = c.cursor(binary=True)
    got = cur.execute(BIG_NOPARAM).fetchall()
    hb = heap.cursor(binary=True).execute(BIG_NOPARAM).fetchall()
    check("binary result format: routed, same values", side(c)[0] == "mirror" and got == hb, str(got[0]))
    desc = [(d.name, d.type_code, d.internal_size, d.precision, d.scale) for d in cur.description]
    hc = heap.cursor()
    hc.execute(BIG_NOPARAM)
    check("cursor.description is the heap's", desc == [(d.name, d.type_code, d.internal_size, d.precision, d.scale) for d in hc.description], str(desc))

    # pipeline mode
    forget(c)
    with c.pipeline():
        c1 = c.execute(BIG, (1,))
        c2 = c.execute(LOOKUP, (7,))
        c3 = c.execute(BIG, (5,))
    got = (c1.fetchall(), c2.fetchall(), c3.fetchall())
    want = (heap.execute(BIG, (1,)).fetchall(), heap.execute(LOOKUP, (7,)).fetchall(), heap.execute(BIG, (5,)).fetchall())
    check("pipeline mode: same answers", got == want)

    # COPY (SELECT ...) TO STDOUT is a utility statement: heap
    with c.cursor().copy("COPY (SELECT count(*) FROM public.orders) TO STDOUT") as cp:
        got = b"".join(cp)
    check("COPY (SELECT ...) TO STDOUT: answered", got.strip() == b"1000000")

    # ---------------------------------------------------------------- the default: a transaction per statement
    t = psycopg.connect(dsn)            # autocommit off: BEGIN is sent before the first statement
    t.execute("SET quicksilver.route = auto")
    t.commit()
    forget(t)
    t.commit()
    got = t.execute(BIG, (1,)).fetchall()
    s = side(t)
    t.commit()
    # the default is safe (route_in_transaction): READ COMMITTED, nothing written, no savepoint open
    check("not autocommit, default settings: routed, correct", s[0] == "mirror" and got == heap.execute(BIG, (1,)).fetchall(), str(s))
    # a nested transaction (SAVEPOINT) after a routed read works (pg_duckdb is idle by then)
    with t.transaction():
        t.execute(BIG, (1,)).fetchall()
        with t.transaction():
            got = t.execute(BIG, (1,)).fetchall()
    t.commit()
    check("not autocommit, nested transaction (SAVEPOINT): works", got == heap.execute(BIG, (1,)).fetchall())

    # opted in: routed inside the transaction block
    t.execute("SET quicksilver.route_in_transaction = on")
    t.commit()
    forget(t)
    t.commit()
    got = t.execute(BIG, (1,)).fetchall()
    s = side(t)
    t.commit()
    check("not autocommit, quicksilver.route_in_transaction = on: routed", s[0] == "mirror" and got == heap.execute(BIG, (1,)).fetchall(), str(s))
    # ... and what that costs: a SAVEPOINT after a routed statement aborts the transaction
    try:
        t.execute(BIG, (1,)).fetchall()
        with t.transaction():
            t.execute("SELECT 1")
        hazard = "no error"
    except psycopg.Error as e:
        hazard = (e.diag.message_primary or str(e))[:80]
    t.rollback()
    check("... and a SAVEPOINT after it works", hazard == "no error", hazard)
    # a server-side cursor
    forget(t)
    t.commit()
    with t.cursor(name="qs_check") as sc:
        sc.execute("SELECT id, amount FROM public.orders WHERE amount > 999.5 ORDER BY id")
        first = sc.fetchmany(5)
        rest = sc.fetchall()
    s = side(t)
    t.commit()
    want = heap.execute("SELECT id, amount FROM public.orders WHERE amount > 999.5 ORDER BY id").fetchall()
    # a cursor declared with neither SCROLL nor NO SCROLL, over a plan that can scan backward, is the heap's
    check("server-side cursor (DECLARE/FETCH): heap, scrollable cursor, same rows", s[0] == "heap" and s[1] == "scrollable cursor" and first + rest == want, f"{s} {len(want)} rows")

    # cancel during a routed statement, then the connection is usable
    c.execute("SET statement_timeout = '30ms'")
    try:
        c.execute("SELECT count(*) FROM public.orders o JOIN public.orders p ON p.customer_id = o.customer_id WHERE o.customer_id = 1")
        cancelled = "finished"
    except psycopg.Error as e:
        # a routed statement's timeout comes back from pg_duckdb as XX000 "Query cancelled",
        # not as 57014 (query_canceled): the error is the client's, the cancel is not lost
        cancelled = "cancelled" if "cancel" in str(e).lower() else str(e)[:120]
    c.execute("RESET statement_timeout")
    forget(c)
    got = c.execute(BIG, (1,)).fetchall()
    check("statement cancelled mid-flight, connection still routes", cancelled == "cancelled" and side(c)[0] == "mirror" and got == heap.execute(BIG, (1,)).fetchall(), cancelled)

    print(f"{sum(results)} of {len(results)} checks passed")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
