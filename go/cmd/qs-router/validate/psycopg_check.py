#!/usr/bin/env python3
"""psycopg 3 through qs-router: prepared statements, a transaction, COPY both
ways, a cancel, and pipeline mode. Exits non-zero on the first thing that is
not as it should be.

    python3 psycopg_check.py 'host=127.0.0.1 port=5490 dbname=app user=postgres'
"""
import sys
import threading
import time

import psycopg

DSN = sys.argv[1] if len(sys.argv) > 1 else "host=127.0.0.1 port=5490 dbname=app user=postgres"
failures = 0


def check(ok, what):
    global failures
    print(("ok    " if ok else "FAIL  ") + what)
    if not ok:
        failures += 1


def last_route(conn):
    # No parameters and text results: psycopg sends this as a simple Query,
    # which the router answers itself.
    # prepare=False keeps it one: psycopg would prepare it after five uses.
    return conn.execute("SHOW qs.last_route", prepare=False).fetchone()[0]


with psycopg.connect(DSN, autocommit=True) as conn:
    print("psycopg", psycopg.__version__, "libpq", psycopg.pq.version())

    # Server-side binding, unprepared (Parse/Bind/Describe/Execute/Sync in one go).
    n = conn.execute("SELECT count(*) FROM orders WHERE customer_id = %s", (1,), prepare=False).fetchone()[0]
    check(290000 < n < 310000 and last_route(conn).startswith("mirror:"), f"unprepared, unselective parameter -> mirror ({n} rows)")
    n = conn.execute("SELECT count(*) FROM orders WHERE customer_id = %s", (5,), prepare=False).fetchone()[0]
    check(30 < n < 200 and last_route(conn).startswith("heap:"), f"unprepared, selective parameter -> heap ({n} rows)")

    # A named prepared statement, executed with both.
    q = "SELECT count(*), sum(amount) FROM orders WHERE customer_id = %s"
    for i in range(4):
        n, s = conn.execute(q, (1,), prepare=True).fetchone()
        r1 = last_route(conn)
        m, _ = conn.execute(q, (5,), prepare=True).fetchone()
        r2 = last_route(conn)
        if not (r1.startswith("mirror:") and r2.startswith("heap:")):
            break
    names = conn.execute("SELECT count(*) FROM pg_prepared_statements WHERE NOT from_sql").fetchone()[0]
    check(r1.startswith("mirror:") and r2.startswith("heap:") and names >= 1,
          f"named prepared statement, 4 rounds: unselective -> {r1.split(':')[0]}, selective -> {r2.split(':')[0]} ({names} server-side statements)")

    row = conn.execute("SELECT id, status, amount, created_at, note FROM orders WHERE id = %s", (4242,)).fetchone()
    check(row[0] == 4242 and row[4] == "n-4242" and last_route(conn).startswith("heap:"), f"primary-key lookup -> heap: {row[:3]}")

    # Binary results from the mirror, with the heap's types.
    cur = conn.cursor(binary=True)
    mir = cur.execute("SELECT status, count(*), sum(amount), max(created_at) FROM orders WHERE customer_id > %s GROUP BY status", (1,)).fetchall()
    rm = last_route(conn)
    conn.execute("SET qs.route = 'heap'")
    heap = cur.execute("SELECT status, count(*), sum(amount), max(created_at) FROM orders WHERE customer_id > %s GROUP BY status", (1,)).fetchall()
    rh = last_route(conn)
    conn.execute("RESET qs.route")
    check(rm.startswith("mirror:") and rh.startswith("heap:") and sorted(mir) == sorted(heap),
          f"binary-format aggregate: mirror and heap return the same {len(heap)} rows, same Python types")

    # COPY out and in.
    with conn.cursor() as cur:
        with cur.copy("COPY (SELECT id, status FROM orders WHERE id <= 5000) TO STDOUT") as cp:
            lines = sum(1 for _ in cp.rows())
        check(lines == 5000, f"COPY ... TO STDOUT: {lines} rows")
        with cur.copy("COPY audit_log(what) FROM STDIN") as cp:
            for i in range(20000):
                cp.write_row(("psycopg-copy",))
        n = conn.execute("DELETE FROM audit_log WHERE what = 'psycopg-copy'").rowcount
        check(n == 20000, f"COPY ... FROM STDIN: {n} rows")
        # With a parameter psycopg sends COPY through the extended protocol,
        # where the client owes a second Sync after CopyDone.
        with cur.copy("COPY (SELECT id FROM orders WHERE id <= %s) TO STDOUT", (300,)) as cp:
            lines = sum(1 for _ in cp.rows())
        check(lines == 300, f"COPY with a parameter (extended protocol): {lines} rows")

    # Cancel.
    t = threading.Timer(0.3, conn.cancel)
    t.start()
    start = time.time()
    try:
        conn.execute("SELECT pg_sleep(30)")
        check(False, "cancel: the sleep finished")
    except psycopg.errors.QueryCanceled:
        check(time.time() - start < 5, f"cancel: QueryCanceled after {time.time() - start:.2f}s")
    check(conn.execute("SELECT 1").fetchone()[0] == 1, "connection usable after the cancel")

    # Pipeline mode: libpq sends Flush as well as Sync.
    with conn.pipeline():
        c1 = conn.execute("SELECT count(*) FROM orders WHERE id <= %s", (10,))
        c2 = conn.execute("SELECT count(*) FROM customers")
        c3 = conn.execute("SELECT %s::int + 1", (41,))
        got = (c1.fetchone()[0], c2.fetchone()[0], c3.fetchone()[0])
    check(got == (10, 10000, 42), f"pipeline mode: {got}")
    n = conn.execute("SELECT count(*) FROM orders").fetchone()[0]
    check(n >= 1000000 and last_route(conn).startswith("mirror:"), "routing resumes after the pipeline: " + last_route(conn))
    sp = conn.execute("SHOW search_path").fetchone()[0]
    check("qs_mirror" not in sp, f"search_path as the client left it: {sp}")

# psycopg's default: every statement opens a transaction.
with psycopg.connect(DSN) as conn:
    conn.execute("INSERT INTO audit_log(what) VALUES (%s)", ("psycopg-tx",))
    n = conn.execute("SELECT count(*) FROM orders").fetchone()[0]
    route = last_route(conn)
    with conn.transaction():  # SAVEPOINT
        conn.execute("DELETE FROM audit_log WHERE what = %s", ("psycopg-tx",))
        raise_rollback = psycopg.Rollback()
        try:
            raise raise_rollback
        except psycopg.Rollback:
            pass
    conn.rollback()
    left = conn.execute("SELECT count(*) FROM audit_log WHERE what = 'psycopg-tx'").fetchone()[0]
    check(route.startswith("heap: inside a transaction") and left == 0,
          f"transaction: count(*) inside it -> {route.split(' [')[0]}; rolled back, {left} rows left")
    conn.execute("INSERT INTO audit_log(what) VALUES (%s)", ("psycopg-tx",))
    conn.commit()
    left = conn.execute("SELECT count(*) FROM audit_log WHERE what = 'psycopg-tx'").fetchone()[0]
    conn.execute("DELETE FROM audit_log WHERE what = 'psycopg-tx'")
    conn.commit()
    check(left == 1, f"transaction: committed, {left} row")

sys.exit(1 if failures else 0)
