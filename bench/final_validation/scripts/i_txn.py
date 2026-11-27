#!/usr/bin/env python3
"""Scenario i: routing inside a transaction block, READ COMMITTED, on the standby.

The standby is read-only, so an INSERT cannot run there. The statement that
the rule turns on is therefore run as the standby allows it: the first aggregate
in a block with no writes, then a SAVEPOINT (which the rule also names as a
reason to stay on the heap), and an INSERT attempt, which the standby refuses.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import psycopg
import fv_routing as fr

PORT = int(os.environ.get("SPORT", "5801"))
AGG = "SELECT count(*), sum(amount) FROM fv.events WHERE amount > 1000"
conn = psycopg.connect(host="/tmp", port=PORT, user="postgres", dbname="app", autocommit=False)
cur = conn.cursor()


def ans(sql, route=None):
    if route:
        cur.execute(f"SET quicksilver.route = '{route}'")
    cur.execute(sql)
    rows = [tuple(map(str, r)) for r in cur.fetchall()]
    if route:
        cur.execute("RESET quicksilver.route")
    return rows


def plan(sql):
    cur.execute("EXPLAIN (VERBOSE, COSTS OFF) " + sql)
    return [r[0].strip() for r in cur.fetchall() if "Quicksilver" in r[0]]


cur.execute("SHOW transaction_isolation")
print("isolation:", cur.fetchone()[0])
cur.execute("SELECT pg_is_in_recovery()")
print("standby (in recovery):", cur.fetchone()[0])

print("--- BEGIN; aggregate with no writes")
p1 = plan(AGG)
r1 = ans(AGG)
h1 = ans(AGG, route="heap")
print("plan:", p1)
print("routed_to_mirror:", "Quicksilver: mirror" in p1, "| rows:", r1, "heap:", h1, "EQUAL" if r1 == h1 else "DIFFERENT")
first_ok = "Quicksilver: mirror" in p1 and r1 == h1

print("--- SAVEPOINT open, then the aggregate")
cur.execute("SAVEPOINT sp1")
p2 = plan(AGG)
r2 = ans(AGG)
print("plan:", p2)
print("routed_to_mirror:", "Quicksilver: mirror" in p2, "| rows:", r2, "EQUAL" if r2 == h1 else "DIFFERENT")
cur.execute("ROLLBACK TO SAVEPOINT sp1")
cur.execute("RELEASE SAVEPOINT sp1")

print("--- INSERT in the same block (standby refuses it)")
try:
    cur.execute("INSERT INTO fv.lookup VALUES (999999, 'in-txn')")
    print("insert accepted")
except psycopg.Error as e:
    print("refused:", str(e).splitlines()[0])
    conn.rollback()
conn.rollback()
print("first half (routed inside a block with no writes):", "PASS" if first_ok else "FAIL")
print("second half (not routed after a write, sees the row): NOT EXERCISED on a read-only standby")
conn.close()
