#!/usr/bin/env python3
"""Cached routed plans and changes of their mirror view (docs/40, "Lock order").

A statement with no parameters is routed from a generic plan, which the plan
cache keeps and reuses in later transactions without locking the mirror view.
Nothing that changes the view may leave such a plan answering with the wrong
rows, types, or an error. This drives what sql/cachedplan.sql cannot: the DDL
arriving by replay on a standby, and DDL that runs while a long routed EXECUTE
is in progress.

  seq   one session on the reader node keeps a cached plan and runs EXECUTE
        between DDL made on the primary: CREATE OR REPLACE with the same
        columns, a column of another type, DROP VIEW. On the standby the DDL
        arrives by replay. Each EXECUTE must return exactly the rows that the
        mirror (or the heap) has at that point.
  race  a cached plan is primed, then a long routed scan (3M rows) runs on the
        reader node while another session (on the primary) replaces, retypes or
        drops the mirror view. The scan must return whole rows of one answer:
        the old mirror, the new mirror, or the heap. No error, no mixed or
        mistyped rows.

Prints PASS/FAIL counts per scenario; exit status 1 on any FAIL.

    cached_plan.py --pport 8001 --sport 8002 [--iterations 50] [--node primary|standby]
"""
import argparse
import random
import sys
import threading
import time

import psycopg

HEAP_ROWS = [(1, "heap1"), (2, "heap2"), (3, "heap3"), (4, "heap4"), (5, "heap5")]
BIG_ROWS = 3_000_000
TABLE = "cpx.h"
VIEW = "cpx.m"


def conn(port):
    return psycopg.connect(f"host=/tmp port={port} user=postgres dbname=app", autocommit=True)


def small_view_sql(tag):
    rows = ", ".join(f"({i}, {i * 10}, '{tag}-{i}')" for i in range(1, 6))
    return (f"SELECT r['id']::int AS id, r['a']::int AS a, r['b']::text AS b "
            f"FROM duckdb.query($$ SELECT * FROM (VALUES {rows}) t(id, a, b) $$) r")


def small_view_int_sql():
    rows = ", ".join(f"({i}, {i * 10}, {i * 111})" for i in range(1, 6))
    return (f"SELECT r['id']::int AS id, r['a']::int AS a, r['b']::int AS b "
            f"FROM duckdb.query($$ SELECT * FROM (VALUES {rows}) t(id, a, b) $$) r")


def big_view_sql(tag, scale=1):
    return (f"SELECT r['id']::int AS id, r['a']::int AS a, r['b']::text AS b "
            f"FROM duckdb.query($$ SELECT i AS id, i * 10 AS a, '{tag}-' || i AS b "
            f"FROM range(1, {BIG_ROWS + 1}) t(i) $$) r")


def big_view_int_sql():
    return (f"SELECT r['id']::int AS id, r['a']::int AS a, r['b']::int AS b "
            f"FROM duckdb.query($$ SELECT i AS id, i * 10 AS a, i * 111 AS b "
            f"FROM range(1, {BIG_ROWS + 1}) t(i) $$) r")


def run_ddl(c, text):
    """Each statement of text, in order (no ';' occurs inside any of the bodies above)."""
    for stmt in text.split(";"):
        if stmt.strip():
            c.execute(stmt.strip())


def replace_view(c, body):
    run_ddl(c, f"CREATE OR REPLACE VIEW {VIEW} AS {body}")


def fresh_view(c, body):
    run_ddl(c, f"DROP VIEW IF EXISTS {VIEW}; CREATE VIEW {VIEW} AS {body}")


def setup_heap(p):
    p.execute("DROP SCHEMA IF EXISTS cpx CASCADE")
    p.execute("CREATE SCHEMA cpx")
    p.execute("CREATE TABLE cpx.h (id int PRIMARY KEY, a int, b text)")
    p.execute("INSERT INTO cpx.h SELECT g, g * 10, 'heap' || g FROM generate_series(1, 5) g")


def register(p, cap=100000000):
    p.execute(f"SELECT quicksilver.register_mirror('{TABLE}', '{VIEW}', {cap})")


class Node:
    """Brings the reader node's view of the mirror up to date with the primary: a standby
    replays and reports its replay position; the primary reports its own position."""

    def __init__(self, p, r, node):
        self.p, self.r, self.node = p, r, node

    def sync(self, timeout=60.0):
        if self.node == "primary":
            self.p.execute(f"SELECT quicksilver.report_applied_lsn('{TABLE}', pg_current_wal_lsn())")
            return True
        want = self.p.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
        end = time.time() + timeout
        while time.time() < end:
            if self.r.execute("SELECT pg_last_wal_replay_lsn() >= %s", (want,)).fetchone()[0]:
                self.r.execute(f"SELECT quicksilver.report_applied_lsn('{TABLE}', pg_last_wal_replay_lsn())")
                return True
            time.sleep(0.05)
        return False


class Tally:
    def __init__(self):
        self.by = {}
        self.fails = []
        self.seen = {}

    def record(self, group, name, ok, got):
        ok_n, bad_n = self.by.get(group, (0, 0))
        self.by[group] = (ok_n + 1, bad_n) if ok else (ok_n, bad_n + 1)
        short = got.split(" (")[0]
        counts = self.seen.setdefault(group, {})
        counts[short] = counts.get(short, 0) + 1
        if not ok:
            self.fails.append((name, got))

    def totals(self):
        return sum(v[0] for v in self.by.values()), sum(v[1] for v in self.by.values())


def classify_small(rows):
    if rows == HEAP_ROWS:
        return "heap"
    for tag in ("v1", "v2"):
        if rows == [(i, f"{tag}-{i}") for i in range(1, 6)]:
            return tag
    return f"BAD({rows[:2]}...)"


def classify_big(rows):
    if rows == HEAP_ROWS:
        return "heap"
    if len(rows) != BIG_ROWS:
        return f"BAD(rows={len(rows)})"
    tag = rows[0][1].split("-")[0]
    if tag not in ("v1", "v2"):
        return f"BAD(first={rows[0][1]})"
    for i, (rid, b) in enumerate(rows):
        if rid != i + 1 or b != f"{tag}-{rid}":
            return f"BAD(row {i + 1}: {rid}, {b})"
    return tag


def seq(pport, sport, node, tally):
    """Sequential DDL between EXECUTEs, in one session on the reader node."""
    p = conn(pport)
    r = conn(pport if node == "primary" else sport)
    n = Node(p, r, node)
    setup_heap(p)
    fresh_view(p, small_view_sql("v1"))
    register(p)
    n.sync()
    r.execute("SET quicksilver.route = mirror")
    r.execute("SET plan_cache_mode = force_generic_plan")
    r.execute("PREPARE sq AS SELECT id, b FROM cpx.h ORDER BY id")

    def expect(group, name, want):
        n.sync()
        got = classify_small(r.execute("EXECUTE sq").fetchall())
        tally.record(group, name, got == want, f"got {got}, want {want}")

    group = f"seq/{node}"
    expect(group, "start: mirror v1", "v1")
    expect(group, "start again", "v1")
    replace_view(p, small_view_sql("v2"))
    expect(group, "same columns replaced: mirror v2", "v2")
    replace_view(p, small_view_sql("v1"))
    expect(group, "same columns replaced back: mirror v1", "v1")
    fresh_view(p, small_view_int_sql())
    register(p)
    expect(group, "column of another type: heap", "heap")
    fresh_view(p, small_view_sql("v1"))
    register(p)
    expect(group, "registered again: mirror v1", "v1")
    run_ddl(p, f"DROP VIEW {VIEW}")
    expect(group, "dropped: heap", "heap")
    fresh_view(p, small_view_sql("v1"))
    register(p)
    expect(group, "created and registered again: mirror v1", "v1")
    r.close()
    p.close()


def race(pport, sport, node, kind, rng, tally):
    p = conn(pport)
    r = conn(pport if node == "primary" else sport)
    n = Node(p, r, node)
    setup_heap(p)
    fresh_view(p, big_view_sql("v1"))
    register(p)
    n.sync()
    r.execute("SET quicksilver.route = mirror")
    r.execute("SET plan_cache_mode = force_generic_plan")
    r.execute("PREPARE big AS SELECT id, b FROM cpx.h ORDER BY id")
    primed = classify_big(r.execute("EXECUTE big").fetchall())   # plans and keeps the generic routed plan
    result = {}

    def run():
        try:
            result["rows"] = r.execute("EXECUTE big").fetchall()
        except psycopg.Error as e:
            result["error"] = f"{e.sqlstate}: {str(e).strip()}"

    t = threading.Thread(target=run)
    t.start()
    time.sleep(rng.uniform(0.1, 1.2))
    w = conn(pport)
    if kind == 0:
        allowed = {"v1", "v2", "heap"}
        replace_view(w, big_view_sql("v2"))
    elif kind == 1:
        allowed = {"v1", "heap"}
        run_ddl(w, f"DROP VIEW {VIEW}")
    else:
        allowed = {"v1", "heap"}
        fresh_view(w, big_view_int_sql())
    during = t.is_alive()          # the DDL landed while the EXECUTE was still running
    t.join()
    w.close()
    if "error" in result:
        got = "ERROR " + result["error"]
        ok = False
    else:
        got = classify_big(result["rows"])
        ok = got in allowed
    kind_name = ["replaced (same columns)", "dropped", "column of another type"][kind]
    timing = "during" if during else "after"
    tally.record(f"race/{node}/{kind_name}/DDL {timing}", f"primed {primed}", ok, f"got {got} (allowed {sorted(allowed)})")
    r.close()
    p.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pport", type=int, required=True)
    ap.add_argument("--sport", type=int, required=True)
    ap.add_argument("--node", choices=["primary", "standby"], default="standby")
    ap.add_argument("--iterations", type=int, default=50, help="race iterations per DDL kind")
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()
    rng = random.Random(args.seed)
    tally = Tally()
    seq(args.pport, args.sport, args.node, tally)
    for i in range(args.iterations * 3):
        race(args.pport, args.sport, args.node, i % 3, rng, tally)
    for group, (ok_n, bad_n) in sorted(tally.by.items()):
        print(f"{'PASS' if bad_n == 0 else 'FAIL'} {group}: {ok_n} pass, {bad_n} fail; answers {tally.seen.get(group, {})}")
    for name, got in tally.fails[:20]:
        print(f"  FAIL {name}: {got}")
    ok_total, bad_total = tally.totals()
    print(f"total: {ok_total} PASS, {bad_total} FAIL")
    sys.exit(1 if bad_total else 0)


if __name__ == "__main__":
    main()
