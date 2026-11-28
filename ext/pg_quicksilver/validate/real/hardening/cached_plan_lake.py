#!/usr/bin/env python3
"""A cached routed plan on a standby, while the sidecar publishes the lake (docs/40, "Lock order").

The mirror of ex.t_num is a DuckLake table the sidecar publishes. A statement with
no parameters is cached as a generic routed plan on the standby. Each step here
changes the table on the primary, waits until the sidecar reports that it has
applied the change, and then reuses the cached plan: its rows must equal the
heap's rows on the same standby. The schema steps add and drop a column, which
the sidecar rebuilds. One EXECUTE is started while the ALTER is on its way; it
must not fail and must return rows that exist in the heap at some point.

    cached_plan_lake.py --pport 8001 --sport 8002 [--rounds 5]

The table is restored (inserted rows deleted, probe column dropped) at the end.
"""
import argparse
import sys
import threading
import time

import psycopg

QUERY = "SELECT id, n FROM ex.t_num WHERE id >= 99000 AND id < 99100 ORDER BY id"
QUERY_ALL = "SELECT id, n FROM ex.t_num WHERE id < 40 ORDER BY id"


def conn(port):
    return psycopg.connect(f"host=/tmp port={port} user=postgres dbname=app", autocommit=True)


def wait_applied(p, s, timeout=120.0):
    """Until the standby's sidecar has applied everything the primary has written."""
    want = p.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
    end = time.time() + timeout
    while time.time() < end:
        row = s.execute("SELECT applied_lsn >= %s FROM quicksilver.mirror_status() WHERE heap = 'ex.t_num'::regclass",
                        (want,)).fetchone()
        if row and row[0]:
            return True
        time.sleep(0.2)
    return False


def heap_rows(s, sql):
    s.execute("SET quicksilver.route = heap")
    rows = s.execute(sql).fetchall()
    s.execute("SET quicksilver.route = mirror")
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pport", type=int, required=True)
    ap.add_argument("--sport", type=int, required=True)
    ap.add_argument("--rounds", type=int, default=5)
    args = ap.parse_args()
    p = conn(args.pport)
    s = conn(args.sport)
    fails = []
    passes = 0

    s.execute("SET quicksilver.route = mirror")
    s.execute("SET plan_cache_mode = force_generic_plan")
    s.execute(f"PREPARE lk AS {QUERY}")
    s.execute(f"PREPARE lk_all AS {QUERY_ALL}")

    def compare(name, plan_name, sql):
        nonlocal passes
        mirror = s.execute(f"EXECUTE {plan_name}").fetchall()
        heap = heap_rows(s, sql)
        if mirror == heap:
            passes += 1
        else:
            fails.append(f"{name}: mirror {mirror[:3]}... ({len(mirror)} rows) != heap {heap[:3]}... ({len(heap)} rows)")

    compare("start", "lk_all", QUERY_ALL)
    for k in range(args.rounds):
        base = 99000 + k * 10
        p.execute(f"INSERT INTO ex.t_num (id, n) VALUES ({base}, {k}), ({base + 1}, {k + 1})")
        if not wait_applied(p, s):
            fails.append(f"round {k}: the sidecar never applied the insert")
            break
        compare(f"round {k}: after insert", "lk", QUERY)
        compare(f"round {k}: all rows", "lk_all", QUERY_ALL)
        p.execute(f"DELETE FROM ex.t_num WHERE id IN ({base}, {base + 1})")
        if not wait_applied(p, s):
            fails.append(f"round {k}: the sidecar never applied the delete")
            break
        compare(f"round {k}: after delete", "lk", QUERY)

    # a column added: the mirror is rebuilt with it; the cached plan must not answer with a wrong shape
    p.execute("ALTER TABLE ex.t_num ADD COLUMN qs_probe int DEFAULT 7")
    during = {}

    def run_during():
        try:
            during["rows"] = s.execute("EXECUTE lk_all").fetchall()
        except psycopg.Error as e:
            during["error"] = f"{e.sqlstate}: {e}"

    t = threading.Thread(target=run_during)
    t.start()
    t.join()
    if "error" in during:
        fails.append(f"ALTER ADD COLUMN: an EXECUTE during it failed: {during['error']}")
    else:
        passes += 1
    if not wait_applied(p, s):
        fails.append("ADD COLUMN: the sidecar never applied it")
    compare("after ADD COLUMN", "lk_all", QUERY_ALL)
    p.execute("ALTER TABLE ex.t_num DROP COLUMN qs_probe")
    if not wait_applied(p, s):
        fails.append("DROP COLUMN: the sidecar never applied it")
    compare("after DROP COLUMN", "lk_all", QUERY_ALL)
    compare("after DROP COLUMN (keyed)", "lk", QUERY)

    s.execute("DEALLOCATE lk")
    s.execute("DEALLOCATE lk_all")
    print(f"{'PASS' if not fails else 'FAIL'} lake: {passes} pass, {len(fails)} fail")
    for f in fails:
        print(f"  FAIL {f}")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
