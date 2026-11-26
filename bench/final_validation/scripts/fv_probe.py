#!/usr/bin/env python3
"""Probe one or more statements on the standby: plan line, routed vs heap answer.

  fv_probe.py --port 5801 --sql "SELECT ..." [--sql "SELECT ..."] [--expect-mirror]

Uses fv_routing.check(): the same statement with the database's routing
setting, then with quicksilver.route = heap, compared as sets of rows.
Exit 1 if the two answers differ, or if --expect-mirror is given and a statement
was not routed.
"""
import argparse, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fv_routing as fr

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=5801)
    ap.add_argument("--sql", action="append", required=True)
    ap.add_argument("--expect-mirror", action="store_true")
    ap.add_argument("--user", default="postgres")
    a = ap.parse_args()
    conn = fr.connect(a.port, a.user)
    bad = 0
    for i, sql in enumerate(a.sql):
        same, mirror = fr.check(conn, f"probe{i}", sql)
        if not same or (a.expect_mirror and not mirror):
            bad += 1
        print(f"[probe{i}] routed_to_mirror={mirror}")
        print()
    print(f"problems {bad}")
    sys.exit(1 if bad else 0)
