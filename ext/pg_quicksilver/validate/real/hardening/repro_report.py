#!/usr/bin/env python3
# repro_report.py N: calls, N times, on the STANDBY as superuser, the statement the sidecar runs to
# report applied LSNs (quicksilver.report_applied_lsn over the registry's names), with the standby's
# replay position, and prints one line per call. Used to reproduce the backend crash of the
# standby log (22:51:06, "Failed process was running: SELECT quicksilver.report_applied_lsn(c.oid, v.lsn)").
# Run it while another session runs ALTER TABLE public.orders ADD COLUMN on the primary.
import sys, time
import psycopg

n = int(sys.argv[1])
conn = psycopg.connect("host=/tmp port=5861 dbname=app user=postgres", autocommit=True)
Q = """SELECT quicksilver.report_applied_lsn(c.oid, v.lsn)
  FROM unnest(%s::text[], %s::text[], %s::pg_lsn[]) AS v(s, t, lsn)
  JOIN pg_catalog.pg_namespace n ON n.nspname = v.s
  JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = v.t"""
for i in range(n):
    lsn = conn.execute("SELECT pg_last_wal_replay_lsn()::text").fetchone()[0]
    names = ["public", "public", "public", "public"]
    rels = ["orders", "customers", "qs_types", "orders"]
    try:
        conn.execute(Q, (names, rels, [lsn] * 4))
        print(i, "ok", lsn, flush=True)
    except Exception as e:  # a backend crash ends the connection, and this process with it
        print(i, "error", type(e).__name__, str(e).strip()[:200], flush=True)
        if conn.closed or conn.broken:
            break
    time.sleep(0.01)
