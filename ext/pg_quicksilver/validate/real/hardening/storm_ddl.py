#!/usr/bin/env python3
"""ALTER storm on the primary, readers on the standby: which readers see DeadlockDetected?

A writer runs ALTER TABLE public.orders ADD COLUMN on the primary (the sidecar
re-creates the mirror view after each one, so the standby replays the heap's
DDL and the mirror's DDL). Readers on the standby run an aggregate over
public.orders for the whole time, in two kinds:

    heap     quicksilver.route = off   (only the heap is read)
    routed   quicksilver.route = auto  (the router tries the mirror first)

Counts per reader kind of each SQLSTATE that is not a success (40P01 is a
deadlock). The DDL is replayed on the standby by its startup process, so a
deadlock is a cycle between that process and a reader.

    storm_ddl.py --pport 5600 --sport 5601 --seconds 330 --heap 2 --routed 2
"""
import argparse
import collections
import threading
import time

import psycopg


def writer(pport, deadline, out, interval):
    conn = psycopg.connect(f"host=/tmp port={pport} user=postgres dbname=app", autocommit=True)
    n = 0
    while time.time() < deadline:
        n += 1
        try:
            conn.execute(f"ALTER TABLE public.orders ADD COLUMN IF NOT EXISTS storm_{n} int")
            out["alters"] += 1
        except psycopg.Error as e:
            out["alter_errors"][e.sqlstate or "?"] += 1
        time.sleep(interval)
    conn.close()


def reporter(sport, deadline, out, interval):
    """Keeps the mirror fresh: the replay position is reported every interval seconds."""
    def connect():
        return psycopg.connect(f"host=/tmp port={sport} user=postgres dbname=app", autocommit=True)

    conn = connect()
    while time.time() < deadline:
        try:
            conn.execute("SELECT quicksilver.report_applied_lsn('public.orders'::regclass, pg_last_wal_replay_lsn())")
            out["reports"] += 1
        except psycopg.Error as e:
            out["report_errors"][e.sqlstate or "?"] += 1
            if conn.closed or conn.broken:    # terminated by a recovery conflict: reconnect (as the readers do)
                try:
                    conn.close()
                except psycopg.Error:
                    pass
                try:
                    conn = connect()
                except psycopg.Error:
                    time.sleep(0.2)
                    continue
        time.sleep(interval)
    conn.close()


def reader(sport, route, deadline, out, lock, expected=None, max_lag=None):
    def connect():
        c = psycopg.connect(f"host=/tmp port={sport} user=postgres dbname=app", autocommit=True)
        c.execute(f"SET quicksilver.route = {route}")
        if max_lag and route == "auto":
            c.execute(f"SET quicksilver.max_lag = '{max_lag}'")
        return c

    conn = connect()
    stats = collections.Counter()
    while time.time() < deadline:
        try:
            got = conn.execute("SELECT count(*), sum(amount) FROM public.orders").fetchall()
            stats["ok"] += 1
            if expected is not None and got != expected:
                stats["WRONG ANSWER"] += 1
            if route == "auto":
                side, reason = conn.execute("SELECT side, reason FROM quicksilver.last_decision()").fetchone()
                stats[f"decision {side}: {reason}"] += 1
        except psycopg.Error as e:
            stats[e.sqlstate or "?"] += 1
            # A backend the standby terminates (FATAL: terminating connection due to conflict
            # with recovery) leaves the session dead: reconnect, or every later statement fails
            # at once and the loop spins without measuring anything.
            if conn.closed or conn.broken:
                stats["reconnect"] += 1
                try:
                    conn.close()
                except psycopg.Error:
                    pass
                time.sleep(0.05)
                try:
                    conn = connect()
                except psycopg.Error:
                    stats["connect failed"] += 1
                    time.sleep(0.2)
                continue
            try:
                conn.rollback()
            except psycopg.Error:
                pass
    with lock:
        out.append((route, dict(stats)))
    conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pport", type=int, default=5600)
    ap.add_argument("--sport", type=int, default=5601)
    ap.add_argument("--seconds", type=int, default=330)
    ap.add_argument("--heap", type=int, default=2)
    ap.add_argument("--routed", type=int, default=2)
    ap.add_argument("--alter-interval", type=float, default=0.3, help="seconds between ALTERs")
    ap.add_argument("--max-lag", default=None, help="quicksilver.max_lag for the routed readers (e.g. 64MB)")
    ap.add_argument("--report-interval", type=float, default=0.025,
                    help="seconds between replay-position reports on the standby (0 = none)")
    args = ap.parse_args()

    # The storm only ALTERs orders, so the answer must never change: the heap's count and sum on the
    # standby, read before the storm, is the value every answer is compared with.
    hc = psycopg.connect(f"host=/tmp port={args.sport} user=postgres dbname=app", autocommit=True)
    hc.execute("SET quicksilver.route = off")
    expected = hc.execute("SELECT count(*), sum(amount) FROM public.orders").fetchall()
    hc.close()
    print("expected (heap, standby, before the storm):", expected)
    deadline = time.time() + args.seconds
    out = {"alters": 0, "alter_errors": collections.Counter(), "reports": 0,
           "report_errors": collections.Counter()}
    results = []
    lock = threading.Lock()
    threads = [threading.Thread(target=writer, args=(args.pport, deadline, out, args.alter_interval))]
    if args.report_interval > 0:
        threads.append(threading.Thread(target=reporter, args=(args.sport, deadline, out, args.report_interval)))
    for _ in range(args.heap):
        threads.append(threading.Thread(target=reader, args=(args.sport, "off", deadline, results, lock, expected)))
    for _ in range(args.routed):
        threads.append(threading.Thread(target=reader, args=(args.sport, "auto", deadline, results, lock, expected, args.max_lag)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print(f"seconds={args.seconds} alters={out['alters']} alter_errors={dict(out['alter_errors'])} "
          f"reports={out['reports']} report_errors={dict(out['report_errors'])}")
    for kind in ("off", "auto"):
        total = collections.Counter()
        for route, stats in results:
            if route == kind:
                total.update(stats)
        name = "heap" if kind == "off" else "routed"
        print(f"{name}: {dict(total)}")


if __name__ == "__main__":
    main()
