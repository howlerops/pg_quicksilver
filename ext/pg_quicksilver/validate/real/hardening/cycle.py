#!/usr/bin/env python3
"""The lock cycle between a reader on a standby and the standby's replay of a DDL.

Deterministic, unlike the storm (storm_ddl.py): the cycle needs three things at
once, and this makes each of them happen in order.

  1. A reader on the standby holds AccessShareLock on public.orders (its
     transaction has read it) and then plans a routed statement.
  2. The primary runs one transaction that takes AccessExclusiveLock on the
     mirror view quicksilver_mirror.orders, then on public.orders (an ADD COLUMN).
     The standby's startup process replays the first lock, then waits for the
     second, which the reader's transaction blocks.
  3. The routed statement needs the mirror view. Its lock is held by the startup
     process, which waits for the reader: a cycle. The heap-only reader in the
     same position takes no lock on the view, and so makes no cycle.

The freshness of the mirror is reported at the replay position the startup
process is stuck at, so the routed statement is judged fresh and tries the mirror.

    cycle.py --pport 5600 --sport 5601 [--kind routed|heap] [--tag N]
"""
import argparse
import sys
import threading
import time

import psycopg


def conn_to(port, autocommit):
    return psycopg.connect(f"host=/tmp port={port} user=postgres dbname=app", autocommit=autocommit)


def wait_startup_blocked(standby, timeout=30.0):
    end = time.time() + timeout
    while time.time() < end:
        row = standby.execute(
            "SELECT wait_event_type, wait_event FROM pg_stat_activity WHERE backend_type = 'startup'"
        ).fetchone()
        if row and row[0] == "Lock":
            return row
        time.sleep(0.05)
    return None


def run(pport, sport, kind, tag):
    primary = conn_to(pport, True)
    standby = conn_to(sport, True)
    reader = conn_to(sport, False)          # transactional: the lock is held until the end

    # the reader's transaction reads the heap table first (AccessShareLock on public.orders)
    reader.execute("SET quicksilver.route = off")
    reader.execute("BEGIN")
    reader.execute("SELECT count(*) FROM public.orders").fetchall()

    def ddl():
        p = conn_to(pport, False)
        try:
            p.execute("BEGIN")
            p.execute('LOCK TABLE quicksilver_mirror."public.orders" IN ACCESS EXCLUSIVE MODE')
            p.execute(f"ALTER TABLE public.orders ADD COLUMN IF NOT EXISTS cyc_{tag} int")
            p.execute("COMMIT")
        except psycopg.Error as e:
            print(f"ddl error {e.sqlstate}: {e}")
            try:
                p.execute("ROLLBACK")
            except psycopg.Error:
                pass
        finally:
            p.close()

    t = threading.Thread(target=ddl)
    t.start()
    blocked = wait_startup_blocked(standby)
    print(f"startup process: {blocked}")

    # the replay position the startup process is stuck at: reported from the reader's own
    # transaction (it already holds the heap lock; another session would queue behind the
    # startup process's pending request and be killed with the reader), so that the mirror is
    # judged fresh for the statement below
    reader.execute("SELECT quicksilver.report_applied_lsn('public.orders'::regclass, pg_last_wal_replay_lsn())")

    result = "not run"
    try:
        if kind == "routed":
            reader.execute("SET quicksilver.route = mirror")
        else:
            reader.execute("SET quicksilver.route = off")
        n = reader.execute("SELECT count(*) FROM public.orders").fetchone()[0]
        result = f"ok count={n}"
    except psycopg.Error as e:
        result = f"error {e.sqlstate}: {str(e).splitlines()[0]}"
        try:
            reader.execute("ROLLBACK")
        except psycopg.Error:
            pass
    decision = None
    try:
        decision = reader.execute("SELECT side, reason, left(detail, 90) FROM quicksilver.last_decision()").fetchone()
    except psycopg.Error:
        pass
    t.join(timeout=60)
    try:
        reader.execute("ROLLBACK")
    except psycopg.Error:
        pass
    print(f"reader kind={kind}: {result}; last decision {decision}")
    standby.execute("SELECT 1")
    primary.close()
    standby.close()
    reader.close()
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pport", type=int, default=5600)
    ap.add_argument("--sport", type=int, default=5601)
    ap.add_argument("--kind", choices=["routed", "heap"], default="routed")
    ap.add_argument("--tag", type=int, default=int(time.time()))
    args = ap.parse_args()
    run(args.pport, args.sport, args.kind, args.tag)


if __name__ == "__main__":
    sys.exit(main())
