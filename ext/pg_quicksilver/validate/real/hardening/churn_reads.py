#!/usr/bin/env python3
"""Forced-mirror reads on the standby while the primary's catalogs churn.

Each iteration is a fresh backend on the standby (as each regression file gets), with
quicksilver.route = mirror, and reads the decision the statement left. The churn runs on the
primary and reaches the standby by replay:

    unrelated  DDL that touches nothing the registry names: tables, types and functions created and
               dropped in a loop (every one is a syscache or relcache invalidation for the standby)
    relevant   the allowlist row of count() is deleted and put back in a loop (the allowlist trigger
               reaches the standby as an invalidation of the registry; the statement is heap while
               the row is missing, and the answer must not change)
    none       no churn (the control)

Counts: the answer against the heap's (a wrong answer is counted, never hidden), the decision's side
and reason, and any SQLSTATE the read got.

    churn_reads.py --pport 5600 --sport 5601 --iters 300 --churn unrelated
"""
import argparse
import collections
import threading
import time

import psycopg
from psycopg import sql

EXPECTED = (1000000, 499995000)  # public.orders as the storm and setup.sh leave it


def churn_unrelated(pport, stop, counts, pause):
    conn = psycopg.connect(f"host=/tmp port={pport} user=postgres dbname=app", autocommit=True)
    n = 0
    while not stop.is_set():
        n += 1
        for stmt in (
            f"CREATE TABLE public.ch_t{n} (a int, b text)",
            f"INSERT INTO public.ch_t{n} VALUES (1, 'x')",
            f"CREATE TYPE public.ch_e{n} AS ENUM ('a', 'b')",
            f"CREATE FUNCTION public.ch_f{n}() RETURNS int LANGUAGE sql AS 'SELECT 1'",
            f"DROP FUNCTION public.ch_f{n}()",
            f"DROP TYPE public.ch_e{n}",
            f"DROP TABLE public.ch_t{n}",
        ):
            try:
                conn.execute(stmt)
                counts["churn_ddl"] += 1
            except psycopg.Error as e:
                counts[f"churn_error {e.sqlstate}"] += 1
            time.sleep(pause)
    conn.close()


def churn_relevant(pport, stop, counts):
    conn = psycopg.connect(f"host=/tmp port={pport} user=postgres dbname=app", autocommit=True)
    row = conn.execute("SELECT * FROM quicksilver.allowlist WHERE kind = 'aggregate' AND ident = 'count()'").fetchone()
    cols = [d.name for d in conn.execute("SELECT * FROM quicksilver.allowlist LIMIT 0").description]
    if row is None:
        counts["churn_error no allowlist row"] += 1
        conn.close()
        return
    while not stop.is_set():
        try:
            conn.execute("DELETE FROM quicksilver.allowlist WHERE kind = 'aggregate' AND ident = 'count()'")
            counts["churn_removed"] += 1
            time.sleep(0.05)
            conn.execute(sql.SQL("INSERT INTO quicksilver.allowlist ({}) VALUES ({})").format(
                sql.SQL(", ").join(sql.Identifier(c) for c in cols),
                sql.SQL(", ").join(sql.Placeholder() for _ in cols)), row)
            counts["churn_restored"] += 1
            time.sleep(0.05)
        except psycopg.Error as e:
            counts[f"churn_error {e.sqlstate}"] += 1
    conn.close()


def reporter(sport, stop, counts, interval=0.025):
    """The stand-in the storm uses (storm_ddl.py): the replay position is reported as applied, so
    that the mirror is fresh for what the standby has replayed. Churn is what is measured here, not
    the sidecar's ingest, which would otherwise keep every read on the heap for freshness."""
    conn = psycopg.connect(f"host=/tmp port={sport} user=postgres dbname=app", autocommit=True)
    while not stop.is_set():
        try:
            conn.execute("SELECT quicksilver.report_applied_lsn('public.orders'::regclass, pg_last_wal_replay_lsn())")
            counts["reports"] += 1
        except psycopg.Error as e:
            counts[f"report error {e.sqlstate or '?'}"] += 1
        time.sleep(interval)
    conn.close()


def read_once(sport, counts):
    try:
        conn = psycopg.connect(f"host=/tmp port={sport} user=postgres dbname=app", autocommit=True)
    except psycopg.Error as e:
        counts[f"connect {e.sqlstate}"] += 1
        return
    try:
        conn.execute("SET quicksilver.route = mirror")
        got = conn.execute("SELECT count(*), sum(amount) FROM public.orders").fetchone()
        if tuple(got) != EXPECTED:
            counts["WRONG ANSWER"] += 1
        side, reason, detail = conn.execute(
            "SELECT side, reason, left(coalesce(detail, ''), 90) FROM quicksilver.last_decision()").fetchone()
        counts[f"{side}: {reason}" + (f" [{detail}]" if side == "heap" and detail else "")] += 1
    except psycopg.Error as e:
        counts[f"error {e.sqlstate or '?'}"] += 1
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pport", type=int, default=5600)
    ap.add_argument("--sport", type=int, default=5601)
    ap.add_argument("--iters", type=int, default=300)
    ap.add_argument("--churn", choices=["none", "unrelated", "relevant"], default="unrelated")
    ap.add_argument("--pause", type=float, default=0.0, help="seconds between churn statements (unrelated)")
    args = ap.parse_args()

    counts = collections.Counter()
    stop = threading.Event()
    churner = None
    if args.churn == "unrelated":
        churner = threading.Thread(target=churn_unrelated, args=(args.pport, stop, counts, args.pause))
    elif args.churn == "relevant":
        churner = threading.Thread(target=churn_relevant, args=(args.pport, stop, counts))
    rep = threading.Thread(target=reporter, args=(args.sport, stop, counts))
    rep.start()
    if churner:
        churner.start()
        time.sleep(0.5)
    try:
        for _ in range(args.iters):
            read_once(args.sport, counts)
    finally:
        stop.set()
        rep.join()
        if churner:
            churner.join()
    print(f"churn={args.churn} iterations={args.iters}")
    for k, v in sorted(counts.items()):
        print(f"  {k}: {v}")
    reg = sum(v for k, v in counts.items() if "registry changed" in k)
    print(f"registry changed while planning: {reg}")


if __name__ == "__main__":
    main()
