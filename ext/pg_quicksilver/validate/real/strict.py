#!/usr/bin/env python3
"""How often is a qualifying statement routed under strict freshness?

quicksilver.max_lag = 0 (the default) routes a statement only if the mirror
has reached everything the statement can see on this node when it starts,
waiting up to quicksilver.freshness_wait for it. This measures what that costs
in routing, on the standby, while the primary is written to:

    load       a steady stream of small transactions on the primary, either on
               a table that is NOT mirrored or on the mirrored table itself
    statement  SELECT count(*), sum(amount) FROM public.orders, every 20 ms
    settings   strict with freshness_wait 0, 20 ms (default), 200 ms;
               and an explicit max_lag of 1MB

For each cell: the fraction answered by the mirror, the median and p95 latency
of the routed statements, and of the ones the heap answered. The wait is the
difference between a strict cell's routed latency and the max_lag cell's.

    strict.py [--seconds 15] [--tps 200]
"""
import argparse, os, statistics, threading, time
import psycopg

PPORT, SPORT = int(os.environ.get("PPORT", 5580)), int(os.environ.get("SPORT", 5581))
Q = "SELECT count(*), sum(amount) FROM public.orders"


def conn(port):
    return psycopg.connect(f"host=/tmp port={port} dbname=app user=postgres", autocommit=True, prepare_threshold=None)


def writer(stop, kind, tps, counter):
    c = conn(PPORT)
    i, period = 0, 1.0 / tps
    nxt = time.time()
    while not stop.is_set():
        i += 1
        if kind == "unmirrored":
            c.execute("INSERT INTO public.audit_log(what) VALUES (%s)", ("strict-%d" % i,))
        else:
            c.execute("UPDATE public.orders SET amount = amount + 0.01 WHERE id = %s", (1 + i % 100000,))
        counter[0] += 1
        nxt += period
        d = nxt - time.time()
        if d > 0:
            time.sleep(d)
    c.close()


def cell(settings, seconds):
    c = conn(SPORT)
    for s in settings:
        c.execute(s)
    routed, heap, why = [], [], {}
    t_end = time.time() + seconds
    while time.time() < t_end:
        c.execute("SELECT quicksilver.forget_last()")
        t0 = time.perf_counter()
        c.execute(Q).fetchall()
        ms = (time.perf_counter() - t0) * 1000
        d = c.execute("SELECT side, reason FROM quicksilver.last_decision()").fetchone()
        ex = c.execute("SELECT 1").fetchone()
        if d and d[0] == "mirror":
            routed.append(ms)
        else:
            heap.append(ms)
        time.sleep(0.02)
    c.close()
    return routed, heap


def routed_or_fell_back(c):
    """EXPLAIN-free: a routed plan that fell back at execution reports it in reason_stats."""
    return dict((r[1], r[2]) for r in c.execute("SELECT * FROM quicksilver.reason_stats").fetchall())


def pct(v, p):
    if not v:
        return float("nan")
    v = sorted(v)
    return v[min(len(v) - 1, int(len(v) * p))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=15)
    ap.add_argument("--tps", type=int, default=200)
    a = ap.parse_args()
    cells = [
        ("strict, freshness_wait 0", ["SET quicksilver.max_lag = 0", "SET quicksilver.freshness_wait = 0"]),
        ("strict, freshness_wait 20ms (default)", ["SET quicksilver.max_lag = 0", "SET quicksilver.freshness_wait = '20ms'"]),
        ("strict, freshness_wait 200ms", ["SET quicksilver.max_lag = 0", "SET quicksilver.freshness_wait = '200ms'"]),
        ("max_lag 1MB (opt-in)", ["SET quicksilver.max_lag = '1MB'", "SET quicksilver.freshness_wait = 0"]),
    ]
    s = conn(SPORT)
    for load in ("none", "unmirrored", "mirrored"):
        stop, counter = threading.Event(), [0]
        th = None
        if load != "none":
            th = threading.Thread(target=writer, args=(stop, load, a.tps, counter))
            th.start()
            time.sleep(1)
        print(f"load on the primary: {load}" + ("" if load == "none" else f" table, {a.tps} transactions/s asked for"))
        for name, settings in cells:
            n0 = counter[0]
            before = routed_or_fell_back(s)
            t0 = time.time()
            routed, heap = cell(settings, a.seconds)
            after = routed_or_fell_back(s)
            # last_decision() is the heap's when a routed plan found its mirror stale as it started
            n = len(routed) + len(heap)
            real = len(routed)
            tps = (counter[0] - n0) / (time.time() - t0)
            print("  %-38s routed %4d of %4d (%5.1f%%)  median ms: routed %5.1f, heap %5.1f, all %5.1f; p95 all %5.1f  [writes %.0f/s]" % (
                name, real, n, 100.0 * real / max(n, 1), pct(routed, 0.5), pct(heap, 0.5), pct(routed + heap, 0.5), pct(routed + heap, 0.95), tps))
        if th:
            stop.set(); th.join()
        time.sleep(2)


if __name__ == "__main__":
    main()
