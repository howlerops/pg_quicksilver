#!/usr/bin/env python3
"""What the freshness wait buys, and what it costs, while the mirrored table is written.

    python3 validate/real/waitcost.py [--pport 5580] [--sport 5581] [--rates 10,100,1000]

A writer commits single-row UPDATEs of public.orders on the primary at a
steady rate. On the standby, where freshness is strict (quicksilver.max_lag =
0), three statements whose heap plans take about 50 ms, 500 ms and seconds are
run again and again under three settings:

    heap       quicksilver.route = heap: what the statement costs with no router
    fixed      the old default: wait 20 ms whatever the statement costs
    cost       wait up to freshness_wait_fraction (0.5, and 1.0) of the heap
               plan's estimated time, capped at freshness_wait_max

For each: how many were answered by the mirror, and the latency the client
saw (median, p95), which for a statement that waited and was then answered by
the heap includes the wait. Needs the bench dataset (DATASET=bench in
validate/real/setup.sh): the statements are over its orders table.
"""
import argparse
import random
import statistics
import threading
import time

import psycopg

ap = argparse.ArgumentParser()
ap.add_argument("--pport", type=int, default=5580)
ap.add_argument("--sport", type=int, default=5581)
ap.add_argument("--host", default="/tmp")
ap.add_argument("--rates", default="10,100,1000")
ap.add_argument("--scale", type=float, default=1.0, help="multiply the sample counts")
args = ap.parse_args()
P = f"host={args.host} port={args.pport} dbname=app user=postgres"
S = f"host={args.host} port={args.sport} dbname=app user=postgres"

CLASSES = [
    ("~50 ms", "SELECT count(*), sum(amount) FROM public.orders WHERE created_at >= timestamptz '2025-06-01 00:00:00+00' "
               "AND created_at < timestamptz '2025-06-13 00:00:00+00'", 60, 2),
    ("~500 ms", "SELECT count(*) FROM public.orders", 30, 2),
    # one process, as a busy server would give it: the heap takes seconds
    ("seconds", "SELECT status, count(*), sum(amount), min(amount), max(amount), min(created_at), max(created_at) "
                "FROM public.orders GROUP BY status", 10, 0),
]
CONFIGS = [
    ("heap", ["SET quicksilver.route = heap"]),
    ("fixed 20 ms (old default)", ["SET quicksilver.route = auto", "SET quicksilver.freshness_wait = '20ms'",
                                   "SET quicksilver.freshness_wait_fraction = 0"]),
    ("half the estimate", ["SET quicksilver.route = auto", "RESET quicksilver.freshness_wait",
                           "SET quicksilver.freshness_wait_fraction = 0.5"]),
    ("the estimate", ["SET quicksilver.route = auto", "RESET quicksilver.freshness_wait",
                      "SET quicksilver.freshness_wait_fraction = 1.0"]),
]


class Writer(threading.Thread):
    def __init__(self, rate, ids):
        super().__init__(daemon=True)
        self.rate, self.ids, self.stop, self.n = rate, ids, False, 0

    def run(self):
        conn = psycopg.connect(P, autocommit=True)
        conn.execute("SET synchronous_commit = off")
        t0 = time.monotonic()
        while not self.stop:
            conn.execute("UPDATE public.orders SET note = %s WHERE id = %s", (f"w{self.n}", random.choice(self.ids)), prepare=True)
            self.n += 1
            due = t0 + self.n / self.rate
            d = due - time.monotonic()
            if d > 0:
                time.sleep(d)
        self.elapsed = time.monotonic() - t0
        conn.close()


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p * (len(xs) - 1))))]


primary = psycopg.connect(P, autocommit=True)
ids = [r[0] for r in primary.execute("SELECT id FROM public.orders TABLESAMPLE SYSTEM (0.05) LIMIT 2000")]
rd = psycopg.connect(S, autocommit=True)
print("settings on the standby: " + ", ".join(
    f"{n.split('.')[1]}={rd.execute('SELECT current_setting(%s)', (n,)).fetchone()[0]}"
    for n in ("quicksilver.max_lag", "quicksilver.freshness_wait", "quicksilver.freshness_wait_fraction",
              "quicksilver.freshness_wait_max", "quicksilver.cost_unit_ms")))
for name, sql, _, workers in CLASSES:
    rd.execute(f"SET max_parallel_workers_per_gather = {workers}")
    plan = rd.execute("EXPLAIN (FORMAT JSON) " + sql).fetchone()[0][0]["Plan"]
    cost = plan["Total Cost"]
    unit = float(rd.execute("SHOW quicksilver.cost_unit_ms").fetchone()[0])
    frac = float(rd.execute("SHOW quicksilver.freshness_wait_fraction").fetchone()[0])
    cap = rd.execute("SELECT setting::int FROM pg_settings WHERE name = 'quicksilver.freshness_wait_max'").fetchone()[0]
    print(f"  {name:8} heap plan cost {cost:>10.0f}: estimated {cost * unit:6.0f} ms on the heap; waits at most "
          f"{min(cap, cost * unit * 0.5):.0f} ms at fraction 0.5, {min(cap, cost * unit):.0f} ms at 1.0")

print(f"\n{'writes/s':>9} {'heap class':>10}  {'setting':32} {'routed':>10} {'median ms':>10} {'p95 ms':>9}")
for rate in [int(x) for x in args.rates.split(",")]:
    w = Writer(rate, ids)
    w.start()
    time.sleep(3)
    for name, sql, n, workers in CLASSES:
        n = max(4, int(n * args.scale))
        rd.execute(f"SET max_parallel_workers_per_gather = {workers}")
        for cfg, sets in CONFIGS:
            for s in sets:
                rd.execute(s)
            lat, routed, why = [], 0, {}
            for _ in range(n):
                time.sleep(random.uniform(0.0, 0.15))
                t = time.monotonic()
                rd.execute(sql).fetchall()
                lat.append((time.monotonic() - t) * 1000)
                side, reason = rd.execute("SELECT side, reason FROM quicksilver.last_decision()").fetchone()
                routed += side == "mirror"
                if side != "mirror":
                    why[reason] = why.get(reason, 0) + 1
            print(f"{rate:>9} {name:>10}  {cfg:32} {routed:>4} of {n:<3} {statistics.median(lat):>10.1f} {pct(lat, 0.95):>9.1f}"
                  + ("" if cfg == "heap" or not why else "   heap because: " + ", ".join(f"{k} ({v})" for k, v in why.items())), flush=True)
    w.stop = True
    w.join()
    print(f"{'':>9} (the writer committed {w.n} rows in {w.elapsed:.0f} s: {w.n / w.elapsed:.0f}/s)", flush=True)
