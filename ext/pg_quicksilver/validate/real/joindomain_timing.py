#!/usr/bin/env python3
"""Median client-side time of one join aggregate, three ways, on the standby (docs/40, "Join keys").

  heap            quicksilver.route = heap
  routed, no rewrite   quicksilver.route = mirror, quicksilver.join_domains = off
  routed, rewrite      quicksilver.route = mirror, quicksilver.join_domains = on

The three are interleaved, RUNS times each, every run a new plan (prepare_threshold None), and
the rows are fetched and counted by the client. Each routed run must answer the heap's rows;
the script checks that once per configuration, before timing.

  PORT (the standby), DB (default app), RUNS (default 7), STATEMENT (default below).
"""
import os, statistics, time
import psycopg

PORT = int(os.environ.get("PORT", "5771"))
RUNS = int(os.environ.get("RUNS", "7"))
STATEMENT = os.environ.get("STATEMENT",
    "SELECT c.tenant_id, count(*), sum(o.amount) FROM public.jo o "
    "JOIN public.jc c ON c.id = o.customer_id GROUP BY c.tenant_id")
CONFIGS = [
    ("heap", ["SET quicksilver.route = heap"]),
    ("routed, no rewrite", ["SET quicksilver.route = mirror", "SET quicksilver.join_domains = off"]),
    ("routed, rewrite", ["SET quicksilver.route = mirror", "SET quicksilver.join_domains = on"]),
]


def run(c, settings):
    for s in settings:
        c.execute(s)
    t0 = time.perf_counter()
    rows = c.execute(STATEMENT).fetchall()
    dt = (time.perf_counter() - t0) * 1000
    side = c.execute("SELECT side, reason FROM quicksilver.last_decision()").fetchone()
    return dt, rows, side


def main():
    c = psycopg.connect("host=/tmp port=%d user=postgres dbname=%s" % (PORT, os.environ.get("DB", "app")),
                        autocommit=True, prepare_threshold=None)
    c.execute("SET quicksilver.freshness_wait = '10s'")
    truth = run(c, CONFIGS[0][1])[1]
    times = {name: [] for name, _ in CONFIGS}
    for name, settings in CONFIGS[1:]:
        _, rows, side = run(c, settings)
        print("%-20s answer %s the heap's; decided: %s (%s)" % (
            name, "equal to" if rows == truth else "DIFFERENT from", side[0], side[1]), flush=True)
    for i in range(RUNS):
        for name, settings in CONFIGS:
            dt, rows, side = run(c, settings)
            if name != "heap" and rows != truth:
                raise SystemExit("%s differs from the heap in run %d" % (name, i))
            times[name].append(dt)
    for name, _ in CONFIGS:
        ts = sorted(times[name])
        print("%-20s median %9.1f ms   runs %s" % (name, statistics.median(ts),
              " ".join("%.1f" % t for t in ts)), flush=True)


if __name__ == "__main__":
    main()
