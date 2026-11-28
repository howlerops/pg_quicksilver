#!/usr/bin/env python3
"""Scenario A runner: heap vs routed on the standby, warm, best and median of five.

usage: bench_a.py OUT.json LABEL TZ [--standby 6611] [--primary 6610]
Each query: one warm-up run, then five timed runs per engine (heap = quicksilver.route heap,
routed = the database's setting, auto). The full result set of every timed run is hashed
(md5 over the sorted rows, as text) and compared with the heap answer of the same state.
"""
import argparse, hashlib, json, statistics, sys, time
import psycopg

ap = argparse.ArgumentParser()
ap.add_argument("out"); ap.add_argument("label"); ap.add_argument("tz")
ap.add_argument("--standby", type=int, default=6611)
ap.add_argument("--primary", type=int, default=6610)
ap.add_argument("--rounds", type=int, default=5)
ap.add_argument("--skip-timing", action="store_true")
args = ap.parse_args()

QUERIES = [
    ("headline_revenue_by_month_join",
     "SELECT date_trunc('month', o.created_at) AS month, sum(o.amount) AS revenue, count(*) AS orders "
     "FROM public.orders o JOIN public.customers c ON c.id = o.customer_id GROUP BY 1"),
    ("count_by_status", "SELECT status, count(*) FROM public.orders GROUP BY status"),
    ("sum_amount", "SELECT sum(amount) FROM public.orders"),
    ("count_star", "SELECT count(*) FROM public.orders"),
    ("one_month_filter",
     "SELECT count(*), sum(amount) FROM public.orders WHERE created_at >= timestamptz '2025-06-01 00:00:00+00' "
     "AND created_at < timestamptz '2025-07-01 00:00:00+00'"),
]


def canon(rows):
    lines = sorted("|".join("" if v is None else str(v) for v in r) for r in rows)
    return hashlib.md5("\n".join(lines).encode()).hexdigest(), len(lines)


def conn(port):
    c = psycopg.connect(f"host=/tmp port={port} dbname=app user=postgres", autocommit=True)
    c.execute(f"SET TimeZone = '{args.tz}'")
    return c


sb = conn(args.standby)
result = {"label": args.label, "tz": args.tz, "queries": {}}
for name, sql in QUERIES:
    entry = {"sql": sql}
    # answers first (untimed), heap and routed, full result sets compared
    sb.execute("SET quicksilver.route = 'heap'")
    heap_rows = sb.execute(sql).fetchall()
    h_md5, h_n = canon(heap_rows)
    sb.execute("SET quicksilver.route = 'auto'")
    sb.execute("SELECT quicksilver.forget_last()")
    routed_rows = sb.execute(sql).fetchall()
    decision = sb.execute("SELECT quicksilver.last_decision()").fetchone()[0]
    r_md5, r_n = canon(routed_rows)
    entry.update(rows=h_n, heap_md5=h_md5, routed_md5=r_md5, identical=(h_md5 == r_md5 and h_n == r_n),
                 routed_decision=str(decision)[:300])
    if not args.skip_timing:
        for engine in ("heap", "auto"):
            sb.execute(f"SET quicksilver.route = '{engine}'")
            sb.execute(sql).fetchall()  # warm-up
            times = []
            for _ in range(args.rounds):
                sb.execute("SELECT quicksilver.forget_last()")
                t0 = time.perf_counter()
                sb.execute(sql).fetchall()
                times.append((time.perf_counter() - t0) * 1000)
            entry[engine] = {"ms": [round(t, 2) for t in times], "best": round(min(times), 2),
                             "median": round(statistics.median(times), 2)}
        sb.execute("SET quicksilver.route = 'auto'")
    result["queries"][name] = entry
    print(f"{name}: rows={h_n} identical={entry['identical']} decision={str(decision)[:70]}"
          + (f" heap best/med={entry['heap']['best']}/{entry['heap']['median']} routed best/med={entry['auto']['best']}/{entry['auto']['median']}" if not args.skip_timing else ""), flush=True)

json.dump(result, open(args.out, "w"), indent=1)
print("wrote", args.out)
