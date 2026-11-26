#!/usr/bin/env python3
"""Heap against routed, for the statements the extension does route.

Each statement is run REPS times with quicksilver.route = heap and REPS times
with quicksilver.route = auto on the same server, alternating; the median of
each is reported, and the extension is asked which side answered. One client,
client-side wall clock, results fetched.

    routed_timing.py [--port 5541] [--reps 9]
"""
import argparse, os, statistics, time
import psycopg

STATEMENTS = [
    ("count(*)", "SELECT count(*) FROM public.orders"),
    ("group by", "SELECT status, count(*), sum(amount) FROM public.orders GROUP BY status"),
    ("range aggregate", "SELECT sum(amount) FROM public.orders WHERE created_at >= '2024-01-03 00:00:00+00' AND created_at < '2024-01-05 00:00:00+00'"),
    ("join + group by", "SELECT c.region, sum(o.amount) FROM public.orders o JOIN public.customers c ON c.id = o.customer_id GROUP BY c.region"),
    ("10k-row result", "SELECT id, amount FROM public.orders WHERE amount > 990.00"),
    ("top-N", "SELECT id, amount FROM public.orders ORDER BY amount DESC, id LIMIT 10"),
    ("unselective parameter", "SELECT count(*), sum(amount) FROM public.orders WHERE customer_id = 1"),
    ("min/max", "SELECT min(id), max(id), min(amount), max(amount), min(created_at), max(created_at) FROM public.orders"),
    ("pk lookup (stays on the heap)", "SELECT * FROM public.orders WHERE id = 4242"),
    ("selective parameter (stays)", "SELECT count(*), sum(amount) FROM public.orders WHERE customer_id = 5"),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=int(os.environ.get("PPORT", 5541)))
    ap.add_argument("--reps", type=int, default=9)
    args = ap.parse_args()
    dsn = f"host=/tmp port={args.port} dbname=app user=postgres"
    heap = psycopg.connect(dsn, autocommit=True)
    auto = psycopg.connect(dsn, autocommit=True)
    heap.execute("SET quicksilver.route = heap")
    auto.execute("SET quicksilver.route = auto")
    print("| statement | side | heap ms | routed ms | ratio |")
    print("|---|---|---|---|---|")
    for name, sql in STATEMENTS:
        th, ta = [], []
        for c in (heap, auto):
            c.execute(sql).fetchall()       # warm both sides
        for _ in range(args.reps):
            for c, acc in ((heap, th), (auto, ta)):
                t0 = time.perf_counter()
                c.execute(sql).fetchall()
                acc.append((time.perf_counter() - t0) * 1000)
        side = auto.execute("SELECT side FROM quicksilver.last_decision()").fetchone()[0]
        h, a = statistics.median(th), statistics.median(ta)
        fmt = (lambda x: f"{x:.2f}" if x < 10 else f"{x:.0f}")
        print(f"| {name}: `{sql if len(sql) < 70 else sql[:67] + '...'}` | {side} | {fmt(h)} | {fmt(a)} | {h / a:.1f}x |")


if __name__ == "__main__":
    main()
