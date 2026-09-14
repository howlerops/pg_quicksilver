#!/usr/bin/env python3
"""Measure the `C` term from docs/10 — concurrency efficiency.

docs/10 models replica consolidation as depending on speedup S *and* on how
well one columnar node absorbs concurrent load relative to a PG replica.
docs/09 OQ-13 flags this as the least-known number in the business case.

Method: run a fixed analytical query from N concurrent clients for a fixed
duration; report completed queries/sec. A column store that is 40x faster
single-threaded but scales badly under concurrency does NOT replace 40
replicas' worth of work.
"""
import argparse, statistics, sys, threading, time

import duckdb
import psycopg

PG_DSN = "host=/tmp port=5433 user=postgres dbname=postgres"
PARQUET = "/var/lib/postgresql/qsbench/parquet/events.parquet"

# Mid-weight analytical query — representative dashboard load.
QUERY = """
SELECT country, device, count(*) AS n, sum(amount) AS revenue
FROM events
WHERE ts >= TIMESTAMP '2026-05-01' AND status = 'ok'
GROUP BY 1,2 ORDER BY revenue DESC LIMIT 20
"""


def run_pool(make_exec, clients, duration):
    """Spawn `clients` threads hammering the query; return (qps, latencies)."""
    stop = time.time() + duration
    counts, lats, lock = [0] * clients, [], threading.Lock()

    def worker(i):
        ex = make_exec()
        local = []
        n = 0
        while time.time() < stop:
            t = time.perf_counter()
            ex(QUERY)
            local.append((time.perf_counter() - t) * 1000)
            n += 1
        counts[i] = n
        with lock:
            lats.extend(local)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(clients)]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - t0
    return sum(counts) / elapsed, lats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--levels", default="1,2,4,8,16,32")
    ap.add_argument("--duration", type=float, default=12.0)
    args = ap.parse_args()

    # One shared DuckDB database; each thread gets its own cursor, which is
    # how pg_duckdb-style embedded serving would actually work.
    duck = duckdb.connect()
    duck.execute("SET memory_limit='8GB'; SET temp_directory='/var/lib/postgresql/qsbench/tmp';")
    duck.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet('{PARQUET}')")

    def pg_exec():
        conn = psycopg.connect(PG_DSN, autocommit=True)
        cur = conn.cursor()
        return lambda q: cur.execute(q).fetchall()

    def duck_exec():
        cur = duck.cursor()
        return lambda q: cur.execute(q).fetchall()

    print(f"{'clients':>8}{'PG qps':>11}{'Duck qps':>11}{'ratio':>10}"
          f"{'PG p95ms':>11}{'Duck p95ms':>12}")
    print("-" * 63)
    rows = []
    for c in [int(x) for x in args.levels.split(",")]:
        pg_qps, pg_l = run_pool(pg_exec, c, args.duration)
        dd_qps, dd_l = run_pool(duck_exec, c, args.duration)
        p95 = lambda L: sorted(L)[min(len(L) - 1, int(0.95 * len(L)))]
        rows.append((c, pg_qps, dd_qps))
        print(f"{c:>8}{pg_qps:>11.1f}{dd_qps:>11.1f}{dd_qps/pg_qps:>9.1f}x"
              f"{p95(pg_l):>10.0f}ms{p95(dd_l):>11.0f}ms")
        sys.stdout.flush()

    print("\nThroughput ratio is the `C`-adjusted speedup that actually drives")
    print("replica consolidation in docs/10 — not the single-query number.")


if __name__ == "__main__":
    main()
