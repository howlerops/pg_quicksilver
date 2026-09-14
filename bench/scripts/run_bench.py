#!/usr/bin/env python3
"""Spike S2 runner: row store (PostgreSQL) vs column store (DuckDB/Parquet).

Both engines run identical SQL against identical data on the same hardware.
Per bench rules in docs/08: report OLTP shapes alongside analytical ones,
and never report a ratio without saying what the baseline was.
"""
import argparse, json, re, statistics, sys, time
from pathlib import Path

import duckdb
import psycopg

PG_DSN = "host=/tmp port=5433 user=postgres dbname=postgres"
PARQUET = "/var/lib/postgresql/qsbench/parquet/events.parquet"


def load_queries(path: Path):
    """Parse `-- name: X` delimited statements out of a .sql file."""
    out, name, buf = [], None, []
    for line in path.read_text().splitlines():
        m = re.match(r"^--\s*name:\s*(\S+)", line)
        if m:
            if name:
                out.append((name, "\n".join(buf).strip()))
            name, buf = m.group(1), []
        elif name is not None and not line.strip().startswith("--"):
            buf.append(line)
    if name:
        out.append((name, "\n".join(buf).strip()))
    return [(n, q) for n, q in out if q]


def time_it(fn, warmup: int, runs: int):
    for _ in range(warmup):
        fn()
    samples = []
    for _ in range(runs):
        t = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - t) * 1000.0)
    samples.sort()
    return {
        "min_ms": round(samples[0], 3),
        "p50_ms": round(statistics.median(samples), 3),
        "p95_ms": round(samples[min(len(samples) - 1, int(0.95 * len(samples)))], 3),
        "runs": runs,
    }


def fmt_ratio(s):
    """Speedups span ~1e-3 to ~70, so render both directions readably."""
    if s == float("inf"):
        return "inf"
    return f"{s:.1f}x faster" if s >= 1 else f"{1/s:,.0f}x SLOWER"


def pg_buffers(cur, sql):
    """Shared blocks actually touched — the I/O the row store really does."""
    try:
        cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql)
        plan = json.dumps(cur.fetchone()[0])
        hit = sum(int(x) for x in re.findall(r'"Shared Hit Blocks": (\d+)', plan))
        read = sum(int(x) for x in re.findall(r'"Shared Read Blocks": (\d+)', plan))
        return (hit + read) * 8192
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warmup", type=int, default=2)
    ap.add_argument("--runs", type=int, default=7)
    ap.add_argument("--oltp-runs", type=int, default=50)
    ap.add_argument("--out", default="/home/user/pg_quicksilver/bench/results/s2_results.json")
    args = ap.parse_args()

    qdir = Path(__file__).resolve().parent.parent / "queries"
    suites = [("analytical", load_queries(qdir / "analytical.sql"), args.runs),
              ("oltp", load_queries(qdir / "oltp.sql"), args.oltp_runs)]

    pg = psycopg.connect(PG_DSN, autocommit=True)
    pgcur = pg.cursor()

    duck = duckdb.connect()
    duck.execute("SET memory_limit='8GB'; SET temp_directory='/var/lib/postgresql/qsbench/tmp';")
    duck.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet('{PARQUET}')")

    results = []
    for suite, queries, runs in suites:
        for name, sql in queries:
            sys.stderr.write(f"  {suite:<11} {name:<28} ")
            sys.stderr.flush()

            pg_stats = time_it(lambda: pgcur.execute(sql).fetchall(), args.warmup, runs)
            dd_stats = time_it(lambda: duck.execute(sql).fetchall(), args.warmup, runs)

            speedup = pg_stats["p50_ms"] / dd_stats["p50_ms"] if dd_stats["p50_ms"] else float("inf")
            row = {
                "suite": suite, "query": name,
                "pg": pg_stats, "duckdb": dd_stats,
                # full precision: OLTP speedups are ~1e-3 and round to 0.0
                "speedup_p50": speedup,
                "pg_bytes_read": pg_buffers(pgcur, sql),
            }
            results.append(row)
            sys.stderr.write(
                f"pg {pg_stats['p50_ms']:>9.1f}ms   duck {dd_stats['p50_ms']:>8.1f}ms   "
                f"{speedup:>7.1f}x\n")

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(results, indent=2))

    # ---- summary -------------------------------------------------------
    print("\n" + "=" * 78)
    print(f"{'suite':<11}{'query':<28}{'PG p50':>11}{'Duck p50':>11}{'speedup':>12}")
    print("=" * 78)
    for r in results:
        print(f"{r['suite']:<11}{r['query']:<28}{r['pg']['p50_ms']:>10.2f}ms"
              f"{r['duckdb']['p50_ms']:>10.2f}ms{fmt_ratio(r['speedup_p50']):>16}")
    for suite in ("analytical", "oltp"):
        sp = [r["speedup_p50"] for r in results if r["suite"] == suite]
        if sp:
            print(f"\n{suite}: median {fmt_ratio(statistics.median(sp))}  "
                  f"(range {fmt_ratio(min(sp))} .. {fmt_ratio(max(sp))})")
    print(f"\nwritten: {args.out}")


if __name__ == "__main__":
    main()
