#!/usr/bin/env python3
"""Spike S2b — does the S2 speedup survive a real Postgres front-end?

S2 measured standalone DuckDB, which bypasses the Postgres executor entirely.
The product requires queries to arrive over the Postgres wire protocol, be
planned by Postgres, and execute through pg_duckdb. This measures that.

Four configurations, same SQL, same data, same box:

  A  pg_native    PostgreSQL executor over the Postgres heap     (from S2)
  B  pgduck_heap  pg_duckdb (force_execution) over the same heap  <- vectorised exec, ROW storage
  C  pgduck_pq    pg_duckdb over the Parquet mirror via a         <- THE PRODUCT PATH
                  generated view                                     vectorised exec + COLUMN storage
  D  duck_native  standalone DuckDB over Parquet                  (from S2 — the upper bound)

B vs A isolates how much of the win is execution engine rather than storage.
C vs D is the extension-boundary tax — the number S2b exists to find.
"""
import argparse, json, re, statistics, sys, time
from pathlib import Path

import psycopg

DSN = "host=/tmp port=5433 user=postgres dbname=postgres"
PARQUET = "/var/lib/postgresql/qsbench/parquet/events.parquet"


def load_queries(path: Path):
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


def build_view(cur):
    """Generate the mirror view: pg_duckdb needs r['col'] syntax, which is NOT
    identical SQL. A generated view hides it so application SQL is unchanged —
    this is the mechanism Quicksilver would ship."""
    cur.execute("""SELECT column_name, format_type(a.atttypid, a.atttypmod)
                   FROM information_schema.columns ic
                   JOIN pg_attribute a ON a.attrelid='events'::regclass
                                      AND a.attname=ic.column_name
                   WHERE ic.table_name='events' ORDER BY ordinal_position""")
    cols = cur.fetchall()
    sel = ", ".join(f"r['{n}']::{t} AS {n}" for n, t in cols)
    cur.execute("DROP VIEW IF EXISTS events_pq")
    cur.execute(f"CREATE VIEW events_pq AS SELECT {sel} FROM read_parquet('{PARQUET}') r")
    return len(cols)


def uses_duckdb(cur, sql):
    """True if the plan actually executed in DuckDB rather than falling back."""
    try:
        cur.execute("EXPLAIN " + sql)
        return any("DuckDBScan" in (r[0] or "") for r in cur.fetchall())
    except Exception:
        return None


def timed(cur, sql, warmup, runs):
    for _ in range(warmup):
        cur.execute(sql).fetchall()
    s = []
    for _ in range(runs):
        t = time.perf_counter()
        cur.execute(sql).fetchall()
        s.append((time.perf_counter() - t) * 1000)
    s.sort()
    return {"min_ms": round(s[0], 3), "p50_ms": round(statistics.median(s), 3),
            "p95_ms": round(s[min(len(s) - 1, int(0.95 * len(s)))], 3)}


def backend_startup_cost():
    """Does every new connection pay a DuckDB instantiation tax? This decides
    whether a connection-pooled app sees the speedup at all."""
    q = "SELECT count(*) FROM events WHERE tenant_id = 7"
    cold = []
    for _ in range(5):
        c = psycopg.connect(DSN, autocommit=True)
        cur = c.cursor()
        cur.execute("SET duckdb.force_execution=true")
        t = time.perf_counter()
        cur.execute(q).fetchall()
        cold.append((time.perf_counter() - t) * 1000)
        c.close()
    c = psycopg.connect(DSN, autocommit=True)
    cur = c.cursor()
    cur.execute("SET duckdb.force_execution=true")
    cur.execute(q).fetchall()
    warm = []
    for _ in range(10):
        t = time.perf_counter()
        cur.execute(q).fetchall()
        warm.append((time.perf_counter() - t) * 1000)
    c.close()
    return statistics.median(cold), statistics.median(warm)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--oltp-runs", type=int, default=20)
    ap.add_argument("--out", default="results/s2b_results.json")
    args = ap.parse_args()

    qdir = Path(__file__).resolve().parent.parent / "queries"
    prior = {r["query"]: r for r in json.loads(Path("results/s2_results.json").read_text())}

    conn = psycopg.connect(DSN, autocommit=True)
    cur = conn.cursor()
    ncols = build_view(cur)
    sys.stderr.write(f"mirror view over {ncols} columns\n\n")

    results = []
    for suite, runs in (("analytical", args.runs), ("oltp", args.oltp_runs)):
        for name, sql in load_queries(qdir / f"{suite}.sql"):
            sys.stderr.write(f"  {name:<28} ")
            sys.stderr.flush()
            row = {"suite": suite, "query": name}

            # B — pg_duckdb over the Postgres heap
            cur.execute("SET duckdb.force_execution=true")
            row["B_pgduck_heap"] = timed(cur, sql, args.warmup, runs)
            row["B_used_duckdb"] = uses_duckdb(cur, sql)

            # C — pg_duckdb over the Parquet mirror (the product path)
            sql_pq = re.sub(r"\bFROM events\b", "FROM events_pq", sql)
            cur.execute("SET duckdb.force_execution=false")
            try:
                row["C_pgduck_parquet"] = timed(cur, sql_pq, args.warmup, runs)
                row["C_used_duckdb"] = uses_duckdb(cur, sql_pq)
                row["C_error"] = None
            except Exception as e:
                row["C_pgduck_parquet"] = None
                row["C_error"] = str(e).split("\n")[0][:110]

            p = prior.get(name, {})
            row["A_pg_native"] = p.get("pg")
            row["D_duck_native"] = p.get("duckdb")
            results.append(row)

            b = row["B_pgduck_heap"]["p50_ms"]
            c = row["C_pgduck_parquet"]["p50_ms"] if row["C_pgduck_parquet"] else None
            sys.stderr.write(f"B {b:>9.1f}ms   C {c if c is None else f'{c:>9.1f}ms'}"
                             f"   {'' if row.get('C_error') is None else 'ERR'}\n")

    cold, warm = backend_startup_cost()
    meta = {"cold_connection_ms": round(cold, 2), "warm_connection_ms": round(warm, 2)}

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps({"meta": meta, "results": results}, indent=2))

    # ---- report -------------------------------------------------------
    print("\n" + "=" * 96)
    print(f"{'query':<28}{'A pg':>11}{'B duck/heap':>13}{'C duck/pq':>12}"
          f"{'D duck raw':>12}{'C vs A':>10}{'C vs D':>9}")
    print("=" * 96)
    for r in results:
        A = (r["A_pg_native"] or {}).get("p50_ms")
        B = r["B_pgduck_heap"]["p50_ms"]
        C = r["C_pgduck_parquet"]["p50_ms"] if r["C_pgduck_parquet"] else None
        D = (r["D_duck_native"] or {}).get("p50_ms")
        ca = f"{A/C:.1f}x" if (A and C) else "—"
        cd = f"{C/D:.2f}x" if (C and D) else "—"
        cs = f"{C:>10.1f}ms" if C else f"{'ERR':>12}"
        print(f"{r['query']:<28}{A:>9.1f}ms{B:>11.1f}ms{cs}{D:>10.1f}ms{ca:>10}{cd:>9}")

    for suite in ("analytical", "oltp"):
        rs = [r for r in results if r["suite"] == suite and r["C_pgduck_parquet"]]
        if rs:
            ca = [(r["A_pg_native"]["p50_ms"] / r["C_pgduck_parquet"]["p50_ms"]) for r in rs]
            cd = [(r["C_pgduck_parquet"]["p50_ms"] / r["D_duck_native"]["p50_ms"]) for r in rs]
            print(f"\n{suite}: median C-vs-A {statistics.median(ca):.1f}x   "
                  f"median extension tax (C/D) {statistics.median(cd):.2f}x")

    fell_back = [r["query"] for r in results if r.get("C_used_duckdb") is False]
    print(f"\nfell back to Postgres executor on the mirror: {fell_back or 'none'}")
    print(f"connection cost: first query on a fresh backend {meta['cold_connection_ms']} ms, "
          f"subsequent {meta['warm_connection_ms']} ms")
    print(f"\nwritten: {args.out}")


if __name__ == "__main__":
    main()
