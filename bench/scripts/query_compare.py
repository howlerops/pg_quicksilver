#!/usr/bin/env python3
"""Run the same analytical queries against PostgreSQL and against the mirror.

The mirror is read exactly as a serving node would read it: the live Parquet
files, minus the deletion vectors. Nothing is pre-loaded into DuckDB and nothing
is cached between the two sides beyond what each engine does on its own.

Two honesty constraints, both learned the hard way in this project:

  * Every query is checked for AGREEMENT first. A faster engine that returns a
    different answer is not faster, it is wrong, and a speedup table that does
    not check would happily report the wrong number as a win.
  * Both sides run the same number of times and the MEDIAN is reported. The
    first run of anything is a cache measurement.
"""

import argparse
import json
import os
import statistics
import sys
import time

import duckdb
import psycopg


QUERIES = [
    ("A1 count", "SELECT count(*) FROM {t}"),
    ("A2 sum+avg", "SELECT sum(amount)::numeric(20,2), round(avg(amount),4) FROM {t}"),
    # ORDER BY must be total. With 100k distinct skus over 5M rows the sums tie,
    # and two engines break ties differently — which the first run of this
    # script reported as "RESULTS DIFFER", i.e. as a mirror bug. It was a
    # benchmark bug. A comparison that cannot distinguish the two is worthless.
    ("A3 group by sku", """SELECT sku, count(*) c, sum(amount)::numeric(20,2) s
                             FROM {t} GROUP BY sku ORDER BY s DESC, sku LIMIT 10"""),
    ("A4 filtered agg", """SELECT count(*), sum(amount)::numeric(20,2)
                             FROM {t} WHERE amount > 50"""),
    ("A5 distinct", "SELECT count(DISTINCT sku) FROM {t}"),
    # width_bucket exists in PostgreSQL and not in DuckDB; expressed with
    # arithmetic it runs identically on both and measures the same work.
    ("A6 bucketed", """SELECT floor(amount / 15) b, count(*) c
                         FROM {t} GROUP BY b ORDER BY b"""),
]

# The OLTP shape, for the comparison that matters to the product decision.
OLTP = [
    ("O1 pk lookup", "SELECT sku, amount FROM {t} WHERE id = 123456"),
    ("O2 small range", "SELECT count(*) FROM {t} WHERE id BETWEEN 500000 AND 500100"),
    ("O3 limit 20", "SELECT id, sku FROM {t} ORDER BY id LIMIT 20"),
]


REPEATS = 5


def live_files(mirror_dir):
    """Every live parquet file with its deletion vector.

    Base and delta files are treated identically. They used not to be — a
    superseded row inside a delta was removed by rewriting the file — but
    rewriting every delta on every batch is O(mirror) work per batch, so deltas
    now carry deletion vectors like everything else. A reader that skips a
    delta's vector resurrects every superseded row in it.

    A deletion vector is NAMED BY GENERATION — dv/000001.000000007.dv.parquet —
    and this looked for dv/000001.dv.json, a name generations made obsolete. So
    it never found one, which is not an error anywhere: a vector that cannot be
    found reads as "nothing in this file is dead", and every superseded row in
    the mirror came back. The numbers this script printed were measured over a
    view that was quietly wrong.

    That is why the lookup below asks the manifest which generation to read and
    RAISES when it is not there, rather than falling back to an empty set. The
    only honest default for "I could not find the deletion vector" is to stop.
    """
    with open(os.path.join(mirror_dir, "state.json")) as f:
        state = json.load(f)
    gens = state.get("dv_gen") or {}
    files = []
    for sub, key in (("base", "base_files"), ("delta", "delta_files")):
        for n in state.get(key) or []:
            path = os.path.join(mirror_dir, sub, n)
            stem = n.replace(".parquet", "")
            dead = set()
            gen = gens.get(stem, 0)
            if gen:
                dead = read_dv(mirror_dir, stem, gen)
            files.append((sub, path, dead))
    return files


def read_dv(mirror_dir, stem, gen):
    """One generation of one deletion vector, in whichever encoding it is in.

    Vectors are Parquet now — parsing 13 MB of JSON positions was over a third
    of the cost of every query against a file with deletes — but a mirror
    written before that still has .dv.json on disk and both must be read.
    """
    base = os.path.join(mirror_dir, "dv", f"{stem}.{gen:09d}.dv")
    if os.path.exists(base + ".parquet"):
        import duckdb

        return {
            r[0]
            for r in duckdb.connect()
            .execute(f"SELECT p FROM read_parquet('{base}.parquet')")
            .fetchall()
        }
    if os.path.exists(base + ".json"):
        with open(base + ".json") as f:
            return set(json.load(f))
    raise SystemExit(
        f"the manifest says {stem} is at deletion-vector generation {gen} and "
        f"neither {base}.parquet nor {base}.json exists. Measuring anyway would "
        f"resurrect every row that vector retired and report the result as a "
        f"correct view."
    )


def build_view(con, mirror_dir):
    """A view over the live rows. This is the read path a serving node would
    use, not a pre-loaded table — loading first would measure DuckDB's loader
    rather than the design."""
    parts, nbase, ndelta = [], 0, 0
    for sub, path, dead in live_files(mirror_dir):
        nbase += sub == "base"
        ndelta += sub == "delta"
        if dead:
            # file_row_number, not row_number() OVER (). A deletion vector holds
            # PHYSICAL positions in the file, and an unordered window function
            # over a parallel scan is not obliged to produce them — it produces
            # whatever order the scan happened to emit, which on one thread
            # looks exactly right and on eight retires arbitrary rows.
            rows = ",".join(str(i) for i in sorted(dead))
            parts.append(
                f"SELECT * EXCLUDE (file_row_number) FROM "
                f"read_parquet('{path}', file_row_number = true) "
                f"WHERE file_row_number NOT IN ({rows})"
            )
        else:
            parts.append(f"SELECT * FROM read_parquet('{path}')")
    if not parts:
        raise SystemExit("mirror has no parquet files")
    con.execute("CREATE OR REPLACE VIEW mirror AS " + " UNION ALL ".join(parts))
    return nbase, ndelta


def norm(rows):
    """Compare values as text, since the two engines return different Python
    types for the same numeric."""
    out = []
    for r in rows:
        out.append(tuple("" if v is None else str(v).rstrip("0").rstrip(".")
                         if isinstance(v, float) or "." in str(v) else str(v)
                         for v in r))
    return out


def timeit(fn):
    samples = []
    result = None
    for _ in range(REPEATS):
        t0 = time.perf_counter()
        result = fn()
        samples.append(time.perf_counter() - t0)
    return statistics.median(samples) * 1000, result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--mirror", required=True)
    args = ap.parse_args()

    con = duckdb.connect()
    nbase, ndelta = build_view(con, args.mirror)
    pg = psycopg.connect(args.dsn)
    pg.autocommit = True

    mrows = con.execute("SELECT count(*) FROM mirror").fetchone()[0]
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM events")
        prows = cur.fetchone()[0]
    print(f"  mirror: {nbase} base + {ndelta} delta files, {mrows:,} live rows")
    print(f"  source: {prows:,} rows")
    if mrows != prows:
        print(f"  FAIL: mirror has {mrows} rows, source has {prows} — not comparable")
        return 1

    fail = 0
    for title, block in (("analytical", QUERIES), ("OLTP-shaped", OLTP)):
        print(f"\n  {title}")
        print(f"  {'query':<18} {'postgres':>11} {'mirror':>11} {'speedup':>10}   agree")
        ratios = []
        for name, sql in block:
            def run_pg():
                with pg.cursor() as cur:
                    cur.execute(sql.format(t="events"))
                    return cur.fetchall()

            def run_mirror():
                return con.execute(sql.format(t="mirror")).fetchall()

            try:
                pg_ms, pg_rows = timeit(run_pg)
                mi_ms, mi_rows = timeit(run_mirror)
            except Exception as e:  # noqa: BLE001
                print(f"  {name:<18} ERROR {e}")
                fail = 1
                continue

            agree = norm(pg_rows) == norm(mi_rows)
            if not agree:
                fail = 1
            ratio = pg_ms / mi_ms if mi_ms > 0 else float("inf")
            ratios.append(ratio)
            arrow = f"{ratio:.1f}x" if ratio >= 1 else f"{1/ratio:.1f}x slower"
            print(f"  {name:<18} {pg_ms:>9.2f}ms {mi_ms:>9.2f}ms {arrow:>10}   "
                  f"{'yes' if agree else 'NO — RESULTS DIFFER'}")
        if ratios:
            print(f"  {'median':<18} {'':>11} {'':>11} "
                  f"{statistics.median(ratios):>9.1f}x")

    if fail:
        print("\n  FAIL: at least one query disagreed between the two engines")
    return fail


if __name__ == "__main__":
    sys.exit(main())
