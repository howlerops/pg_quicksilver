#!/usr/bin/env python3
"""Does a deletion vector cost the same on every shape?

docs/29 measured the deletion-vector penalty on ONE shape — narrow, 40-byte
rows — and found something with a direct consequence for compaction policy:

    dead positions      count(*)
            0             1.9 ms   <- answered from the Parquet footer
        1,000            25.0 ms
    1,563,904           146.8 ms

Most of the cost is that a vector EXISTS, not how large it is. If that holds
generally, then trimming dead rows buys almost nothing and the only lever worth
pulling is how often a base file is allowed to carry any vector at all — which
is a statement about compaction FREQUENCY, not about a dead-row ceiling.

That conclusion rests on one shape, and docs/29 says so. This checks the others,
because the reasoning has an obvious way to be wrong: the penalty competes with
whatever else the query is doing. On a 40-byte row a scan is cheap and the
anti-join dominates. On a 6 KB jsonb document the scan should dominate instead,
and the same vector would be a rounding error. If that is what happens, the
narrow shape is the worst case and a policy tuned for it is tuned for the right
end.

    python3 bench/scripts/dv_cost_by_shape.py

Reads only. Needs no source database — it measures the files a previous run
left behind, so it is cheap and repeatable.
"""

import glob
import json
import os
import statistics
import sys
import time

try:
    import duckdb
except ImportError:
    sys.exit("duckdb is required: pip install duckdb")

BASE = os.environ.get("QS_BASE", "/var/lib/postgresql/qs17")
SHAPES = ["mx-narrow", "mx-wide", "mx-jsonb", "large-mirror"]


def dv_path(root, stem, gen):
    """One generation of one vector, in whichever encoding it is in."""
    for ext in (".parquet", ".json"):
        p = os.path.join(root, "dv", f"{stem}.{gen:09d}.dv{ext}")
        if os.path.exists(p):
            return p
    return None


def dv_subquery(path):
    if path.endswith(".json"):
        return (f"(SELECT unnest(v) FROM read_json('{path}', "
                f"columns = {{'v': 'BIGINT[]'}}, format = 'unstructured'))")
    return f"(SELECT p FROM read_parquet('{path}'))"


def timed(con, sql, n=5):
    """Median of n, after a warm-up. The first evaluation of anything in a
    fresh connection pays DuckDB's own initialisation, which docs/27 recorded
    as a 14.6x phantom when it landed on the first measured query."""
    con.execute(sql).fetchall()
    ts = []
    for _ in range(n):
        s = time.perf_counter()
        con.execute(sql).fetchall()
        ts.append((time.perf_counter() - s) * 1000)
    return statistics.median(ts)


def base_file(tdir, st):
    for b in st.get("base_files") or []:
        p = os.path.join(tdir, "base", b)
        if os.path.exists(p):
            return p
    return None


def main():
    """One file per shape, SYNTHETIC vectors of varying size.

    The first version of this compared whichever file already had the biggest
    vector on each shape, and that comparison was worthless: the narrow, wide
    and jsonb mirrors had 11, 60 and 2 dead rows respectively, because
    compaction had folded their real vectors away. Three measurements of the
    anti-join's fixed floor, being read as a statement about row width.

    Same file, varying dead fraction, per shape is the comparison docs/29
    actually made, and it is the only one that separates "wide rows dilute the
    penalty" from "this file has no dead rows in it".
    """
    con = duckdb.connect()
    con.execute("SELECT 1").fetchall()
    scratch = os.environ.get("QS_SCRATCH", "/tmp/qs-dv-shape")
    os.makedirs(scratch, exist_ok=True)

    for shape in SHAPES:
        root = os.path.join(BASE, shape)
        tables = glob.glob(os.path.join(root, "*.*/"))
        if not tables:
            continue
        tdir = tables[0].rstrip("/")
        try:
            st = json.load(open(os.path.join(tdir, "state.json")))
        except OSError:
            continue
        path = base_file(tdir, st)
        if not path:
            continue

        nrows = con.execute(
            f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]
        if nrows < 1000:
            continue
        size_mb = os.path.getsize(path) / 1e6
        bytes_per_row = os.path.getsize(path) / nrows

        # The denominator: a query that has to touch a real column, so the
        # comparison is against work the shape actually implies rather than
        # against a footer read.
        cols = [c[0] for c in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()]
        wide_col = cols[-1]
        scan = timed(con, f'SELECT count("{wide_col}") FROM read_parquet(\'{path}\')')
        footer = timed(con, f"SELECT count(*) FROM read_parquet('{path}')")

        print(f"\n  {shape}: {nrows:,} rows, {size_mb:.0f} MB on disk, "
              f"{bytes_per_row:.0f} B/row")
        print(f"    a column scan costs {scan:.1f} ms; the footer answers in {footer:.1f} ms")
        print(f"    {'dead':>12}{'count(*)':>12}{'penalty':>11}{'vs a scan':>12}")

        for frac in (0.0, 0.001, 0.01, 0.10):
            n = int(nrows * frac)
            if n == 0:
                print(f"    {'0 (no vector)':>12}{footer:>11.1f}m{'—':>11}{'—':>12}")
                continue
            f = os.path.join(scratch, f"{shape}.{n}.parquet")
            if not os.path.exists(f):
                con.execute(
                    f"COPY (SELECT i AS p FROM range(0, {nrows}, "
                    f"{max(1, nrows // n)}) t(i) LIMIT {n}) "
                    f"TO '{f}' (FORMAT parquet, COMPRESSION zstd)")
            t = timed(con,
                      f"SELECT count(*) FROM read_parquet('{path}', file_row_number = true) "
                      f"WHERE file_row_number NOT IN (SELECT p FROM read_parquet('{f}'))")
            print(f"    {n:>12,}{t:>11.1f}m{t-footer:>10.1f}m{(t-footer)/scan:>11.1f}x")

    print("""
  WHAT THIS DECIDES. If the penalty relative to a column scan stays large on
  every shape, compaction frequency is a general problem and the trigger should
  change. If it shrinks as rows get wider — because the scan the vector competes
  with gets more expensive — then the narrow shape is the worst case, docs/29's
  numbers are the worst case, and a policy tuned to them is tuned correctly
  rather than over-tuned.
""")


if __name__ == "__main__":
    main()
