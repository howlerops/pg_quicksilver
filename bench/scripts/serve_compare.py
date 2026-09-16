#!/usr/bin/env python3
"""Ask the question the project exists to answer: is reading the mirror faster
than reading PostgreSQL, and does it give the same answers?

Every number in docs/19 is the write path. This is the other side. It runs the
same analytical queries against the source and against the mirror -- through
DuckDB, over the SELECT that qs-query generates from the manifest -- and reports
both the timings and whether the two agreed.

CORRECTNESS FIRST. A faster answer that differs from PostgreSQL is not a result,
and the mirror's layout gives three separate ways to be silently wrong about it
(retired rows, partial deltas, a stale manifest), so every query here is checked
value by value before its timing is believed.

    python3 bench/scripts/serve_compare.py --shape jsonb
    python3 bench/scripts/serve_compare.py --shape narrow --repeat 5
"""

import argparse
import datetime
import re
import statistics
import subprocess
import sys
import time

try:
    import duckdb
except ImportError:
    sys.exit("duckdb is required: pip install duckdb")
try:
    import psycopg
    CONNECT = psycopg.connect
except ImportError:
    try:
        import psycopg2
        CONNECT = psycopg2.connect
    except ImportError:
        sys.exit("psycopg or psycopg2 is required")


# The queries are the ones a column store is supposed to win: they touch a few
# columns out of many and aggregate over the whole table. A SELECT of one row by
# primary key is deliberately included as the case it should LOSE -- reporting
# only the wins would make this a sales sheet rather than a measurement.
#
# Every ORDER BY here names its NULL placement explicitly, because the two
# engines disagree about the default and the disagreement is silent. See
# check_null_ordering below: this benchmark found it by failing.
QUERIES = {
    "narrow": [
        ("count(*)", "SELECT count(*) FROM {t}"),
        ("sum one column", "SELECT sum(amount) FROM {t}"),
        ("group by sku top 10",
         "SELECT sku, count(*) c, sum(amount) s FROM {t} GROUP BY sku ORDER BY s DESC NULLS LAST, sku NULLS LAST LIMIT 10"),
        ("filter + aggregate",
         "SELECT count(*), sum(amount) FROM {t} WHERE amount > 100"),
        ("point lookup by key", "SELECT * FROM {t} WHERE id = 12345"),
    ],
    "wide": [
        ("count(*)", "SELECT count(*) FROM {t}"),
        ("two columns of twelve",
         "SELECT region, sum(amount) FROM {t} GROUP BY region ORDER BY 2 DESC NULLS LAST, 1 NULLS LAST"),
        ("three-column group by",
         "SELECT region, channel, status, count(*) FROM {t} GROUP BY 1,2,3 ORDER BY 4 DESC NULLS LAST, 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST LIMIT 20"),
        ("filter + aggregate",
         "SELECT count(*), sum(qty), avg(discount) FROM {t} WHERE status = 'shipped'"),
        ("point lookup by key", "SELECT * FROM {t} WHERE id = 12345"),
    ],
    "jsonb": [
        ("count(*)", "SELECT count(*) FROM {t}"),
        ("group by a scalar column",
         "SELECT status, count(*) FROM {t} GROUP BY status ORDER BY 2 DESC NULLS LAST, 1 NULLS LAST"),
        ("filter on a scalar column",
         "SELECT count(*) FROM {t} WHERE status = 'ClosedWon'"),
        ("point lookup by key", "SELECT id, status FROM {t} WHERE id = 12345"),
    ],
    "inline": [
        ("count(*)", "SELECT count(*) FROM {t}"),
        ("group by a scalar column",
         "SELECT status, count(*) FROM {t} GROUP BY status ORDER BY 2 DESC NULLS LAST, 1 NULLS LAST"),
        ("filter on a scalar column",
         "SELECT count(*) FROM {t} WHERE status = 'ClosedWon'"),
        ("point lookup by key", "SELECT id, status FROM {t} WHERE id = 12345"),
    ],
    "churn": [
        ("count(*)", "SELECT count(*) FROM {t}"),
        ("sum one column", "SELECT sum(amount) FROM {t}"),
        ("group by sku top 10",
         "SELECT sku, count(*) c, sum(amount) s FROM {t} GROUP BY sku ORDER BY s DESC NULLS LAST, sku NULLS LAST LIMIT 10"),
        ("point lookup by key", "SELECT * FROM {t} WHERE id = 12345"),
    ],
    "deletes": [
        ("count(*)", "SELECT count(*) FROM {t}"),
        ("sum one column", "SELECT sum(amount) FROM {t}"),
        ("group by sku top 10",
         "SELECT sku, count(*) c, sum(amount) s FROM {t} GROUP BY sku ORDER BY s DESC NULLS LAST, sku NULLS LAST LIMIT 10"),
        ("point lookup by key", "SELECT * FROM {t} WHERE id = 12345"),
    ],
}


def check_null_ordering(pg, duck, table):
    """Report, rather than trip over, the one incompatibility this found.

    PostgreSQL orders DESC with NULLS FIRST; DuckDB orders DESC with NULLS LAST.
    So `ORDER BY total DESC LIMIT 10` over a column that has any NULL returns a
    DIFFERENT TOP TEN from the two engines, with identical data, no error, and
    nothing in either plan to suggest it.

    This is the shape of problem a serving layer has to answer for: not "is the
    data right" -- it is -- but "does the same SQL mean the same thing". It is
    checked and printed on every run so it cannot quietly stop being true, or
    quietly become true of something else.
    """
    q = "SELECT x FROM (VALUES (1),(NULL),(3)) t(x) ORDER BY x DESC"
    with pg.cursor() as cur:
        cur.execute(q)
        pg_order = [r[0] for r in cur.fetchall()]
    duck_order = [r[0] for r in duck.execute(q).fetchall()]
    same = pg_order == duck_order
    print(f"  ORDER BY x DESC:  postgres {pg_order}   duckdb {duck_order}"
          f"   {'same' if same else 'DIFFERENT — NULL placement'}")
    return same


# PostgreSQL prints a UTC timestamptz as "...+00"; a Python datetime prints
# "...+00:00"; PostgreSQL prints "+05:30" for a half-hour zone and so does
# Python. Canonicalising on PostgreSQL's spelling is what lets the comparison be
# about the instant rather than about two client libraries.
_TZ_SUFFIX = re.compile(r"([+-]\d{2}):00$")


def canon(v):
    if isinstance(v, datetime.datetime):
        v = v.isoformat(sep=" ")
    return _TZ_SUFFIX.sub(r"\1", str(v))


def normalise(rows):
    """Compare what the values MEAN, not how each engine spells them.

    PostgreSQL returns Decimal for numeric and DuckDB may return a float; an int
    and a Decimal of the same value are the same answer.

    Timestamps need the same treatment for a REASON worth knowing: the mirror
    stores timestamptz as TEXT, because the writer maps anything it does not
    recognise to a string. So the mirror hands back PostgreSQL's own rendering
    while psycopg hands back a Python datetime, and the two disagree about how
    to spell a UTC offset. Same instant, different string -- and the underlying
    fact, that the mirror's timestamps are not timestamps to a query engine, is
    a real serving limitation rather than a harness detail.
    """
    out = []
    for r in rows:
        row = []
        for v in r:
            if v is None:
                row.append(None)
            elif isinstance(v, (int, float)) or type(v).__name__ == "Decimal":
                row.append(round(float(v), 4))
            else:
                row.append(canon(v))
        out.append(tuple(row))
    # Sorted by a string rendering, because a result set can legitimately mix
    # NULL with values in the same column and Python refuses to order those.
    return sorted(out, key=lambda r: tuple("\x00" if v is None else str(v) for v in r))


def timed(fn, repeat):
    """Report the MEDIAN of repeated runs, and the first run separately.

    The first run pays for opening files and filling the page cache and the rest
    do not, so reporting only a mean would hide which of the two a reader should
    expect. Both are printed.
    """
    times, rows = [], None
    for _ in range(repeat):
        t0 = time.perf_counter()
        rows = fn()
        times.append((time.perf_counter() - t0) * 1000)
    return times[0], statistics.median(times), rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shape", default="narrow")
    ap.add_argument("--mirror", default=None)
    ap.add_argument("--table", default="public.m")
    ap.add_argument("--dsn", default="postgres://postgres@localhost:5443/app")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--qs-query", default="/tmp/qs-query")
    args = ap.parse_args()
    mirror = args.mirror or f"/var/lib/postgresql/qs17/mx-{args.shape}"

    out = subprocess.run(
        [args.qs_query, "-mirror", mirror, "-table", args.table, "-dsn", args.dsn],
        capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"qs-query failed: {out.stderr.strip()}")
    header, view_sql = out.stdout.split("\n", 1)
    print(f"  {header.strip()}")

    duck = duckdb.connect()
    duck.execute(f"CREATE VIEW qs AS {view_sql}")
    pg = CONNECT(args.dsn)

    # A row-count agreement check before any timing. If the two sides do not
    # even hold the same number of rows, every number below is meaningless and
    # printing them anyway is how a broken mirror looks fast.
    with pg.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {args.table}")
        pg_n = cur.fetchone()[0]
    duck_n = duck.execute("SELECT count(*) FROM qs").fetchone()[0]
    if pg_n != duck_n:
        sys.exit(f"  MISMATCH: source has {pg_n} rows, mirror view has {duck_n}. "
                 f"Nothing below would mean anything.")
    print(f"  both sides hold {pg_n:,} rows")
    check_null_ordering(pg, duck, args.table)
    print()
    print(f"  {'query':<28} {'postgres':>12} {'mirror':>12} {'speedup':>9}  agree")
    print(f"  {'-' * 28} {'-' * 12} {'-' * 12} {'-' * 9}  -----")

    failures = 0
    for label, q in QUERIES.get(args.shape, QUERIES["narrow"]):
        def run_pg():
            with pg.cursor() as cur:
                cur.execute(q.format(t=args.table))
                return cur.fetchall()

        def run_duck():
            return duck.execute(q.format(t="qs")).fetchall()

        try:
            pg_first, pg_med, pg_rows = timed(run_pg, args.repeat)
            qs_first, qs_med, qs_rows = timed(run_duck, args.repeat)
        except Exception as e:  # a query the shape does not support
            print(f"  {label:<28} {'—':>12} {'—':>12} {'—':>9}  ERROR {str(e)[:40]}")
            failures += 1
            continue

        agree = normalise(pg_rows) == normalise(qs_rows)
        if not agree:
            failures += 1
        ratio = pg_med / qs_med if qs_med else 0
        print(f"  {label:<28} {pg_med:>9.1f} ms {qs_med:>9.1f} ms "
              f"{ratio:>8.2f}x  {'yes' if agree else 'NO'}")
        if not agree:
            print(f"      postgres: {normalise(pg_rows)[:2]}")
            print(f"      mirror:   {normalise(qs_rows)[:2]}")
        print(f"  {'':<28} {'(first ' + format(pg_first, '.0f') + ' ms)':>12} "
              f"{'(first ' + format(qs_first, '.0f') + ' ms)':>12}")

    print()
    if failures:
        print(f"  {failures} queries did not agree — this is a correctness failure, "
              f"not a slow result")
        sys.exit(1)
    print("  all queries agreed")


if __name__ == "__main__":
    main()
