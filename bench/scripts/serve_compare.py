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
    python3 bench/scripts/serve_compare.py --shape narrow --cold   # see below

COLD AND WARM ARE DIFFERENT QUESTIONS. By default every timing here is a warm
page cache on both sides: the data has just been written, it is still in RAM,
and neither engine touches a disk. That is a real scenario -- a dashboard
refreshing the same query -- and it is the one that flatters a column store
least, because the row store's disadvantage is BYTES READ and bytes that are
already in RAM are nearly free.

--cold measures the other one. Before every single timing it restarts
PostgreSQL, which is the only way to empty shared_buffers, and drops the OS page
cache. Requires root, and each side is run ONCE, because the second run of a
cold query is a warm query.
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
        # Date arithmetic. Before temporal.go the mirror stored ts as VARCHAR,
        # and this query did not fail -- it did not PARSE, which is worse,
        # because it means a query written against PostgreSQL cannot be pointed
        # at the mirror at all.
        ("date arithmetic",
         "SELECT count(*) FROM {t} WHERE ts > now() - interval '1 hour'"),
        ("truncate to the hour",
         "SELECT date_trunc('hour', ts) h, count(*) FROM {t} "
         "GROUP BY 1 ORDER BY 1 NULLS LAST LIMIT 5"),
        ("sum one column", "SELECT sum(amount) FROM {t}"),
        ("group by sku top 10",
         "SELECT sku, count(*) c, sum(amount) s FROM {t} GROUP BY sku ORDER BY s DESC NULLS LAST, sku NULLS LAST LIMIT 10"),
        ("filter + aggregate",
         "SELECT count(*), sum(amount) FROM {t} WHERE amount > 100"),
        ("point lookup by key", "SELECT * FROM {t} WHERE id = 12345"),
    ],
    "wide": [
        ("count(*)", "SELECT count(*) FROM {t}"),
        ("date arithmetic",
         "SELECT count(*) FROM {t} WHERE ts > now() - interval '1 hour'"),
        ("two columns of twelve",
         "SELECT region, sum(amount) FROM {t} GROUP BY region ORDER BY 2 DESC NULLS LAST, 1 NULLS LAST"),
        ("three-column group by",
         "SELECT region, channel, status, count(*) FROM {t} GROUP BY 1,2,3 ORDER BY 4 DESC NULLS LAST, 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST LIMIT 20"),
        # 'pending' and not 'shipped'. The wide shape only ever generates
        # ok/pending/refunded, so this filter matched NOTHING for as long as it
        # existed -- and a filter that matches nothing measures row-group
        # pruning, not filtering. It looked like the most ordinary line in the
        # table. See check_empty_results below.
        ("filter + aggregate",
         "SELECT count(*), sum(qty), avg(discount) FROM {t} WHERE status = 'pending'"),
        # The same filter shape over a column whose values are NOT clustered.
        # The pair matters: 'pending' exists only in the seeded rows, which sit
        # at the front of the mirror, so DuckDB prunes almost every row group
        # from its footers and the speedup is mostly about PRUNING. qty is
        # 1+id%9, uniform across every row group, so nothing can be skipped and
        # what is left is the columnar scan on its own. Reporting only the
        # first would credit the format for a property of the data.
        ("filter + aggregate, unclustered",
         "SELECT count(*), sum(qty), avg(discount) FROM {t} WHERE qty = 5"),
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



# The mirror is only a drop-in for a read replica if the same SQL means the same
# thing, and ORDER BY does not by default: PostgreSQL sorts NULLs last on ASC
# and FIRST on DESC, DuckDB sorts them last on both. docs/20 says a reader must
# pin this; a benchmark that did not pin it would be measuring a configuration
# the documentation tells nobody to use.
NULL_ORDER = "NULLS_LAST_ON_ASC_FIRST_ON_DESC"


def duck_connect(view_sql=None):
    """A DuckDB connection configured the way docs/20 tells readers to."""
    con = duckdb.connect()
    con.execute(f"SET default_null_order = '{NULL_ORDER}'")
    if view_sql is not None:
        con.execute(f"CREATE VIEW qs AS {view_sql}")
    return con



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
    if not same:
        # This used to be a standing report of a known difference, printed on
        # every run forever. It is now an assertion, because the connection was
        # opened with default_null_order set to what docs/20 tells readers to
        # use -- so a difference here means that setting no longer does what it
        # claims, which is a mirror that silently answers ORDER BY differently
        # from the source.
        print(f"  FAIL: default_null_order = '{NULL_ORDER}' did not reproduce "
              f"PostgreSQL's ordering. docs/20 tells readers to set this; if it "
              f"no longer works, the mirror answers ORDER BY over a nullable "
              f"column differently from the source and nothing says so.")
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


_WHOLE_DISK = re.compile(r"^(vd[a-z]+|sd[a-z]+|nvme\d+n\d+)$")


def disk_read_bytes():
    """Sectors read from the block device, for every process on the machine.

    The whole system rather than one PID, on purpose: a PostgreSQL query can
    fan out to parallel workers whose I/O never appears in the leader's
    /proc/<pid>/io, and undercounting one side of a comparison is worse than
    including a little background noise. Nothing else runs during a timing.

    This is the MECHANISM behind every number in the table. A column store is
    not faster because it is clever; it is faster because it reads fewer bytes,
    and on a warm cache that advantage is nearly free to the row store.
    """
    total = 0
    with open("/proc/diskstats") as f:
        for line in f:
            parts = line.split()
            # Whole disks only. A partition is counted again by its parent, and
            # summing both would double every number here.
            if len(parts) > 5 and _WHOLE_DISK.match(parts[2]):
                total += int(parts[5]) * 512  # field 6: sectors read
    return total


def is_empty_result(rows):
    """A filtered query that matched nothing, which is not a measurement.

    The wide shape's `filter + aggregate` filtered on status = 'shipped' for as
    long as it existed, and the shape only ever generates ok/pending/refunded.
    Both engines agreed -- on count 0 -- so the correctness check passed, and
    the timing went into a published table. What it actually measured was how
    fast each engine can establish that there is nothing to do: PostgreSQL scans
    341 MB to find out, DuckDB reads a row-group footer, and the ratio is a
    number about pruning wearing the label of a number about filtering.

    An empty result is never wrong, which is exactly why it has to be SAID.
    """
    if not rows:
        return True
    # A single aggregate row of count 0 / all NULLs is empty in every sense that
    # matters here.
    if len(rows) == 1:
        vals = list(rows[0])
        return all(v is None or v == 0 for v in vals)
    return False


def mb(n):
    if n is None:
        return "—"
    if n < 0:  # a counter that went backwards is noise, not a measurement
        return "?"
    return f"{n / 1e6:.1f} MB"


def drop_page_cache():
    """Empty the OS page cache. Everything the mirror reads goes through it."""
    subprocess.run(["sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"],
                   check=True)


def restart_postgres(pgdata, pgbin, logfile):
    """Empty shared_buffers, which nothing short of a restart does.

    Dropping the OS page cache alone would be a rigged comparison: PostgreSQL
    would still answer from its own buffer pool while the mirror went to disk.

    `-l` is not optional and the reason is worth writing down. Without it the
    restarted postmaster INHERITS this script's stdout and stderr pipes, and
    capture_output reads a pipe until EOF -- which never comes, because the
    server holds the write end open for as long as it runs. The result is not a
    crash but a hang: pg_ctl exits, the child shows up as a zombie, and the
    benchmark waits forever on a database that is already serving queries. The
    timeout turns a future version of that into an error instead of a stall.
    """
    r = subprocess.run(
        ["su", "postgres", "-c",
         f"{pgbin}/pg_ctl -D {pgdata} -l {logfile} -w -m fast restart"],
        capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        sys.exit(f"  could not restart PostgreSQL to empty shared_buffers: "
                 f"{r.stderr.strip() or r.stdout.strip()}")


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
    ap.add_argument("--cold", action="store_true",
                    help="empty shared_buffers and the page cache before every "
                         "timing; needs root")
    ap.add_argument("--pgdata", default="/var/lib/postgresql/qs17/primary")
    ap.add_argument("--pgbin", default="/usr/lib/postgresql/17/bin")
    ap.add_argument("--pglog", default="/var/lib/postgresql/qs17/primary.log")
    args = ap.parse_args()
    mirror = args.mirror or f"/var/lib/postgresql/qs17/mx-{args.shape}"

    out = subprocess.run(
        [args.qs_query, "-mirror", mirror, "-table", args.table, "-dsn", args.dsn],
        capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"qs-query failed: {out.stderr.strip()}")
    header, view_sql = out.stdout.split("\n", 1)
    print(f"  {header.strip()}")

    duck = duck_connect(view_sql)
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
    null_order_ok = check_null_ordering(pg, duck, args.table)
    print()
    if args.cold:
        print("  COLD: shared_buffers and the page cache are emptied before "
              "every timing below,")
        print("  and each side runs exactly once, because a second run is a "
              "warm run.")
        print()
    print(f"  {'query':<28} {'postgres':>12} {'mirror':>12} {'speedup':>9}  agree")
    print(f"  {'-' * 28} {'-' * 12} {'-' * 12} {'-' * 9}  -----")

    failures, empty = 0, []
    for label, q in QUERIES.get(args.shape, QUERIES["narrow"]):
        def run_pg():
            with pg.cursor() as cur:
                cur.execute(q.format(t=args.table))
                return cur.fetchall()

        def run_duck():
            return duck.execute(q.format(t="qs")).fetchall()

        try:
            if args.cold:
                # Restart FIRST, then drop the cache: a restart reads its own
                # files back in, so dropping before it would warm the very
                # thing being measured.
                restart_postgres(args.pgdata, args.pgbin, args.pglog)
                pg.close()
                drop_page_cache()
                pg = CONNECT(args.dsn)
                b0 = disk_read_bytes()
                pg_first, pg_med, pg_rows = timed(run_pg, 1)
                pg_bytes = disk_read_bytes() - b0

                # A fresh DuckDB for the same reason PostgreSQL is restarted:
                # its buffer manager is as much a cache as shared_buffers.
                duck.close()
                drop_page_cache()
                duck = duck_connect(view_sql)
                b0 = disk_read_bytes()
                qs_first, qs_med, qs_rows = timed(run_duck, 1)
                qs_bytes = disk_read_bytes() - b0
            else:
                pg_bytes = qs_bytes = None
                pg_first, pg_med, pg_rows = timed(run_pg, args.repeat)
                qs_first, qs_med, qs_rows = timed(run_duck, args.repeat)
        except Exception as e:  # a query the shape does not support
            print(f"  {label:<28} {'—':>12} {'—':>12} {'—':>9}  ERROR {str(e)[:40]}")
            failures += 1
            continue

        agree = normalise(pg_rows) == normalise(qs_rows)
        if not agree:
            failures += 1
        if is_empty_result(pg_rows):
            empty.append(label)
        ratio = pg_med / qs_med if qs_med else 0
        print(f"  {label:<28} {pg_med:>9.1f} ms {qs_med:>9.1f} ms "
              f"{ratio:>8.2f}x  {'yes' if agree else 'NO'}")
        if not agree:
            print(f"      postgres: {normalise(pg_rows)[:2]}")
            print(f"      mirror:   {normalise(qs_rows)[:2]}")
        if args.cold:
            # The mechanism, not just the outcome: bytes off the block device.
            fewer = (f"{pg_bytes / qs_bytes:>8.2f}x" if qs_bytes else f"{'—':>9}")
            print(f"  {'  bytes read':<28} {mb(pg_bytes):>12} {mb(qs_bytes):>12} "
                  f"{fewer}")
        else:
            print(f"  {'':<28} {'(first ' + format(pg_first, '.0f') + ' ms)':>12} "
                  f"{'(first ' + format(qs_first, '.0f') + ' ms)':>12}")

    print()
    if empty:
        print(f"  WARNING: {', '.join(empty)} returned an empty result on BOTH "
              f"sides.")
        print(f"  Those timings measure how fast each engine finds nothing, not "
              f"how fast it")
        print(f"  filters. Do not quote them as filter numbers — fix the "
              f"predicate instead.")
        print()
    if failures:
        print(f"  {failures} queries did not agree — this is a correctness failure, "
              f"not a slow result")
        sys.exit(1)
    if not null_order_ok:
        # Its return value used to be discarded, so the difference was printed
        # on every run and failed none of them.
        sys.exit(1)
    print("  all queries agreed")


if __name__ == "__main__":
    main()
