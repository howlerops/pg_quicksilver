#!/usr/bin/env python3
# Generates, per session zone, a psql script that compares PostgreSQL's date_trunc(unit, timestamptz)
# with DuckDB's (DuckDB's TimeZone set to the same zone) over a grid of instants and units.
# Both sides are compared as integer microseconds since the epoch. Output: one line per unit with
# the number of (instant, unit) pairs, and how many of them differ.
import os, random, sys, datetime as dt
OUT = sys.argv[2] if len(sys.argv) > 2 else '.'

ZONES = sys.argv[1].split(',') if len(sys.argv) > 1 and sys.argv[1] else ['UTC', 'America/New_York', 'Europe/London',
                                                         'Asia/Kolkata', 'Australia/Lord_Howe', '+03']
UNITS = ['microseconds', 'milliseconds', 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter',
         'year', 'decade', 'century', 'millennium']


def grid():
    rnd = random.Random(20261127)
    out = []
    # DST transitions of each zone, every 15 minutes across the change
    for day, hours in [((2024, 3, 10), (5, 9)), ((2024, 11, 3), (4, 8)), ((2024, 3, 31), (0, 4)),
                       ((2024, 10, 27), (0, 4)), ((2024, 4, 7), (13, 17)), ((2024, 10, 6), (14, 18)),
                       ((2024, 3, 30), (23, 24)), ((2024, 10, 26), (23, 24)),
                       ((1883, 11, 18), (16, 19)), ((1916, 6, 14), (21, 24))]:
        d = dt.datetime(*day, tzinfo=dt.timezone.utc)
        t = d + dt.timedelta(hours=hours[0])
        end = d + dt.timedelta(hours=hours[1])
        while t < end:
            out.append(t)
            t += dt.timedelta(minutes=15)
    # edges of weeks, months, quarters, years, decades, centuries, in every zone's offset
    for y in (1950, 1969, 1970, 1971, 1999, 2000, 2024, 2038, 2100):
        for m in range(1, 13):
            for dd in (1, 2, 28):
                out.append(dt.datetime(y, m, dd, 0, 0, tzinfo=dt.timezone.utc))
                out.append(dt.datetime(y, m, dd, 23, 59, 59, 999999, tzinfo=dt.timezone.utc))
    # pre-1970 (LMT era), far future, and random instants
    for _ in range(120):
        out.append(dt.datetime(1850, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(seconds=rnd.randrange(0, 4 * 10**9)))
    for _ in range(120):
        out.append(dt.datetime(2200, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(seconds=rnd.randrange(0, 2 * 10**10)))
    for _ in range(300):
        out.append(dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(seconds=rnd.randrange(0, 2 * 10**9)))
    for _ in range(60):
        out.append(dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(microseconds=rnd.randrange(0, 10**15)))
    return [t for t in out if 1 <= t.year <= 9999]


def lit(t):
    return "'%s+00'" % t.strftime('%Y-%m-%d %H:%M:%S.%f')


def main():
    inst = grid()
    arr = ','.join(lit(t) for t in inst)
    units = ','.join("'%s'" % u for u in UNITS)
    for zone in ZONES:
        name = zone.replace('/', '_').replace('+', 'p')
        with open(os.path.join(OUT, 'tzproof_%s.sql' % name), 'w') as f:
            f.write("\\set ON_ERROR_STOP off\n")
            f.write("SET TimeZone = '%s';\n" % zone)
            f.write("SELECT duckdb.raw_query('SET TimeZone = ''%s''');\n" % zone)
            f.write("DROP TABLE IF EXISTS pg_out, duck_out;\n")
            f.write("CREATE TEMP TABLE pg_out AS SELECT u, (extract(epoch FROM t) * 1000000)::bigint AS tk,\n"
                    "  (extract(epoch FROM date_trunc(u, t)) * 1000000)::bigint AS dk\n"
                    "  FROM unnest(ARRAY[%s]::timestamptz[]) t CROSS JOIN unnest(ARRAY[%s]::text[]) u;\n"
                    % (arr, units))
            f.write("CREATE TEMP TABLE duck_out AS SELECT r['u'] AS u, r['tk'] AS tk, r['dk'] AS dk FROM duckdb.query(\n"
                    "  $$ SELECT u, epoch_us(t)::BIGINT AS tk, epoch_us(date_trunc(u, t))::BIGINT AS dk\n"
                    "     FROM (SELECT unnest([%s]::TIMESTAMPTZ[]) AS t) CROSS JOIN (SELECT unnest([%s]) AS u) $$) r;\n"
                    % (arr, units))
            f.write("SELECT p.u, count(*) AS pairs, count(*) FILTER (WHERE p.dk IS DISTINCT FROM q.dk) AS differ,\n"
                    "  count(q.dk) AS duck_rows\n"
                    "  FROM pg_out p LEFT JOIN duck_out q ON q.u = p.u AND q.tk = p.tk GROUP BY p.u ORDER BY p.u;\n")
    print(len(inst))


if __name__ == '__main__':
    main()
