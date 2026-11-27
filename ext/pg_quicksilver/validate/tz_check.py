#!/usr/bin/env python3
"""Per-zone proof that date_trunc(unit, timestamptz) agrees in PostgreSQL and DuckDB.

For every zone in pg_timezone_names, on a live primary with pg_duckdb:

  1. SET TimeZone in PostgreSQL; pg_duckdb's GUC hook sets DuckDB's TimeZone to the
     same name. A zone either side refuses is denied, with the message.
  2. Transitions: the UTC offset is sampled daily from 1900 to 2100 (in SQL, only the
     days where it changes are returned); each change is refined to the minute in
     two more SQL steps. Every transition is kept, with its offset direction.
  3. Instants: every transition at 15-minute steps from -2 h to +2 h, one second
     before it, plus 2000 seeded random instants in 1900-2100, plus the edges
     (infinity, -infinity, NULL, 1970-01-01, 2100-01-01).
  4. For each instant and each unit, PostgreSQL's date_trunc and DuckDB's date_trunc
     (in the same zone) are compared, and the wall clock (t::timestamp in PostgreSQL,
     CAST(t AS TIMESTAMP) in DuckDB, i.e. the offset) is compared too.

A zone is admitted for the proven units (microseconds ... decade: the grid of
this tool) only if every row of those units matches, on both the truncated value and
the wall clock, and DuckDB accepted the zone. hour is admitted in addition only if
the zone has no offset decrease in 1900-2100 (a repeated wall-clock hour needs one)
and every hour row matches. century and millennium are measured and never admitted.

Writes validate/results/tz_check.tsv (one row per zone, with the reason for a
denial) and validate/results/tz_check.txt (counts); with --extension FILE it
rewrites the generated time-zone block of the extension script (between the
BEGIN/END markers). Run it against a primary with pg_duckdb and pg_quicksilver:

    python3 validate/tz_check.py --port 5593 --extension pg_quicksilver--0.1.sql
"""
import argparse
import datetime as dt
import os
import random
import sys

import psycopg

HERE = os.path.dirname(os.path.abspath(__file__))
UNITS = ['microseconds', 'milliseconds', 'second', 'minute', 'hour', 'day', 'week', 'month',
         'quarter', 'year', 'decade', 'century', 'millennium']
PROVEN = ['microseconds', 'milliseconds', 'second', 'minute', 'day', 'week', 'month', 'quarter',
          'year', 'decade']
BEGIN = '-- BEGIN time zones (validate/tz_check.py)'
END = '-- END time zones'


def lit(t):
    if t is None:
        return 'NULL'
    if isinstance(t, str):
        return "'%s'::timestamptz" % t
    t = t.astimezone(dt.timezone.utc)
    return "'%s+00'::timestamptz" % t.strftime('%Y-%m-%d %H:%M:%S.%f')


def random_instants(n, seed):
    rnd = random.Random(seed)
    base = dt.datetime(1900, 1, 1, tzinfo=dt.timezone.utc)
    span = int((dt.datetime(2100, 1, 1, tzinfo=dt.timezone.utc) - base).total_seconds())
    return [base + dt.timedelta(seconds=rnd.randrange(span), microseconds=rnd.randrange(10 ** 6))
            for _ in range(n)]


def transitions(cur):
    """(minute, direction) for every offset change in 1900-2100, direction +1 or -1."""
    cur.execute("""
        WITH s AS (SELECT d, extract(timezone FROM d)::int AS off
                   FROM generate_series('1899-12-31 00:00+00'::timestamptz, '2100-01-02 00:00+00',
                                        interval '1 day') d)
        SELECT d, off, prev FROM (SELECT d, off, lag(off) OVER (ORDER BY d) AS prev FROM s) x
        WHERE prev IS NOT NULL AND off <> prev ORDER BY d""")
    days = cur.fetchall()
    if not days:
        return []
    dirs = {d: (1 if off > prev else -1) for d, off, prev in days}
    day_list = [d for d, _, _ in days]
    cur.execute("""
        SELECT DISTINCT ON (b.d) b.d, h.t FROM unnest(%s::timestamptz[]) AS b(d)
        CROSS JOIN LATERAL (
            SELECT t, extract(timezone FROM t)::int AS off,
                   lag(extract(timezone FROM t)::int) OVER (ORDER BY t) AS prev
            FROM generate_series(b.d - interval '25 hours', b.d, interval '1 hour') t) h
        WHERE h.prev IS NOT NULL AND h.off <> h.prev ORDER BY b.d, h.t""", (day_list,))
    hours = cur.fetchall()
    if not hours:
        return []
    cur.execute("""
        SELECT DISTINCT ON (b.h) b.h, m.t FROM unnest(%s::timestamptz[]) AS b(h)
        CROSS JOIN LATERAL (
            SELECT t, extract(timezone FROM t)::int AS off,
                   lag(extract(timezone FROM t)::int) OVER (ORDER BY t) AS prev
            FROM generate_series(b.h - interval '61 minutes', b.h, interval '1 minute') t) m
        WHERE m.prev IS NOT NULL AND m.off <> m.prev ORDER BY b.h, m.t""", ([h for _, h in hours],))
    mins = {h: m for h, m in cur.fetchall()}
    out = []
    for d, h in hours:
        if h in mins:
            # the day's direction, from the day-level comparison that contains this change
            day = min(x for x in day_list if x >= h)
            out.append((mins[h], dirs[day]))
    return out


def check_zone(conn, zone, ninst, seed):
    cur = conn.cursor()
    try:
        cur.execute("SELECT set_config('TimeZone', %s, false)", (zone,))
        cur.execute("SELECT current_setting('TimeZone')")
        canon = cur.fetchone()[0]
    except psycopg.Error as e:
        return {'admitted': False, 'reason': 'PostgreSQL rejects the zone: %s' % str(e).splitlines()[0]}
    try:
        cur.execute("SELECT r['z'] FROM duckdb.query($$ SELECT current_setting('TimeZone') AS z $$) r")
        duck = cur.fetchone()[0]
    except psycopg.Error as e:
        return {'admitted': False, 'reason': 'DuckDB rejects the zone: %s' % str(e).splitlines()[0]}
    if duck != canon:
        return {'admitted': False, 'reason': 'DuckDB TimeZone %r differs from %r' % (duck, canon)}

    trans = transitions(cur)
    decreases = sum(1 for _, d in trans if d < 0)
    inst = [None, 'infinity', '-infinity', '1970-01-01 00:00:00+00', '2100-01-01 00:00:00+00']
    for m, _ in trans:
        for k in range(-8, 9):
            inst.append(m + dt.timedelta(minutes=15 * k))
        inst.append(m - dt.timedelta(seconds=1))
    inst.extend(random_instants(ninst, seed))
    # Each instant carries its index, so the two sides join on (index, unit) with a hash join.
    arr = ','.join(lit(t) for t in inst)
    idx = ','.join(str(i) for i in range(len(inst)))
    units = ','.join("'%s'" % u for u in UNITS)
    cur.execute("DROP TABLE IF EXISTS tzp, tzd")
    cur.execute("CREATE TEMP TABLE tzp AS SELECT i, u, date_trunc(u, t) AS d, t::timestamp AS w "
                "FROM unnest(ARRAY[%s]::timestamptz[], ARRAY[%s]::int[]) AS a(t, i) "
                "CROSS JOIN unnest(ARRAY[%s]::text[]) u" % (arr, idx, units))
    cur.execute("CREATE TEMP TABLE tzd AS SELECT r['i']::int AS i, r['u'] AS u, (r['d'])::timestamptz AS d, "
                "(r['w'])::timestamp AS w FROM duckdb.query($$ "
                "SELECT i, u, date_trunc(u, t::TIMESTAMPTZ) AS d, CAST(t::TIMESTAMPTZ AS TIMESTAMP) AS w "
                "FROM (SELECT unnest([%s]::TIMESTAMPTZ[]) AS t, unnest([%s]::INTEGER[]) AS i) "
                "CROSS JOIN (SELECT unnest([%s]) AS u) $$) r"
                % (arr, idx, units))
    cur.execute("""
        SELECT p.u, count(*) AS n, count(q.u) AS matched,
               count(*) FILTER (WHERE p.d IS DISTINCT FROM q.d) AS d_diff,
               count(*) FILTER (WHERE p.w IS DISTINCT FROM q.w) AS w_diff
        FROM tzp p LEFT JOIN tzd q ON q.i = p.i AND q.u = p.u
        GROUP BY p.u""")
    per = {u: (n, matched, dd, wd) for u, n, matched, dd, wd in cur.fetchall()}
    cur.execute("DROP TABLE IF EXISTS tzp, tzd")

    def clean(u):
        n, matched, dd, wd = per[u]
        return n == matched and dd == 0 and wd == 0

    bad = [u for u in PROVEN if not clean(u)]
    res = {'canon': canon, 'transitions': len(trans), 'decreases': decreases, 'instants': len(inst),
           'diffs': ' '.join('%s:%d' % (u, per[u][2]) for u in UNITS if per[u][2]),
           'wall_diffs': sum(per[u][3] for u in UNITS)}
    if bad:
        res.update(admitted=False, reason='differs for %s' % ', '.join(bad))
        return res
    res['admitted'] = True
    res['reason'] = ''
    res['hour'] = clean('hour') and decreases == 0
    if not res['hour']:
        res['hour_reason'] = 'hour differs' if not clean('hour') else 'offset decreases %d times 1900-2100' % decreases
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--host', default='localhost')
    ap.add_argument('--port', type=int, default=int(os.environ.get('PPORT', '5593')))
    ap.add_argument('--db', default='app')
    ap.add_argument('--user', default='postgres')
    ap.add_argument('--instants', type=int, default=2000)
    ap.add_argument('--seed', type=int, default=20261127)
    ap.add_argument('--zones', default='', help='comma list (default: all of pg_timezone_names)')
    ap.add_argument('--out', default=os.path.join(HERE, 'results'))
    ap.add_argument('--extension', default='', help='rewrite the generated block of this extension script')
    ap.add_argument('--from-tsv', default='', help='with --extension: take the rows from this tz_check.tsv '
                    'instead of checking again')
    args = ap.parse_args()
    if args.from_tsv:
        # the admitted rows of an earlier run: zone, admitted, hour
        rows = []
        with open(args.from_tsv) as f:
            next(f)
            for line in f:
                zone, adm, hour = line.split('\t')[:3]
                rows.append({'zone': zone, 'canon': zone, 'admitted': adm == 'yes', 'hour': hour == 'yes'})
        return write_extension(args.extension, [r for r in rows if r['admitted']])

    conn = psycopg.connect(host=args.host, port=args.port, user=args.user, dbname=args.db, autocommit=True)
    cur = conn.cursor()
    cur.execute("SELECT name FROM pg_timezone_names ORDER BY name")
    zones = [r[0] for r in cur.fetchall()]
    if args.zones:
        zones = args.zones.split(',')
    rows = []
    for i, z in enumerate(zones):
        r = check_zone(conn, z, args.instants, args.seed)
        r['zone'] = z
        rows.append(r)
        print('%4d/%d %-36s %s %s' % (i + 1, len(zones), z, 'ADMIT' if r['admitted'] else 'deny ',
                                     r.get('hour', '') and 'hour' or ''), file=sys.stderr)
    os.makedirs(args.out, exist_ok=True)
    tsv = os.path.join(args.out, 'tz_check.tsv')
    with open(tsv, 'w') as f:
        f.write('zone\tadmitted\thour\ttransitions\tdecreases\tinstants\tdiffs\twall_diffs\treason\n')
        for r in rows:
            reason = r.get('reason') or ''
            if r['admitted'] and not r.get('hour'):
                reason = r.get('hour_reason', '')
            f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (
                r['zone'], 'yes' if r['admitted'] else 'no', 'yes' if r.get('hour') else 'no',
                r.get('transitions', ''), r.get('decreases', ''), r.get('instants', ''),
                r.get('diffs', ''), r.get('wall_diffs', ''), reason))
    adm = [r for r in rows if r['admitted']]
    hr = [r for r in adm if r.get('hour')]
    summary = ('zones %d\nadmitted %d (hour also %d)\ndenied %d\n' % (len(rows), len(adm), len(hr), len(rows) - len(adm)))
    reasons = {}
    for r in rows:
        if not r['admitted']:
            key = r.get('reason', '')
            key = key.split(':')[0] if key.startswith(('PostgreSQL', 'DuckDB')) else key.split(' for ')[0]
            reasons[key] = reasons.get(key, 0) + 1
    for k in sorted(reasons):
        summary += '  denied, %s: %d\n' % (k, reasons[k])
    with open(os.path.join(args.out, 'tz_check.txt'), 'w') as f:
        f.write(summary)
    print(summary)

    if args.extension:
        return write_extension(args.extension, adm)
    return 0


def write_extension(path, adm):
    lines = [BEGIN + ': generated; do not edit by hand. Each zone was compared with',
             '-- date_trunc in PostgreSQL and DuckDB over the instants described there; see',
             '-- validate/results/tz_check.tsv for the zones denied, and why.',
             'INSERT INTO quicksilver.allowlist (kind, ident, collation_rule, options, verified_by) VALUES']
    vals = []
    for r in adm:
        opts = "'{hour}'" if r.get('hour') else "'{}'"
        vals.append("  ('timezone', '%s', 'none', %s, 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)')"
                    % (r['canon'], opts))
    if vals:
        lines.append(',\n'.join(vals) + ';')
    lines.append(END)
    block = '\n'.join(lines) + '\n'
    with open(path) as f:
        text = f.read()
    if BEGIN in text:
        head, rest = text.split(BEGIN, 1)
        _, tail = rest.split(END + '\n', 1) if (END + '\n') in rest else rest.split(END, 1)
        text = head + block + tail
    else:
        anchor = '-- The join domain objects are last in this script on purpose'
        text = text.replace(anchor, block + '\n' + anchor, 1)
    with open(path, 'w') as f:
        f.write(text)
    print('wrote %d zone rows to %s' % (len(adm), path), file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main())
