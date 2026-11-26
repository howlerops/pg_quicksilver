#!/usr/bin/env python3
"""Ten million rows, and answers that do not fit in work_mem.

    DATASET=bench ROWS=10000000 LOCALE=C validate/real/setup.sh
    python3 validate/real/scale.py [--sport 5581] [--mirror /var/lib/postgresql/qsint/mirror]

On the standby, against the lake the sidecar built from the orders/customers
of bench/engine_compare/four_engines:

1. The six statements of that benchmark (bench.py), heap and routed: which side
   answered and why, the median of five runs each, and that the rows are equal.
2. A 10M-row result streamed to the client (work_mem is 4MB): heap and routed,
   rows and a checksum of every row.
3. The same statement with the lake's data file cut short while its rows are
   arriving. The client must get an error after the rows it already has: not
   the end of a result, not the heap's rows after the mirror's.
4. The same in a transaction block, where the mirror's answer is taken whole
   before the first row: the heap answers, completely, and the block goes on.
5. A sort DuckDB has to spill, with pg_duckdb confined: what happens when its
   temporary directory is not in duckdb.allowed_directories.

Run it as root: step 3 truncates a file that belongs to the mirror (and puts it back).
"""
import argparse
import glob
import hashlib
import os
import shutil
import statistics
import sys
import time

import psycopg

ap = argparse.ArgumentParser()
ap.add_argument("--sport", type=int, default=5581)
ap.add_argument("--host", default="/tmp")
ap.add_argument("--mirror", default="/var/lib/postgresql/qsint/mirror")
ap.add_argument("--runs", type=int, default=5)
ap.add_argument("--skip", default="", help="comma-separated step numbers to skip")
args = ap.parse_args()
S = f"host={args.host} port={args.sport} dbname=app user=postgres"
skip = set(args.skip.split(","))
rc = 0


def connect(*sets):
    c = psycopg.connect(S, autocommit=True)
    for s in sets:
        c.execute(s)
    return c


def decision(c):
    side, reason, detail = c.execute("SELECT side, reason, detail FROM quicksilver.last_decision()").fetchone()
    return side, reason, (detail or "")


heap = connect("SET quicksilver.route = heap")
auto = connect("SET quicksilver.route = auto")
rows = heap.execute("SELECT count(*) FROM public.orders").fetchone()[0]
lookup = heap.execute("SELECT id FROM public.orders LIMIT 1").fetchone()[0]
print(f"public.orders: {rows} rows; settings: " + ", ".join(
    f"{n}={auto.execute('SELECT current_setting(%s)', (n,)).fetchone()[0]}"
    for n in ("work_mem", "max_parallel_workers_per_gather", "quicksilver.max_lag", "duckdb.max_memory")))

if "1" not in skip:
    QUERIES = [
        ("count_by_status", "SELECT status, count(*) FROM orders GROUP BY status ORDER BY status"),
        ("revenue_by_month_join",
         "SELECT date_trunc('month', o.created_at) AS month, date_trunc('year', c.created_at) AS signup_year, "
         "sum(o.amount) AS revenue FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY 1, 2 ORDER BY 1, 2"),
        ("pk_lookup", f"SELECT * FROM orders WHERE id = '{lookup}'"),
        ("count_star", "SELECT count(*) FROM orders"),
        ("sum_amount", "SELECT sum(amount) FROM orders"),
        ("month_filter_agg",
         "SELECT count(*), sum(amount) FROM orders WHERE created_at >= timestamptz '2025-06-01 00:00:00+00' "
         "AND created_at < timestamptz '2025-07-01 00:00:00+00'"),
    ]
    print(f"\n1. the six benchmark statements, median of {args.runs}\n")
    print(f"| statement | heap ms | routed ms | | answered by | rows equal |\n|---|---|---|---|---|---|")
    for name, sql in QUERIES:
        th, ta, ra, rh = [], [], None, None
        auto.execute(sql).fetchall()        # the session's first routed statement attaches the lake
        for _ in range(args.runs):
            t = time.monotonic(); rh = heap.execute(sql).fetchall(); th.append((time.monotonic() - t) * 1000)
            t = time.monotonic(); ra = auto.execute(sql).fetchall(); ta.append((time.monotonic() - t) * 1000)
        side, reason, detail = decision(auto)
        mh, ma = statistics.median(th), statistics.median(ta)
        why = "" if side == "mirror" else f" ({reason}: {detail[:90]})"
        gain = f"{mh / ma:.1f}x" if side == "mirror" else ""
        print(f"| `{name}` | {mh:.1f} | {ma:.1f} | {gain} | {side}{why} | {'yes' if ra == rh else 'NO'} |", flush=True)
        rc |= ra != rh

BIG = "SELECT id, tenant_id, amount, created_at FROM public.orders"


def stream(c, sql, on_row=None):
    """Rows as they arrive (libpq single-row mode): count, checksum, duplicates, seconds, error."""
    h, n, seen, err = 0, 0, set(), None
    t = time.monotonic()
    try:
        with c.cursor() as cur:
            for r in cur.stream(sql):
                n += 1
                h ^= int.from_bytes(hashlib.blake2b(repr(r).encode(), digest_size=8).digest(), "big")
                seen.add(r[0].int & 0xFFFFFFFFFFFF)
                if on_row:
                    on_row(n)
    except psycopg.Error as e:
        err = e
    return n, h, n - len(seen), time.monotonic() - t, err


if "2" not in skip:
    print(f"\n2. {rows} rows streamed to the client: {BIG}\n")
    nh, hh, _, sh, eh = stream(heap, BIG)
    na, ha, dup, sa, ea = stream(auto, BIG)
    side, reason, detail = decision(auto)
    print(f"   heap:   {nh} rows in {sh:.1f} s, checksum {hh:016x}{' ERROR ' + str(eh) if eh else ''}")
    print(f"   routed: {na} rows in {sa:.1f} s, checksum {ha:016x}, answered by the {side} ({detail[:70]}){' ERROR ' + str(ea) if ea else ''}")
    print(f"   equal: {'yes' if (nh, hh) == (na, ha) and not ea and not eh else 'NO'}; rows received twice: {dup}")
    rc |= (nh, hh) != (na, ha) or side != "mirror"

files = sorted(glob.glob(os.path.join(args.mirror, "public.orders", "**", "*.parquet"), recursive=True), key=os.path.getsize)
big = files[-1] if files else None
backup = big + ".scale-backup" if big else None


def cut(n, at=200000):
    if n == at:
        os.truncate(big, os.path.getsize(big) // 3)


if "3" not in skip and big:
    print(f"\n3. the lake's data file ({os.path.getsize(big) >> 20} MB) cut to a third after 200,000 rows have arrived\n")
    shutil.copy2(big, backup)
    try:
        c = connect("SET quicksilver.route = auto")
        c.execute("SELECT count(*) FROM public.customers").fetchall()
        n, h, dup, s, err = stream(c, BIG, cut)
        side, reason, detail = decision(c) if not c.broken else ("?", "connection lost", "")
        print(f"   routed: {n} rows arrived, then: {'ERROR ' + (err.sqlstate or '') + ' ' + str(err).splitlines()[0][:150] if err else 'THE RESULT ENDED WITHOUT AN ERROR'}")
        if err and err.diag.message_detail:
            print(f"           DETAIL: {err.diag.message_detail[:200]}")
        print(f"   decided: {side} ({reason}); rows received twice: {dup}; connection {'lost' if c.broken else 'usable'}")
        ok = err is not None and 0 < n < rows and dup == 0 and not c.broken
        rc |= not ok
        if "4" not in skip and not c.broken:
            print("\n4. the same statement in a transaction block, the file still cut short\n")
            c.execute("SELECT count(*) FROM public.customers").fetchall()
            c.execute("BEGIN")
            t = time.monotonic()
            n2, h2, dup2, s2, err2 = stream(c, BIG)
            side, reason, detail = decision(c)
            try:
                c.execute("SAVEPOINT s"); c.execute("RELEASE s"); c.execute("COMMIT"); after = "SAVEPOINT, RELEASE and COMMIT worked"
            except psycopg.Error as e:
                after = "AFTER IT: " + str(e).splitlines()[0]
                rc |= 1
            print(f"   {n2} rows in {s2:.1f} s{', ERROR ' + str(err2).splitlines()[0][:120] if err2 else ', no error'}; "
                  f"answered by the {side} ({reason}: {detail[:100]})")
            print(f"   rows received twice: {dup2}; {after}")
            rc |= n2 != rows or dup2 != 0 or err2 is not None
        c.close()
    finally:
        shutil.copy2(backup, big)
        os.remove(backup)
    c = connect("SET quicksilver.route = auto")
    n, h, dup, s, err = stream(c, "SELECT id, tenant_id FROM public.orders WHERE tenant_id = 7")
    side, _, _ = decision(c)
    print(f"\n   file restored: the next routed statement returned {n} rows from the {side}{', ERROR ' + str(err) if err else ''}")
    rc |= side != "mirror" or err is not None

if "5" not in skip:
    print("\n5. a sort that DuckDB must spill (duckdb.max_memory = 300MB in this session), pg_duckdb confined\n")
    SORT = "SELECT id, tenant_id, amount, created_at FROM public.orders ORDER BY amount, created_at, tenant_id LIMIT 5 OFFSET 9000000"
    for label, sets in (("temporary directory not allowed", []),
                        ("temporary directory allowed", ["SET duckdb.allowed_directories = '{dirs},{tmp}'"])):
        c = connect()
        dirs = c.execute("SHOW duckdb.allowed_directories").fetchone()[0]
        tmp = c.execute("SHOW duckdb.temporary_directory").fetchone()[0]
        c.execute("SET duckdb.max_memory = '300MB'")
        for s in sets:
            c.execute(s.format(dirs=dirs, tmp=tmp))
        c.execute("SET quicksilver.route = auto")
        t = time.monotonic()
        try:
            r = c.execute(SORT).fetchall()
            out = f"{len(r)} rows"
        except psycopg.Error as e:
            r, out = None, "ERROR " + str(e).splitlines()[0][:160]
        s = time.monotonic() - t
        side, reason, detail = decision(c)
        hr = heap.execute(SORT).fetchall() if r is not None else None
        print(f"   {label}: {out} in {s:.1f} s, answered by the {side} ({reason}: {detail[:130]}); "
              f"equal to the heap's: {'yes' if r == hr else 'NO'}")
        rc |= r != hr
        c.close()

print("\n" + ("FAILED" if rc else "ok"))
sys.exit(1 if rc else 0)
