#!/usr/bin/env python3
"""Lake vs heap for the Part 1 tables, one line per table.

For each table the same aggregate is computed twice: on the PostgreSQL heap
(psycopg, the source the mirror claims to copy) and on the DuckLake table
(python duckdb, the catalog the sidecar publishes). The aggregate is count(*),
sums of the numeric columns, sums of the timestamps as microseconds since the
epoch, and an md5 over the text of every row in key order. Equal tuples mean the
lake holds the same typed values as the heap, not only the same row count.

  fv_compare.py --port 5800 --tables fv.events,fv.accounts,...  [--wait-lsn]
Exit 1 if any table differs.
"""
import argparse, subprocess, sys, time
import duckdb, psycopg

# table: (key column, numeric columns, timestamp columns, text columns in row text)
TABLES = {
    "public.orders": ("id::text", ["amount"], ["created_at"], ["id::text", "customer_id::text", "status", "coalesce(note, '<n>')"]),
    "public.customers": ("id::text", [], [], ["id::text", "name", "region"]),
    "public.qs_types": ("id::text", ["n", "n2", "n4"], ["ts", "ts2", "tz", "tz2"], ["id::text", "coalesce(t, '<n>')", "coalesce(u::text, '<n>')"]),
    "ex.t_num": ("id::text", ["n", "n0"], [], ["id::text", "coalesce(nu::text, '<n>')"]),
    "ex.t_str": ("id::text", [], [], ["id::text", "coalesce(t, '<n>')", "coalesce(u::text, '<n>')"]),
    "ex.t_time": ("id::text", [], ["ts", "tz"], ["id::text", "coalesce(d::text, '<n>')"]),
    "ex.t_doc": ("id::text", [], [], ["id::text", "coalesce(j::text, '<n>')"]),
    "ex.customers": ("id::text", [], [], ["id::text", "name", "region"]),
    "ex.orders": ("id::text", ["amount"], ["placed"], ["id::text", "cust_id::text", "status", "coalesce(note, '<n>')"]),
    "fv.events": ("id::text", ["amount"], ['"at"'], ["id::text", "coalesce(note, '<n>')"]),
    "fv.accounts": ("code", ["balance"], ["opened"], ["code", "coalesce(label, '<n>')"]),
    "fv.lookup": ("id::text", [], [], ["id::text", "name"]),
}


TS_TEXT = False   # set by --ts-text: the lake's timestamps are VARCHAR


def aggregate_sql(table, engine):
    key, nums, tss, texts = TABLES[table]
    parts = ["count(*)::text"]
    for c in nums:
        parts.append(f"sum(round({c} * 100))::text")
    for c in tss:
        if TS_TEXT:
            # the lake holds the timestamps as the text PostgreSQL prints (QS_TEMPORAL_TYPES=0):
            # compare that text, row by row, in key order
            parts.append(f"md5(string_agg(coalesce({c}::text, '<n>'), chr(10) ORDER BY ({key})))")
            continue
        # finite values as microseconds since the epoch; infinities counted apart
        # (PostgreSQL sums an infinite epoch to NaN, DuckDB keeps infinity)
        if engine == "pg":
            parts.append(f"sum(floor(extract(epoch from {c}) * 1000000)) FILTER (WHERE isfinite({c}))::text")
        else:
            parts.append(f"sum(epoch_us({c})) FILTER (WHERE isfinite({c}))::text")
        parts.append(f"count(*) FILTER (WHERE NOT isfinite({c}))::text")
    concat = " || '|' || ".join(f"({t})" for t in texts)
    if engine == "pg":
        parts.append(f"md5(string_agg({concat}, chr(10) ORDER BY ({key}) COLLATE \"C\"))")
    else:
        parts.append(f"md5(string_agg({concat}, chr(10) ORDER BY ({key})))")
    return "SELECT " + ", ".join(parts) + f" FROM {table}"


def heap_row(cur, table):
    cur.execute(aggregate_sql(table, "pg"))
    return tuple(cur.fetchone())


def lake_row(con, table):
    return tuple(con.execute(aggregate_sql(table, "duck").replace(f" FROM {table}", f" FROM lake.{table}")).fetchone())


def settle(port, tables, timeout=600):
    """Wait until each table's mirror has applied the primary's WAL position."""
    lsn = subprocess.run(["su", "postgres", "-c", f"{PG}/psql -X -h /tmp -p {port} -U postgres -d app -Atc 'select pg_current_wal_lsn()'"],
                         capture_output=True, text=True).stdout.strip()
    deadline = time.time() + timeout
    for t in tables:
        while True:
            out = subprocess.run([QUERY, "-mirror", MIRROR, "-table", t, "-lsn"], capture_output=True, text=True).stdout.strip()
            if out and lsn_ge(out, lsn):
                break
            if time.time() > deadline:
                raise SystemExit(f"settle timeout on {t}: applied {out} primary {lsn}")
            time.sleep(0.2)
    return lsn


def lsn_ge(a, b):
    def n(x):
        h, l = x.split("/")
        return (int(h, 16) << 32) | int(l, 16)
    return n(a) >= n(b)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=5800)
    ap.add_argument("--tables", required=True)
    ap.add_argument("--base", default="/var/lib/postgresql/qsfinal")
    ap.add_argument("--mirror", default=None, help="mirror directory (default BASE/mirror)")
    ap.add_argument("--no-settle", action="store_true")
    ap.add_argument("--ts-text", action="store_true", help="the lake holds timestamps as text (QS_TEMPORAL_TYPES=0)")
    a = ap.parse_args()
    PG = "/var/lib/postgresql/qspgd/pg17/usr/lib/postgresql/17/bin"
    MIRROR = a.mirror or f"{a.base}/mirror"
    QUERY = f"{a.base}/qs-query"
    TS_TEXT = a.ts_text
    tables = a.tables.split(",")
    if not a.no_settle:
        settle(a.port, tables)
    con = duckdb.connect()
    con.execute("LOAD ducklake")
    con.execute("LOAD sqlite")
    con.execute(f"ATTACH 'ducklake:sqlite:{MIRROR}/_ducklake/catalog.sqlite' AS lake (READ_ONLY)")
    pg = psycopg.connect(host="/tmp", port=a.port, user="postgres", dbname="app", autocommit=True)
    bad = 0
    for t in tables:
        h = heap_row(pg.cursor(), t)
        l = lake_row(con, t)
        same = h == l
        bad += not same
        print(f"{t}: {'EQUAL' if same else 'DIFFERENT'} heap={h[0]} lake={l[0]} " + ("" if same else f"heap={h} lake={l}"))
    print(f"tables {len(tables)} different {bad}")
    sys.exit(1 if bad else 0)
