"""Read times: the mirror's published view SQL (snapshot.json) against the
DuckLake table the Go publisher maintains, same files, same engine, one
thread. Run under `taskset -c 1`.

  read_bench.py MIRROR_ROOT  ->  a table on stdout

Warm: one untimed run, then five timed; best and median reported. Each side
gets its own connection, and the answers must be equal.

The view's SQL is NOT snapshot.json's "sql" as it stands: that names files
relative to the table directory. It is rendered under MIRROR_ROOT by
`qs-query -published -root`, which is the one place that rule is written
(mirror.Published.Render). The binary is $QS_QUERY, else the one setup.sh
built beside the mirror, else one on PATH, else `go run` from this module.
"""
import json
import os
import shutil
import statistics
import subprocess
import sys
import time

import duckdb

root = os.path.abspath(sys.argv[1])
pub = json.load(open(f"{root}/public.orders/snapshot.json"))


def qs_query():
    beside = os.path.join(os.path.dirname(root), "qs-query")
    for exe in (os.environ.get("QS_QUERY"), beside if os.access(beside, os.X_OK) else None, shutil.which("qs-query")):
        if exe:
            return [exe], None
    module = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
    return ["go", "run", "./cmd/qs-query"], module


def published_sql():
    """The published view, with its files named under root."""
    cmd, cwd = qs_query()
    out = subprocess.run(cmd + ["-mirror", root, "-table", "public.orders", "-published", "-root", root],
                         cwd=cwd, check=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True).stdout
    # The first line is a header comment; the rest is one SELECT.
    return "\n".join(line for line in out.splitlines() if not line.startswith("-- quicksilver mirror"))


QUERIES = {
    "count(*)": "select count(*) from {t}",
    "group by status": "select status, count(*) from {t} group by status order by status",
    "sum(amount)": "select sum(amount) from {t}",
    "one-month range": "select count(*), sum(amount) from {t} where created_at >= timestamptz '2024-06-01 00:00:00+00' "
                       "and created_at < timestamptz '2024-07-01 00:00:00+00'",
}


def con():
    c = duckdb.connect()
    c.execute("SET threads = 1")
    c.execute("SET TimeZone = 'UTC'")
    return c


v = con()
v.execute("CREATE VIEW orders AS " + published_sql())
l = con()
l.execute("LOAD ducklake")
l.execute("LOAD sqlite")
l.execute(f"ATTACH 'ducklake:sqlite:{root}/_ducklake/catalog.sqlite' AS lake (READ_ONLY)")
files = l.execute("select count(*), count(delete_file) from ducklake_list_files('lake', 'orders', schema => 'public')").fetchone()
# The effective applied LSN: the mirror's watermark where it speaks for the
# manifest the lake table was published from (ducklake.FreshnessSQL).
lsn = l.execute("select coalesce(w.applied_lsn, t.applied_lsn) from __ducklake_metadata_lake.qs_mirror_table t "
                "left join __ducklake_metadata_lake.qs_mirror_watermark w "
                "on w.table_name = t.table_name and w.manifest_lsn = t.applied_lsn "
                "where t.table_name = 'public.orders'").fetchone()[0]
print(f"view: snapshot_seq {pub['snapshot_seq']} at {pub['applied_lsn']}, {len(pub['files'])} files named")
print(f"lake: at {lsn}, {files[0]} data files, {files[1]} with a delete file")
print(f"{'query':18} {'view best/median ms':>22} {'lake best/median ms':>22}  answers")
ok = True
for name, q in QUERIES.items():
    cells, answers = [], []
    for c, t in ((v, "orders"), (l, "lake.public.orders")):
        sql = q.format(t=t)
        answers.append(c.execute(sql).fetchall())
        ms = []
        for _ in range(5):
            t0 = time.perf_counter()
            c.execute(sql).fetchall()
            ms.append((time.perf_counter() - t0) * 1000)
        cells.append(f"{min(ms):.1f} / {statistics.median(ms):.1f}")
    same = answers[0] == answers[1]
    ok = ok and same
    print(f"{name:18} {cells[0]:>22} {cells[1]:>22}  {'equal' if same else 'DIFFERENT'}  {str(answers[1][0])[:60]}")
sys.exit(0 if ok else 1)
