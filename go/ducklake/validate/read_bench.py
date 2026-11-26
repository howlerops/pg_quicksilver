"""Read times: the mirror's published view SQL (snapshot.json) against the
DuckLake table the Go publisher maintains, same files, same engine, one
thread. Run under `taskset -c 1`.

  read_bench.py MIRROR_ROOT  ->  a table on stdout

Warm: one untimed run, then five timed; best and median reported. Each side
gets its own connection, and the answers must be equal.
"""
import json
import statistics
import sys
import time

import duckdb

root = sys.argv[1]
pub = json.load(open(f"{root}/public.orders/snapshot.json"))
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
v.execute("CREATE VIEW orders AS " + pub["sql"])
l = con()
l.execute("LOAD ducklake")
l.execute("LOAD sqlite")
l.execute(f"ATTACH 'ducklake:sqlite:{root}/_ducklake/catalog.sqlite' AS lake (READ_ONLY)")
files = l.execute("select count(*), count(delete_file) from ducklake_list_files('lake', 'orders', schema => 'public')").fetchone()
lsn = l.execute("select applied_lsn from __ducklake_metadata_lake.qs_mirror_table").fetchone()[0]
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
