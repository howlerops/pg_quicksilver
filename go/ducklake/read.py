"""Read times: the mirror's generated view SQL against the published DuckLake
table, same data, same engine, one thread. Run under `taskset -c 1`.

  read.py VIEW_SQL_FILE CATALOG  ->  JSON on stdout

Warm: one untimed run, then five timed; best and median reported. Each engine
gets its own connection, and the answers must be equal.
"""
import json
import statistics
import sys
import time

import duckdb

view = open(sys.argv[1]).read()
QUERIES = {
    "count_star": "select count(*) from {t}",
    "count_by_status": "select status, count(*) from {t} group by status order by status",
    "sum_amount": "select sum(amount) from {t}",
    "month_filter": "select count(*), sum(amount) from {t} where created_at >= timestamptz '2024-06-01 00:00:00+00' "
                    "and created_at < timestamptz '2024-07-01 00:00:00+00'",
}


def con():
    c = duckdb.connect()
    c.execute("SET threads = 1")
    c.execute("SET TimeZone = 'UTC'")
    return c


v = con()
v.execute("CREATE VIEW orders AS " + view)
l = con()
l.execute("LOAD ducklake")
l.execute(f"ATTACH 'ducklake:{sys.argv[2]}' AS lake (READ_ONLY)")
out = {}
for name, q in QUERIES.items():
    row = {}
    answers = []
    for label, c, t in (("view", v, "orders"), ("ducklake", l, "lake.orders")):
        sql = q.format(t=t)
        answers.append(c.execute(sql).fetchall())
        ms = []
        for _ in range(5):
            t0 = time.perf_counter()
            c.execute(sql).fetchall()
            ms.append((time.perf_counter() - t0) * 1000)
        row[label] = {"best": round(min(ms), 2), "median": round(statistics.median(ms), 2)}
    row["equal"] = answers[0] == answers[1]
    row["answer"] = str(answers[0][:2])
    out[name] = row
print(json.dumps(out))
