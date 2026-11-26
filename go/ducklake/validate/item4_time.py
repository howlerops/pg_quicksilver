"""Item 4: time SELECT status, count(*) FROM <file> GROUP BY status, single thread, best of 10,
alternating between files over ROUNDS rounds. Prints the encodings and compression of the
status column of each file first.

Usage: item4_time.py ROUNDS NAME=FILE [NAME=FILE ...]
"""
import statistics
import sys
import time

import duckdb

rounds = int(sys.argv[1])
files = [a.split("=", 1) for a in sys.argv[2:]]

con = duckdb.connect()
con.sql("SET threads = 1")
Q = "SELECT status, count(*) FROM read_parquet('{f}') GROUP BY status"

for name, f in files:
    print(f"== {name}: {f}")
    for r in con.sql(f"SELECT path_in_schema, encodings, compression, count(*) AS chunks, "
                     f"sum(total_compressed_size) AS bytes FROM parquet_metadata('{f}') "
                     f"WHERE path_in_schema = 'status' GROUP BY ALL").fetchall():
        print("   status:", r)
    print("   rows:", con.sql(f"SELECT count(*) FROM read_parquet('{f}')").fetchall()[0][0])

best = {name: [] for name, _ in files}
for rnd in range(rounds):
    for name, f in files:
        q = Q.format(f=f)
        con.sql(q).fetchall()  # warm
        times = []
        for _ in range(10):
            t0 = time.perf_counter()
            con.sql(q).fetchall()
            times.append((time.perf_counter() - t0) * 1000)
        best[name].append(min(times))
        print(f"round {rnd + 1} {name}: best-of-10 {min(times):.2f} ms")

print()
for name, _ in files:
    v = best[name]
    print(f"{name}: best {min(v):.2f} ms, median of rounds {statistics.median(v):.2f} ms, "
          f"rounds {[round(x, 2) for x in v]}")
if len(files) == 2:
    a, b = (best[files[0][0]], best[files[1][0]])
    print(f"ratio {files[1][0]}/{files[0][0]} of medians: "
          f"{statistics.median(b) / statistics.median(a):.3f}")
