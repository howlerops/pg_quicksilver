#!/usr/bin/env python3
"""Export the Postgres events table to Parquet — the columnar mirror.

Writes it the way Quicksilver would: natural (time-ordered) clustering so
zone maps prune on `ts`, zstd compression, 1M-row row groups.
"""
import duckdb, time, os, sys
OUT = sys.argv[1] if len(sys.argv) > 1 else "/var/lib/postgresql/qsbench/parquet"
os.makedirs(OUT, exist_ok=True)
os.makedirs("/var/lib/postgresql/qsbench/tmp", exist_ok=True)

con = duckdb.connect()
con.execute("SET memory_limit='8GB'; SET temp_directory='/var/lib/postgresql/qsbench/tmp';")
con.execute("INSTALL postgres; LOAD postgres;")
con.execute("ATTACH 'dbname=postgres host=/tmp port=5433 user=postgres' AS pg (TYPE postgres, READ_ONLY);")

t0 = time.time()
con.execute(f"""
  COPY (SELECT * FROM pg.public.events)
  TO '{OUT}/events.parquet'
  (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000)
""")
dt = time.time() - t0
size = os.path.getsize(f"{OUT}/events.parquet")
print(f"exported in {dt:.1f}s -> {size/1e9:.2f} GB")

# per-column compressed sizes: this is what drives projection-pruning wins
rows = con.execute(f"""
  SELECT path_in_schema, sum(total_compressed_size) b
  FROM parquet_metadata('{OUT}/events.parquet') GROUP BY 1 ORDER BY b DESC
""").fetchall()
total = sum(r[1] for r in rows)
print(f"\n{'column':<14}{'compressed':>12}{'% of file':>11}")
for name, b in rows:
    print(f"{name:<14}{b/1e6:>10.1f}MB{100*b/total:>10.1f}%")
print(f"{'TOTAL':<14}{total/1e6:>10.1f}MB")
