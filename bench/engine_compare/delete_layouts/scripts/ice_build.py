#!/usr/bin/env python3
"""Build Iceberg tables (v2 positional-delete files, v3 deletion vectors) with DuckDB's iceberg extension.
Table created locally by COPY TO (FORMAT iceberg); deletes/inserts go through the local REST stub."""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *
ver = sys.argv[1]
WH = f"{DATA}/iceberg/wh"
T = f"orders_v{ver}"
st = state()
BASE = f"{M}/base/000001.parquet"
BASEDV = f"{M}/dv/000001.{st['dv_gen']['000001']:09d}.dv.parquet"
c = connect(2)
c.execute("LOAD iceberg")
t = time.time()
c.execute(f"""COPY (SELECT {COLS} FROM read_parquet('{BASE}')) TO '{WH}/default/{T}' (FORMAT iceberg, "format-version" {ver})""")
print("copy base s", round(time.time() - t, 1), flush=True)
c.execute("ATTACH 'wh' AS ice (TYPE iceberg, ENDPOINT 'http://127.0.0.1:18181', AUTHORIZATION_TYPE 'none')")
tb = f"ice.default.{T}"
c.execute(f"CREATE TEMP TABLE dead AS SELECT id FROM read_parquet('{BASE}', file_row_number=true) WHERE file_row_number IN (SELECT p FROM read_parquet('{BASEDV}'))")
t = time.time()
c.execute(f"DELETE FROM {tb} WHERE id IN (SELECT id FROM dead)")
print("delete 700k s", round(time.time() - t, 1), c.execute(f"select count(*) from {tb}").fetchall(), flush=True)
t = time.time()
for f in sorted(st["delta_files"]):
    p = f"{M}/delta/{f}"
    c.execute(f"DELETE FROM {tb} WHERE id IN (SELECT id FROM read_parquet('{p}'))")
    c.execute(f"INSERT INTO {tb} SELECT {COLS} FROM read_parquet('{p}')")
print("deltas s", round(time.time() - t, 1), c.execute(f"select count(*) from {tb}").fetchall(), flush=True)
for r in c.execute(f"select content, file_format, record_count, count(*) n from (select manifest_content content, file_format, record_count from iceberg_metadata({tb})) group by all order by 1,3 desc").fetchall():
    print(r)
cur = open(f"{WH}/default/{T}/metadata/current.txt").read().strip()
print("current metadata", cur)
import json
print("format-version", json.load(open(cur))["format-version"])
