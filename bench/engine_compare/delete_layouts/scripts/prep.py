#!/usr/bin/env python3
"""Build the alternative layouts from the copied mirror. prep.py <step>"""
import sys, os, json, time, shutil
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *

step = sys.argv[1]
c = connect(2)
c.execute("SET preserve_insertion_order = true")
st = state()
BASE = f"{M}/base/000001.parquet"
BASEDV = f"{M}/dv/000001.{st['dv_gen']['000001']:09d}.dv.parquet"
PQ = "(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 65536)"


def dvof(f):
    k = f.replace(".parquet", "")
    g = st["dv_gen"].get(k)
    return f"{M}/dv/{k}.{g:09d}.dv.parquet" if g else None


if step == "dist":
    print("base dv entries / base rows:", c.execute(f"select count(*) from read_parquet('{BASEDV}')").fetchall()[0], st["base_rows"])
    print(c.sql(f"""
      with rg as (select p // 65536 as rg, count(*) n from read_parquet('{BASEDV}') group by 1)
      select count(*) as row_groups_with_dead, min(n), max(n), round(avg(n)) avg, round(stddev(n)) sd,
             round(100.0*min(n)/65536,2) min_pct, round(100.0*max(n)/65536,2) max_pct from rg"""))
    print("row groups in base:", c.execute(f"select count(distinct row_group_id) from parquet_metadata('{BASE}')").fetchall()[0])
    print("per 16th of file:")
    print(c.sql(f"select p // 625000 as part16, count(*) dead from read_parquet('{BASEDV}') group by 1 order by 1"))
    print("deciles of gap between consecutive dead positions:")
    print(c.sql(f"""with g as (select p - lag(p) over (order by p) gap from read_parquet('{BASEDV}'))
                    select quantile_cont(gap,[0.1,0.5,0.9,0.99]) q, max(gap) from g"""))
    print("delta files: rows / dead")
    for f in st["delta_files"]:
        d = dvof(f)
        n = c.execute(f"select count(*) from read_parquet('{d}')").fetchall()[0][0] if d else 0
        print(" ", f, st["file_rows"][f], n)

elif step == "compact":
    os.makedirs(f"{DATA}/compact", exist_ok=True)
    c.execute("CREATE VIEW orders AS " + base_view())
    t = time.time()
    c.execute(f"COPY (SELECT * FROM orders) TO '{DATA}/compact/orders.parquet' {PQ}")
    print("compact rewrite s:", round(time.time() - t, 1), os.path.getsize(f"{DATA}/compact/orders.parquet"))
    t = time.time()
    c.execute(f"COPY (SELECT * FROM orders ORDER BY created_at) TO '{DATA}/compact/orders_sorted.parquet' {PQ}")
    print("compact sorted rewrite s:", round(time.time() - t, 1))

elif step == "part":
    for n in (16, 64):
        os.makedirs(f"{DATA}/part{n}", exist_ok=True)
        per = st["base_rows"] // n
        for k in range(n):
            lo, hi = k * per, ((k + 1) * per if k < n - 1 else st["base_rows"])
            c.execute(f"COPY (SELECT {COLS} FROM read_parquet('{BASE}', file_row_number=true) WHERE file_row_number >= {lo} "
                      f"AND file_row_number < {hi} ORDER BY file_row_number) TO '{DATA}/part{n}/base_{k:02d}.parquet' {PQ}")
            cnt = c.execute(f"select count(*) from read_parquet('{BASEDV}') where p >= {lo} and p < {hi}").fetchall()[0][0]
            if cnt:
                c.execute(f"COPY (SELECT p - {lo} AS p FROM read_parquet('{BASEDV}') WHERE p >= {lo} AND p < {hi} ORDER BY p) "
                          f"TO '{DATA}/part{n}/dv_{k:02d}.parquet' (FORMAT parquet)")
        print(n, "files; with dv:", len(glob.glob(f"{DATA}/part{n}/dv_*.parquet")))
    # hypothetical skew: 625000-row last file loses every row -> use 700k? file has only 625k rows; kill all of them
    c.execute(f"COPY (SELECT range AS p FROM range(625000)) TO '{DATA}/part16/skew_dv_15.parquet' (FORMAT parquet)")

elif step == "mask":
    os.makedirs(f"{DATA}/mask", exist_ok=True)
    files = [("base/000001.parquet", "000001.parquet", st["base_rows"])] + \
            [(f"delta/{f}", f, st["file_rows"][f]) for f in st["delta_files"]]
    for rel, f, n in files:
        d = dvof(f)
        if not d:
            continue
        c.execute(f"""COPY (SELECT d.p IS NULL AS live FROM range({n}) r LEFT JOIN read_parquet('{d}') d ON d.p = r.range
                      ORDER BY r.range) TO '{DATA}/mask/{f.replace('.parquet', '.live.parquet')}' {PQ}""")
        print(f, n, os.path.getsize(f"{DATA}/mask/{f.replace('.parquet', '.live.parquet')}"))

elif step == "ducklake":
    c.execute("LOAD ducklake")
    root = f"{DATA}/ducklake_a"
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root)
    c.execute(f"ATTACH 'ducklake:{root}/meta.ducklake' AS lake (DATA_PATH '{root}/files/')")
    c.execute(f"CREATE TABLE lake.orders AS SELECT {COLS} FROM read_parquet('{BASE}') LIMIT 0")
    log = {}
    t = time.time()
    c.execute(f"INSERT INTO lake.orders SELECT {COLS} FROM read_parquet('{BASE}')")
    log["insert_base_s"] = round(time.time() - t, 2)
    c.execute(f"CREATE TEMP TABLE dead AS SELECT id FROM read_parquet('{BASE}', file_row_number=true) WHERE file_row_number IN (SELECT p FROM read_parquet('{BASEDV}'))")
    t = time.time()
    c.execute("DELETE FROM lake.orders WHERE id IN (SELECT id FROM dead)")
    log["delete_700k_s"] = round(time.time() - t, 2)
    # deltas in sequence order, as updates: retire the old version of each id, then append the new one
    t = time.time()
    for f in sorted(st["delta_files"]):
        c.execute(f"DELETE FROM lake.orders WHERE id IN (SELECT id FROM read_parquet('{M}/delta/{f}'))")
        c.execute(f"INSERT INTO lake.orders SELECT {COLS} FROM read_parquet('{M}/delta/{f}')")
    log["deltas_s"] = round(time.time() - t, 2)
    print(log)
    print(c.execute("select count(*) from lake.orders").fetchall()[0])
    c.execute("DETACH lake")
    meta = connect(1)
    meta.execute(f"ATTACH '{root}/meta.ducklake' AS m (READ_ONLY)")
    print(meta.sql("select data_file_id, path, record_count, file_size_bytes from m.ducklake_data_file where end_snapshot is null"))
    print(meta.sql("select delete_file_id, data_file_id, path, delete_count, file_size_bytes, format from m.ducklake_delete_file where end_snapshot is null"))
    try:
        print(meta.sql("select table_name from information_schema.tables where table_catalog='m' and table_name like '%inlin%'"))
    except Exception as e:
        print(e)

elif step in ("ducklake_merge", "ducklake_rewrite"):
    src, dst = ("a", "b") if step == "ducklake_merge" else ("b", "c")
    shutil.rmtree(f"{DATA}/ducklake_{dst}", ignore_errors=True)
    shutil.copytree(f"{DATA}/ducklake_{src}", f"{DATA}/ducklake_{dst}")
    root = f"{DATA}/ducklake_{dst}"
    c.execute("LOAD ducklake")
    c.execute(f"ATTACH 'ducklake:{root}/meta.ducklake' AS lake (DATA_PATH '{root}/files/', OVERRIDE_DATA_PATH true)")
    t = time.time()
    if step == "ducklake_merge":
        print(c.execute("CALL ducklake_merge_adjacent_files('lake')").fetchall())
    else:
        for stmt in sys.argv[2:]:
            print(stmt, "->", c.execute(stmt).fetchall())
    print(step, "s:", round(time.time() - t, 1))
    print(c.execute("select count(*) from lake.orders").fetchall()[0])
    c.execute("DETACH lake")
    meta = connect(1)
    meta.execute(f"ATTACH '{root}/meta.ducklake' AS m (READ_ONLY)")
    print(meta.sql("select data_file_id, path, record_count, file_size_bytes from m.ducklake_data_file where end_snapshot is null"))
    print(meta.sql("select delete_file_id, data_file_id, delete_count, file_size_bytes from m.ducklake_delete_file where end_snapshot is null"))
