"""What DuckLake accepts as a delete, tried in the order the question was asked.

  python3 probe.py WORK      (WORK/mirror must exist: run `run.py functional` first)

Prints, for one base file and its vector:
  1. whether any function registers an existing delete file
  2. the mirror's vector registered as it is, by a row in ducklake_delete_file
  3. the same positions in DuckLake's layout, and what the reader checks
  4. positions written into the catalog's inlined-delete table
"""
import glob
import json
import os
import shutil
import sys

import duckdb

work = os.path.abspath(sys.argv[1])
M = work + "/mirror/public.orders"
D = work + "/probe"
shutil.rmtree(D, ignore_errors=True)
os.makedirs(D)
MD = "__ducklake_metadata_lake"
st = json.load(open(M + "/state.json"))
base = st["base_files"][0]
stem = base[:-8]
if not st.get("dv_gen", {}).get(stem):
    sys.exit("the base file has no vector; run a few applies first")
data = f"{M}/base/{base}"
dv = glob.glob(f"{M}/dv/{stem}.{st['dv_gen'][stem]:09d}.dv.parquet")[0]

c = duckdb.connect()
c.execute("LOAD ducklake")
print("1. functions:", [r[0] for r in c.execute(
    "select distinct function_name from duckdb_functions() where function_name like 'ducklake_%' "
    "and (function_name like '%add%' or function_name like '%delete%') order by 1").fetchall()])
c.execute(f"ATTACH 'ducklake:{D}/c.db' AS lake (DATA_PATH '{D}/data/')")
c.execute("create table lake.orders(id varchar, amount decimal(12,2), created_at timestamptz, status varchar, note varchar)")
c.execute(f"CALL ducklake_add_data_files('lake', 'orders', '{data}')")
fid = c.execute(f"select data_file_id from {MD}.ducklake_data_file").fetchone()[0]
n = c.execute(f"select count(*) from '{dv}'").fetchone()[0]
truth = c.execute(f"select count(id), sum(amount) from read_parquet('{data}', file_row_number = true) "
                  f"where file_row_number not in (select p from '{dv}')").fetchall()
print("   truth:", truth, " dead positions:", n)


def snapshot(files):
    c.execute(f"insert into {MD}.ducklake_snapshot select snapshot_id + 1, now(), schema_version, next_catalog_id, "
              f"next_file_id + {files} from {MD}.ducklake_snapshot order by snapshot_id desc limit 1")
    return c.execute(f"select snapshot_id, next_file_id - {files} from {MD}.ducklake_snapshot "
                     "order by 1 desc limit 1").fetchone()


def show(label):
    for what, q in (("count(*)", "select count(*) from lake.orders"),
                    ("scan", "select count(id), sum(amount) from lake.orders")):
        try:
            r = c.execute(q).fetchall()
            ok = r == truth if what == "scan" else r[0][0] == truth[0][0]
            print(f"   {label}: {what} -> {r} {'OK' if ok else 'WRONG'}")
        except duckdb.Error as e:
            print(f"   {label}: {what} -> ERROR {e}")


s, did = snapshot(1)
c.execute(f"insert into {MD}.ducklake_delete_file (delete_file_id, table_id, begin_snapshot, data_file_id, path, "
          f"path_is_relative, format, delete_count, file_size_bytes) values ({did}, 1, {s}, {fid}, '{dv}', false, "
          f"'parquet', {n}, {os.path.getsize(dv)})")
print("2. the mirror's vector (one column, p) registered as-is:")
show("as-is")

print("3. converted:")
for label, sel in (("(pos) only", "p as pos"),
                   ("(file_path, pos)", f"'{data}' as file_path, p as pos"),
                   ("(file_path, pos), file_path = 'nonsense'", "'nonsense' as file_path, p as pos")):
    out = f"{D}/conv-delete.parquet"
    c.execute(f"copy (select {sel} from '{dv}') to '{out}' (format parquet)")
    c.execute(f"update {MD}.ducklake_delete_file set path = '{out}'")
    show(label)

print("4. inlined in the catalog instead of a file:")
c.execute(f"delete from {MD}.ducklake_delete_file")
s, _ = snapshot(0)
c.execute(f"create table if not exists {MD}.ducklake_inlined_delete_1(file_id bigint, row_id bigint, begin_snapshot bigint)")
c.execute(f"insert into {MD}.ducklake_inlined_delete_1 select {fid}, p, {s} from '{dv}'")
show("inlined")
