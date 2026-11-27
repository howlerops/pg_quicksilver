"""Time a key lookup and a join over the mirrors TestWriteUUIDBenchMirrors writes.

usage: taskset -c 0 python3 uuid_bench.py <QS_TYPES_BENCH_DIR>"""
import glob, os, statistics, sys, time
import duckdb

root = sys.argv[1]
print(f"duckdb {duckdb.__version__}, threads=1, cpus={sorted(os.sched_getaffinity(0))}")
for variant in ("old", "new-nobloom", "new"):
    d = f"{root}/{variant}"
    keys = open(f"{d}/keys.txt").read().split()
    size = {t: sum(os.path.getsize(f) for f in glob.glob(f"{d}/public.{t}/base/*.parquet")) for t in ("orders", "customers")}
    con = duckdb.connect()
    con.execute("SET threads = 1")
    for t in ("orders", "customers"):
        con.execute(f"CREATE VIEW {t} AS " + open(f"{d}/public.{t}/view.sql").read())
    qs = {
        "key lookup": [f"SELECT * FROM orders WHERE id = '{k}'" for k in keys],
        "fk lookup": [f"SELECT count(*), sum(qty) FROM orders WHERE customer = (SELECT customer FROM orders WHERE id = '{keys[0]}')"],
        "join + group": ["SELECT c.tier, count(*), sum(o.qty) FROM orders o JOIN customers c ON o.customer = c.id GROUP BY 1 ORDER BY 1"],
        "count(distinct id)": ["SELECT count(DISTINCT id) FROM orders"],
    }
    typ = con.execute("SELECT typeof(id) FROM orders LIMIT 1").fetchone()[0]
    print(f"\n{variant}: orders {size['orders'] / 1e6:.1f} MB, customers {size['customers'] / 1e6:.1f} MB, id is {typ}")
    for name, stmts in qs.items():
        ts, res = [], None
        for i in range(5):
            q = stmts[i % len(stmts)]
            t0 = time.perf_counter()
            res = con.execute(q).fetchall()
            ts.append((time.perf_counter() - t0) * 1000)
        print(f"  {name:20s} best {min(ts):8.1f} ms   median {statistics.median(ts):8.1f} ms   -> {str(res[:2])[:70]}")
    if variant == "new":
        k = keys[0]
        plan = con.execute(f"EXPLAIN ANALYZE SELECT * FROM orders WHERE id = '{k}'").fetchall()[0][1]
        for line in plan.splitlines():
            if "Bloom" in line or "Row Group" in line or "row group" in line.lower():
                print("   ", line.strip())
