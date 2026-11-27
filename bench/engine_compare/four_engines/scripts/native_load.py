import duckdb, time, os, resource, json
Q="/tmp/qsb"
t0=time.perf_counter()
c=duckdb.connect(f"{Q}/native/native.duckdb")
c.execute("SET threads=3"); c.execute("SET TimeZone='UTC'")
c.execute(f"ATTACH 'host={Q}/sock port=5440 dbname=app user=postgres' AS pg (TYPE postgres, READ_ONLY)")
t1=time.perf_counter()
c.execute("CREATE TABLE customers AS SELECT * FROM pg.public.customers")
t2=time.perf_counter()
c.execute("CREATE TABLE orders AS SELECT * FROM pg.public.orders")
t3=time.perf_counter()
c.execute("DETACH pg"); c.execute("CHECKPOINT")
t4=time.perf_counter()
desc=c.execute("describe orders").fetchall()
n=c.execute("select count(*) from orders").fetchone()[0]
c.close()
out=dict(customers_s=t2-t1, orders_s=t3-t2, checkpoint_s=t4-t3, total_s=time.perf_counter()-t0, rows=n,
 file_bytes=os.path.getsize(f"{Q}/native/native.duckdb"), peak_rss_kb=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
 schema=[(d[0],d[1]) for d in desc], duckdb=duckdb.__version__)
print(json.dumps(out,indent=1)); json.dump(out,open(f"{Q}/results/native_load.json","w"),indent=1)
