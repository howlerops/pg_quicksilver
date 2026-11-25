import duckdb, time, os, json, resource
Q="/tmp/qsb"
# untimed prep: the staging tables, copied out of Postgres into their own file
if not os.path.exists(f"{Q}/native/wl.duckdb"):
    p=duckdb.connect(f"{Q}/native/wl.duckdb")
    p.execute(f"ATTACH 'host={Q}/sock port=5440 dbname=app user=postgres' AS pg (TYPE postgres, READ_ONLY)")
    for t in ("picks","payload_new","new_orders"): p.execute(f"CREATE TABLE {t} AS SELECT * FROM pg.wl.{t}")
    p.close()
c=duckdb.connect(f"{Q}/native/native.duckdb")
c.execute("SET threads=3"); c.execute("SET TimeZone='UTC'")
c.execute(f"ATTACH '{Q}/native/wl.duckdb' AS wl (READ_ONLY)")
tot=[0,0,0,0]; per=[]
t0=time.perf_counter()
for k in range(200):
    t=time.perf_counter()
    c.execute("BEGIN")
    a=c.execute("UPDATE orders SET status='returned' FROM wl.picks p WHERE p.kind='s' AND p.batch=? AND orders.id=p.id",[k]).fetchone()[0]
    b=c.execute("UPDATE orders SET payload=p.new_payload FROM wl.payload_new p WHERE p.batch=? AND orders.id=p.id",[k]).fetchone()[0]
    d=c.execute("DELETE FROM orders USING wl.picks p WHERE p.kind='d' AND p.batch=? AND orders.id=p.id",[k]).fetchone()[0]
    i=c.execute("INSERT INTO orders SELECT id,tenant_id,customer_id,status,amount,created_at,payload,note FROM wl.new_orders WHERE batch=?",[k]).fetchone()[0]
    c.execute("COMMIT")
    per.append(time.perf_counter()-t)
    for j,v in enumerate((a,b,d,i)): tot[j]+=v
t1=time.perf_counter()
c.close()
t2=time.perf_counter()
out=dict(apply_200_txns_s=t1-t0, close_checkpoint_s=t2-t1, total_s=t2-t0, totals=tot, txn_median_s=sorted(per)[100], txn_max_s=max(per),
         file_bytes=os.path.getsize(f"{Q}/native/native.duckdb"), peak_rss_kb=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
json.dump(out,open(f"{Q}/results/native_apply.json","w"),indent=1); print(out)
