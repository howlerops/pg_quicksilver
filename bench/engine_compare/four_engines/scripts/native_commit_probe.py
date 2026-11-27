import duckdb, time, os, json, shutil, sys
Q="/tmp/qsb"; out={}
for label,thr in (("default_checkpoint_threshold",None),("checkpoint_threshold_4GB","4GB")):
    shutil.copy(f"{Q}/native/native_A.duckdb", f"{Q}/native/probe.duckdb")
    c=duckdb.connect(f"{Q}/native/probe.duckdb"); c.execute("SET threads=3")
    if thr: c.execute(f"SET checkpoint_threshold='{thr}'")
    out[label+"_setting"]=c.execute("select current_setting('checkpoint_threshold')").fetchone()[0]
    c.execute(f"ATTACH '{Q}/native/wl.duckdb' AS wl (READ_ONLY)")
    rows=[]
    n=5 if thr is None else 200
    t00=time.perf_counter()
    for k in range(n):
        t=time.perf_counter(); c.execute("BEGIN")
        c.execute("UPDATE orders SET status='returned' FROM wl.picks p WHERE p.kind='s' AND p.batch=? AND orders.id=p.id",[k])
        c.execute("UPDATE orders SET payload=p.new_payload FROM wl.payload_new p WHERE p.batch=? AND orders.id=p.id",[k])
        c.execute("DELETE FROM orders USING wl.picks p WHERE p.kind='d' AND p.batch=? AND orders.id=p.id",[k])
        c.execute("INSERT INTO orders SELECT id,tenant_id,customer_id,status,amount,created_at,payload,note FROM wl.new_orders WHERE batch=?",[k])
        t1=time.perf_counter(); c.execute("COMMIT"); t2=time.perf_counter()
        rows.append((t1-t,t2-t1))
    t01=time.perf_counter()
    wal=os.path.getsize(f"{Q}/native/probe.duckdb.wal") if os.path.exists(f"{Q}/native/probe.duckdb.wal") else 0
    c.close(); t02=time.perf_counter()
    out[label]=dict(txns=n, total_s=t01-t00, statements_s_median=sorted(r[0] for r in rows)[n//2], commit_s_median=sorted(r[1] for r in rows)[n//2],
                    commit_s_max=max(r[1] for r in rows), wal_bytes_before_close=wal, close_s=t02-t01, file_bytes=os.path.getsize(f"{Q}/native/probe.duckdb"))
json.dump(out,open(f"{Q}/results/native_commit_probe.json","w"),indent=1); print(json.dumps(out,indent=1))
