import duckdb, time, os, json
Q="/tmp/qsb"
c=duckdb.connect(f"{Q}/native/native_bulk.duckdb"); c.execute("SET threads=3"); c.execute("SET TimeZone='UTC'")
c.execute(f"ATTACH '{Q}/native/wl.duckdb' AS wl (READ_ONLY)")
out={}
# one per-batch transaction, each statement timed, then rolled back
c.execute("BEGIN"); st={}
for name,sql in [("status_update","UPDATE orders SET status='returned' FROM wl.picks p WHERE p.kind='s' AND p.batch=0 AND orders.id=p.id"),
 ("payload_update","UPDATE orders SET payload=p.new_payload FROM wl.payload_new p WHERE p.batch=0 AND orders.id=p.id"),
 ("delete","DELETE FROM orders USING wl.picks p WHERE p.kind='d' AND p.batch=0 AND orders.id=p.id"),
 ("insert","INSERT INTO orders SELECT id,tenant_id,customer_id,status,amount,created_at,payload,note FROM wl.new_orders WHERE batch=0")]:
    t=time.perf_counter(); c.execute(sql).fetchall(); st[name]=time.perf_counter()-t
t=time.perf_counter(); c.execute("ROLLBACK"); st["rollback"]=time.perf_counter()-t
out["one_batch_statement_s"]=st
t0=time.perf_counter(); c.execute("BEGIN")
c.execute("UPDATE orders SET status='returned' FROM wl.picks p WHERE p.kind='s' AND orders.id=p.id")
c.execute("UPDATE orders SET payload=p.new_payload FROM wl.payload_new p WHERE orders.id=p.id")
c.execute("DELETE FROM orders USING wl.picks p WHERE p.kind='d' AND orders.id=p.id")
c.execute("INSERT INTO orders SELECT id,tenant_id,customer_id,status,amount,created_at,payload,note FROM wl.new_orders")
c.execute("COMMIT"); t1=time.perf_counter(); c.close(); t2=time.perf_counter()
out.update(bulk_one_txn_s=t1-t0, close_checkpoint_s=t2-t1, file_bytes=os.path.getsize(f"{Q}/native/native_bulk.duckdb"))
json.dump(out,open(f"{Q}/results/native_bulk_apply.json","w"),indent=1); print(out)
