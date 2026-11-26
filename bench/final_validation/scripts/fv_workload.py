# Copy of bench/engine_compare/four_engines/scripts/workload.py with the directory and the port taken from the environment
# (BENCH_DIR, BENCH_PRIMARY). The change workload is unchanged: 200 transactions over wl.picks, wl.payload_new, wl.new_orders.
import psycopg, time, json
import os
Q=os.environ.get("BENCH_DIR", "/var/lib/postgresql/qsfinal/bench")
c=psycopg.connect(f"host=/tmp port={os.environ['BENCH_PRIMARY']} dbname=app user=postgres")
c.autocommit=True
def lsn(): return c.execute("select pg_current_wal_lsn()::text").fetchone()[0]
out={"lsn_start":lsn(),"t_start":time.time(),"txns":[]}
counts=[0,0,0,0]
for k in range(200):
    if k==199: out["lsn_before_last_txn"]=lsn()
    t=time.perf_counter()
    with c.transaction():
        a=c.execute("UPDATE public.orders o SET status='returned' FROM wl.picks p WHERE p.kind='s' AND p.batch=%s AND o.id=p.id",(k,)).rowcount
        b=c.execute("UPDATE public.orders o SET payload=p.new_payload FROM wl.payload_new p WHERE p.batch=%s AND o.id=p.id",(k,)).rowcount
        d=c.execute("DELETE FROM public.orders o USING wl.picks p WHERE p.kind='d' AND p.batch=%s AND o.id=p.id",(k,)).rowcount
        i=c.execute("INSERT INTO public.orders SELECT id,tenant_id,customer_id,status,amount,created_at,payload,note FROM wl.new_orders WHERE batch=%s",(k,)).rowcount
    out["txns"].append([a,b,d,i,time.perf_counter()-t])
    for j,v in enumerate((a,b,d,i)): counts[j]+=v
out["t_last_commit"]=time.time(); out["lsn_end"]=lsn()
out["totals"]=dict(status_updates=counts[0],payload_updates=counts[1],deletes=counts[2],inserts=counts[3])
out["workload_s"]=out["t_last_commit"]-out["t_start"]
out["wal_bytes"]=c.execute("select %s::pg_lsn - %s::pg_lsn",(out["lsn_end"],out["lsn_start"])).fetchone()[0]
out["marker_insert_id"]=str(c.execute("select id from wl.new_orders where batch=199 order by id limit 1").fetchone()[0])
out["wal_bytes"]=int(out["wal_bytes"])
json.dump(out,open(f"{Q}/results/workload.json","w"))
print({k:v for k,v in out.items() if k!="txns"})
