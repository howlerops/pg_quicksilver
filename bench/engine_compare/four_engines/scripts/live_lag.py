import json, sys, time, psycopg
Q="/tmp/qsb"; eng=sys.argv[1]; tagbase=sys.argv[2]
pg=psycopg.connect(f"host={Q}/sock port=5440 dbname=app user=postgres",autocommit=True)
def L(s):
    a,b=s.split("/"); return (int(a,16)<<32)|int(b,16)
probe=str(pg.execute("select id from wl.picks where kind='s' and batch=0 order by id limit 1").fetchone()[0])
mc=psycopg.connect(open(f"{Q}/results/mason_url.txt").read().strip(),autocommit=True) if eng=="mason" else None
if mc: mc.execute("SELECT 1 FROM orders LIMIT 0").fetchall()
res=[]
for r in range(5):
    val=f"{tagbase}{r}"
    before=L(pg.execute("select pg_current_wal_lsn()::text").fetchone()[0])
    with pg.transaction():
        n=pg.execute("UPDATE public.orders o SET status=%s FROM wl.picks p WHERE p.kind='s' AND p.batch IN (0,1) AND o.id=p.id",(val,)).rowcount
    t0=time.perf_counter()
    while True:
        if eng=="qs":
            try:
                if L(json.load(open(f"{Q}/mirror/public.orders/state.json"))["applied_lsn"])>before: break
            except Exception: pass
            time.sleep(0.005)
        else:
            if mc.execute(f"SELECT count(*) FROM orders WHERE id='{probe}' AND status='{val}'").fetchone()[0]==1: break
            time.sleep(0.25)
    res.append((time.perf_counter()-t0)*1000); time.sleep(2)
out=dict(engine=eng, rows_per_txn=n, commit_to_visible_ms=res, median_ms=sorted(res)[2], best_ms=min(res))
json.dump(out,open(f"{Q}/results/live_lag_{eng}.json","w"),indent=1); print(out)
