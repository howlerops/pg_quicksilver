"""mixed insert/update/delete workload. wl.py <txns> <rows_per_txn> [sleep_between_s] [seed]"""
import sys, random, time, json, psycopg
from psycopg.types.json import Jsonb
port=5450; sock="/var/lib/postgresql/qse2e/sock"
T=int(sys.argv[1]); K=int(sys.argv[2]); S=float(sys.argv[3]) if len(sys.argv)>3 else 0; random.seed(int(sys.argv[4]) if len(sys.argv)>4 else 1)
c=psycopg.connect(f"host={sock} port={port} dbname=app user=postgres",autocommit=True)
tot=[0,0,0]; t0=time.time()
for t in range(T):
    lo,hi=c.execute("select min(seq),max(seq) from items").fetchone()
    lo=lo or 0; hi=hi or 0
    with c.transaction():
        c.execute("INSERT INTO items SELECT gen_random_uuid(), %s+g, 1+g%%200, (ARRAY['pending','paid','new'])[1+g%%3], "
                  "CASE WHEN g%%17=0 THEN NULL ELSE round((random()*1e10-1e9)::numeric,2) END, now(), "
                  "CASE WHEN g%%13=0 THEN NULL ELSE jsonb_build_object('w',%s,'i',g,'t','ins ☃') END, "
                  "CASE WHEN g%%5=0 THEN NULL ELSE 'ins '||g END, (random()*4e18-2e18)::bigint, "
                  "CASE g%%3 WHEN 0 THEN true WHEN 1 THEN false END, current_date FROM generate_series(1,%s) g",(hi,t,K))
        tot[0]+=K
        ids=[random.randint(lo,hi) for _ in range(K)]
        tot[1]+=c.execute("UPDATE items SET amount=coalesce(amount,0)+0.01, status='upd', payload=coalesce(payload,'{}'::jsonb)||jsonb_build_object('rev',%s::int), note=CASE WHEN seq%%2=0 THEN NULL ELSE 'u'||seq END, created_at=created_at+interval '1 microsecond' WHERE seq = ANY(%s)",(t,ids)).rowcount
        ids=[random.randint(lo,hi) for _ in range(K//2)]
        tot[2]+=c.execute("DELETE FROM items WHERE seq = ANY(%s)",(ids,)).rowcount
    if S: time.sleep(S)
print(json.dumps(dict(txns=T,inserts=tot[0],updates=tot[1],deletes=tot[2],seconds=round(time.time()-t0,2),rows_now=c.execute("select count(*) from items").fetchone()[0])))
