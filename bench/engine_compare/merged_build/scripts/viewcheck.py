"""viewcheck.py <mirror_root> : build the qs-query view in DuckDB and compare answers with PostgreSQL."""
import sys, subprocess, duckdb, psycopg, hashlib
E="/tmp/claude-0/-home-user-pg-quicksilver/58894071-fd3a-5a32-94ab-41a0530826ff/scratchpad/e2e"
root=sys.argv[1]
sql=subprocess.run([f"{E}/bin/qs-query","-mirror",root,"-table","public.items"],capture_output=True,text=True,check=True).stdout
print("view sql bytes", len(sql), "| reads dv:", "dv" in sql, "| files:", sql.count(".parquet"))
d=duckdb.connect(); d.execute("SET TimeZone='UTC'"); d.execute(f"CREATE VIEW items AS {sql}")
pg=psycopg.connect("host=/var/lib/postgresql/qse2e/sock port=5450 dbname=app user=postgres options='-c timezone=UTC'",autocommit=True)
qs=["SELECT count(*) FROM items",
    "SELECT sum(amount)::text FROM items",
    "SELECT status, count(*), count(note), sum(qty)::text FROM items GROUP BY status ORDER BY status",
    "SELECT count(*) FROM items WHERE note='churn-e'",
    "SELECT count(DISTINCT id) FROM items"]
ok=True
for q in qs:
    a=[tuple(r) for r in d.execute(q).fetchall()]; b=[tuple(r) for r in pg.execute(q).fetchall()]
    same = a==b; ok&=same
    print("SAME" if same else "DIFF", q, "->", a if len(str(a))<200 else str(a)[:200], "" if same else f"pg={b}")
print("VIEW_ANSWERS_MATCH" if ok else "VIEW_ANSWERS_DIFFER")
