import duckdb, sys, os, json
D="/tmp/claude-0/-home-user-pg-quicksilver/58894071-fd3a-5a32-94ab-41a0530826ff/scratchpad/dvexp/data/icetest/wh"
c=duckdb.connect(); c.execute("LOAD iceberg"); c.execute("SET iceberg_default_format_version=3")
c.execute(f"COPY (select range a, range%7 b from range(100000)) TO '{D}/default/t' (FORMAT iceberg)")
md=[f for f in os.listdir(f"{D}/default/t/metadata") if f.endswith('.json')]
print(md, json.load(open(f"{D}/default/t/metadata/{md[0]}"))["format-version"])
c.execute("ATTACH 'wh' AS ice (TYPE iceberg, ENDPOINT 'http://127.0.0.1:18181', AUTHORIZATION_TYPE 'none')")
print(c.execute("select count(*) from ice.default.t").fetchall())
c.execute("DELETE FROM ice.default.t WHERE a % 10 = 3")
print(c.execute("select count(*) from ice.default.t").fetchall())
c.execute("INSERT INTO ice.default.t select range a, 1 b from range(100000,100500)")
print(c.execute("select count(*), sum(a) from ice.default.t").fetchall())
print(c.execute("select * from iceberg_metadata(ice.default.t)").fetchall())
