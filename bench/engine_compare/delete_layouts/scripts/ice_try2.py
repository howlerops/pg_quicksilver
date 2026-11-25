import duckdb, sys, os, json, shutil
D="/tmp/claude-0/-home-user-pg-quicksilver/58894071-fd3a-5a32-94ab-41a0530826ff/scratchpad/dvexp/data/icetest/wh"
c=duckdb.connect(); c.execute("LOAD iceberg"); c.execute("SET iceberg_default_format_version=3")
for i,opt in enumerate(["FORMAT_VERSION 3", "\"format-version\" 3", "TABLE_PROPERTIES {'format-version':'3'}", "PROPERTIES {'format-version':'3'}"]):
    try:
        c.execute(f"COPY (select range a, range%7 b from range(100000)) TO '{D}/default/v{i}' (FORMAT iceberg, {opt})")
        md=[f for f in os.listdir(f"{D}/default/v{i}/metadata") if f.endswith('.json')]
        print(opt, 'OK version', json.load(open(f"{D}/default/v{i}/metadata/{md[0]}"))["format-version"])
    except Exception as e: print(opt, 'FAIL', str(e)[:300])
