"""Item 2 reader: SELECT count(*) on a DuckLake table every 100 ms, recording each result.

Usage: item2_reader.py CATALOG TABLE_SCHEMA TABLE OUT_FILE SECONDS

Each line of OUT_FILE is "<unix time> <count>" or "<unix time> ERROR <message>".
A new DuckDB connection is opened whenever a query fails, so an error during a
rebuild does not stop the reader; it only records the error and retries.
"""
import sys
import time

import duckdb

catalog, schema, table, out_path, seconds = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], float(sys.argv[5])
out = open(out_path, "w", buffering=1)


def connect():
    con = duckdb.connect()
    con.sql("LOAD ducklake")
    con.sql(f"ATTACH 'ducklake:sqlite:{catalog}' AS lake (READ_ONLY)")
    return con


con = None
end = time.time() + seconds
while time.time() < end:
    t0 = time.time()
    try:
        if con is None:
            con = connect()
        n = con.sql(f'SELECT count(*) FROM lake."{schema}"."{table}"').fetchall()[0][0]
        out.write(f"{t0:.3f} {n}\n")
    except Exception as e:  # any failure is recorded verbatim; the reader carries on
        msg = " ".join(str(e).split())[:300]
        out.write(f"{t0:.3f} ERROR {msg}\n")
        try:
            if con is not None:
                con.close()
        except Exception:
            pass
        con = None
    time.sleep(max(0.0, 0.1 - (time.time() - t0)))
out.close()
