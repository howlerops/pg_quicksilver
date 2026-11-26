"""Run one statement in DuckDB and print its rows.

usage: duck_rows.py <sql>

One line per row, columns separated by a tab, each value as DuckDB casts it to
VARCHAR (NULL as \\N). The session is pinned to UTC and to one thread so that
what is printed depends on the files and nothing else. An engine error is
printed as a single line starting with ERROR.
"""
import sys

import duckdb

con = duckdb.connect()
con.execute("SET TimeZone = 'UTC'")
con.execute("SET threads = 1")
try:
    rel = con.sql(sys.argv[1])
    quoted = ['"' + c.replace('"', '""') + '"' for c in rel.columns]
    cols = ", ".join(f"CAST({c} AS VARCHAR)" for c in quoted)
    for row in con.sql(f"SELECT {cols} FROM rel").fetchall():
        print("\t".join("\\N" if v is None else v for v in row))
except Exception as e:  # the point is to show what the engine says
    print(f"ERROR {type(e).__name__}: {str(e).splitlines()[0][:300]}")
