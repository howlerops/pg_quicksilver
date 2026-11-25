"""Evaluate a published quicksilver snapshot in DuckDB.

usage: snapshot_query.py <snapshot.json>

Prints one line: seq, applied_lsn, then count / sum(n) / a checksum over every
row, or the engine's error. The snapshot is read from the file given, so a
caller can keep a COPY of snapshot N and re-run it after the mirror has moved.
"""
import json
import sys

import duckdb

snap = json.load(open(sys.argv[1]))
head = f"duckdb {duckdb.__version__} snapshot_seq={snap['snapshot_seq']} applied_lsn={snap['applied_lsn']}"
try:
    row = duckdb.sql(
        "SELECT count(*), sum(n), bit_xor(hash(id, v, n)) FROM (" + snap["sql"] + ")"
    ).fetchall()[0]
    print(f"{head} count={row[0]} sum_n={row[1]} checksum={row[2]}")
except Exception as e:  # the point is to show what the engine says
    print(f"{head} ERROR {type(e).__name__}: {str(e).splitlines()[0][:160]}")
