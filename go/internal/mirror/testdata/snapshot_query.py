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
if snap.get("paths") == "relative":
    # Published paths are relative to the table directory; base_dir is where
    # the writer has it, which is where this test runs. (A reader elsewhere
    # uses `qs-query -published -root`, which parses the SQL rather than
    # pattern-matching it; these test tables have no literal that needs that.)
    for call in ("read_parquet('", "read_json('"):
        snap["sql"] = snap["sql"].replace(call, call + snap["base_dir"] + "/")
head = f"duckdb {duckdb.__version__} snapshot_seq={snap['snapshot_seq']} applied_lsn={snap['applied_lsn']}"
try:
    row = duckdb.sql(
        "SELECT count(*), sum(n), bit_xor(hash(id, v, n)) FROM (" + snap["sql"] + ")"
    ).fetchall()[0]
    print(f"{head} count={row[0]} sum_n={row[1]} checksum={row[2]}")
except Exception as e:  # the point is to show what the engine says
    print(f"{head} ERROR {type(e).__name__}: {str(e).splitlines()[0][:160]}")
