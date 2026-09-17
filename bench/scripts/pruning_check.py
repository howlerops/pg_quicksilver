#!/usr/bin/env python3
"""Does the published view let an engine skip what it does not need?

docs/23 and docs/24 both list "prune on the statistics" as a next step, on the
assumption that the mirror should decide which FILES a query needs. Before
building that, the question worth asking is whether it would add anything: the
engine already has the predicate and the Parquet footers, and it prunes at ROW
GROUP granularity, which is finer than anything the manifest can offer.

What the manifest CAN do is get in the way. The view wraps each file in a
projection, a cast for temporal columns, and -- where a deletion vector exists --
a `WHERE file_row_number NOT IN (SELECT ...)` semi-join. Any of those can stop a
predicate reaching the scan, and then the statistics are irrelevant because
nothing is pushed down to use them.

So this measures bytes off the block device for the same predicate three ways:

  raw      read_parquet over the manifest's files, no view machinery
  view     the SELECT qs-query publishes
  all      no predicate at all, as an upper bound

If view ~ raw, the machinery is transparent and file-level pruning would only
save opening footers. If view ~ all, something in the view is defeating
pushdown, and THAT is the bug to fix rather than a new pruning layer.

    python3 bench/scripts/pruning_check.py --mirror /var/lib/postgresql/qs17/mx-narrow
"""

import argparse
import json
import os
import re
import statistics
import subprocess
import sys

try:
    import duckdb
except ImportError:
    sys.exit("duckdb is required: pip install duckdb")

_WHOLE_DISK = re.compile(r"^(vd[a-z]+|sd[a-z]+|nvme\d+n\d+)$")


def disk_read_bytes():
    total = 0
    with open("/proc/diskstats") as f:
        for line in f:
            p = line.split()
            if len(p) > 5 and _WHOLE_DISK.match(p[2]):
                total += int(p[5]) * 512
    return total


def drop_caches():
    subprocess.run(["sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"], check=True)


def measure(sql, label, repeat=3):
    """Bytes off the device for a cold evaluation, plus the answer.

    A fresh DuckDB connection loads its own binaries and extensions from disk,
    and with the page cache just dropped that lands on whichever query runs
    FIRST. The first version of this script measured the baseline first and
    reported the deletion-vector machinery as costing 14.6x, which was entirely
    DuckDB starting up. Every query now pays its init before the counter
    starts, and the median of several runs is reported rather than one.
    """
    reads = []
    answer = None
    for _ in range(repeat):
        drop_caches()
        con = duckdb.connect()
        con.execute("SELECT 1").fetchall()   # init, before the counter
        b0 = disk_read_bytes()
        rows = con.execute(sql).fetchall()
        reads.append(disk_read_bytes() - b0)
        con.close()
        answer = rows[0]
    read = statistics.median(reads)
    print(f"  {label:<34} {read / 1e6:>8.1f} MB   -> {answer}")
    return read, answer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mirror", default="/var/lib/postgresql/qs17/mx-narrow")
    ap.add_argument("--table", default="public.m")
    ap.add_argument("--qs-query", default="/tmp/qs-query")
    args = ap.parse_args()

    root = os.path.join(args.mirror, args.table)
    st = json.load(open(os.path.join(root, "state.json")))
    files = [os.path.join(root, "base", f) for f in st.get("base_files", [])] + \
            [os.path.join(root, "delta", f) for f in st.get("delta_files", [])]
    files = [f for f in files if os.path.exists(f)]
    if not files:
        sys.exit("no data files in the manifest")
    flist = ", ".join("'" + f + "'" for f in files)
    print(f"  {len(files)} files in the manifest")

    out = subprocess.run(
        [args.qs_query, "-mirror", args.mirror, "-table", args.table],
        capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"qs-query failed: {out.stderr.strip()}")
    view_sql = out.stdout.split("\n", 1)[1]

    # A predicate on the primary key, which is the best case for statistics:
    # rows arrive in key order, so a row group's min/max is a tight range and
    # almost every group can be skipped.
    pred = "id BETWEEN 1000 AND 2000"
    print(f"\n  predicate: {pred}\n")

    con = duckdb.connect()
    con.execute(f"CREATE VIEW qs AS {view_sql}")
    con.close()

    raw, raw_ans = measure(
        f"SELECT count(*) FROM read_parquet([{flist}]) WHERE {pred}",
        "raw read_parquet + predicate")
    view, view_ans = measure(
        f"CREATE VIEW qs AS {view_sql}; SELECT count(*) FROM qs WHERE {pred}",
        "the published view + predicate")
    allb, all_ans = measure(
        f"CREATE VIEW qs AS {view_sql}; SELECT count(*) FROM qs",
        "the published view, no predicate")

    # What does the deletion-vector machinery itself cost? Strip it out and
    # re-measure. The answer is WRONG without it -- that is the point of the
    # machinery -- but the bytes say what it costs to be right, and on this
    # mirror eleven files carry a vector that between them retires thirty rows
    # out of 3.8 million.
    stripped = re.sub(
        r"\s*WHERE file_row_number NOT IN \(SELECT unnest\(v\) FROM read_json\([^)]*\)[^)]*\)\)",
        "", view_sql)
    stripped = stripped.replace(", file_row_number = true", "")
    if stripped != view_sql:
        nodv, nodv_ans = measure(
            f"CREATE VIEW qs AS {stripped}; SELECT count(*) FROM qs WHERE {pred}",
            "same, deletion vectors stripped")
        print(f"\n  the deletion-vector machinery costs "
              f"{(view - nodv) / 1e6:.1f} MB on this predicate "
              f"({view / max(nodv, 1):.2f}x)")
        print(f"  (its answer {nodv_ans} may differ; it is not applying the vectors)")

    print()
    if view <= raw * 2.0:
        print("  The view does not defeat pushdown: a predicate reaches the scan")
        print("  and the statistics are used. File-level pruning in the manifest")
        print("  would save opening footers and nothing else.")
        print(f"  The predicate alone already saves {allb / max(view, 1):.1f}x.")
    else:
        print(f"  PUSHDOWN IS BLOCKED: the view reads {view / max(raw, 1):.1f}x what")
        print("  the same predicate over the same files reads. Something in the")
        print("  view's structure is stopping the predicate reaching the scan.")
    print(f"  (no-predicate upper bound: {allb / 1e6:.1f} MB)")
    # The raw count may legitimately differ: it does not apply deletion vectors.
    print(f"  answers — raw {raw_ans}, view {view_ans}, view-all {all_ans}")


if __name__ == "__main__":
    main()
