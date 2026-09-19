#!/usr/bin/env python3
"""Both halves of the QS_COMPACT_DEAD_FRACTION ledger, on the shape that reaches it.

docs/32 recorded an A/B of that flag which measured nothing: across narrow,
churn, jsonb and deletes the compaction counts were identical in both arms,
because ShouldCompact tests CHURN first and every one of those shapes writes as
fast as it deletes. The trigger never pulled, so a throughput comparison between
the arms was a configuration compared with itself.

The `purge` shape exists to reach it: a retention sweep, deletes with almost no
accompanying writes. A DELETE writes no delta row — it marks a position dead in
the file the row already lives in — so dead-in-base climbs while DeltaRows stays
in the low thousands, and the dead-fraction trigger is the only one that can
fire.

The write side comes from workload_matrix.sh. This is the read side: the same
query against the mirror each arm actually produced, which is the benefit the
write cost is supposed to buy.

    python3 bench/scripts/purge_ledger.py /tmp/mirror-0 /tmp/mirror-0.01

Reads only, and needs no source database.
"""

import json
import os
import statistics
import subprocess
import sys
import time

try:
    import duckdb
except ImportError:
    sys.exit("duckdb is required: pip install duckdb")

TABLE = "public.m"
REPEATS = 7


def select_for(mirror):
    """The SELECT the serving path would publish for this mirror."""
    out = subprocess.run(
        ["/tmp/qs-query", "-mirror", mirror, "-table", TABLE],
        capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"qs-query failed for {mirror}: {out.stderr.strip()}")
    return out.stdout.strip()


def describe(mirror):
    """What this arm's mirror is actually made of."""
    state = os.path.join(mirror, TABLE, "state.json")
    with open(state) as f:
        s = json.load(f)
    dead = 0
    for b in s.get("base_files", []):
        stem = b.rsplit(".", 1)[0]
        gen = (s.get("dv_gen") or {}).get(stem, 0)
        if not gen:
            continue
        for ext in (".parquet", ".json"):
            p = os.path.join(mirror, TABLE, "dv", f"{stem}.{gen:09d}.dv{ext}")
            if os.path.exists(p):
                if ext == ".parquet":
                    dead += duckdb.connect().execute(
                        f"SELECT count(*) FROM read_parquet('{p}')").fetchone()[0]
                else:
                    with open(p) as f:
                        dead += len(json.load(f))
                break
    return s.get("base_rows", 0), len(s.get("delta_files", [])), dead


def bytes_on_disk(mirror):
    total = 0
    for root, _, files in os.walk(mirror):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total


def timed(sql, repeats=REPEATS):
    """Median of repeats, each on a fresh connection.

    A fresh connection per repeat because DuckDB caches Parquet metadata per
    connection: reusing one turns the second read into a different measurement
    from the first, and the median of those is neither.
    """
    out = []
    for _ in range(repeats):
        con = duckdb.connect()
        t0 = time.perf_counter()
        con.execute(sql).fetchall()
        out.append((time.perf_counter() - t0) * 1000)
        con.close()
    return statistics.median(out)


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    arms = [("off (0)", sys.argv[1]), ("on (0.01)", sys.argv[2])]

    for label, mirror in arms:
        if not os.path.isdir(os.path.join(mirror, TABLE)):
            sys.exit(f"no {TABLE} under {mirror} — run the matrix for this arm first")

    print(f"{'arm':<12}{'base rows':>11}{'deltas':>8}{'dead in base':>14}"
          f"{'on disk':>10}{'count(*)':>11}{'sum':>10}{'filtered':>11}")
    rows = []
    for label, mirror in arms:
        sel = select_for(mirror)
        base_rows, deltas, dead = describe(mirror)
        disk = bytes_on_disk(mirror) / 1e6
        q_count = timed(f"SELECT count(*) FROM ({sel})")
        q_sum = timed(f"SELECT sum(amount) FROM ({sel})")
        q_filt = timed(f"SELECT count(*) FROM ({sel}) WHERE amount > 50")
        rows.append((label, q_count, q_sum, q_filt))
        print(f"{label:<12}{base_rows:>11,}{deltas:>8}{dead:>14,}"
              f"{disk:>9.1f}M{q_count:>10.1f}ms{q_sum:>9.1f}ms{q_filt:>10.1f}ms")

    (_, c0, s0, f0), (_, c1, s1, f1) = rows
    print()
    print(f"{'speedup from rewriting':<26}"
          f"count(*) {c0/c1:.2f}x   sum {s0/s1:.2f}x   filtered {f0/f1:.2f}x")
    print()
    print("A number below 1.00 means the rewrite made reads SLOWER, which is a")
    print("result and not a bug in the harness — say so rather than re-running")
    print("until it comes out the other way.")


if __name__ == "__main__":
    main()
