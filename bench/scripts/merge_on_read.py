#!/usr/bin/env python3
"""Measure merge-on-read cost as a function of delta backlog.

docs/08 bench rule 2: "Always run with a live change stream. A static mirror
is not the product." A mirror that is fast only immediately after compaction
is self-deception — between compactions it must union base + N delta files
and anti-join pending tombstones.

This simulates that steady state: base Parquet + N delta files + tombstones,
queried with `_lsn` version resolution as designed in docs/04.

Answers OQ-11: what IS the true merge-on-read cost per delta file?
"""
import argparse, os, statistics, time
from pathlib import Path

import duckdb

BASE = "/var/lib/postgresql/qsbench/parquet/events.parquet"
DELTA_DIR = "/var/lib/postgresql/qsbench/parquet/delta"

QUERY_TMPL = """
WITH unioned AS (
    SELECT event_id, ts, amount, status, country, device, _lsn FROM base_v
    UNION ALL
    SELECT event_id, ts, amount, status, country, device, _lsn FROM delta_v
),
resolved AS (   -- keep newest version per key, per docs/04 `_lsn` design
    SELECT * FROM (
      SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY _lsn DESC) rn
      FROM unioned
    ) WHERE rn = 1
)
SELECT country, device, count(*) n, sum(amount) revenue
FROM resolved
WHERE ts >= TIMESTAMP '2026-05-01' AND status = 'ok'
  AND event_id NOT IN (SELECT event_id FROM tombstones_v)
GROUP BY 1,2 ORDER BY revenue DESC LIMIT 20
"""

COMPACTED = """
SELECT country, device, count(*) n, sum(amount) revenue
FROM base_v
WHERE ts >= TIMESTAMP '2026-05-01' AND status = 'ok'
GROUP BY 1,2 ORDER BY revenue DESC LIMIT 20
"""


def timed(con, sql, runs=5):
    con.execute(sql).fetchall()          # warm
    s = []
    for _ in range(runs):
        t = time.perf_counter()
        con.execute(sql).fetchall()
        s.append((time.perf_counter() - t) * 1000)
    return statistics.median(s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--deltas", default="0,1,4,16,64")
    ap.add_argument("--rows-per-delta", type=int, default=50_000)
    args = ap.parse_args()

    Path(DELTA_DIR).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'; SET temp_directory='/var/lib/postgresql/qsbench/tmp';")
    # Base carries a synthetic _lsn; real mirror writes it per docs/04.
    con.execute(f"CREATE VIEW base_v AS SELECT *, event_id::BIGINT AS _lsn "
                f"FROM read_parquet('{BASE}')")

    max_deltas = max(int(x) for x in args.deltas.split(","))
    for i in range(max_deltas):
        p = f"{DELTA_DIR}/delta_{i:04d}.parquet"
        if not os.path.exists(p):
            con.execute(f"""
              COPY (SELECT *, (1000000000 + {i}*1000000 + row_number() OVER ())::BIGINT AS _lsn
                    FROM read_parquet('{BASE}')
                    USING SAMPLE {args.rows_per_delta} ROWS)
              TO '{p}' (FORMAT parquet, COMPRESSION zstd)""")

    # Tombstones: 1% of base, deleted and pending compaction.
    con.execute(f"""CREATE OR REPLACE VIEW tombstones_v AS
                    SELECT event_id FROM read_parquet('{BASE}')
                    WHERE event_id % 100 = 7""")

    compacted_ms = timed(con, COMPACTED)
    print(f"compacted baseline (no deltas, no tombstones): {compacted_ms:8.1f} ms\n")
    print(f"{'deltas':>8}{'delta rows':>12}{'query ms':>11}{'vs compacted':>15}")
    print("-" * 46)
    for n in [int(x) for x in args.deltas.split(",")]:
        if n == 0:
            con.execute("CREATE OR REPLACE VIEW delta_v AS "
                        "SELECT *, 0::BIGINT AS _lsn FROM read_parquet('%s') LIMIT 0" % BASE)
        else:
            files = ",".join(f"'{DELTA_DIR}/delta_{i:04d}.parquet'" for i in range(n))
            con.execute(f"CREATE OR REPLACE VIEW delta_v AS SELECT * FROM read_parquet([{files}])")
        ms = timed(con, QUERY_TMPL)
        print(f"{n:>8}{n*args.rows_per_delta:>12,}{ms:>10.1f}ms{ms/compacted_ms:>13.1f}x")


if __name__ == "__main__":
    main()
