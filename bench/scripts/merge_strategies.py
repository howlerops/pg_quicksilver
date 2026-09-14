#!/usr/bin/env python3
"""Compare merge-on-read strategies (OQ-11).

merge_on_read.py showed the `_lsn` global-dedup design sketched in docs/04
costs ~30x even with ZERO deltas: the cost is the
`row_number() OVER (PARTITION BY event_id ORDER BY _lsn DESC)` over every
row in the table, not the delta backlog. That tax cancels the analytical
speedup entirely.

This compares three designs against the same data:

  S1 global_dedup   — docs/04 as written. Window function over all rows.
  S2 anti_join      — base ANTI JOIN (changed keys), UNION ALL latest delta.
                      Touches the delta key set, not every base row.
  S3 deletion_vec   — Iceberg-v2-style. Deleted/superseded row positions are
                      precomputed at compaction time; the scan applies a
                      position filter and never joins at all.
"""
import argparse, os, statistics, time
from pathlib import Path

import duckdb

BASE = "/var/lib/postgresql/qsbench/parquet/events.parquet"
DELTA_DIR = "/var/lib/postgresql/qsbench/parquet/delta"

AGG = """SELECT country, device, count(*) n, sum(amount) revenue
         FROM {src}
         WHERE ts >= TIMESTAMP '2026-05-01' AND status = 'ok'
         GROUP BY 1,2 ORDER BY revenue DESC LIMIT 20"""

COMPACTED = AGG.format(src="base_v")

S1 = """
WITH unioned AS (
    SELECT event_id, ts, amount, status, country, device, _lsn FROM base_v
    UNION ALL SELECT event_id, ts, amount, status, country, device, _lsn FROM delta_v
),
resolved AS (
    SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY _lsn DESC) rn
                   FROM unioned) WHERE rn = 1
)
""" + AGG.format(src="resolved") .replace("FROM resolved", "FROM resolved") \
      .replace("WHERE ts", "WHERE event_id NOT IN (SELECT event_id FROM tombstones_v) AND ts")

S2 = """
WITH live_base AS (
    SELECT b.event_id, b.ts, b.amount, b.status, b.country, b.device
    FROM base_v b
    ANTI JOIN changed_keys c ON b.event_id = c.event_id
),
latest_delta AS (
    SELECT event_id, ts, amount, status, country, device FROM (
        SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY _lsn DESC) rn
        FROM delta_v) WHERE rn = 1
),
merged AS (SELECT * FROM live_base UNION ALL SELECT * FROM latest_delta)
""" + AGG.format(src="merged")

S3 = """
WITH live_base AS (
    SELECT event_id, ts, amount, status, country, device
    FROM base_pos WHERE NOT deleted
),
merged AS (SELECT * FROM live_base UNION ALL SELECT * FROM latest_delta_mat)
""" + AGG.format(src="merged")


def timed(con, sql, runs=5):
    con.execute(sql).fetchall()
    s = []
    for _ in range(runs):
        t = time.perf_counter()
        con.execute(sql).fetchall()
        s.append((time.perf_counter() - t) * 1000)
    return statistics.median(s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--deltas", default="0,4,16,64")
    ap.add_argument("--rows-per-delta", type=int, default=50_000)
    args = ap.parse_args()

    Path(DELTA_DIR).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'; SET temp_directory='/var/lib/postgresql/qsbench/tmp';")
    con.execute(f"CREATE VIEW base_v AS SELECT *, event_id::BIGINT AS _lsn FROM read_parquet('{BASE}')")
    con.execute(f"""CREATE OR REPLACE VIEW tombstones_v AS
                    SELECT event_id FROM read_parquet('{BASE}') WHERE event_id % 100 = 7""")

    for i in range(max(int(x) for x in args.deltas.split(","))):
        p = f"{DELTA_DIR}/delta_{i:04d}.parquet"
        if not os.path.exists(p):
            con.execute(f"""COPY (SELECT *, (1000000000 + {i}*1000000 + row_number() OVER ())::BIGINT AS _lsn
                            FROM read_parquet('{BASE}') USING SAMPLE {args.rows_per_delta} ROWS)
                            TO '{p}' (FORMAT parquet, COMPRESSION zstd)""")

    baseline = timed(con, COMPACTED)
    print(f"compacted baseline (no merge machinery at all): {baseline:.1f} ms\n")
    print(f"{'deltas':>7}{'S1 global_dedup':>19}{'S2 anti_join':>16}{'S3 deletion_vec':>18}")
    print("-" * 60)

    for n in [int(x) for x in args.deltas.split(",")]:
        if n == 0:
            con.execute(f"CREATE OR REPLACE VIEW delta_v AS SELECT *, 0::BIGINT AS _lsn "
                        f"FROM read_parquet('{BASE}') LIMIT 0")
        else:
            files = ",".join(f"'{DELTA_DIR}/delta_{i:04d}.parquet'" for i in range(n))
            con.execute(f"CREATE OR REPLACE VIEW delta_v AS SELECT * FROM read_parquet([{files}])")

        # S2 support: the set of keys superseded by a delta or tombstoned.
        con.execute("""CREATE OR REPLACE TEMP VIEW changed_keys AS
                       SELECT event_id FROM delta_v
                       UNION SELECT event_id FROM tombstones_v""")

        # S3 support: compaction-time artefacts. A deletion bitmap over base
        # positions, and the already-resolved delta rows.
        con.execute("""CREATE OR REPLACE TEMP TABLE base_pos AS
                       SELECT b.event_id, b.ts, b.amount, b.status, b.country, b.device,
                              (c.event_id IS NOT NULL) AS deleted
                       FROM base_v b LEFT JOIN changed_keys c ON b.event_id = c.event_id""")
        con.execute("""CREATE OR REPLACE TEMP TABLE latest_delta_mat AS
                       SELECT event_id, ts, amount, status, country, device FROM (
                         SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY _lsn DESC) rn
                         FROM delta_v) WHERE rn = 1""")

        r = []
        for sql in (S1, S2, S3):
            ms = timed(con, sql)
            r.append(f"{ms:>8.0f}ms {ms/baseline:>5.1f}x")
        print(f"{n:>7}{r[0]:>19}{r[1]:>16}{r[2]:>18}")

    print("\nS3's cost is paid at compaction time, not query time — that is the")
    print("point. The comparison assumes compaction keeps up (docs/04 SLO).")


if __name__ == "__main__":
    main()
