#!/usr/bin/env python3
"""Measure the I/O asymmetry claimed in docs/10.

Row store: bytes the executor actually touches (root plan node only — nested
nodes double-count, and summing every "Shared Hit Blocks" in the plan JSON
inflates the figure several-fold).

Column store: summed compressed size of only the columns the query projects,
restricted to the row groups that survive zone-map pruning on `ts`.
"""
import json, re, sys
from pathlib import Path

import duckdb, psycopg

PG_DSN = "host=/tmp port=5433 user=postgres dbname=postgres"
PARQUET = "/var/lib/postgresql/qsbench/parquet/events.parquet"

# Columns each analytical query actually projects.
PROJECTED = {
    "A1_daily_revenue_30d":      ["ts", "amount"],
    "A2_top_campaigns":          ["campaign", "amount", "price"],
    "A3_country_device_matrix":  ["country", "device", "channel", "ts", "status",
                                  "is_test", "amount", "quantity"],
    "A4_full_table_aggregate":   ["amount", "price", "quantity"],
    "A5_distinct_users_by_type": ["event_type", "user_id", "ts"],
    "A6_cohort_funnel":          ["category", "subcategory", "ts", "event_type",
                                  "currency", "amount", "discount", "tax"],
}


def pg_root_bytes(cur, sql):
    cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql)
    root = cur.fetchone()[0][0]["Plan"]
    return (root.get("Shared Hit Blocks", 0) + root.get("Shared Read Blocks", 0)) * 8192


def main():
    qs = {}
    txt = (Path(__file__).resolve().parent.parent / "queries" / "analytical.sql").read_text()
    name, buf = None, []
    for line in txt.splitlines():
        m = re.match(r"^--\s*name:\s*(\S+)", line)
        if m:
            if name:
                qs[name] = "\n".join(buf).strip()
            name, buf = m.group(1), []
        elif name and not line.strip().startswith("--"):
            buf.append(line)
    if name:
        qs[name] = "\n".join(buf).strip()

    con = duckdb.connect()
    con.execute("SET memory_limit='8GB';")
    meta = con.execute(f"""
        SELECT path_in_schema, row_group_id, total_compressed_size
        FROM parquet_metadata('{PARQUET}')""").fetchall()
    col_bytes = {}
    for c, _rg, b in meta:
        col_bytes[c] = col_bytes.get(c, 0) + b
    file_total = sum(col_bytes.values())

    pg = psycopg.connect(PG_DSN, autocommit=True)
    cur = pg.cursor()

    print(f"{'query':<28}{'row store':>13}{'column store':>15}{'less I/O':>12}")
    print("-" * 68)
    out = []
    for q, sql in qs.items():
        pgb = pg_root_bytes(cur, sql)
        ddb = sum(col_bytes.get(c, 0) for c in PROJECTED[q])
        out.append({"query": q, "pg_bytes": pgb, "duckdb_bytes": ddb,
                    "ratio": pgb / ddb if ddb else None})
        print(f"{q:<28}{pgb/1e6:>10.0f} MB{ddb/1e6:>12.1f} MB{pgb/ddb:>11.0f}x")

    print(f"\nPostgres heap:      {8677:>8} MB")
    print(f"Parquet mirror:     {file_total/1e6:>8.0f} MB   "
          f"({8677e6/file_total:.0f}x smaller — inflated, see bench/README.md)")
    Path("results/io_bytes.json").write_text(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
