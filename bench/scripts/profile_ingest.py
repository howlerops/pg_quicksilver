#!/usr/bin/env python3
"""Where does the ingest actually spend its time, and what is the ceiling?

The language question (Python vs Go vs Rust) should be answered with a profile,
not a preference. Two things decide it:

  1. What fraction of the ingest is Python orchestration vs C/C++ libraries
     (pyarrow, parquet) that a rewrite would not speed up at all?
  2. Where is the ceiling relative to what Postgres can even produce? Logical
     decoding is single-threaded ON THE SOURCE, so past some rate the consumer
     language stops mattering — that is walshadow's whole argument for physical
     WAL.
"""
from __future__ import annotations

import cProfile
import io
import json
import pstats
import time
from decimal import Decimal

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

DSN = "host=/tmp port=5433 user=postgres dbname=postgres"
OUT = "/var/lib/postgresql/qsbench/prof"


def stage_timings(rows: int = 40000):
    """Time each stage of the pipeline on a fixed payload."""
    c = psycopg.connect(DSN, autocommit=True)
    k = c.cursor()
    k.execute("DROP TABLE IF EXISTS prof_t")
    k.execute("CREATE TABLE prof_t(id int primary key, tenant int, "
              "amount numeric(12,2), sku text, note text)")
    k.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name='prof_slot'")
    if k.fetchone():
        k.execute("SELECT pg_drop_replication_slot('prof_slot')")
    k.execute("SELECT pg_create_logical_replication_slot('prof_slot','wal2json')")

    k.execute(f"""INSERT INTO prof_t
                  SELECT g, g%50, (g%10000)/100.0, 'SKU-'||g, repeat('x',80)
                  FROM generate_series(1,{rows}) g""")

    # --- stage 1: get the raw JSON out of Postgres ---------------------
    t0 = time.perf_counter()
    k.execute("""SELECT data FROM pg_logical_slot_get_changes('prof_slot',NULL,NULL,
                 'format-version','2','include-lsn','true','include-transaction','true')""")
    raw = [r[0] for r in k.fetchall()]
    t_fetch = time.perf_counter() - t0
    nbytes = sum(len(r) for r in raw)

    # --- stage 2a: JSON parse, C scanner (float) -----------------------
    # Both variants must RETAIN their output, or the comparison measures list
    # construction as well as parsing and overstates the Decimal penalty.
    t0 = time.perf_counter()
    _ = [json.loads(r) for r in raw]
    t_json_c = time.perf_counter() - t0

    # --- stage 2b: JSON parse with parse_float=Decimal -----------------
    # This is what correctness requires (money must not go through float64),
    # but it DISABLES CPython's C scanner and falls back to the pure-Python
    # one. Measure the price of that.
    t0 = time.perf_counter()
    msgs = [json.loads(r, parse_float=Decimal) for r in raw]
    t_json_dec = time.perf_counter() - t0

    # --- stage 3: decode into dicts (pure Python) ----------------------
    t0 = time.perf_counter()
    decoded = []
    for m in msgs:
        if m.get("action") != "I":
            continue
        decoded.append({c["name"]: c["value"] for c in m.get("columns", [])})
    t_decode = time.perf_counter() - t0

    # --- stage 4: group by key (pure Python) ---------------------------
    t0 = time.perf_counter()
    upserts = {}
    for r in decoded:
        upserts[r["id"]] = r
    t_group = time.perf_counter() - t0

    # --- stage 5: Python objects -> Arrow ------------------------------
    schema = pa.schema([
        pa.field("id", pa.int64()), pa.field("tenant", pa.int64()),
        pa.field("amount", pa.decimal128(12, 2)),
        pa.field("sku", pa.string()), pa.field("note", pa.string())])
    vals = list(upserts.values())
    t0 = time.perf_counter()
    tbl = pa.Table.from_pylist(
        [{"id": int(r["id"]), "tenant": int(r["tenant"]),
          "amount": Decimal(str(r["amount"])), "sku": r["sku"], "note": r["note"]}
         for r in vals], schema=schema)
    t_arrow = time.perf_counter() - t0

    # --- stage 6: Parquet write (C++) ----------------------------------
    import os
    os.makedirs(OUT, exist_ok=True)
    t0 = time.perf_counter()
    pq.write_table(tbl, f"{OUT}/p.parquet", compression="zstd")
    t_parquet = time.perf_counter() - t0

    k.execute("SELECT pg_drop_replication_slot('prof_slot')")
    k.execute("DROP TABLE prof_t")

    n = len(decoded)
    stages = [
        ("fetch from slot (libpq + PG decode)", t_fetch, "PG + C"),
        ("json.loads, C scanner (float)", t_json_c, "C"),
        ("json.loads, parse_float=Decimal", t_json_dec, "PYTHON"),
        ("decode to dicts", t_decode, "PYTHON"),
        ("group by key", t_group, "PYTHON"),
        ("python objects -> Arrow", t_arrow, "PYTHON/C++ boundary"),
        ("parquet write (zstd)", t_parquet, "C++"),
    ]
    return n, nbytes, stages, t_json_c, t_json_dec


def main():
    n, nbytes, stages, t_c, t_dec = stage_timings()
    pipeline = sum(t for name, t, _ in stages if "C scanner" not in name)

    print(f"payload: {n:,} change rows, {nbytes/1e6:.1f} MB of wal2json JSON\n")
    print(f"{'stage':<38}{'seconds':>9}{'% of pipe':>11}{'rows/s':>12}   runs in")
    print("-" * 92)
    for name, t, where in stages:
        if "C scanner" in name:
            continue
        print(f"{name:<38}{t:>9.3f}{100*t/pipeline:>10.1f}%{n/t:>12,.0f}   {where}")
    print("-" * 92)
    print(f"{'TOTAL pipeline':<38}{pipeline:>9.3f}{100:>10.1f}%{n/pipeline:>12,.0f}")

    py = sum(t for name, t, where in stages
             if "PYTHON" in where and "C scanner" not in name)
    print(f"\nPython-side work: {py:.3f}s = {100*py/pipeline:.0f}% of the pipeline")
    print(f"C/C++ work:       {pipeline-py:.3f}s = {100*(pipeline-py)/pipeline:.0f}%")
    print(f"\nparse_float=Decimal costs {t_dec/t_c:.1f}x vs the C JSON scanner "
          f"({t_c:.3f}s -> {t_dec:.3f}s)")
    print(f"  (required for correctness — money must not round-trip through float64)")

    print(f"\nCeiling: this pipeline sustains ~{n/pipeline:,.0f} rows/s single-threaded.")
    print("Compare: PeerDB ~120k rows/s, walshadow ~289k rows/s (Rust, physical WAL),")
    print("and Postgres' own logical decoder is single-threaded on the SOURCE.")

    print("\n--- top functions by cumulative time (full pipeline) ---")
    pr = cProfile.Profile()
    pr.enable()
    stage_timings(rows=20000)
    pr.disable()
    st = io.StringIO()
    pstats.Stats(pr, stream=st).sort_stats("tottime").print_stats(12)
    for line in st.getvalue().splitlines():
        if line.strip() and not line.startswith(" " * 20):
            print("   ", line[:120])


if __name__ == "__main__":
    main()
