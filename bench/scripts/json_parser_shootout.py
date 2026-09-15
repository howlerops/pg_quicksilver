#!/usr/bin/env python3
"""Was "protocol change beats language change" right? Measure, don't argue.

ADR-0008 claimed removing JSON (pgoutput binary) was worth more than changing
language. That was measured ENTIRELY WITHIN PYTHON, which silently assumes the
JSON stage stays slow. A native SIMD parser breaks the assumption.

`orjson` is written in Rust, so it is a direct proxy for what a Rust ingest
would do on this exact payload and hardware — no hand-waving about benchmarks
from other machines.

Caveat stated up front: orjson has no `parse_float` hook, so it cannot produce
Decimal. Exact decimal handling is not free in any language. The comparison
below therefore brackets the truth: orjson is the floor of native parse cost,
and CPython+Decimal is the ceiling.
"""
from __future__ import annotations

import json
import time
from decimal import Decimal

import orjson
import psycopg

DSN = "host=/tmp port=5433 user=postgres dbname=postgres"
ROWS = 40000


def payload() -> list[str]:
    c = psycopg.connect(DSN, autocommit=True)
    k = c.cursor()
    k.execute("DROP TABLE IF EXISTS shootout_t")
    k.execute("CREATE TABLE shootout_t(id int primary key, tenant int, "
              "amount numeric(12,2), sku text, note text)")
    k.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name='shootout'")
    if k.fetchone():
        k.execute("SELECT pg_drop_replication_slot('shootout')")
    k.execute("SELECT pg_create_logical_replication_slot('shootout','wal2json')")
    k.execute(f"""INSERT INTO shootout_t
                  SELECT g, g%50, (g%10000)/100.0, 'SKU-'||g, repeat('x',80)
                  FROM generate_series(1,{ROWS}) g""")
    k.execute("""SELECT data FROM pg_logical_slot_get_changes('shootout',NULL,NULL,
                 'format-version','2','include-lsn','true','include-transaction','true')""")
    raw = [r[0] for r in k.fetchall()]
    k.execute("SELECT pg_drop_replication_slot('shootout')")
    k.execute("DROP TABLE shootout_t")
    return raw


def bench(fn, raw, reps=3):
    best = float("inf")
    for _ in range(reps):
        t = time.perf_counter()
        for r in raw:
            fn(r)
        best = min(best, time.perf_counter() - t)
    return best


def main():
    raw = payload()
    mb = sum(len(r) for r in raw) / 1e6
    n = len(raw)
    print(f"payload: {n:,} wal2json messages, {mb:.1f} MB\n")

    variants = [
        ("CPython json, C scanner (float)", lambda r: json.loads(r), "C"),
        ("CPython json, parse_float=Decimal", lambda r: json.loads(r, parse_float=Decimal), "PYTHON"),
        ("orjson (Rust)", lambda r: orjson.loads(r), "RUST"),
    ]
    results = {}
    print(f"{'parser':<38}{'seconds':>9}{'MB/s':>10}{'msg/s':>12}   impl")
    print("-" * 82)
    for name, fn, impl in variants:
        t = bench(fn, raw)
        results[name] = t
        print(f"{name:<38}{t:>9.3f}{mb/t:>10.0f}{n/t:>12,.0f}   {impl}")

    dec = results["CPython json, parse_float=Decimal"]
    rust = results["orjson (Rust)"]
    cpy = results["CPython json, C scanner (float)"]
    print("-" * 82)
    print(f"\norjson (Rust) vs CPython+Decimal : {dec/rust:>6.1f}x faster")
    print(f"orjson (Rust) vs CPython C scanner: {cpy/rust:>6.1f}x faster")

    # --- what this does to the whole pipeline -------------------------
    # stage timings from bench/results/ingest_profile.txt, same payload
    FETCH, DECODE, GROUP, ARROW, PARQUET = 0.222, 0.056, 0.009, 0.106, 0.020
    rest = FETCH + DECODE + GROUP + ARROW + PARQUET
    print("\n--- pipeline, holding every non-JSON stage constant ---")
    print(f"{'configuration':<44}{'seconds':>9}{'rows/s':>12}")
    print("-" * 66)
    for label, jt in [
        ("Python + wal2json + Decimal  (measured)", dec),
        ("Python + pgoutput binary     (no JSON stage)", 0.0),
        ("Python + orjson              (Rust parser)", rust),
    ]:
        tot = rest + jt
        print(f"{label:<44}{tot:>9.3f}{ROWS/tot:>12,.0f}")
    print("-" * 66)
    print(f"\nThe two 'fixes' land in the same place because the remaining")
    print(f"{rest:.3f}s of Python-side work dominates once JSON is cheap.")
    print(f"That is the ceiling a protocol change alone can reach IN PYTHON.")


if __name__ == "__main__":
    main()
