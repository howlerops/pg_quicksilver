#!/usr/bin/env python3
"""Phase 1 walking skeleton, end to end.

Runs the real pipeline against a live Postgres and proves the property that
docs/06 says kills CDC products if it is wrong: the mirror converges to the
source, exactly, under concurrent writes — and is never observed holding half
a transaction.

    python3 -m quicksilver.run_slice --seconds 30

What it exercises:
  * logical replication -> Transaction objects cut on COMMIT boundaries
  * micro-batched Parquet writes with deletion vectors (docs/11 Result 5)
  * durable applied_lsn, written after the data
  * compaction
  * continuous checksum verification against the source (goal G7)
  * cross-table atomicity: a transaction that moves a row between two tables is
    never seen half-applied
"""
from __future__ import annotations

import argparse
import random
import shutil
import threading
import time
from pathlib import Path

import duckdb
import psycopg

from .changestream import LogicalChangeStream, lsn_to_int
from .mirror import TableMirror, source_checksum

DSN = "host=/tmp port=5433 user=postgres dbname=postgres"
ROOT = Path("/var/lib/postgresql/qsbench/mirror")
SLOT = "quicksilver_slice"

COLUMNS = {"id": "int", "tenant_id": "int", "amount": "numeric(12,2)", "status": "text"}
ARCHIVE = dict(COLUMNS)

DDL = """
DROP TABLE IF EXISTS qs_orders, qs_orders_archive;
CREATE TABLE qs_orders(id int primary key, tenant_id int, amount numeric(12,2), status text);
CREATE TABLE qs_orders_archive(id int primary key, tenant_id int, amount numeric(12,2), status text);
"""

stop = threading.Event()
moved = {"n": 0}
violations = {"n": 0}


def writer_thread():
    """Concurrent OLTP-ish load, including cross-table moves inside one
    transaction — the case that exposes non-atomic batching."""
    c = psycopg.connect(DSN, autocommit=False)
    k = c.cursor()
    next_id = 1
    while not stop.is_set():
        try:
            for _ in range(random.randint(1, 20)):
                next_id += 1
                k.execute("INSERT INTO qs_orders VALUES (%s,%s,%s,'new')",
                          (next_id, random.randint(1, 5), round(random.uniform(1, 500), 2)))
            k.execute("UPDATE qs_orders SET amount = amount + 1, status='upd' "
                      "WHERE id IN (SELECT id FROM qs_orders ORDER BY random() LIMIT 5)")
            k.execute("DELETE FROM qs_orders WHERE id IN "
                      "(SELECT id FROM qs_orders ORDER BY random() LIMIT 2)")
            c.commit()

            # cross-table move in ONE transaction: must never be half-visible
            k.execute("""WITH picked AS (
                           SELECT id, tenant_id, amount, status FROM qs_orders
                           ORDER BY random() LIMIT 2),
                         ins AS (
                           INSERT INTO qs_orders_archive
                           SELECT * FROM picked ON CONFLICT (id) DO NOTHING
                           RETURNING id)
                         DELETE FROM qs_orders WHERE id IN (SELECT id FROM picked)""")
            moved["n"] += k.rowcount or 0
            c.commit()
        except Exception:
            c.rollback()
        time.sleep(0.02)
    c.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--batch-ms", type=float, default=500)
    ap.add_argument("--compact-every", type=int, default=10)
    args = ap.parse_args()

    if ROOT.exists():
        shutil.rmtree(ROOT)
    ROOT.mkdir(parents=True)

    admin = psycopg.connect(DSN, autocommit=True)
    acur = admin.cursor()
    acur.execute(DDL)

    stream = LogicalChangeStream(
        admin, SLOT, tables=["public.qs_orders", "public.qs_orders_archive"])
    stream.drop_slot()
    stream.ensure_slot()

    orders = TableMirror(ROOT, "public", "qs_orders", "id", COLUMNS)
    archive = TableMirror(ROOT, "public", "qs_orders_archive", "id", ARCHIVE)
    mirrors = {m.qualified: m for m in (orders, archive)}
    con = duckdb.connect()

    t = threading.Thread(target=writer_thread, daemon=True)
    t.start()

    print(f"{'t':>5}{'txns':>7}{'upserts':>9}{'deletes':>9}{'lag_ms':>9}{'slot_MB':>9}  applied_lsn")
    deadline = time.time() + args.seconds
    rounds = tot_txn = 0
    try:
        while time.time() < deadline:
            time.sleep(args.batch_ms / 1000)
            t0 = time.time()
            txns = list(stream.transactions())
            if not txns:
                continue
            stats = {q: m.apply(txns) for q, m in mirrors.items()}
            last = txns[-1].commit_lsn
            stream.confirm(last)                    # only after the write is durable
            tot_txn += len(txns)
            rounds += 1

            acur.execute("SELECT pg_current_wal_lsn()")
            head = acur.fetchone()[0]
            lag_bytes = lsn_to_int(head) - lsn_to_int(last)
            print(f"{time.time()-t0:>5.2f}{len(txns):>7}"
                  f"{sum(s['upserts'] for s in stats.values()):>9}"
                  f"{sum(s['deletes'] for s in stats.values()):>9}"
                  f"{(time.time()-t0)*1000:>9.0f}"
                  f"{stream.slot_lag_bytes()/1e6:>9.1f}  {last}")

            # Atomicity probe after EVERY batch, not just at the end. A row
            # visible in both tables means a cross-table move was applied
            # half-way — the failure docs/06 section 2 is about. Checking only
            # the final state would not catch a transient violation.
            mid = con.execute(
                f"SELECT count(*) FROM {orders._live_sql()} o "
                f"JOIN {archive._live_sql()} a USING (id)").fetchone()[0]
            if mid:
                violations["n"] += 1
                print(f"  !! ATOMICITY VIOLATION: {mid} rows in both tables")

            if rounds % args.compact_every == 0:
                for m in mirrors.values():
                    m.compact(con)
    finally:
        stop.set()
        t.join(timeout=5)

    # drain whatever the writer produced after the loop ended
    for _ in range(5):
        txns = list(stream.transactions())
        if not txns:
            break
        for m in mirrors.values():
            m.apply(txns)
        stream.confirm(txns[-1].commit_lsn)
        tot_txn += len(txns)

    print(f"\napplied {tot_txn} transactions; {moved['n']} cross-table moves\n")
    print("=== convergence check (goal G7) ===")
    ok = True
    for m in mirrors.values():
        mc, mh = m.checksum(con)
        sc, sh = source_checksum(acur, *m.qualified.split("."), m.columns)
        match = (mc, mh) == (sc, sh)
        ok &= match
        print(f"  {m.qualified:<26} mirror {mc:>6} rows  source {sc:>6} rows   "
              f"{'MATCH' if match else 'DIVERGED'}")
        if not match:
            print(f"      mirror hash {mh}\n      source hash {sh}")

    print("\n=== cross-table atomicity ===")
    print(f"  per-batch probes: {rounds}, violations observed: {violations['n']}")
    acur.execute("""SELECT count(*) FROM qs_orders o
                    JOIN qs_orders_archive a USING (id)""")
    src_dupes = acur.fetchone()[0]
    dupes = con.execute(
        f"SELECT count(*) FROM {orders._live_sql()} o "
        f"JOIN {archive._live_sql()} a USING (id)").fetchone()[0]
    print(f"  rows in BOTH tables — source {src_dupes}, mirror {dupes}  "
          f"{'OK' if dupes == src_dupes else 'ATOMICITY VIOLATION'}")
    ok &= dupes == src_dupes and violations['n'] == 0

    stream.drop_slot()
    print(f"\n{'PASS — mirror converged' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
