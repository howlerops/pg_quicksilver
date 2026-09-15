#!/usr/bin/env python3
"""Phase 1: DDL barriers and readiness gating, tested under live write load.

Two properties, both of which must hold while the source is being written to:

  A. **DDL is handled, not survived.** An `ALTER TABLE ... ADD COLUMN` lands
     mid-stream as a barrier in the change stream (via the event trigger in
     ddl.py). The mirror evolves its schema, old Parquet files NULL-fill, and
     the mirror still converges exactly afterwards. Then a DROP COLUMN. Then an
     unsafe change (retype), which must be REFUSED loudly rather than applied.

  B. **Readiness gates on lag.** Stall the ingest and the pod must go
     NOT READY within the freshness SLO; resume and it must recover. This is
     what makes bounded staleness enforced rather than promised (goal G4).

    python3 -m quicksilver.run_phase1
"""
from __future__ import annotations

import argparse
import json
import random
import shutil
import threading
import time
import urllib.request
from pathlib import Path

import duckdb
import psycopg

from . import ddl
from .changestream import LogicalChangeStream, lsn_to_int
from .health import MirrorHealth
from .mirror import TableMirror, source_checksum

DSN = "host=/tmp port=5433 user=postgres dbname=postgres"
ROOT = Path("/var/lib/postgresql/qsbench/mirror_p1")
SLOT = "quicksilver_p1"
PORT = 8089

DDL_INIT = """
DROP TABLE IF EXISTS qs_items;
CREATE TABLE qs_items(id int primary key, sku text, price numeric(12,2));
"""

stop = threading.Event()
paused = threading.Event()


def writer():
    c = psycopg.connect(DSN, autocommit=True)
    k = c.cursor()
    n = 0
    while not stop.is_set():
        if paused.is_set():
            time.sleep(0.02)
            continue
        try:
            n += 1
            k.execute("INSERT INTO qs_items(id,sku,price) VALUES (%s,%s,%s) "
                      "ON CONFLICT (id) DO UPDATE SET price = EXCLUDED.price",
                      (n, f"SKU-{n%500:04d}", round(random.uniform(1, 999), 2)))
            if n % 7 == 0:
                k.execute("DELETE FROM qs_items WHERE id = %s", (max(1, n - 50),))
        except Exception:
            # an ALTER COLUMN ... TYPE invalidates cached plans; reconnect
            # rather than spin failing, which would starve the test of data
            try:
                c.close()
            except Exception:
                pass
            c = psycopg.connect(DSN, autocommit=True)
            k = c.cursor()
        time.sleep(0.01)
    c.close()


def probe(path: str) -> tuple[int, dict]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{PORT}{path}", timeout=2) as r:
            return r.status, json.loads(r.read() or "{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or "{}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slo", type=float, default=3.0)
    args = ap.parse_args()

    if ROOT.exists():
        shutil.rmtree(ROOT)
    ROOT.mkdir(parents=True)

    admin = psycopg.connect(DSN, autocommit=True)
    cur = admin.cursor()
    cur.execute(DDL_INIT)
    ddl.setup(cur)

    cols = ddl.live_columns(cur, "public", "qs_items")
    mirror = TableMirror(ROOT, "public", "qs_items", "id", cols)
    con = duckdb.connect()
    health = MirrorHealth(freshness_slo_seconds=args.slo)
    health.serve(PORT)

    stream = LogicalChangeStream(admin, SLOT,
                                 tables=["public.qs_items", "quicksilver.ddl_log"])
    stream.drop_slot()
    stream.ensure_slot()

    threading.Thread(target=writer, daemon=True).start()
    failures: list[str] = []
    halted: list[str] = []

    def pump(label=""):
        """One ingest cycle, honouring DDL barriers.

        A barrier must STOP the apply, not merely be noticed. Applying rows
        that arrived after a DDL with the pre-DDL column list silently drops
        the new column — which is precisely the bug barriers exist to prevent,
        and it is invisible: row counts still match, only the values are wrong.

        So: apply up to and including the DDL transaction, confirm only that
        far, re-read the catalog, evolve. The remainder is re-read on the next
        pump (peek/confirm makes that safe).
        """
        txns = list(stream.transactions())
        if not txns:
            health.record_caught_up()          # idle != stale
            return False

        cut = None
        for i, t in enumerate(txns):
            if any(c.qualified == "quicksilver.ddl_log" for c in t.changes):
                cut = i
                break

        head = txns if cut is None else txns[:cut + 1]
        mirror.apply(head)
        last = head[-1].commit_lsn
        stream.confirm(head[-1].next_lsn)
        cur.execute("SELECT pg_current_wal_lsn()")
        health.record_apply(last, cur.fetchone()[0], len(mirror.state.delta_files))

        if cut is None:
            return False

        live = ddl.live_columns(cur, "public", "qs_items")
        d = ddl.diff(mirror.columns, live)
        if not d.empty:
            if d.safe:
                mirror.evolve(live)
            else:
                halted.append(d.describe())      # refuse, do not corrupt
        return True

    def converged() -> bool:
        """Pause writes, drain the stream fully, then compare. Comparing while
        the writer runs races: the source moves ahead of whatever the mirror
        has consumed, and a healthy mirror looks diverged."""
        paused.set()
        time.sleep(0.3)                     # let in-flight commits land
        for _ in range(40):                 # drain until the slot is empty
            if not pump() and not list(stream.transactions()):
                break
            time.sleep(0.05)
        mc, mh = mirror.checksum(con)
        sc, sh = source_checksum(cur, "public", "qs_items", mirror.columns)
        health.record_verification(diverged=(mc, mh) != (sc, sh))
        print(f"      mirror {mc} rows / source {sc} rows -> "
              f"{'MATCH' if (mc, mh) == (sc, sh) else 'DIVERGED'}")
        paused.clear()
        return (mc, mh) == (sc, sh)

    print("=" * 72)
    print("A. DDL barriers")
    print("=" * 72)
    for _ in range(10):
        pump(); time.sleep(0.15)
    print(f"  baseline columns: {list(mirror.columns)}")
    if not converged():
        failures.append("baseline did not converge")

    # --- ADD COLUMN, mid-stream, while writes continue -------------------
    print("\n  ALTER TABLE qs_items ADD COLUMN category text ...")
    cur.execute("ALTER TABLE qs_items ADD COLUMN category text")
    cur.execute("UPDATE qs_items SET category='books' WHERE id % 3 = 0")
    hit = False
    for _ in range(12):
        hit |= pump(); time.sleep(0.15)
    print(f"      DDL barrier seen in change stream: {hit}")
    if not hit:
        failures.append("ADD COLUMN produced no barrier")
    print(f"      evolved columns (by the barrier, not by hand): {list(mirror.columns)}")
    if "category" not in mirror.columns:
        failures.append("barrier did not evolve the schema")
    if not converged():
        failures.append("did not converge after ADD COLUMN")

    # old files predate the column and must NULL-fill rather than error
    nulls = con.execute(
        f"SELECT count(*) FROM {mirror._live_sql()} WHERE category IS NULL").fetchone()[0]
    print(f"      rows with NULL category (old files NULL-filled): {nulls}")

    # --- DROP COLUMN -----------------------------------------------------
    print("\n  ALTER TABLE qs_items DROP COLUMN sku ...")
    cur.execute("ALTER TABLE qs_items DROP COLUMN sku")
    for _ in range(10):
        pump(); time.sleep(0.15)
    print(f"      evolved columns: {list(mirror.columns)}")
    if "sku" in mirror.columns:
        failures.append("barrier did not drop the column")
    if not converged():
        failures.append("did not converge after DROP COLUMN")

    # --- an UNSAFE change must be refused, not applied -------------------
    print("\n  ALTER TABLE qs_items ALTER COLUMN price TYPE text ...")
    cur.execute("ALTER TABLE qs_items ALTER COLUMN price TYPE text")
    for _ in range(8):
        pump(); time.sleep(0.15)
    print(f"      halted: {halted or 'NOTHING — the retype was applied!'}")
    if not halted:
        failures.append("retype was NOT flagged unsafe — would corrupt silently")
    else:
        print("      REFUSED: mirroring halts for this table rather than "
              "writing a lossy conversion (docs/06 policy)")

    # --- B. readiness gating --------------------------------------------
    print("\n" + "=" * 72)
    print(f"B. readiness gating (freshnessSLO = {args.slo}s)")
    print("=" * 72)
    for _ in range(6):
        pump(); time.sleep(0.1)
    code, body = probe("/readyz")
    print(f"  while ingesting     HTTP {code}  {body.get('reason')}")
    if code != 200:
        failures.append("not ready while healthy")

    print(f"  stalling ingest for {args.slo + 2:.0f}s (writes continue) ...")
    t0 = time.time()
    lag_at_stall = health.snapshot().lag_seconds
    flipped_lag = None
    while time.time() - t0 < args.slo + 2:
        time.sleep(0.25)
        code, body = probe("/readyz")
        if code == 503 and flipped_lag is None:
            flipped_lag = body.get("lag_seconds", 0)
            print(f"  NOT READY at lag     {flipped_lag:.1f}s "
                  f"(stall began with {lag_at_stall:.1f}s already on the clock)")
    if flipped_lag is None:
        failures.append("readiness never failed despite exceeding SLO")
    elif flipped_lag < args.slo:
        # assert on LAG crossing the SLO, not on wall time since the stall began
        failures.append(f"flipped at lag {flipped_lag:.1f}s, below SLO {args.slo}s")

    print("  resuming ingest ...")
    t1 = time.time()
    code = 503
    while time.time() - t1 < 15:
        applied = pump()
        code, body = probe("/readyz")
        if code == 200:
            break
        time.sleep(0.2)
    print(f"  after resume        HTTP {code}  {body.get('reason')}  "
          f"(recovered in {time.time()-t1:.1f}s)")
    if code != 200:
        failures.append(f"did not recover after resume: {body.get('reason')}")

    print("\n  /metrics:")
    for line in probe_metrics().splitlines():
        if not line.startswith("#"):
            print(f"    {line}")

    stop.set()
    time.sleep(0.3)
    stream.drop_slot()
    ddl.teardown(cur)

    print("\n" + "=" * 72)
    print("PASS — DDL barriers and readiness gating both hold" if not failures
          else "FAIL:\n  - " + "\n  - ".join(failures))
    return 0 if not failures else 1


def probe_metrics() -> str:
    with urllib.request.urlopen(f"http://127.0.0.1:{PORT}/metrics", timeout=2) as r:
        return r.read().decode()


if __name__ == "__main__":
    raise SystemExit(main())
