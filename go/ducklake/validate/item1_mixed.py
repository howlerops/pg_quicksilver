"""Item 1: four mirrored tables, DuckLake on, four concurrent writers for DURATION seconds.

Writer i works on one table (ev_uuid, ledger, wide_text, lookup) from its own
connection: inserts, updates and deletes in small transactions. At TRUNCATE_AT
writer 3 TRUNCATEs lookup and reloads it in the same transaction. At ALTER_AT the
driver adds a column to ledger while the writers run. Every CHECK_EVERY seconds
the writers are paused (no transaction in flight), the mirror is waited up to the
primary's current WAL position, and then for every table: lake_equals_heap and
qs-verify. The last check is at DURATION.

Usage: item1_mixed.py MIRROR_PATH OUT_FILE
"""
import json
import os
import random
import sys
import threading
import time

import psycopg

import lakecheck as L

DURATION = int(os.environ.get("ITEM1_DURATION", 600))
CHECK_EVERY = int(os.environ.get("ITEM1_CHECK_EVERY", 60))
TRUNCATE_AT = int(os.environ.get("ITEM1_TRUNCATE_AT", 180))
ALTER_AT = int(os.environ.get("ITEM1_ALTER_AT", 300))
TABLES = ["public.ev_uuid", "public.ledger", "public.wide_text", "public.lookup"]

mirror, out_path = sys.argv[1], sys.argv[2]
out = open(out_path, "w", buffering=1)


def log(msg):
    line = time.strftime("%H:%M:%S") + " " + msg
    print(line, flush=True)
    out.write(line + "\n")


pause = threading.Event()
stop = threading.Event()
idle = [threading.Event() for _ in range(4)]
state = {"ledger_extra": False, "txns": [0, 0, 0, 0], "errors": []}
started = [0.0]


def connect():
    return psycopg.connect(L.PG_DSN, autocommit=False)


def writer(i):
    rng = random.Random(1000 + i)
    conn = connect()
    base = 10_000_000 * (i + 1)
    counter = base
    try:
        while not stop.is_set():
            if pause.is_set():
                idle[i].set()
                while pause.is_set() and not stop.is_set():
                    time.sleep(0.02)
                idle[i].clear()
                continue
            elapsed = time.time() - started[0]
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        if i == 0:  # ev_uuid: uuid key
                            for _ in range(rng.randint(3, 8)):
                                counter += 1
                                cur.execute("INSERT INTO ev_uuid (id, n, payload) VALUES (gen_random_uuid(), %s, %s)",
                                            (counter, "p%d" % counter))
                            a = rng.randint(1, max(2, counter))
                            cur.execute("UPDATE ev_uuid SET payload = payload || '*', updated = now() "
                                        "WHERE n BETWEEN %s AND %s", (a, a + 6))
                            if rng.random() < 0.3:
                                a = rng.randint(1, max(2, counter))
                                cur.execute("DELETE FROM ev_uuid WHERE n BETWEEN %s AND %s", (a, a + 2))
                        elif i == 1:  # ledger: numeric and timestamp columns
                            for _ in range(rng.randint(3, 8)):
                                counter += 1
                                cur.execute("INSERT INTO ledger (id, amount, ts, note) VALUES "
                                            "(%s, %s, now() - (%s || ' seconds')::interval, %s)",
                                            (counter, "%d.%03d" % (rng.randint(0, 99999), rng.randint(0, 999)),
                                             str(rng.randint(0, 10**6)), "n%d" % counter))
                            a = rng.randint(1, max(2, counter))
                            cur.execute("UPDATE ledger SET amount = amount + 0.125 WHERE id BETWEEN %s AND %s",
                                        (a, a + 9))
                            if state["ledger_extra"]:
                                cur.execute("UPDATE ledger SET extra = id %% 97 WHERE id BETWEEN %s AND %s",
                                            (a, a + 20))
                            if rng.random() < 0.3:
                                a = rng.randint(1, max(2, counter))
                                cur.execute("DELETE FROM ledger WHERE id BETWEEN %s AND %s", (a, a + 3))
                        elif i == 2:  # wide_text: twelve text columns
                            for _ in range(rng.randint(2, 5)):
                                counter += 1
                                cur.execute("INSERT INTO wide_text SELECT %s, " + ", ".join(
                                    ["repeat(%s, 40)"] * 12),
                                    (counter,) + tuple("c%02d-%d" % (k, counter) for k in range(1, 13)))
                            a = rng.randint(1, max(2, counter))
                            cur.execute("UPDATE wide_text SET c07 = c07 || %s WHERE id BETWEEN %s AND %s",
                                        (str(counter), a, a + 4))
                            if rng.random() < 0.3:
                                a = rng.randint(1, max(2, counter))
                                cur.execute("DELETE FROM wide_text WHERE id BETWEEN %s AND %s", (a, a + 1))
                        else:  # lookup: small, truncated once
                            if elapsed >= TRUNCATE_AT and not state.get("truncated"):
                                state["truncated"] = True
                                cur.execute("TRUNCATE lookup")
                                cur.execute("INSERT INTO lookup SELECT g, 'reload' || g FROM generate_series(1,50) g")
                                log("writer 3: TRUNCATE lookup and reload of 50 rows committed in this transaction")
                            else:
                                code = rng.randint(1, 400)
                                cur.execute("INSERT INTO lookup VALUES (%s, %s) ON CONFLICT (code) "
                                            "DO UPDATE SET label = EXCLUDED.label || '+'",
                                            (code, "l%d" % counter))
                                counter += 1
                                cur.execute("UPDATE lookup SET label = label || '.' WHERE code BETWEEN %s AND %s",
                                            (code, code + 3))
                                if rng.random() < 0.2:
                                    cur.execute("DELETE FROM lookup WHERE code BETWEEN %s AND %s",
                                                (code + 4, code + 5))
                state["txns"][i] += 1
            except psycopg.errors.Error as e:
                state["errors"].append((i, str(e)[:200]))
                conn.rollback()
            time.sleep(0.01)
    finally:
        conn.close()


def alter_ledger():
    conn = connect()
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE ledger ADD COLUMN extra int")
    conn.commit()
    conn.close()
    state["ledger_extra"] = True
    log("ALTER TABLE ledger ADD COLUMN extra int committed")


def pause_writers():
    pause.set()
    for k, ev in enumerate(idle):
        if writers[k].is_alive() and not ev.wait(60):
            raise RuntimeError("writer %d did not reach a transaction boundary in 60 s" % k)


def resume_writers():
    pause.clear()


def check(tag):
    rec = {"check": tag, "at_s": round(time.time() - started[0], 1)}
    if any(w.is_alive() for w in writers):
        pause_writers()
    try:
        admin = psycopg.connect(L.PG_DSN, autocommit=True)
        target = L.current_wal_lsn(admin)
        admin.close()
        rec["target_lsn"] = target
        t0 = time.time()
        for t in TABLES:
            L.wait_published(mirror, t, target, timeout=180)
        rec["wait_s"] = round(time.time() - t0, 2)
        results = {}
        for t in TABLES:
            eq = L.lake_equals_heap(t, mirror)
            verdict, raw = L.qs_verify(t, mirror)
            results[t] = {"lake_equals_heap": eq if eq is True else [list(map(str, d)) for d in eq],
                          "qs_verify": verdict, "qs_verify_out": raw.splitlines()[-3:]}
        rec["tables"] = results
        cur_rss, peak = L.rss_mb(sidecar_pid)
        rec["sidecar_rss_mb"] = cur_rss
        rec["sidecar_peak_rss_mb"] = peak
        rec["catalog_bytes"] = catalog_bytes()
        rec["txns"] = list(state["txns"])
    finally:
        if any(w.is_alive() for w in writers):
            resume_writers()
    ok = all(r["lake_equals_heap"] is True and r["qs_verify"] == "MATCH" for r in rec["tables"].values())
    rec["all_equal"] = ok
    log(json.dumps(rec, sort_keys=True))
    return ok


def catalog_bytes():
    import os
    p = L.catalog_path(mirror)
    return sum(os.path.getsize(f) for f in (p, p + "-wal") if os.path.exists(f))


sidecar_pid = int(open(sys.argv[3]).read().strip())

log("start: catalog bytes %d" % catalog_bytes())
started[0] = time.time()
writers = [threading.Thread(target=writer, args=(i,), daemon=True) for i in range(4)]
for t in writers:
    t.start()

all_ok = True
next_check = CHECK_EVERY
altered = False
while time.time() - started[0] < DURATION:
    time.sleep(0.5)
    el = time.time() - started[0]
    if not altered and el >= ALTER_AT:
        altered = True
        alter_ledger()
    if el >= next_check:
        all_ok &= check("minute %d" % (next_check // 60))
        next_check += CHECK_EVERY
stop.set()
for t in writers:
    t.join(60)
time.sleep(0.5)
all_ok &= check("final at %ds" % DURATION)
log("writer transactions %s, writer errors %d" % (state["txns"], len(state["errors"])))
for e in state["errors"][:5]:
    log("writer error %s" % (e,))
log("RESULT " + ("ALL CHECKS EQUAL" if all_ok else "SOME CHECK DIVERGED"))
out.close()
