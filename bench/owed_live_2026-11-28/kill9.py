#!/usr/bin/env python3
"""Scenario B: kill -9 the sidecar during a replay hold, restart it still paused, resume, verify.

usage: kill9.py MODE OUT.json      MODE = hold | drain | spool
Cluster: primary :6610, standby :6611 (replay paused by this script), sidecar health :6612,
sidecar pid in BASE/mirror.pid. The sidecar is started/stopped with ext/.../validate/real/sidecar.sh
and the environment of env.sh (JOIN=on: join domains as the cluster was bootstrapped).
"""
import json, os, signal, subprocess, sys, time, urllib.request, hashlib
import psycopg

MODE, OUT = sys.argv[1], sys.argv[2]
RUN = str(int(time.time()))[-7:]  # unique tags: customer names must not repeat across runs
WT = "/home/user/pg_quicksilver/.claude/worktrees/agent-a63c879ecb16e1405"
BASE = "/var/lib/postgresql/w2e"
PG = "/var/lib/postgresql/w2e/pg/usr/lib/postgresql/17/bin"
ENV = dict(os.environ, PG=PG, BASE=BASE, PPORT="6610", SPORT="6611", HPORT="6612",
           DUCK_EXTERNAL="off", DUCK_DIRS="", QS_TABLES_OVERRIDE="public.orders,public.customers",
           SIDECAR_ENV="QS_JOIN_DOMAINS=cust=public.customers.id,public.orders.customer_id")
SIDECAR = f"{WT}/ext/pg_quicksilver/validate/real/sidecar.sh"
LOG = {"mode": MODE, "events": []}


def ev(msg, **kw):
    rec = {"t": round(time.time(), 3), "event": msg, **kw}
    LOG["events"].append(rec)
    print(time.strftime("%H:%M:%S"), msg, {k: v for k, v in kw.items()}, flush=True)


def conn(port, auto=True):
    return psycopg.connect(f"host=/tmp port={port} dbname=app user=postgres", autocommit=auto)


P = conn(6610)
S = conn(6611)
SQL = {"route_auto": "SET quicksilver.route = 'auto'", "route_heap": "SET quicksilver.route = 'heap'"}
S.execute(SQL["route_auto"])


def readyz():
    try:
        with urllib.request.urlopen("http://127.0.0.1:6612/readyz", timeout=5) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:  # sidecar down
        return 0, str(e)


def sidecar_pid():
    try:
        return int(open(f"{BASE}/mirror.pid").read().strip())
    except Exception:
        return 0


def alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def kill9():
    pid = sidecar_pid()
    os.kill(pid, signal.SIGKILL)
    for _ in range(200):
        if not alive(pid):
            break
        time.sleep(0.05)
    ev("kill -9", pid=pid, dead=not alive(pid))


def start_sidecar():
    r = subprocess.run([SIDECAR, "start"], env=ENV, capture_output=True, text=True)
    ev("sidecar start", rc=r.returncode, pid=sidecar_pid(), err=(r.stderr or r.stdout)[-300:])
    return r.returncode == 0


def sample_counts(n=10):
    out = []
    for _ in range(n):
        S.execute(SQL["route_auto"])
        routed = S.execute("SELECT count(*) FROM public.orders").fetchone()[0]
        S.execute(SQL["route_heap"])
        heap = S.execute("SELECT count(*) FROM public.orders").fetchone()[0]
        out.append((routed, heap))
        time.sleep(0.1)
    S.execute(SQL["route_auto"])
    return out


def slot_and_applied():
    flush = P.execute("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name LIKE 'qs_int%'").fetchone()
    rows = S.execute("SELECT heap::text, applied_lsn::text FROM quicksilver.mirror_status() WHERE applied_lsn IS NOT NULL").fetchall()
    applied = min((r[1] for r in rows), key=lsn_to_int) if rows else None
    detail = {"slot_confirmed_flush": flush[0] if flush else None, "min_applied": applied,
              "per_table": {r[0]: r[1] for r in rows},
              "slot_le_applied_plus_1": bool(flush and applied and lsn_to_int(flush[0]) <= lsn_to_int(applied) + 1)}
    ev("slot and applied", **detail)
    return detail["slot_confirmed_flush"], applied


def lsn_to_int(s):
    hi, lo = s.split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


def workload_small(n, tag):
    for i in range(n):
        P.execute("BEGIN")
        P.execute("INSERT INTO public.customers VALUES (gen_random_uuid(), 1 + %s %% 200, %s, %s, now())",
                  (i, f"{tag}{i}", f"{tag}{i}@example.com"))
        P.execute("INSERT INTO public.orders (id, tenant_id, customer_id, status, amount, created_at, payload, note) "
                  "SELECT gen_random_uuid(), 1, id, 'paid', 3.50, now(), '{}'::jsonb, %s FROM public.customers "
                  "WHERE name = %s", (tag, f"{tag}{i}"))
        P.execute("COMMIT")


def workload_big(rows):
    P.execute("INSERT INTO public.orders (id, tenant_id, customer_id, status, amount, created_at, payload, note) "
              "SELECT gen_random_uuid(), 2, (SELECT id FROM public.customers ORDER BY id LIMIT 1), 'shipped', 9.99, "
              "now(), '{}'::jsonb, 'big' FROM generate_series(1, %s)", (rows,))


def count_pair():
    S.execute(SQL["route_auto"])
    r = S.execute("SELECT count(*) FROM public.orders").fetchone()[0]
    S.execute(SQL["route_heap"])
    h = S.execute("SELECT count(*) FROM public.orders").fetchone()[0]
    S.execute(SQL["route_auto"])
    return r, h


def wait_ready_text(pat, timeout=120):
    t0 = time.time()
    while time.time() - t0 < timeout:
        code, body = readyz()
        if pat in body:
            return True, body
        time.sleep(0.2)
    return False, readyz()[1]


def wait_caught_up(timeout=300):
    t0 = time.time()
    want = P.execute("SELECT count(*) FROM public.orders").fetchone()[0]
    while time.time() - t0 < timeout:
        r, h = count_pair()
        code, body = readyz()
        if r == want and h == want and "waiting for local replay" not in body:
            return round(time.time() - t0, 2)
        time.sleep(0.2)
    return None


def verify_tables():
    res = {}
    for t in ("public.orders", "public.customers"):
        q = "SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id)) FROM " + t
        p = list(P.execute(q).fetchone())
        S.execute(SQL["route_heap"])
        s = list(S.execute(q).fetchone())
        S.execute(SQL["route_auto"])
        r = list(S.execute(q).fetchone())
        res[t] = {"primary": p, "standby_heap": s, "standby_routed": r, "equal": p == s == r and p[0] == p[1]}
    S.execute(SQL["route_auto"])
    # qs-verify: mirror on disk against the standby heap
    v = {}
    for t in ("public.orders", "public.customers"):
        r = subprocess.run([f"{BASE}/qs-verify", "-dsn", "host=/tmp port=6611 dbname=app user=postgres",
                            "-mirror", f"{BASE}/mirror", "-table", t], env=ENV, capture_output=True, text=True, timeout=900)
        v[t] = {"rc": r.returncode, "out": (r.stdout + r.stderr)[-400:]}
    res["qs_verify"] = v
    return res


ALL_SAMPLES = []

# ---- set up: pause replay, record starting point --------------------------------
S.execute("SELECT pg_wal_replay_pause()")
ev("replay paused")
base_count = P.execute("SELECT count(*) FROM public.orders").fetchone()[0]
ev("primary orders before", n=base_count)

if MODE in ("hold", "drain"):
    workload_small(60, f"{RUN}{MODE[0]}a")
    workload_big(100000)
    workload_small(60, f"{RUN}{MODE[0]}b")
elif MODE == "spool":
    # a large transaction that is still open while the sidecar spools it
    big = conn(6610, auto=False)
    big.execute("SET application_name = 'w2e_big'")
    for chunk in range(4):
        big.execute("INSERT INTO public.orders (id, tenant_id, customer_id, status, amount, created_at, payload, note) "
                    "SELECT gen_random_uuid(), 3, (SELECT id FROM public.customers ORDER BY id LIMIT 1), 'paid', 1, now(), "
                    "'{}'::jsonb, 'spool' FROM generate_series(1, 40000)")
        time.sleep(4)
    workload_small(40, f"{RUN}sa")
    ev("big txn open, rows written", rows=160000)
else:
    sys.exit("mode")

# ---- hold: the sidecar must hold the commits it cannot replay yet ----------------------
ok, body = wait_ready_text("waiting for local replay" if MODE != "spool" else "waiting", 120)
ev("readiness during hold", seen=ok, body=body[:400])
flush, applied = slot_and_applied()
ev("slot vs applied", confirmed_flush=flush, applied_lsn=applied)
ALL_SAMPLES.append({"when": "hold", "pairs": sample_counts(8)})
ev("routed vs heap during hold", pairs=ALL_SAMPLES[-1]["pairs"])

if MODE == "hold":
    # kill during the hold
    kill9()
    fl, ap = slot_and_applied()
    ev("slot after kill", confirmed_flush=fl, applied_lsn=ap)
    start_sidecar()
    ok, body = wait_ready_text("waiting for local replay", 120)
    ev("readiness after restart (still paused)", seen=ok, body=body[:400])
    ALL_SAMPLES.append({"when": "after_restart_paused", "pairs": sample_counts(8)})
    ev("routed vs heap after restart while paused", pairs=ALL_SAMPLES[-1]["pairs"])
    flush, applied = slot_and_applied()
    ev("slot vs applied after restart", confirmed_flush=flush, applied_lsn=applied)
    S.execute("SELECT pg_wal_replay_resume()")
    ev("replay resumed")
    ev("caught up after resume (s)", s=wait_caught_up())
elif MODE == "drain":
    S.execute("SELECT pg_wal_replay_resume()")
    ev("replay resumed")
    # kill while the mirror drains the held commits
    t0 = time.time()
    while time.time() - t0 < 20:
        r, h = count_pair()
        if h > base_count and r < P.execute("SELECT count(*) FROM public.orders").fetchone()[0]:
            break
        time.sleep(0.02)
    ev("draining observed", routed=r, heap=h, want=P.execute("SELECT count(*) FROM public.orders").fetchone()[0])
    kill9()
    flush, applied = slot_and_applied()
    ev("slot vs applied after kill", confirmed_flush=flush, applied_lsn=applied)
    start_sidecar()
    ok, body = wait_ready_text("ready", 120)
    ev("readiness after restart (resumed)", body=body[:300])
    ev("caught up after restart (s)", s=wait_caught_up())
elif MODE == "spool":
    kill9()
    start_sidecar()
    ok, body = wait_ready_text("ready", 120)
    ev("readiness after restart (paused)", body=body[:300])
    ALL_SAMPLES.append({"when": "after_restart_paused", "pairs": sample_counts(8)})
    # commit the large transaction now that the sidecar is back (replay still paused)
    big.commit()
    ev("big txn committed while paused", rows=160000)
    ok, body = wait_ready_text("waiting for local replay", 120)
    ev("readiness after big commit (paused)", seen=ok, body=body[:400])
    ALL_SAMPLES.append({"when": "after_big_commit_paused", "pairs": sample_counts(8)})
    ev("routed vs heap after big commit while paused", pairs=ALL_SAMPLES[-1]["pairs"])
    S.execute("SELECT pg_wal_replay_resume()")
    ev("replay resumed")
    ev("caught up after resume (s)", s=wait_caught_up())

LOG["samples"] = ALL_SAMPLES
LOG["verify"] = verify_tables()
ev("verify", verify=LOG["verify"])
json.dump(LOG, open(OUT, "w"), indent=1, default=str)
print("wrote", OUT)
