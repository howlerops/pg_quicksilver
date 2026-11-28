#!/usr/bin/env python3
"""Scenario C (namespaced cluster from c2_setup.sh): stop the primary P (6620), promote standby A
(6621, sidecar health 6623), repoint standby B (6622, sidecar health 6624) at A, then two minutes
under writes on A. Both standbys keep their own mirror, seen at the same path (mount namespaces).

Pollers on A from before the stop: every 100 ms, routed count (quicksilver.max_lag = 0, the default,
and = 1GB) and heap count and the decision of the routed statement.
Samples on B, every second, after B is following A: routed and heap counts with the decision, and
the replay LSN before and after the pair; a pair is "stable" when B's replay LSN did not move
between its statements (same data on both sides, so routed must equal heap).
usage: c_promote2.py OUT.json
"""
import json, os, signal, statistics, subprocess, sys, threading, time
import psycopg

OUT = sys.argv[1]
POLL_S = float(os.environ.get("POLL_S", "0.1"))
BASE = os.environ.get("BASE", "/var/lib/postgresql/w2e")
C = f"{BASE}/c"
PG = os.environ.get("PG", f"{BASE}/pg/usr/lib/postgresql/17/bin")
MIR = f"{BASE}/mirror"
SD = os.path.dirname(os.path.abspath(__file__))
PP, PA, PB, HA, HB = (int(os.environ.get(k, d)) for k, d in
                      (("PP", 6620), ("PA", 6621), ("PB", 6622), ("HA", 6623), ("HB", 6624)))
T0 = time.time()
EV = []
# POLL_HEAP=0 drops the heap count from each poll (the routed count and the decision remain);
# NPOLL_1GB sessions poll with max_lag = 1GB, from before the stop, in parallel.
POLL_HEAP = os.environ.get("POLL_HEAP", "1") != "0"
NPOLL_1GB = int(os.environ.get("NPOLL_1GB", "1"))
POLL = {"strict": []}
for _i in range(NPOLL_1GB):
    POLL["lag1gb" if _i == 0 else f"lag1gb_{_i}"] = []
WRITES = []
stop_all = threading.Event()
writer_port = [PP]
ts = lambda: round(time.time() - T0, 3)


def ev(name, **kw):
    EV.append({"t": ts(), "event": name, **kw})
    print(f"{ts():9.3f} {name} {kw if kw else ''}", flush=True)


def conn(port, db="app"):
    return psycopg.connect(f"host=/tmp port={port} dbname={db} user=postgres", autocommit=True)


def sh(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"FAILED: {cmd}\n{r.stdout}\n{r.stderr}")
    return (r.stdout + r.stderr).strip()


def readyz(port):
    import urllib.request
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=3) as r:
            return r.status, r.read().decode()
    except Exception as e:
        return 0, str(e)


def qs_mirror_child(runuser_pid):
    """the qs-mirror process under a runuser (its parent)"""
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        try:
            st = open(f"/proc/{d}/stat").read().rsplit(")", 1)[1].split()
            ppid = int(st[1])
            cmd = open(f"/proc/{d}/cmdline", "rb").read().replace(b"\0", b" ").decode()
        except Exception:
            continue
        if ppid == runuser_pid and "qs-mirror" in cmd:
            return int(d)
    return None


def writer():
    c = None
    n = 0
    while not stop_all.is_set():
        try:
            if c is None:
                c = conn(writer_port[0])
            c.execute("INSERT INTO public.orders (id, tenant_id, customer_id, status, amount, created_at, payload, note) "
                      "SELECT gen_random_uuid(), 4, id, 'paid', 2.00, now(), '{}'::jsonb, 'c' FROM public.customers "
                      "ORDER BY id LIMIT 1")
            n += 1
        except Exception:
            if c is not None:
                try:
                    c.close()
                except Exception:
                    pass
            c = None
            time.sleep(0.05)
        time.sleep(0.02)
    WRITES.append(n)


def poller(name, max_lag):
    c = None
    while not stop_all.is_set():
        t = ts()
        try:
            if c is None:
                c = conn(PA)
                c.execute("SET quicksilver.route = 'auto'")
                if max_lag:
                    c.execute(f"SET quicksilver.max_lag = '{max_lag}'")
            routed = c.execute("SELECT count(*) FROM public.orders").fetchone()[0]
            dec = c.execute("SELECT quicksilver.last_decision()").fetchone()[0]
            heap = None
            if POLL_HEAP:
                c.execute("SET quicksilver.route = 'heap'")
                heap = c.execute("SELECT count(*) FROM public.orders").fetchone()[0]
                c.execute("SET quicksilver.route = 'auto'")
            POLL[name].append({"t": t, "routed": routed, "heap": heap, "decision": dec[0], "reason": dec[1],
                               "detail": str(dec[2])[:160]})
        except Exception as e:
            POLL[name].append({"t": t, "error": str(e)[:200]})
            try:
                c.close()
            except Exception:
                pass
            c = None
            time.sleep(0.3)
        time.sleep(POLL_S)


FLIP = {"observed": None}


def flip_monitor():
    """The first moment pg_is_in_recovery() reads false on A, from a session of its own."""
    c = conn(PA)
    while not stop_all.is_set():
        try:
            if not c.execute("SELECT pg_is_in_recovery()").fetchone()[0]:
                FLIP["observed"] = ts()
                return
        except Exception:
            time.sleep(0.05)
        time.sleep(0.005)


# ---- 1. pollers and writer -----------------------------------------------------------------
ev("start: writer on P, pollers on A (max_lag default, and 1GB)")
ths = [threading.Thread(target=writer, daemon=True),
       threading.Thread(target=poller, args=("strict", None), daemon=True),
       threading.Thread(target=flip_monitor, daemon=True)]
ths += [threading.Thread(target=poller, args=(n, "1GB"), daemon=True) for n in POLL if n.startswith("lag1gb")]
for th in ths:
    th.start()
time.sleep(15)
pre = POLL["lag1gb"][-1] if POLL["lag1gb"] else None
ev("baseline before stop", lag1gb_last=pre)

def headline_plan(port):
    c = conn(port)
    c.execute("SET quicksilver.route = 'auto'")
    rows = [r[0] for r in c.execute("EXPLAIN (ANALYZE, COSTS OFF) SELECT date_trunc('month', o.created_at), sum(o.amount), count(*) FROM public.orders o JOIN public.customers c ON c.id = o.customer_id GROUP BY 1").fetchall()]
    c.close()
    return [r.strip() for r in rows[:4]]


ev("headline plan on A before the stop", plan=headline_plan(PA))
# ---- 2. stop P, promote A --------------------------------------------------------------------
ev("stop primary P (fast)")
sh(f"su postgres -c '{PG}/pg_ctl -D {C}/primary -m fast -w stop'")
t_stop = ts()
ev("P stopped")
ev("promote A")
sh(f"su postgres -c '{PG}/pg_ctl -D {C}/standbyA -w promote'")
t_promote = ts()
writer_port[0] = PA
ev("A promoted", in_recovery=conn(PA).execute("SELECT pg_is_in_recovery()").fetchone()[0])
ev("headline plan on A after promotion", plan=headline_plan(PA))

# sidecar A: its health endpoint and log after promotion
t_standdown = None
for _ in range(200):
    if sh(f"grep -c 'standing down' {C}/side_A.log || true").strip() not in ("", "0"):
        t_standdown = ts()
        break
    time.sleep(0.1)
ev("sidecar A stand-down", seen=t_standdown is not None, at=t_standdown,
   log=sh(f"grep -E 'standing down|dropped this Pod|not a standby|primary' {C}/side_A.log | tail -4 | cut -c1-220"))
time.sleep(2)
ev("sidecar A readyz", code=readyz(HA)[0], body=readyz(HA)[1][:300])
ev("A mirror_status", rows=[tuple(map(str, r)) for r in conn(PA).execute(
    "SELECT heap::text, usable, reason, applied_lsn::text, current_lsn::text, fresh FROM quicksilver.mirror_status()").fetchall()])
ev("A slots", rows=conn(PA, "postgres").execute("SELECT slot_name, slot_type, active FROM pg_replication_slots").fetchall())

# ---- 3. repoint B at A ------------------------------------------------------------------------
conn(PA, "postgres").execute("SELECT pg_create_physical_replication_slot('qsint_b2', true)")
ev("physical slot qsint_b2 on A")
pid_b = int(open(f"{C}/side_B.pid").read().strip())
child = qs_mirror_child(pid_b)
for p in [child, pid_b]:
    if p:
        try:
            os.kill(p, signal.SIGTERM)
        except OSError:
            pass
for _ in range(100):
    if not child or not os.path.exists(f"/proc/{child}"):
        break
    time.sleep(0.1)
ev("sidecar B stopped", qs_mirror_pid=child, runuser_pid=pid_b)
sh(f"su postgres -c '{PG}/pg_ctl -D {C}/standbyB -m fast -w stop'")
ev("standby B stopped")
conf = f"{C}/standbyB/postgresql.auto.conf"
lines = [l for l in open(conf).read().splitlines() if not l.startswith(("primary_conninfo", "primary_slot_name"))]
lines += [f"primary_conninfo = 'host=/tmp port={PA} user=postgres'", "primary_slot_name = 'qsint_b2'"]
open(conf, "w").write("\n".join(lines) + "\n")
sh(f"chown postgres:postgres {conf}")
sh(f"{SD}/ns_exec.sh {C}/mirrorB {MIR} su postgres -c \"{PG}/pg_ctl -D {C}/standbyB -l {C}/standbyB.log -w start\"")
t_b_start = ts()
ev("standby B started in its namespace, following A", wal_receiver=conn(PB).execute(
    "SELECT status, sender_port, slot_name FROM pg_stat_wal_receiver").fetchall())
envf = f"{C}/side_B.env"
txt = open(envf).read().replace(f"QS_PRIMARY_PORT={PP}", f"QS_PRIMARY_PORT={PA}")
open(envf, "w").write(txt)
sh(f"{SD}/ns_exec.sh {C}/mirrorB {MIR} bash {SD}/start_side.sh {envf} {C}/side_B.log {C}/side_B.pid")
for _ in range(600):
    if readyz(HB)[0] == 200:
        break
    time.sleep(0.2)
t_b_ready = ts()
ev("sidecar B ready", seconds_from_start=round(t_b_ready - t_b_start, 2), readyz=readyz(HB)[1][:260],
   log=sh(f"grep -E 'resuming|bootstrapping|new replication slot|slot=|mirror stopped' {C}/side_B.log | tail -4 | cut -c1-220"))

# ---- 4. two minutes under writes -----------------------------------------------------------
samples = []
t_end = time.time() + 120
while time.time() < t_end:
    row = {"t": ts()}
    try:
        ca = conn(PA)
        ca.execute("SET quicksilver.route = 'heap'")
        row["A_heap"] = ca.execute("SELECT count(*) FROM public.orders").fetchone()[0]
        ca.close()
        cb = conn(PB)
        l1 = cb.execute("SELECT pg_last_wal_replay_lsn()::text").fetchone()[0]
        cb.execute("SET quicksilver.route = 'auto'")
        row["B_routed"] = cb.execute("SELECT count(*) FROM public.orders").fetchone()[0]
        row["B_decision"] = cb.execute("SELECT quicksilver.last_decision()").fetchone()[0][0]
        cb.execute("SET quicksilver.route = 'heap'")
        row["B_heap"] = cb.execute("SELECT count(*) FROM public.orders").fetchone()[0]
        l2 = cb.execute("SELECT pg_last_wal_replay_lsn()::text").fetchone()[0]
        row["B_stable"] = (l1 == l2)
        cb.execute("SET quicksilver.route = 'auto'")
        cb.execute("SET quicksilver.max_lag = '1GB'")
        row["B_routed_1gb"] = cb.execute("SELECT count(*) FROM public.orders").fetchone()[0]
        row["B_decision_1gb"] = cb.execute("SELECT quicksilver.last_decision()").fetchone()[0][0]
        cb.close()
        row["B_readyz"] = readyz(HB)[1][:130]
    except Exception as e:
        row["error"] = str(e)[:200]
    samples.append(row)
    time.sleep(1.0)

# ---- 5. end: stop writes, B catches up, compare ---------------------------------------------
stop_all.set()
for th in ths:
    th.join(timeout=30)
ev("writes stopped", writes=sum(WRITES))
want = conn(PA).execute("SELECT count(*) FROM public.orders").fetchone()[0]
t0 = time.time()
while time.time() - t0 < 300:
    if conn(PB).execute("SELECT count(*) FROM public.orders").fetchone()[0] == want:
        break
    time.sleep(0.5)
ev("B caught up", seconds=round(time.time() - t0, 2), want=want)
Q = "SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id)) FROM public.orders"
final = {}
for name, port, route in (("A_heap", PA, "heap"), ("B_heap", PB, "heap"), ("B_routed", PB, "auto")):
    c = conn(port)
    c.execute(f"SET quicksilver.route = '{route}'")
    final[name] = list(c.execute(Q).fetchone())
    c.close()
ev("final comparison", **final)
verify = sh(f"{BASE}/qs-verify -dsn 'host=/tmp port={PB} dbname=app user=postgres' -mirror {C}/mirrorB -table public.orders")
ev("qs-verify B mirror against B heap", out=verify.splitlines()[-1])

# The flip: the first poll that STARTED after the monitor saw pg_is_in_recovery() false on A.
# Every such poll must be heap-answered (the acceptance of the promotion fix).
flip = FLIP["observed"]
after = {}
for name in POLL:
    rows = [r for r in POLL[name] if "decision" in r and flip is not None and r["t"] >= flip]
    span = (rows[-1]["t"] - rows[0]["t"]) if len(rows) > 1 else 0
    after[name] = {"polls": len(rows),
                   "mirror": sum(1 for r in rows if r["decision"] == "mirror"),
                   "heap": sum(1 for r in rows if r["decision"] == "heap"),
                   "hz": round(len(rows) / span, 1) if span > 0 else None}
ev("after the flip (pg_is_in_recovery false on A)", flip_observed_at=flip, polls_after_flip=after)
res = {"t_stop": t_stop, "t_promote": t_promote, "t_b_start": t_b_start, "t_b_ready": t_b_ready,
       "flip_observed_at": flip, "after_flip": after,
       "events": EV, "polls": POLL, "samples": samples, "writes": sum(WRITES), "final": final}
json.dump(res, open(OUT, "w"), indent=1, default=str)
print("wrote", OUT)
