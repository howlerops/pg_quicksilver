#!/usr/bin/env python3
"""16 clients on the standby while the primary is written to and the mirror compacts.

    6  aggregates with an explicit quicksilver.max_lag (64MB): the mirror
       answers during the writes too
    4  aggregates under the strict default
    3  primary-key lookups, parameters over the extended protocol
    3  one server-side prepared statement each, executed with a value the
       mirror should take (customer 1: 30% of the table) and one the heap
       should (customer 5)

The writer moves an amount between two rows of customer 1 in one transaction,
so three things are the same in every committed state and a reader that sees
anything else has seen a state that never existed:

    count(*), sum(amount) of public.orders
    count(*), sum(amount) of customer 1
    count(*), sum(amount) of customer 5

They are computed from the heap on the primary before the writes start. Every
answer of every client, routed or not, is compared with them.

Every PAUSE seconds the writer stops, the primary's LSN is noted, and a
checksum that the writes DO change (count, sum, min, max of amount over a range
of ids) is computed from the heap at that LSN; a strict session on the standby
with quicksilver.min_applied_lsn = that LSN must then be answered by the
mirror, with exactly that value. The pauses are also when the mirror goes idle
and rewrites its base.

Exit status 1 on any error, any wrong answer, or a crash line in a server log.
"""
import argparse, collections, os, random, re, threading, time
import psycopg

BASE = os.environ.get("BASE", "/var/lib/postgresql/qsint")
PPORT, SPORT = int(os.environ.get("PPORT", 5580)), int(os.environ.get("SPORT", 5581))
TOTAL = "SELECT count(*), sum(amount) FROM public.orders"
BYSTATUS = "SELECT status, count(*), sum(amount) FROM public.orders GROUP BY status"
CUST = "SELECT count(*), sum(amount) FROM public.orders WHERE customer_id = %s"
RANGE = "SELECT count(*), sum(amount), min(amount), max(amount) FROM public.orders WHERE id >= 1 AND id <= 250000"

stats = collections.Counter()
errors, wrong = [], []
lock = threading.Lock()


def conn(port, settings=()):
    c = psycopg.connect(f"host=/tmp port={port} dbname=app user=postgres", autocommit=True, prepare_threshold=None)
    for s in settings:
        c.execute(s)
    return c


def note(kind, c, key=None):
    d = c.execute("SELECT side FROM quicksilver.last_decision()").fetchone()
    side = d[0] if d else "heap"
    with lock:
        stats[(kind, side)] += 1
    return side


def client(kind, i, stop, want):
    rnd = random.Random(i)
    try:
        if kind == "lag":
            c = conn(SPORT, ["SET quicksilver.max_lag = '64MB'"])
        else:
            c = conn(SPORT)
        if kind == "prepared":
            c.execute("PREPARE q(int) AS SELECT count(*), sum(amount) FROM public.orders WHERE customer_id = $1")
        while not stop.is_set():
            try:
                if kind in ("lag", "strict"):
                    if rnd.random() < 0.5:
                        c.execute("SELECT quicksilver.forget_last()")
                        got = c.execute(TOTAL).fetchone()
                        side = note(kind, c)
                        if got != want["total"]:
                            wrong.append((kind, side, "total", got, want["total"]))
                    else:
                        c.execute("SELECT quicksilver.forget_last()")
                        rows = c.execute(BYSTATUS).fetchall()
                        side = note(kind, c)
                        got = (sum(r[1] for r in rows), sum(r[2] for r in rows))
                        if got != want["total"]:
                            wrong.append((kind, side, "by status", got, want["total"]))
                elif kind == "lookup":
                    k = rnd.randint(1, 1000000)
                    c.execute("SELECT quicksilver.forget_last()")
                    got = c.execute("SELECT id, customer_id FROM public.orders WHERE id = %s", (k,)).fetchone()
                    side = note(kind, c)
                    if got is None or got[0] != k or side != "heap":
                        wrong.append((kind, side, "lookup", got, k))
                else:
                    cust = 1 if rnd.random() < 0.5 else 5
                    c.execute("SELECT quicksilver.forget_last()")
                    got = c.execute("EXECUTE q(%d)" % cust).fetchone()
                    side = note("prepared, customer %d" % cust, c)
                    if got != want[cust]:
                        wrong.append((kind, side, "customer %d" % cust, got, want[cust]))
            except psycopg.Error as e:
                errors.append((kind, type(e).__name__, str(e)[:300]))
                try:
                    c.close()
                except Exception:
                    pass
                time.sleep(0.5)
                c = conn(SPORT, ["SET quicksilver.max_lag = '64MB'"] if kind == "lag" else [])
                if kind == "prepared":
                    c.execute("PREPARE q(int) AS SELECT count(*), sum(amount) FROM public.orders WHERE customer_id = $1")
    except Exception as e:   # noqa
        errors.append((kind, "client died", repr(e)[:300]))


def writer(stop, hold, tps, ids, counter):
    c = conn(PPORT)
    rnd = random.Random(7)
    period, nxt = 1.0 / tps, time.time()
    while not stop.is_set():
        if hold.is_set():
            time.sleep(0.05)
            nxt = time.time()
            continue
        a, b = rnd.choice(ids), rnd.choice(ids)
        if a != b:
            d = rnd.randint(1, 500) / 100
            with c.transaction():
                c.execute("UPDATE public.orders SET amount = amount + %s WHERE id = %s", (d, a))
                c.execute("UPDATE public.orders SET amount = amount - %s WHERE id = %s", (d, b))
            counter[0] += 1
        nxt += period
        w = nxt - time.time()
        if w > 0:
            time.sleep(w)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=300)
    ap.add_argument("--tps", type=int, default=100)
    ap.add_argument("--pause-every", type=int, default=40)
    ap.add_argument("--pause", type=int, default=9)
    a = ap.parse_args()
    # What is expected comes from the HEAP, whatever this database routes by default.
    p = conn(PPORT, ["SET quicksilver.route = heap"])
    s = conn(SPORT, ["SET quicksilver.freshness_wait = '10s'"])
    want = {"total": p.execute(TOTAL).fetchone(), 1: p.execute(CUST, (1,)).fetchone(), 5: p.execute(CUST, (5,)).fetchone()}
    l0 = p.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
    print(f"expected, from the heap on the primary at {l0}: total {want['total']}, customer 1 {want[1]}, customer 5 {want[5]}")
    ids = [r[0] for r in p.execute("SELECT id FROM public.orders WHERE customer_id = 1 AND id <= 600000 ORDER BY id").fetchall()]
    log0 = {k: sum(1 for l in open(f"{BASE}/mirror.log", errors="replace") if k in l) for k in ("msg=compacted", 'msg="merged deltas')}

    stop, hold, counter = threading.Event(), threading.Event(), [0]
    threads = [threading.Thread(target=writer, args=(stop, hold, a.tps, ids, counter))]
    for i, kind in enumerate(["lag"] * 6 + ["strict"] * 4 + ["lookup"] * 3 + ["prepared"] * 3):
        threads.append(threading.Thread(target=client, args=(kind, i, stop, want)))
    for t in threads:
        t.start()
    t0, checks = time.time(), []
    while time.time() - t0 < a.seconds:
        time.sleep(min(a.pause_every, max(0.0, a.seconds - (time.time() - t0))))
        hold.set()
        time.sleep(0.3)
        lsn = p.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
        heap = p.execute(RANGE).fetchone()
        inv = p.execute(TOTAL).fetchone()
        s.execute("SET quicksilver.min_applied_lsn = '%s'" % lsn)
        for _ in range(200):
            if s.execute("SELECT pg_last_wal_replay_lsn() >= %s::pg_lsn", (lsn,)).fetchone()[0]:
                break
            time.sleep(0.05)
        s.execute("SELECT quicksilver.forget_last()")
        got = s.execute(RANGE).fetchone()
        d = s.execute("SELECT side, reason, detail FROM quicksilver.last_decision()").fetchone()
        fell = s.execute("EXPLAIN (COSTS OFF) " + RANGE).fetchall()
        ok = got == heap and d and d[0] == "mirror" and inv == want["total"]
        checks.append(ok)
        print("%4ds  checksum at %s: heap %s, standby %s by the %s: %s  (%d write transactions so far)%s" % (
            time.time() - t0, lsn, heap[1], got[1], d[0] if d else "heap", "EQUAL" if got == heap else "DIFFERENT", counter[0],
            "" if d and d[0] == "mirror" else "  [%s: %s]" % (d[1], (d[2] or "")[:160]) if d else ""), flush=True)
        if got != heap:
            wrong.append(("checksum", d, got, heap))
            # whose fault: this session's snapshot, or what the sidecar reported?
            n = conn(SPORT, ["SET quicksilver.freshness_wait = '10s'", "SET quicksilver.min_applied_lsn = '%s'" % lsn])
            again_new = n.execute(RANGE).fetchone()
            dn = n.execute("SELECT side FROM quicksilver.last_decision()").fetchone()
            again_old = s.execute(RANGE).fetchone()
            st = n.execute("SELECT applied_lsn, current_lsn FROM quicksilver.mirror_status() WHERE heap = 'public.orders'::regclass").fetchone()
            lake = n.execute("SELECT * FROM duckdb.query($q$ SELECT applied_lsn, manifest_seq, snapshot_id FROM __ducklake_metadata_lake.qs_mirror_table WHERE table_name = 'public.orders' $q$)").fetchone()
            print("      DIAG: new session %s by %s (%s); same session again %s (%s); reported %s; lake says %s; heap now %s" % (
                again_new[1], dn, "EQUAL" if again_new == heap else "DIFFERENT", again_old[1], "EQUAL" if again_old == heap else "DIFFERENT",
                st, lake, p.execute(RANGE).fetchone()[1]), flush=True)
            n.close()
        if inv != want["total"]:
            wrong.append(("the heap's own invariant", inv, want["total"]))
        time.sleep(a.pause)       # idle: the mirror rewrites its base
        hold.clear()
    stop.set()
    for t in threads:
        t.join()

    log1 = {k: sum(1 for l in open(f"{BASE}/mirror.log", errors="replace") if k in l) for k in log0}
    print("statements, by client and by who answered:")
    for (kind, side), n in sorted(stats.items()):
        print("  %-22s %-6s %d" % (kind, side, n))
    print("write transactions on the primary: %d; sidecar: %d delta merges, %d base rewrites during the run" % (
        counter[0], log1['msg="merged deltas'] - log0['msg="merged deltas'], log1["msg=compacted"] - log0["msg=compacted"]))
    crash = 0
    for name in ("primary", "standby"):
        n = sum(1 for l in open(f"{BASE}/{name}.log", errors="replace") if re.search(r"TRAP:|terminated by signal|PANIC:|leak", l))
        print(f"{name}.log: {n} crash or leak lines")
        crash += n
    routed_checks = sum(checks)
    print("errors: %d   wrong answers: %d   checksum checks routed and equal: %d of %d" % (len(errors), len(wrong), routed_checks, len(checks)))
    for e in errors[:10]:
        print("  ERROR", e)
    for w in wrong[:10]:
        print("  WRONG", w)
    return 1 if errors or wrong or crash or routed_checks != len(checks) else 0


if __name__ == "__main__":
    raise SystemExit(main())
