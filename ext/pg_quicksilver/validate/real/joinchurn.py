#!/usr/bin/env python3
"""Routed join aggregates under writes to join keys, with a join domain (docs/40, "Join keys").

A writer on the primary changes public.jo.customer_id (to other existing customers, to
brand-new uuids) and inserts and deletes public.jc rows, for --seconds. Every --pause-every
seconds the writer pauses, the heap's answer is computed on the primary at a known LSN, and
the standby answers the same statement routed with quicksilver.min_applied_lsn set to that LSN.
Every comparison must be equal. Each check also records whether the routed plan rewrote the
join keys to companions ("Join keys" in EXPLAIN), so the run shows the transition when the
domain becomes incomplete (the sidecar was started with QS_JOIN_DOMAIN_MAX_KEYS just above
the current key count, so the writer's brand-new uuids disable it mid-run).

  PPORT, SPORT, BASE in the environment (validate/real/env.sh); --seconds 300 by default.
"""
import argparse, os, random, re, threading, time
import psycopg

PPORT = int(os.environ.get("PPORT", "5580"))
SPORT = int(os.environ.get("SPORT", "5581"))
BASE = os.environ.get("BASE", "/var/lib/postgresql/qsjoin")
DSN = "host=/tmp user=postgres dbname=app port=%d"

AGG = ("SELECT c.tenant_id, count(*), sum(o.amount) FROM public.jo o "
       "JOIN public.jc c ON c.id = o.customer_id GROUP BY c.tenant_id ORDER BY 1")


def conn(port, settings=()):
    c = psycopg.connect(DSN % port, autocommit=True, prepare_threshold=None)
    for s in settings:
        c.execute(s)
    return c


def writer(stop, hold, tps, counter, errors):
    w = conn(PPORT, ["SET quicksilver.route = heap"])
    orders = [r[0] for r in w.execute("SELECT id FROM public.jo ORDER BY random() LIMIT 20000").fetchall()]
    customers = [r[0] for r in w.execute("SELECT id FROM public.jc ORDER BY random() LIMIT 20000").fetchall()]
    rnd = random.Random(7)
    gap = 1.0 / tps
    while not stop.is_set():
        if hold.is_set():
            time.sleep(0.05)
            continue
        op = rnd.random()
        try:
            if op < 0.40:      # to another existing customer
                w.execute("UPDATE public.jo SET customer_id = %s WHERE id = %s",
                          (rnd.choice(customers), rnd.choice(orders)))
            elif op < 0.70:    # to a brand-new uuid
                w.execute("UPDATE public.jo SET customer_id = gen_random_uuid() WHERE id = %s", (rnd.choice(orders),))
            elif op < 0.85:    # a new customer
                cid = w.execute("SELECT gen_random_uuid()").fetchone()[0]
                w.execute("INSERT INTO public.jc (id, tenant_id, name, email, created_at) "
                          "VALUES (%s, 1, 'new', 'new@example.com', now())", (cid,))
                customers.append(cid)
            else:              # a customer goes
                if customers:
                    cid = customers.pop(rnd.randrange(len(customers)))
                    w.execute("DELETE FROM public.jc WHERE id = %s", (cid,))
            counter[0] += 1
        except Exception as e:  # the writer's errors are reported, not hidden
            errors.append(("writer", str(e)[:200]))
        time.sleep(gap)
    w.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=300)
    ap.add_argument("--tps", type=int, default=50)
    ap.add_argument("--pause-every", type=int, default=40)
    ap.add_argument("--pause", type=int, default=4)
    a = ap.parse_args()
    p = conn(PPORT, ["SET quicksilver.route = heap"])
    s = conn(SPORT, ["SET quicksilver.route = mirror", "SET quicksilver.freshness_wait = '10s'"])
    stop, hold, counter, errors = threading.Event(), threading.Event(), [0], []
    t = threading.Thread(target=writer, args=(stop, hold, a.tps, counter, errors))
    t.start()
    rows = []
    t0 = time.time()
    while time.time() - t0 < a.seconds:
        time.sleep(min(a.pause_every, max(0.0, a.seconds - (time.time() - t0))))
        hold.set()
        time.sleep(0.5)
        lsn = p.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
        heap = p.execute(AGG).fetchall()
        domain = p.execute("SELECT count(*) FROM quicksilver.join_domains").fetchone()[0]
        s.execute("SET quicksilver.min_applied_lsn = '%s'" % lsn)
        for _ in range(400):
            if s.execute("SELECT pg_last_wal_replay_lsn() >= %s::pg_lsn", (lsn,)).fetchone()[0]:
                break
            time.sleep(0.05)
        s.execute("SELECT quicksilver.forget_last()")
        got = s.execute(AGG).fetchall()
        d = s.execute("SELECT side, reason FROM quicksilver.last_decision()").fetchone()
        plan = "\n".join(r[0] for r in s.execute("EXPLAIN (COSTS OFF) " + AGG).fetchall())
        rewritten = "Join keys:" in plan
        equal = got == heap
        rows.append((time.time() - t0, equal, d[0] if d else "?", rewritten, domain))
        print("%5.0fs  writes %6d  join_domains rows %d  checksum: heap %s, standby %s (%s): %s, keys %s"
              % (time.time() - t0, counter[0], domain, "rows=%d" % len(heap), "rows=%d" % len(got),
                 d[0] if d else "?", "EQUAL" if equal else "DIFFERENT",
                 "rewritten" if rewritten else "not rewritten"), flush=True)
        if not equal:
            errors.append(("checksum differs", heap[:3], got[:3]))
        s.execute("SELECT 1")
        time.sleep(a.pause)
        hold.clear()
    stop.set()
    t.join()
    routed = sum(1 for r in rows if r[2] == "mirror")
    equal = sum(1 for r in rows if r[1])
    rewritten = sum(1 for r in rows if r[3])
    print("checks %d: routed %d, equal %d, rewritten %d; writer transactions %d" % (
        len(rows), routed, equal, rewritten, counter[0]))
    for e in errors[:10]:
        print("  ERROR", e)
    return 1 if errors or equal != len(rows) or routed != len(rows) else 0


if __name__ == "__main__":
    raise SystemExit(main())
