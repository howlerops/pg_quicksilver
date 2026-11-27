#!/usr/bin/env python3
"""Join completeness per node, two standbys against one primary (joinnodes_setup.sh).

Node 1 (the setup's standby, complete) and node 2 (its domain tripped its cap, incomplete)
read the same registry. The checks:

  1. The join is rewritten to companions on node 1 only; both nodes answer what the heap
     answers, for every join statement below (routed = heap, on both).
  2. Toggle stress on node 1, for --seconds: concurrent routed joins while a superuser
     flips node 1's report of quicksilver.join_domains (ex.jord.cust) between complete and
     incomplete. Every answer equals the heap's; no registry row is inserted or deleted.
     Errors "changed while this statement ran" are counted (the statement may be run again);
     a wrong answer is never acceptable.

    joinnodes.py [--primary 5620] [--node1 5621] [--node2 5623] [--seconds 120]
Exit status 1 on any wrong answer, any rewrite where it must not be, or any registry churn.
"""
import argparse, os, sys, threading, time
import psycopg

JOINS = [
    "SELECT count(*), sum(o.amount) FROM ex.jcust c JOIN ex.jord o ON o.cust = c.id",
    "SELECT c.region, count(*) FROM ex.jcust c JOIN ex.jord o ON o.cust = c.id GROUP BY c.region ORDER BY 1",
    "SELECT o.id, c.region FROM ex.jord o LEFT JOIN ex.jcust c ON c.id = o.cust ORDER BY o.id",
]
ROUTED_ERR = "changed while this statement ran"


def conn(port, user="postgres"):
    c = psycopg.connect(host="/tmp", port=port, user=user, dbname="app", autocommit=True)
    return c


def run(c, sql, route):
    with c.cursor() as cur:
        cur.execute("SET quicksilver.route = %s" % route)
        cur.execute(sql)
        rows = cur.fetchall()
        cur.execute("SET quicksilver.route = off")
    return rows


def explain_rewritten(c, sql):
    with c.cursor() as cur:
        cur.execute("SET quicksilver.route = mirror")
        cur.execute("EXPLAIN (COSTS OFF) " + sql)
        text = "\n".join(r[0] for r in cur.fetchall())
        cur.execute("SET quicksilver.route = off")
    return "Join keys:" in text and "rewritten to companions" in text, text


def wait_rewrite(c, sql, want, seconds=120):
    end = time.time() + seconds
    while time.time() < end:
        got, _ = explain_rewritten(c, sql)
        if got == want:
            return True
        time.sleep(0.5)
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--primary", type=int, default=int(os.environ.get("PPORT", 5620)))
    ap.add_argument("--node1", type=int, default=int(os.environ.get("SPORT", 5621)))
    ap.add_argument("--node2", type=int, default=int(os.environ.get("S2", 5623)))
    ap.add_argument("--seconds", type=int, default=120)
    ap.add_argument("--no-node2", action="store_true", help="node 2 is not running: the toggle stress only")
    a = ap.parse_args()
    bad = []
    p = conn(a.primary)
    n1 = conn(a.node1)
    n2 = None if a.no_node2 else conn(a.node2)
    super1 = conn(a.node1)

    # 1. the rewrite, per node, and the answers
    j = JOINS[0]
    if not wait_rewrite(n1, j, True):
        bad.append("node 1 never rewrote the join (its report says complete)")
    if n2 is not None and wait_rewrite(n2, j, True, seconds=3):
        bad.append("node 2 rewrote a join over a column it does not report complete")
    for name, c in [("node1", n1)] + ([("node2", n2)] if n2 is not None else []):
        for q in JOINS:
            heap = run(c, q, "heap")
            routed = run(c, q, "mirror")
            if sorted(map(repr, heap)) != sorted(map(repr, routed)):
                bad.append("%s: routed answer differs from the heap for %s" % (name, q[:60]))
    print("step 1: node1 rewrites=%s node2 rewrites=%s; answers checked on %s"
          % (explain_rewritten(n1, j)[0], explain_rewritten(n2, j)[0] if n2 is not None else "-",
             "both nodes" if n2 is not None else "node 1"))

    # 2. toggle stress on node 1, with the heap answers taken first
    expect = {q: sorted(map(repr, run(n1, q, "heap"))) for q in JOINS}
    with p.cursor() as cur:
        cur.execute("SELECT coalesce(sum(n_tup_ins + n_tup_del), 0) FROM pg_stat_user_tables "
                    "WHERE schemaname = 'quicksilver' AND relname = 'join_domains'")
        churn0 = cur.fetchone()[0]
    stop = threading.Event()
    stats = {"answers": 0, "wrong": 0, "changed": 0, "other": 0}
    lock = threading.Lock()

    def worker(i):
        c = conn(a.node1)
        k = 0
        while not stop.is_set():
            q = JOINS[k % len(JOINS)]
            k += 1
            try:
                got = sorted(map(repr, run(c, q, "mirror")))
                with lock:
                    stats["answers"] += 1
                    if got != expect[q]:
                        stats["wrong"] += 1
                        bad.append("wrong answer under toggling for %s" % q[:50])
            except psycopg.Error as e:
                with lock:
                    if ROUTED_ERR in str(e):
                        stats["changed"] += 1
                    else:
                        stats["other"] += 1
                        if stats["other"] <= 3:
                            print("worker error:", str(e).splitlines()[0][:200])
        c.close()

    def toggler():
        flips = 0
        val = True
        while not stop.is_set():
            val = not val
            super1.execute("SELECT quicksilver.report_join_complete('ex.jord'::regclass, 'cust', %s)", (val,))
            flips += 1
            time.sleep(0.15)
        stats["flips"] = flips

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    tt = threading.Thread(target=toggler)
    for t in threads + [tt]:
        t.start()
    time.sleep(a.seconds)
    stop.set()
    for t in threads + [tt]:
        t.join()
    # the sidecar's own report is true again within a heartbeat; wait so the churn count is final
    time.sleep(3)
    with p.cursor() as cur:
        cur.execute("SELECT coalesce(sum(n_tup_ins + n_tup_del), 0) FROM pg_stat_user_tables "
                    "WHERE schemaname = 'quicksilver' AND relname = 'join_domains'")
        churn1 = cur.fetchone()[0]
    print("step 2: %(answers)d routed answers, %(wrong)d wrong, %(changed)d changed-while-running, "
          "%(other)d other errors, %(flips)s flips; registry inserts+deletes %(d)d"
          % dict(stats, d=churn1 - churn0))
    if churn1 != churn0:
        bad.append("registry churn: %d inserts/deletes under toggling" % (churn1 - churn0))
    if stats["other"]:
        bad.append("%d errors other than the completeness change" % stats["other"])
    if stats["answers"] == 0:
        bad.append("no routed answer completed during the stress")
    for b in bad:
        print("FAIL:", b)
    print("problems %d" % len(bad))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
