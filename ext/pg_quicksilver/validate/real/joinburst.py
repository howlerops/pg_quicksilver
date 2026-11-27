#!/usr/bin/env python3
"""Routed join comparisons on the STANDBY, taken as fast as possible while a join domain trips.

The writer of joinchurn.py churns the join keys of public.jo and public.jc on the primary.
The sidecar was started with QS_JOIN_DOMAIN_MAX_KEYS just above the starting key count, so the
writer's brand-new uuids disable the domain mid-run. Each comparison here: the writer is held
for a moment, the heap's answer is taken on the primary at a known LSN, the standby replays to
that LSN and answers the same statement routed (quicksilver.min_applied_lsn), and the standby's
answer must equal the heap's. The domain's disabled metric is read with each comparison, so the
comparisons are labelled with the trip. The run stops once --after comparisons have been taken
with the domain disabled (and only then; there is no pause between comparisons).

  PPORT, SPORT, HPORT, BASE in the environment (validate/real/env.sh).
"""
import argparse, os, re, sys, threading, time, urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from joinchurn import AGG, conn, writer  # noqa: E402

PPORT = int(os.environ.get("PPORT", "5580"))
SPORT = int(os.environ.get("SPORT", "5581"))
HPORT = int(os.environ.get("HPORT", "5582"))


def disabled_metric():
    txt = urllib.request.urlopen("http://127.0.0.1:%d/metrics" % HPORT, timeout=5).read().decode()
    m = re.search(r'quicksilver_join_domain_disabled\{domain="jd"\} (\d+)', txt)
    return int(m.group(1)) if m else -1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--after", type=int, default=200, help="comparisons to take after the trip")
    ap.add_argument("--max-seconds", type=int, default=600)
    ap.add_argument("--tps", type=int, default=50)
    ap.add_argument("--window", type=float, default=0.5, help="seconds the writer runs between comparisons")
    a = ap.parse_args()

    p = conn(PPORT, ["SET quicksilver.route = heap"])
    s = conn(SPORT, ["SET quicksilver.route = mirror", "SET quicksilver.freshness_wait = '10s'"])
    stop, hold, counter, errors = threading.Event(), threading.Event(), [0], []
    t = threading.Thread(target=writer, args=(stop, hold, a.tps, counter, errors))
    t.start()

    recs = []          # (index, equal, side, rewritten, disabled)
    trip = None        # index of the first comparison that saw the domain disabled
    t0 = time.time()
    while True:
        if trip is not None and len(recs) - trip >= a.after:
            break
        if time.time() - t0 > a.max_seconds:
            print("stopped at the time limit", flush=True)
            break
        hold.set()
        time.sleep(0.1)
        lsn = p.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
        heap = p.execute(AGG).fetchall()
        s.execute("SET quicksilver.min_applied_lsn = '%s'" % lsn)
        for _ in range(400):
            if s.execute("SELECT pg_last_wal_replay_lsn() >= %s::pg_lsn", (lsn,)).fetchone()[0]:
                break
            time.sleep(0.02)
        s.execute("SELECT quicksilver.forget_last()")
        got = s.execute(AGG).fetchall()
        d = s.execute("SELECT side, reason FROM quicksilver.last_decision()").fetchone()
        plan = "\n".join(r[0] for r in s.execute("EXPLAIN (COSTS OFF) " + AGG).fetchall())
        rewritten = "Join keys:" in plan
        dis = disabled_metric()
        equal = got == heap
        recs.append((len(recs), equal, d[0] if d else "?", rewritten, dis))
        if dis == 1 and trip is None:
            trip = len(recs) - 1
            print("domain disabled at comparison %d (%.1fs, writes %d)" % (trip, time.time() - t0, counter[0]),
                  flush=True)
        if not equal:
            errors.append(("comparison %d differs" % (len(recs) - 1), heap[:3], got[:3]))
        hold.clear()  # the writer runs for a window, then the next comparison holds it again
        time.sleep(a.window)
    stop.set()
    t.join()

    before = [r for r in recs if trip is None or r[0] < trip]
    after = [r for r in recs if trip is not None and r[0] >= trip]
    print("comparisons %d: before the trip %d, after %d" % (len(recs), len(before), len(after)))
    print("equal to the heap: %d of %d" % (sum(1 for r in recs if r[1]), len(recs)))
    print("routed (side mirror): %d of %d" % (sum(1 for r in recs if r[2] == "mirror"), len(recs)))
    print("rewritten to companions: before the trip %d, after %d" % (
        sum(1 for r in before if r[3]), sum(1 for r in after if r[3])))
    print("routed after the trip: %d of %d" % (sum(1 for r in after if r[2] == "mirror"), len(after)))
    print("writer transactions %d" % counter[0])
    for e in errors[:10]:
        print("  ERROR", e)
    ok = not errors and all(r[1] for r in recs) and trip is not None and len(after) >= a.after
    print("RESULT", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
