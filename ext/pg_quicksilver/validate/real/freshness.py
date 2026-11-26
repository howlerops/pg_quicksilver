#!/usr/bin/env python3
"""Freshness on the standby: the sidecar stops, the primary is written to.

  1. Strict (the default, quicksilver.max_lag = 0): the first write the standby
     replays past what the mirror reported takes the statement to the heap.
  2. A session that asked for bounded staleness (max_lag = 1MB) keeps being
     routed, with the mirror's older answer, until the primary has written
     more than that; then it too is the heap's.
  3. The sidecar starts again: routing resumes when the mirror has caught up.
  4. Read your writes: quicksilver.min_applied_lsn = the LSN of a write. While
     the mirror has not applied it the heap answers (freshness_wait = 0), or
     the statement waits for it up to freshness_wait and is then routed.

Exit status 1 if anything is not as described.
"""
import os, subprocess, sys, threading, time
import psycopg

BASE = os.environ.get("BASE", "/var/lib/postgresql/qsint")
PPORT, SPORT = int(os.environ.get("PPORT", 5580)), int(os.environ.get("SPORT", 5581))
HERE = os.path.dirname(os.path.abspath(__file__))
Q = "SELECT count(*), sum(amount) FROM public.orders"
FAIL = []


def conn(port, *settings):
    c = psycopg.connect(f"host=/tmp port={port} dbname=app user=postgres", autocommit=True, prepare_threshold=None)
    for s in settings:
        c.execute(s)
    return c


def ask(c):
    c.execute("SELECT quicksilver.forget_last()")
    t0 = time.perf_counter()
    r = c.execute(Q).fetchone()
    ms = (time.perf_counter() - t0) * 1000
    d = c.execute("SELECT side, reason, detail FROM quicksilver.last_decision()").fetchone() or ("heap", "", "")
    return r, d, ms


def replayed(p, s):
    lsn = p.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
    for _ in range(600):
        if s.execute("SELECT pg_last_wal_replay_lsn() >= %s::pg_lsn", (lsn,)).fetchone()[0]:
            return lsn
        time.sleep(0.05)
    raise SystemExit("the standby is not replaying")


def say(ok, text):
    print(f"[{'ok' if ok else 'FAIL'}] {text}", flush=True)
    if not ok:
        FAIL.append(text)


def sidecar(what):
    return subprocess.run([os.path.join(HERE, "sidecar.sh"), what]).returncode


def until_routed(c, secs):
    t0 = time.time()
    while time.time() - t0 < secs:
        r, d, _ = ask(c)
        if d[0] == "mirror":
            return time.time() - t0, r
        time.sleep(0.1)
    return None, None


def main():
    p = conn(PPORT)
    strict = conn(SPORT)
    loose = conn(SPORT, "SET quicksilver.max_lag = '1MB'", "SET quicksilver.freshness_wait = 0")
    heap = conn(SPORT, "SET quicksilver.route = heap")
    replayed(p, heap)
    took, r0 = until_routed(strict, 30)
    say(took is not None and r0 == heap.execute(Q).fetchone(), f"before: routed under the strict default, equal to the heap {r0}")

    sidecar("stop")
    print("     sidecar stopped (SIGTERM)")
    p.execute("UPDATE public.orders SET amount = amount + 1.00 WHERE id = 1")
    replayed(p, heap)
    hr = heap.execute(Q).fetchone()
    r, d, _ = ask(strict)
    say(d[0] == "heap" and r == hr, f"strict, after ONE write: answered by the {d[0]} ({d[1]}: {d[2][:150]}), equal to the heap: {r == hr}")
    r, d, _ = ask(loose)
    say(d[0] == "mirror" and r == r0 and r != hr,
        f"max_lag = 1MB, after one write: answered by the {d[0]} with the mirror's older answer {r}; the heap says {hr} (this is what the setting asks for)")
    # more than the bound
    p.execute("UPDATE public.orders SET amount = amount + 0.01 WHERE id BETWEEN 2 AND 20000")
    replayed(p, heap)
    behind = heap.execute("SELECT pg_last_wal_replay_lsn() - applied_lsn FROM quicksilver.mirror_status() WHERE heap = 'public.orders'::regclass").fetchone()[0]
    hr = heap.execute(Q).fetchone()
    r, d, _ = ask(loose)
    say(d[0] == "heap" and r == hr, f"max_lag = 1MB, {int(behind)} WAL bytes behind: answered by the {d[0]} ({d[2][:110]}), equal to the heap: {r == hr}")

    # read your writes, while the mirror is still behind
    p.execute("UPDATE public.orders SET amount = amount + 5.00 WHERE id = 2")
    mine = replayed(p, heap)
    hr = heap.execute(Q).fetchone()
    ryw = conn(SPORT, "SET quicksilver.max_lag = '1GB'", "SET quicksilver.freshness_wait = 0", f"SET quicksilver.min_applied_lsn = '{mine}'")
    r, d, ms = ask(ryw)
    say(d[0] == "heap" and r == hr,
        f"read your writes, min_applied_lsn = {mine}, max_lag = 1GB, freshness_wait = 0: the {d[0]} answers in {ms:.0f} ms ({d[2][:120]}), with the write: {r == hr}")
    ryw.execute("SET quicksilver.freshness_wait = '500ms'")
    r, d, ms = ask(ryw)
    say(d[0] == "heap" and r == hr and ms >= 450,
        f"  ... freshness_wait = 500ms, sidecar still down: waits {ms:.0f} ms, then the {d[0]} answers, with the write: {r == hr}")

    # the sidecar comes back while a statement is waiting for it
    ryw.execute("SET quicksilver.freshness_wait = '60s'")
    th = threading.Thread(target=sidecar, args=("start",))
    t0 = time.time()
    th.start()
    r, d, ms = ask(ryw)
    th.join()
    say(d[0] == "mirror" and r == hr,
        f"  ... freshness_wait = 60s, sidecar started at the same moment: waited {ms / 1000:.1f} s and was answered by the {d[0]}, with the write: {r == hr}"
        + ("" if d[0] == "mirror" else f" ({d[1]}: {d[2][:200]})"))

    replayed(p, heap)
    took, r = until_routed(strict, 60)
    hr = heap.execute(Q).fetchone()
    say(took is not None and r == hr, f"after the restart: strict routing resumed {took:.1f} s after the sidecar was ready, equal to the heap {hr}")
    took, r = until_routed(loose, 10)
    say(took is not None and r == hr, "  ... and the max_lag = 1MB session is routed again, equal to the heap")
    print("FAILED: %d" % len(FAIL) if FAIL else "all ok")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
