#!/usr/bin/env python3
"""A routed aggregate on the STANDBY, across everything the mirror goes through.

After validate/real/setup.sh. At every step, in a session opened when this
script started, in one opened just before the step, and in a new one:

  * the aggregate, issued to the standby with the database's own setting
    (quicksilver.route = auto), is answered by the mirror, and its rows are the
    heap's on the same node;
  * a statement prepared once at the start (no parameters: its plan is kept) is
    executed again and is the heap's answer too;
  * EXPLAIN shows the route and the reason;
  * a primary-key lookup stays on the heap;
  * the mirror relation is the same relation (same OID) unless the step is one
    that is supposed to replace it.

Steps: start, write burst, delta merge, idle base rewrite, TRUNCATE + reload,
ADD COLUMN (a DDL barrier), a value the mirror cannot hold (the table halts)
and its REBUILD, sidecar kill -9 and restart, standby restart.

Exit status 1 if any check fails.
"""
import os, subprocess, sys, time
import psycopg

BASE = os.environ.get("BASE", "/var/lib/postgresql/qsint")
PPORT, SPORT = int(os.environ.get("PPORT", 5580)), int(os.environ.get("SPORT", 5581))
HERE = os.path.dirname(os.path.abspath(__file__))
PG = os.environ.get("PG", "/usr/lib/postgresql/17/bin")
AGG = "SELECT status, count(*), sum(amount) FROM public.orders GROUP BY status"
TYPES = "SELECT count(*), sum(bi), max(n) FROM public.qs_types"
LOOKUP = "SELECT * FROM public.orders WHERE id = 4242"
FAIL = []


def conn(port, route=None):
    # prepare_threshold=None: psycopg would otherwise prepare a statement it has
    # sent five times, and a kept plan makes no new routing decision to report
    c = psycopg.connect(f"host=/tmp port={port} dbname=app user=postgres", autocommit=True, prepare_threshold=None)
    if route:
        c.execute(f"SET quicksilver.route = {route}")
    return c


def rows(c, sql):
    return sorted(c.execute(sql).fetchall(), key=repr)


def decision(c):
    d = c.execute("SELECT side, reason, detail FROM quicksilver.last_decision()").fetchone()
    return d or ("heap", "", "")


def replayed(p, s):
    lsn = p.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
    for _ in range(600):
        if s.execute("SELECT pg_last_wal_replay_lsn() >= %s::pg_lsn", (lsn,)).fetchone()[0]:
            return
        time.sleep(0.05)
    raise SystemExit("the standby is not replaying")


def explain(c, sql):
    out = [r[0].strip() for r in c.execute("EXPLAIN (COSTS OFF, VERBOSE) " + sql).fetchall()]
    return "; ".join(l for l in out if l.startswith("Quicksilver"))


def log_count(needle):
    try:
        return sum(1 for l in open(f"{BASE}/mirror.log", errors="replace") if needle in l)
    except OSError:
        return 0


def wait_log(needle, before, secs, poke=None):
    t0 = time.time()
    while time.time() - t0 < secs:
        if log_count(needle) > before:
            return True
        if poke:
            poke()
        time.sleep(0.5)
    return False


def routed_within(c, sql, secs):
    """Run sql until the mirror answers it; (seconds it took or None, rows, last decision)."""
    t0 = time.time()
    first = None
    while True:
        c.execute("SELECT quicksilver.forget_last()")
        r = rows(c, sql)
        d = decision(c)
        if d[0] == "mirror":
            return time.time() - t0, r, (d[0], d[1], d[2], first)
        if first is None:
            first = "%s: %s" % (d[1], (d[2] or "")[:160])
        if time.time() - t0 > secs:
            return None, r, (d[0], d[1], d[2], first)
        time.sleep(0.1)


class Run:
    def __init__(self):
        self.p = conn(PPORT)
        self.old = conn(SPORT)
        self.old.execute("PREPARE kept AS " + AGG)
        self.pre = None
        self.view_oid = None

    def reopen(self):
        self.old = conn(SPORT)
        self.old.execute("PREPARE kept AS " + AGG)

    def before(self):
        """A session opened just before the step."""
        if self.pre:
            self.pre.close()
        self.pre = conn(SPORT)
        self.pre.execute("SELECT 1")

    def check(self, step, sql=AGG, expect_new_view=False, wait=60, sessions=True):
        replayed(self.p, self.old if not self.old.closed else conn(SPORT))
        new = conn(SPORT)
        heap = conn(SPORT, "heap")
        ok, notes = True, []
        who = [("new", new)]
        if sessions:
            who = [("open since the start", self.old), ("opened before the step", self.pre), ("new", new)]
        for name, c in who:
            if c is None:
                continue
            took, got, d = routed_within(c, sql, wait)
            want = rows(heap, sql)
            if took is None:
                ok = False
                notes.append(f"{name}: NOT ROUTED ({d[1]}: {d[2]})")
            elif got != want:
                # the heap may have moved between the two; ask both again, quiesced
                replayed(self.p, heap)
                took2, got, d = routed_within(c, sql, wait)
                want = rows(heap, sql)
                if got != want:
                    ok = False
                    notes.append(f"{name}: DIFFERENT {got[:2]} vs heap {want[:2]}")
                else:
                    notes.append(f"{name}: mirror, equal")
            else:
                notes.append(f"{name}: mirror after {took:.1f}s, equal" + (f" (first on the heap: {d[3]})" if d[3] and took > 2 else ""))
        if sessions and sql == AGG:
            ex = explain(self.old, "EXECUTE kept")
            got, want = rows(self.old, "EXECUTE kept"), rows(heap, AGG)
            if got != want:
                ok = False
            notes.append("kept plan: %s, %s" % ("equal" if got == want else "DIFFERENT", ex or "heap plan"))
        # EXPLAIN plans the statement again: under strict freshness a mirror that
        # is a moment behind shows as "heap (not fresh enough)", so ask until it is not
        for _ in range(50):
            ex = explain(new, sql)
            if "Quicksilver: mirror" in ex:
                break
            time.sleep(0.1)
        lk = explain(new, LOOKUP)
        if "Quicksilver: mirror" not in ex or "heap (cheap on the heap)" not in lk:
            ok = False
        oid = new.execute("SELECT mirror::oid FROM quicksilver.mirrors WHERE heap = 'public.orders'::regclass").fetchone()
        oid = oid[0] if oid else None
        same = "first" if self.view_oid is None else ("same relation" if oid == self.view_oid else "RE-CREATED")
        if self.view_oid is not None and (oid != self.view_oid) != expect_new_view:
            ok = False
        self.view_oid = oid
        total = heap.execute("SELECT count(*), sum(amount) FROM public.orders").fetchone()
        print(f"[{'ok' if ok else 'FAIL'}] {step}: heap {total[0]} rows, sum {total[1]}")
        for n in notes:
            print("       " + n)
        print(f"       EXPLAIN: {ex}")
        print(f"       lookup:  {lk}")
        print(f"       mirror relation: {same}")
        sys.stdout.flush()
        if not ok:
            FAIL.append(step)
        new.close(); heap.close()

    def sidecar(self, what, env=""):
        e = dict(os.environ, SIDECAR_ENV=os.environ.get("SIDECAR_ENV", "") + " " + env)
        return subprocess.run([os.path.join(HERE, "sidecar.sh"), what], env=e).returncode


def main():
    r = Run()
    p = r.p
    r.before()
    r.check("start: an empty cluster, configuration only")

    r.before()
    p.execute("INSERT INTO public.orders (id, customer_id, status, amount, created_at) "
              "SELECT 2000000 + g, 1 + g % 1000, (ARRAY['new','paid','shipped','returned'])[1 + g % 4], (g % 100000) / 100.0, now() "
              "FROM generate_series(1, 50000) g")
    p.execute("UPDATE public.orders SET amount = amount + 1.00 WHERE id BETWEEN 1 AND 20000")
    p.execute("DELETE FROM public.orders WHERE id BETWEEN 500001 AND 505000")
    r.check("write burst: 50,000 inserts, 20,000 updates, 5,000 deletes")

    r.before()
    n = log_count('msg="merged deltas" table=public.orders') + log_count('msg="merged deltas during compaction" table=public.orders')
    k = [0]

    def poke():
        k[0] += 1
        p.execute("UPDATE public.orders SET amount = amount + 0.01 WHERE id BETWEEN %s AND %s", (30000 + k[0] * 500, 30000 + k[0] * 500 + 499))
    seen = wait_log('table=public.orders', log_count('table=public.orders'), 1) or True
    merged = False
    t0 = time.time()
    while time.time() - t0 < 120 and not merged:
        poke()
        time.sleep(0.3)
        merged = (log_count('msg="merged deltas" table=public.orders') + log_count('msg="merged deltas during compaction" table=public.orders')) > n
    r.check("delta merge (%s, after %d small transactions)" % ("seen in the sidecar's log" if merged else "NOT SEEN", k[0]))
    if not merged:
        FAIL.append("delta merge not seen")

    r.before()
    n = log_count('msg=compacted table=public.orders')
    p.execute("DELETE FROM public.orders WHERE id BETWEEN 600001 AND 640000")
    done = wait_log('msg=compacted table=public.orders', n, 180)
    r.check("idle base rewrite (%s)" % ("compacted, per the sidecar's log" if done else "NOT SEEN"))
    if not done:
        FAIL.append("base rewrite not seen")

    r.before()
    p.execute("TRUNCATE public.orders")
    p.execute("INSERT INTO public.orders (id, customer_id, status, amount, created_at) "
              "SELECT g, 1 + g % 1000, (ARRAY['new','paid','shipped','returned'])[1 + g % 4], (g % 100000) / 100.0, "
              "timestamptz '2024-01-01 00:00:00+00' + (g || ' seconds')::interval FROM generate_series(1, 300000) g")
    p.execute("ANALYZE public.orders")
    r.check("TRUNCATE + reload of 300,000 rows")

    r.before()
    p.execute("ALTER TABLE public.orders ADD COLUMN channel text NOT NULL DEFAULT 'web'")
    p.execute("UPDATE public.orders SET channel = 'app' WHERE id % 3 = 0")
    r.check("DDL barrier: ADD COLUMN channel", expect_new_view=True)
    r.check("  ... and the new column, routed", sql="SELECT channel, count(*), sum(amount) FROM public.orders GROUP BY channel", sessions=False)

    # a value the mirror cannot hold halts ONE table
    r.before()
    st = conn(SPORT)
    st.execute("SET quicksilver.min_rows = 0")      # 20,005 rows: under the cost gate otherwise
    st.execute("SET quicksilver.min_cost = 0")
    took, got, d = routed_within(st, TYPES, 60)
    print(f"[{'ok' if took is not None else 'FAIL'}] before the halt: public.qs_types is routed ({d[0]})")
    if took is None:
        FAIL.append("qs_types not routed before the halt")
    p.execute("UPDATE public.qs_types SET d = '5874897-12-31' WHERE id = (SELECT min(id) FROM public.qs_types)")
    halted = wait_log("halt", log_count("halt"), 30)
    replayed(p, st)
    time.sleep(1.0)
    st.execute("SELECT quicksilver.forget_last()")
    got = rows(st, TYPES); d = decision(st)
    want = rows(conn(SPORT, "heap"), TYPES)
    status = st.execute("SELECT coalesce(stale_because, reason, 'registered and fresh') FROM quicksilver.mirror_status() WHERE heap = 'public.qs_types'::regclass").fetchone()
    okh = halted and d[0] == "heap" and got == want
    print(f"[{'ok' if okh else 'FAIL'}] halted (a date the mirror cannot hold): public.qs_types answered by the {d[0]} ({d[1]}: {d[2][:110]}), equal to the heap: {got == want}")
    print(f"       mirror_status: {status[0] if status else 'no longer registered'}")
    if not okh:
        FAIL.append("halt")
    r.check("  ... while the other tables stay routed")

    r.before()
    p.execute("UPDATE public.qs_types SET d = '2024-02-29' WHERE d = '5874897-12-31'")
    open(f"{BASE}/mirror/public.qs_types/REBUILD", "w").close()
    subprocess.run(["chown", "postgres:postgres", f"{BASE}/mirror/public.qs_types/REBUILD"])
    r.sidecar("stop"); rc = r.sidecar("start")
    replayed(p, st)
    took, got, d = routed_within(st, TYPES, 90)
    want = rows(conn(SPORT, "heap"), TYPES)
    okr = rc == 0 and took is not None and got == want
    print(f"[{'ok' if okr else 'FAIL'}] REBUILD of public.qs_types (sidecar restarted with the request): routed again after {took if took is None else round(took, 1)}s, equal to the heap: {got == want}")
    if not okr:
        FAIL.append("rebuild")
    r.check("  ... and public.orders after the sidecar restart")

    r.before()
    r.sidecar("kill9")
    p.execute("UPDATE public.orders SET amount = amount + 0.01 WHERE id BETWEEN 1 AND 1000")
    replayed(p, st)
    st.execute("SELECT quicksilver.forget_last()")
    got = rows(st, AGG); d = decision(st); want = rows(conn(SPORT, "heap"), AGG)
    print(f"[{'ok' if got == want and d[0] == 'heap' else 'FAIL'}] sidecar killed (-9), then a write: answered by the {d[0]} ({d[2][:120]}), equal to the heap: {got == want}")
    if got != want or d[0] != "heap":
        FAIL.append("kill -9 window")
    rc = r.sidecar("start")
    r.check("sidecar kill -9 and restart")

    r.before()
    subprocess.run(["su", "postgres", "-c", f"{PG}/pg_ctl -D {BASE}/standby -l {BASE}/standby.log -w restart"], stdout=subprocess.DEVNULL)
    dead = 0
    for c in (r.old, r.pre):
        try:
            c.execute("SELECT 1")
        except psycopg.Error:
            dead += 1
    r.reopen(); r.pre = conn(SPORT)
    print(f"       standby restarted: {dead} of 2 open sessions were closed by the server, as a restart does; reconnected")
    r.check("standby restart (the applied LSNs in its shared memory are gone, and reported again)")

    print("FAILED: " + ", ".join(FAIL) if FAIL else "all steps ok")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
