#!/usr/bin/env python3
"""Part 3 benchmark driver: three engines, six queries, three states; ten concurrent clients.

  fv_bench.py suite --state A --out OUT.json       the six queries x three engines, warm best/median of 5
  fv_bench.py conc  --state A --out OUT.json       ten concurrent clients, count_by_status and the join, heap vs routed
  fv_bench.py commits --out OUT.json               30 commits of 5,000 rows: commit-to-visible lag (applied LSN)

Engines:
  heap    the standby with quicksilver.route = heap (PostgreSQL's own plan on the heap)
  routed  the standby with the database's setting (auto): the extension routes what it can to the mirror
  duck    python duckdb reading the DuckLake catalog of the mirror directly (no PostgreSQL)

Every timed answer is compared with the heap answer of the same state (canonical text) before it counts.
Environment: BENCH_PRIMARY, BENCH_STANDBY (ports), BENCH_HEALTH (sidecar health port),
BENCH_MIRROR (mirror directory), BENCH_LOCAL_CPUS (taskset list for the timed clients, optional).
"""
import argparse, datetime, decimal, json, os, statistics, sys, threading, time, uuid

import psycopg
import urllib.error

PRIMARY = int(os.environ.get("BENCH_PRIMARY", "5820"))
STANDBY = int(os.environ.get("BENCH_STANDBY", "5821"))
HEALTH = int(os.environ.get("BENCH_HEALTH", "5822"))
MIRROR = os.environ.get("BENCH_MIRROR", "/var/lib/postgresql/qsfinal/bench/mirror")
CATALOG = f"{MIRROR}/_ducklake/catalog.sqlite"
LOOKUP_FILE = os.environ.get("BENCH_LOOKUP", "/var/lib/postgresql/qsfinal/bench/lookup_id.txt")
DB = "app"

LOOKUP_ID = open(LOOKUP_FILE).read().strip()
QUERIES = [
    ("count_by_status", "SELECT status, count(*) FROM orders GROUP BY status ORDER BY status",
     "SELECT status, count(*) FROM lake.public.orders GROUP BY status ORDER BY status"),
    ("revenue_by_month_join",
     "SELECT date_trunc('month', o.created_at) AS month, date_trunc('year', c.created_at) AS signup_year, "
     "sum(o.amount) AS revenue FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY 1, 2 ORDER BY 1, 2",
     "SELECT date_trunc('month', o.created_at) AS month, date_trunc('year', c.created_at) AS signup_year, "
     "sum(o.amount) AS revenue FROM lake.public.orders o JOIN lake.public.customers c ON c.id = o.customer_id GROUP BY 1, 2 ORDER BY 1, 2"),
    ("pk_lookup", f"SELECT * FROM orders WHERE id = '{LOOKUP_ID}'",
     f"SELECT * FROM lake.public.orders WHERE id = '{LOOKUP_ID}'"),
    ("count_star", "SELECT count(*) FROM orders", "SELECT count(*) FROM lake.public.orders"),
    ("sum_amount", "SELECT sum(amount) FROM orders", "SELECT sum(amount) FROM lake.public.orders"),
    ("month_filter_agg",
     "SELECT count(*), sum(amount) FROM orders WHERE created_at >= timestamptz '2025-06-01 00:00:00+00' "
     "AND created_at < timestamptz '2025-07-01 00:00:00+00'",
     "SELECT count(*), sum(amount) FROM lake.public.orders WHERE created_at >= timestamptz '2025-06-01 00:00:00+00' "
     "AND created_at < timestamptz '2025-07-01 00:00:00+00'"),
]
CONC = {"count_by_status": QUERIES[0][1], "revenue_by_month_join": QUERIES[1][1]}


def norm(v):
    if isinstance(v, decimal.Decimal):
        return format(v.normalize(), "f") if v == v.to_integral() else format(v, "f").rstrip("0")
    if isinstance(v, datetime.datetime):
        if v.tzinfo is not None:
            v = v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return v.isoformat(sep=" ")
    if isinstance(v, uuid.UUID):
        return str(v)
    if isinstance(v, (dict, list)):
        return json.dumps(v, sort_keys=True, separators=(",", ":"))
    if isinstance(v, str) and v[:1] in "{[":
        try:
            return json.dumps(json.loads(v), sort_keys=True, separators=(",", ":"))
        except Exception:
            return v
    if isinstance(v, float):
        return repr(v)
    return v


def canon(rows):
    return sorted([[norm(v) for v in r] for r in rows], key=lambda r: [str(x) for x in r])


def pg_conn(port, route=None):
    c = psycopg.connect(host="/tmp", port=port, user="postgres", dbname=DB, autocommit=True)
    if route:
        c.execute(f"SET quicksilver.route = '{route}'")
    return c


def run_pg(conn, sql):
    t = time.perf_counter()
    rows = conn.execute(sql).fetchall()
    return time.perf_counter() - t, rows


def plan_line(conn, sql):
    rows = conn.execute("EXPLAIN (VERBOSE, COSTS OFF) " + sql).fetchall()
    return [r[0].strip() for r in rows if "Quicksilver" in r[0]]


def lake_conn():
    import duckdb
    d = duckdb.connect()
    d.execute("LOAD ducklake"); d.execute("LOAD sqlite")
    d.execute(f"ATTACH 'ducklake:sqlite:{CATALOG}' AS lake (READ_ONLY)")
    d.execute("SET TimeZone = 'UTC'")
    return d


def run_duck(d, sql):
    t = time.perf_counter()
    rows = d.execute(sql).fetchall()
    return time.perf_counter() - t, rows


def timed(fn, n=5):
    times, last = [], None
    for _ in range(n):
        dt, rows = fn()
        times.append(dt)
        last = rows
    return {"best_s": min(times), "median_s": statistics.median(times), "runs_s": times}, last


def suite(state, out):
    heap = pg_conn(STANDBY, route="heap")
    routed = pg_conn(STANDBY)
    d = lake_conn()
    result = {"state": state, "queries": {}, "primary": PRIMARY, "standby": STANDBY}
    for name, pg_sql, duck_sql in QUERIES:
        ref_rows = heap.execute(pg_sql).fetchall()        # the reference answer (heap, cold call)
        ref = canon(ref_rows)
        plan = plan_line(routed, pg_sql)
        routed_to_mirror = any(l == "Quicksilver: mirror" for l in plan)
        # routed: warm runs
        rt, rrows = timed(lambda: run_pg(routed, pg_sql))
        ht, hrows = timed(lambda: run_pg(heap, pg_sql))
        dt, drows = timed(lambda: run_duck(d, duck_sql))
        rec = {
            "routed_to_mirror": routed_to_mirror,
            "plan": plan[:2],
            "heap": {**ht, "equal": canon(hrows) == ref},
            "routed": {**rt, "equal": canon(rrows) == ref},
            "duck": {**dt, "equal": canon(drows) == ref},
            "rows": len(ref),
        }
        result["queries"][name] = rec
        print(f"[{state}] {name:22} heap {ht['best_s']*1000:9.1f}/{ht['median_s']*1000:9.1f} ms  "
              f"routed {rt['best_s']*1000:9.1f}/{rt['median_s']*1000:9.1f} ms ({'mirror' if routed_to_mirror else 'heap'}, "
              f"{'EQUAL' if rec['routed']['equal'] else 'DIFFERENT'})  "
              f"duck {dt['best_s']*1000:9.1f}/{dt['median_s']*1000:9.1f} ms ({'EQUAL' if rec['duck']['equal'] else 'DIFFERENT'})",
              flush=True)
    json.dump(result, open(out, "w"), indent=1)
    return result


def conc(state, out):
    res = {"state": state, "clients": 10, "rounds": 3, "results": {}}
    for name in ("count_by_status", "revenue_by_month_join"):
        sql = CONC[name]
        ref = canon(pg_conn(STANDBY, route="heap").execute(sql).fetchall())
        for engine, route in (("heap", "heap"), ("routed", None)):
            times, bad = [], 0
            for _ in range(res["rounds"]):
                barrier = threading.Barrier(11)
                answers = [None] * 10
                conns = [pg_conn(STANDBY, route=route) for _ in range(10)]
                for c in conns:
                    c.execute(sql).fetchall()                    # warm the connection and plan
                def worker(i):
                    barrier.wait()
                    answers[i] = conns[i].execute(sql).fetchall()
                ths = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
                for t in ths:
                    t.start()
                barrier.wait()
                t0 = time.perf_counter()
                for t in ths:
                    t.join()
                times.append(time.perf_counter() - t0)
                bad += sum(1 for a in answers if canon(a) != ref)
                for c in conns:
                    c.close()
            res["results"][f"{name}/{engine}"] = {"runs_s": times, "median_s": statistics.median(times),
                                                    "answers_different": bad}
            print(f"[{state}] conc {name:22} {engine:7} all-10-answered median {statistics.median(times)*1000:9.1f} ms "
                  f"runs {[round(t*1000) for t in times]} different {bad}", flush=True)
    json.dump(res, open(out, "w"), indent=1)
    return res


def commits(out):
    """30 commits of 5,000 rows on the primary; each one's visibility on the standby is the time from
    its commit to the sidecar's applied LSN reaching the commit's LSN (readyz, polled every 10 ms)."""
    import urllib.request
    p = pg_conn(PRIMARY)
    lags = []
    for k in range(30):
        p.execute("BEGIN")
        p.execute(f"INSERT INTO orders (id, tenant_id, customer_id, status, amount, created_at, payload, note) "
                  f"SELECT gen_random_uuid(), 1 + g % 200, md5('c' || (1 + g % 500000))::uuid, 'paid', 10.5, now(), "
                  f"'{{\"src\":\"commit{k}\"}}'::jsonb, NULL FROM generate_series(1, 5000) g")
        p.execute("COMMIT")
        commit_lsn = p.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0]
        t0 = time.perf_counter()
        while True:
            try:
                body = urllib.request.urlopen(f"http://127.0.0.1:{HEALTH}/readyz", timeout=5).read()
            except urllib.error.HTTPError as e:
                body = e.read()
            applied = json.loads(body).get("applied_lsn", "")
            # visible when the applied LSN has reached the commit's end. pg_current_wal_lsn() is read just after
            # the commit, so it can be a few hundred bytes past the commit record (background WAL); a tolerance
            # of 8 KB covers that and is the only approximation in this measurement.
            if applied and lsn_num(commit_lsn) - lsn_num(applied) <= 8192:
                break
            if time.perf_counter() - t0 > 120:
                raise SystemExit(f"commit {k}: applied LSN never reached {commit_lsn} (last {applied!r})")
            time.sleep(0.01)
        lags.append(time.perf_counter() - t0)
    lags_ms = sorted(x * 1000 for x in lags)
    p95 = lags_ms[int(round(0.95 * (len(lags_ms) - 1)))]
    res = {"commits": 30, "rows_per_commit": 5000, "median_ms": statistics.median(lags_ms), "p95_ms": p95,
           "min_ms": lags_ms[0], "max_ms": lags_ms[-1], "lags_ms": lags_ms}
    json.dump(res, open(out, "w"), indent=1)
    print(f"commit-to-visible: median {res['median_ms']:.0f} ms p95 {p95:.0f} ms (n=30)", flush=True)
    return res


def lsn_num(x):
    h, l = x.split("/")
    return (int(h, 16) << 32) | int(l, 16)


def lsn_ge(a, b):
    def n(x):
        h, l = x.split("/")
        return (int(h, 16) << 32) | int(l, 16)
    return n(a) >= n(b)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["suite", "conc", "commits"])
    ap.add_argument("--state", default="A")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    if a.cmd == "suite":
        suite(a.state, a.out)
    elif a.cmd == "conc":
        conc(a.state, a.out)
    else:
        commits(a.out)
