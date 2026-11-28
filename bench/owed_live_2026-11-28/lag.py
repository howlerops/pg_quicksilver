#!/usr/bin/env python3
"""Commit-to-visible lag, METRICS 0.1 method: one insert per commit every 50 ms for 60 s on the
primary; the standby's routed count (quicksilver.route = auto) is polled every 5 ms; lag of commit i
= (first poll that shows count >= base+i+1) - (time the commit returned on the primary).
usage: lag.py OUT.json LABEL [SECONDS]"""
import json, statistics, sys, threading, time, uuid
import psycopg

out, label = sys.argv[1], sys.argv[2]
secs = float(sys.argv[3]) if len(sys.argv) > 3 else 60.0
P = psycopg.connect("host=/tmp port=6610 dbname=app user=postgres", autocommit=True)
S = psycopg.connect("host=/tmp port=6611 dbname=app user=postgres", autocommit=True)
S.execute("SET quicksilver.route = 'auto'")
cust = P.execute("SELECT id FROM public.customers LIMIT 1").fetchone()[0]
base = S.execute("SELECT count(*) FROM public.orders").fetchone()[0]
commit_end = []  # monotonic seconds when the commit returned
samples = []     # (monotonic, routed count)
stop = threading.Event()


def reader():
    while not stop.is_set():
        t = time.perf_counter()
        n = S.execute("SELECT count(*) FROM public.orders").fetchone()[0]
        samples.append((t, n))
        time.sleep(0.005)


th = threading.Thread(target=reader, daemon=True)
th.start()
end = time.perf_counter() + secs
i = 0
while time.perf_counter() < end:
    P.execute("INSERT INTO public.orders (id, tenant_id, customer_id, status, amount, created_at, payload, note) "
              "VALUES (%s, 1, %s, 'paid', 1.00, now(), '{}'::jsonb, NULL)", (str(uuid.uuid4()), cust))
    commit_end.append(time.perf_counter())
    i += 1
    time.sleep(0.05)
time.sleep(1.0)
stop.set()
th.join()
# first poll showing each commit's row
lags, j = [], 0
for k, ct in enumerate(commit_end):
    want = base + k + 1
    while j < len(samples) and samples[j][1] < want:
        j += 1
    if j >= len(samples):
        break
    lags.append((samples[j][0] - ct) * 1000)
lags.sort()
res = {"label": label, "commits": len(commit_end), "seen": len(lags),
       "median_ms": round(statistics.median(lags), 1) if lags else None,
       "p95_ms": round(lags[int(0.95 * (len(lags) - 1))], 1) if lags else None,
       "max_ms": round(lags[-1], 1) if lags else None, "min_ms": round(lags[0], 1) if lags else None}
json.dump(res, open(out, "w"), indent=1)
print(res)
