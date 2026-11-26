"""Summarise one memspike.sh run: RSS percentiles, per-minute peaks, and the sidecar events
that fall at each peak.

Usage: memspike_summary.py OUT_DIR   (the out/memspike_TAG directory memspike.sh wrote)
"""
import csv
import datetime as dt
import re
import sys
from collections import defaultdict

out = sys.argv[1]
rows = []
with open(f"{out}/rss.csv") as f:
    for r in csv.DictReader(f):
        rows.append((r["utc"], int(r["rss_mb"]), int(r["hwm_mb"])))
day = None
events = []  # (UTC seconds of day, text)
pat = re.compile(r'time=(\S+) .*?msg="([^"]+)"(.*)')
with open(f"{out}/sidecar.log") as f:
    for line in f:
        m = pat.search(line)
        if not m:
            continue
        ts = dt.datetime.fromisoformat(m.group(1).replace("Z", "+00:00"))
        if m.group(2) in ("slow tick: the apply loop overran its interval, which is what a latency tail is made of",):
            continue
        rest = m.group(3).strip()
        tbl = re.search(r"table=(\S+)", rest)
        events.append((ts, m.group(2) + (" " + tbl.group(1) if tbl else "")))
        day = ts.date()
rss = sorted(r[1] for r in rows)


def pct(p):
    return rss[min(len(rss) - 1, int(p / 100 * len(rss)))]


print(f"samples={len(rows)} rss_mb p50={pct(50)} p95={pct(95)} max={rss[-1]} final_hwm_mb={rows[-1][2]}")
perm = defaultdict(int)
for utc, r, _ in rows:
    perm[utc[:5]] = max(perm[utc[:5]], r)
print("per-minute peak RSS (MB):", " ".join(f"{k}={v}" for k, v in sorted(perm.items())))
print("peaks >= 1000 MB, with the sidecar events in the 3 s before and 1 s after:")
for i, (utc, r, _) in enumerate(rows):
    if r < 1000 or (i and rows[i - 1][1] >= 1000):
        continue
    t = dt.datetime.combine(day, dt.datetime.strptime(utc, "%H:%M:%S").time(), tzinfo=dt.timezone.utc)
    near = [e for e in events if t - dt.timedelta(seconds=3) <= e[0] <= t + dt.timedelta(seconds=1)]
    print(f"  {utc} rss={r} " + "; ".join(f"{e[0].strftime('%H:%M:%S.%f')[:-3]} {e[1]}" for e in near))
print("event counts:")
counts = defaultdict(int)
for _, text in events:
    counts[text.split(" public.")[0]] += 1
for k, v in sorted(counts.items(), key=lambda kv: -kv[1]):
    print(f"  {v:6d} {k}")
