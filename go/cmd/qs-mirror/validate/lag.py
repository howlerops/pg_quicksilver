"""Commit-to-visible lag on a standby mirror (item 2 of the event-driven apply work).

For each commit: on the primary, run BEGIN, the inserts, then read
pg_current_wal_insert_lsn() BEFORE the COMMIT. With one writer that is the start
of the commit record, which is the commit's LSN. Then COMMIT, and poll
`qs-query -lsn` on the mirror every 5 ms until it reports that LSN or past it.
The lag is from the moment the commit returned to the moment the poll first saw
the LSN. (pg_current_wal_lsn() after COMMIT was tried first: it can sit a byte
past the commit record's end, which the mirror can only reach with more WAL.)

usage: lag.py LABEL QS_QUERY MIRROR_ROOT SOCKET_DIR PRIMARY_PORT
"""
import statistics
import subprocess
import sys
import time

label, qq, mirror, sock, port = sys.argv[1:6]
PG = "/usr/lib/postgresql/17/bin"
TABLE = "public.bench"
POLL = 0.005


def psql(sql):
    r = subprocess.run(
        ["su", "postgres", "-s", "/bin/bash", "-c",
         f"{PG}/psql -X -A -t -q -v ON_ERROR_STOP=1 -h {sock} -p {port} -U postgres -d app -f -"],
        input=sql, capture_output=True, text=True, check=True)
    return r.stdout.strip()


def lsn_num(s):
    a, b = s.split("/")
    return (int(a, 16) << 32) | int(b, 16)


def mirror_lsn():
    r = subprocess.run([qq, "-mirror", mirror, "-table", TABLE, "-lsn"],
                       capture_output=True, text=True)
    s = r.stdout.strip()
    return lsn_num(s) if s else -1


def pct(xs, p):
    xs = sorted(xs)
    k = max(0, min(len(xs) - 1, int(round(p / 100 * len(xs) + 0.5)) - 1))
    return xs[k]


results = {}
next_id = 500_001
for rows, n in ((50, 30), (5000, 30)):
    lags, polls = [], []
    for i in range(n):
        lo, hi = next_id, next_id + rows - 1
        next_id = hi + 1
        out = psql(
            "BEGIN;\n"
            f"INSERT INTO {TABLE} SELECT g, g % 100, md5(g::text) FROM generate_series({lo}, {hi}) g;\n"
            "SELECT pg_current_wal_insert_lsn()::text;\n"
            "COMMIT;\n")
        t0 = time.perf_counter()
        target = lsn_num([x for x in out.splitlines() if "/" in x][-1].strip())
        k = 0
        while True:
            k += 1
            if mirror_lsn() >= target:
                break
            if time.perf_counter() - t0 > 60:
                sys.exit(f"TIMEOUT waiting for commit {i} ({rows} rows)")
            time.sleep(POLL)
        lags.append((time.perf_counter() - t0) * 1000)
        polls.append(k)
        time.sleep(0.3)  # the next commit starts from a quiet loop
    results[rows] = lags
    print(f"[{label}] {n} commits x {rows} rows: "
          f"median {statistics.median(lags):.1f} ms, p95 {pct(lags, 95):.1f} ms, "
          f"max {max(lags):.1f} ms, polls per wait median {statistics.median(polls):.0f}")
    print(f"[{label}] per-commit ms ({rows} rows): " + " ".join(f"{x:.1f}" for x in lags))
