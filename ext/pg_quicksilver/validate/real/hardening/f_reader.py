#!/usr/bin/env python3
# f_reader.py ROUTE N OUT: one session on the standby as app (psycopg-free: a psql child with a
# pipe), running SELECT count(*) FROM public.orders N times as fast as it can. Every result and
# every error is written to OUT in order, with the time it took. ROUTE is "auto" (the default
# routing) or "heap" (SET quicksilver.route = heap, the comparison run).
# Run as the postgres OS user (PG env: the psql directory). Also records last_decision() per query
# in the same session, so each count can be tied to the side that answered it.
import os, subprocess, sys, time

route, n, out = sys.argv[1], int(sys.argv[2]), sys.argv[3]
pg = os.environ.get("PG", "/var/lib/postgresql/qsexp/pg17/usr/lib/postgresql/17/bin")
psql = [os.path.join(pg, "psql"), "-X", "-q", "-h", "/tmp", "-p", "5861", "-U", "app", "-d", "app",
        "-v", "ON_ERROR_STOP=0", "-A", "-t", "-F", "|"]
sql = ["SELECT current_user, session_user, current_setting('role');"]
if route == "heap":
    sql.append("SET quicksilver.route = heap;")
p = subprocess.Popen(psql, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                     text=True, bufsize=1)

def ask(stmt):
    # one statement, then a marker, so the output can be split into answers
    p.stdin.write(stmt + "\n\\echo __END__\n")
    p.stdin.flush()
    lines = []
    while True:
        line = p.stdout.readline()
        if line == "":
            raise RuntimeError("psql ended")
        if line.strip() == "__END__":
            return lines
        lines.append(line.rstrip("\n"))

with open(out, "w") as f:
    f.write("route=%s queries=%d\n" % (route, n))
    for s in sql:
        f.write("setup: %s -> %s\n" % (s, " / ".join(ask(s))))
    for i in range(n):
        t0 = time.time()
        ans = ask("SELECT count(*) FROM public.orders;")
        dt = (time.time() - t0) * 1000
        dec = ask("SELECT side || '|' || reason FROM quicksilver.last_decision();")
        f.write("%5d ms=%.0f result=%s decision=%s\n" % (i, dt, " / ".join(ans), " / ".join(dec)))
        f.flush()
p.stdin.write("\\q\n")
p.stdin.flush()
p.wait()
