#!/usr/bin/env python3
# stop_c.py: stop the scenario C sidecars (by PID, the runuser parent and its qs-mirror child)
# and the two standbys (pg_ctl immediate). Never touches other processes.
import os, signal, subprocess, time

C = "/var/lib/postgresql/w2e/c"
PG = "/var/lib/postgresql/w2e/pg/usr/lib/postgresql/17/bin"


def children(pp):
    out = []
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        try:
            st = open(f"/proc/{d}/stat").read().rsplit(")", 1)[1].split()
            if int(st[1]) == pp:
                out.append(int(d))
        except Exception:
            pass
    return out


for name in ("side_A.pid", "side_B.pid"):
    try:
        rp = int(open(f"{C}/{name}").read().strip())
    except Exception:
        continue
    for p in [rp] + children(rp):
        try:
            os.kill(p, signal.SIGTERM)
            print("TERM", name, p)
        except OSError:
            pass
time.sleep(3)
for d in ("standbyA", "standbyB", "primary"):
    r = subprocess.run(f"su postgres -c '{PG}/pg_ctl -D {C}/{d} -m immediate stop'", shell=True,
                       capture_output=True, text=True)
    print(d, r.returncode, (r.stdout + r.stderr).strip()[:100])
