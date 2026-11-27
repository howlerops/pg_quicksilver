#!/usr/bin/env python3
"""run.py <variant> <threads> [--noverify] [--only q] : verify vs baseline answers, then warm-up + 5 timed runs."""
import sys, json, time, statistics, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *

variant, threads = sys.argv[1], int(sys.argv[2])
noverify = "--noverify" in sys.argv
only = sys.argv[sys.argv.index("--only") + 1] if "--only" in sys.argv else None
c = connect(threads)
t0 = time.perf_counter()
setup(c, variant)
setup_ms = (time.perf_counter() - t0) * 1000
refp = f"{RAW}/ref.json"
ref = json.load(open(refp)) if os.path.exists(refp) else {}
out = {"variant": variant, "threads": threads, "setup_ms": round(setup_ms, 1), "queries": {}}
newref = {}
for name, sql in QUERIES:
    if only and name != only:
        continue
    rows = canon(c.execute(sql).fetchall())  # warm-up + answer
    if variant == "0_baseline":
        newref[name] = rows
        ok = (name not in ref) or ref[name] == json.loads(json.dumps(rows))
    else:
        ok = ref[name] == json.loads(json.dumps(rows))
    if not ok and not noverify:
        out["queries"][name] = {"ok": False, "got": rows[:3], "want": ref[name][:3]}
        continue
    ts = []
    for _ in range(5):
        t = time.perf_counter()
        c.execute(sql).fetchall()
        ts.append((time.perf_counter() - t) * 1000)
    out["queries"][name] = {"ok": ok, "best": round(min(ts), 1), "median": round(statistics.median(ts), 1),
                            "all": [round(x, 1) for x in ts]}
if variant == "0_baseline" and not os.path.exists(refp):
    json.dump(newref, open(refp, "w"))
json.dump(out, open(f"{RAW}/time_{variant}_t{threads}.json", "w"), indent=1)
print(variant, f"t={threads}", f"setup={setup_ms:.0f}ms",
      " ".join(f"{k}={v.get('best')}/{v.get('median')}{'' if v['ok'] else '(MISMATCH)'}"
               for k, v in out["queries"].items()), flush=True)
