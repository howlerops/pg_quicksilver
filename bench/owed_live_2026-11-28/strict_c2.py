#!/usr/bin/env python3
# strict_c2.py RESULT.json : when did A stop routing from the mirror, under the two freshness settings
import collections, json, sys

d = json.load(open(sys.argv[1]))
t_stop, t_prom = d["t_stop"], d["t_promote"]
for name in ("strict", "lag1gb"):
    P = [p for p in d["polls"][name] if "error" not in p]
    errs = sum(1 for p in d["polls"][name] if "error" in p)
    last_mirror_before = max((p["t"] for p in P if p["decision"] == "mirror" and p["t"] < t_stop), default=None)
    post = [p for p in P if p["t"] >= t_stop]
    mirror_after_stop = [p for p in post if p["decision"] == "mirror"]
    first_ns = next((p["t"] for p in post if p["reason"] and "not a standby" in p["reason"]), None)
    first_heap = next((p["t"] for p in post if p["decision"] == "heap"), None)
    print(f"== {name}: polls {len(P)} errors {errs}")
    print(f"   last poll routed from mirror before stop: {last_mirror_before}  (stop at {t_stop}, promote at {t_prom})")
    print(f"   mirror-decided polls after stop: {len(mirror_after_stop)}"
          f"  (last one at {mirror_after_stop[-1]['t'] if mirror_after_stop else None})")
    print(f"   first heap decision after stop: {first_heap}; first QR_NOT_STANDBY reason: {first_ns}")
    print("   reasons after stop:", dict(collections.Counter((p['decision'], (p['reason'] or '')[:72]) for p in post)))
    print(f"   polls after stop with routed != heap: {sum(1 for p in post if p['routed'] != p['heap'])} of {len(post)}")
