#!/usr/bin/env python3
# analyze_c2.py RESULT.json : post-promotion stale-answer analysis for scenario C
import json, sys
d = json.load(open(sys.argv[1]))
P = [p for p in d["polls"]["lag1gb"] if "error" not in p and p["t"] >= d["t_promote"]]
for i in (0, 1, 2, 10, 100, 300, 500, len(P) - 1):
    p = P[i]
    print(f"t={p['t']:8.2f} routed={p['routed']} heap={p['heap']} diff={p['heap'] - p['routed']} dec={p['decision']}")
print("distinct routed values after promote:", len(set(p["routed"] for p in P)), "of", len(P))
S = [s for s in d["samples"] if "error" not in s]
print("B samples:", len(S), "errors:", sum(1 for s in d["samples"] if "error" in s))
st = [s for s in S if s["B_stable"]]
print("B stable pairs (replay LSN unchanged across the pair):", len(st),
      " routed==heap:", sum(1 for s in st if s["B_routed"] == s["B_heap"]))
print("B decisions:", {k: sum(1 for s in S if s["B_decision"] == k) for k in set(s["B_decision"] for s in S)})
print("B 1GB decisions:", {k: sum(1 for s in S if s["B_decision_1gb"] == k) for k in set(s["B_decision_1gb"] for s in S)})
m = [s for s in S if s["B_decision"] == "mirror"]
print("B mirror-decided samples:", len(m), " stable:", sum(1 for s in m if s["B_stable"]),
      " routed==heap:", sum(1 for s in m if s["B_routed"] == s["B_heap"]))
print("A heap first/last:", S[0]["A_heap"], S[-1]["A_heap"], " B heap first/last:", S[0]["B_heap"], S[-1]["B_heap"])
print("A-B heap lag, first 3:", [s["A_heap"] - s["B_heap"] for s in S[:3]], " last 3:", [s["A_heap"] - s["B_heap"] for s in S[-3:]])
print("B readyz last:", S[-1]["B_readyz"][:120])
