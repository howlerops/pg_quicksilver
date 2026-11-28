#!/usr/bin/env python3
# table_a.py: print scenario A results as a markdown table
import json, os, sys
D = os.path.dirname(os.path.abspath(__file__)) + "/results/"
runs = [("join off", "UTC", "a_join_off_utc.json"), ("join off", "America/New_York", "a_join_off_ny.json"),
        ("join on (bootstrapped with it)", "UTC", "a_joinboot_on_utc.json"),
        ("join on (bootstrapped with it)", "America/New_York", "a_joinboot_on_ny.json")]
print("| join domains | TZ | query | rows | identical | heap best / median (ms) | routed best / median (ms) | speedup (median) | routed decision |")
print("|---|---|---|---|---|---|---|---|---|")
for jd, tz, f in runs:
    d = json.load(open(D + f))
    for name, q in d["queries"].items():
        h, r = q["heap"], q["auto"]
        sp = h["median"] / r["median"] if r["median"] else float("nan")
        import re; dec = re.findall(r"'([^']*)'", q["routed_decision"])[:2]
        print(f"| {jd} | {tz} | {name} | {q['rows']} | {q['identical']} | {h['best']} / {h['median']} | {r['best']} / {r['median']} | {sp:.2f}x | {dec[0]}, {dec[1]} |")
