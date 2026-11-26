#!/usr/bin/env python3
"""Summarise an equality.py output (the full record is over 300 KB): keeps the summary block and, per
statement, its id, the side it ran on, its verdict and the reason. Usage: summarise_equality.py FILE.json"""
import json, os, sys

p = sys.argv[1]
d = json.load(open(p))
slim = {
    "summary": d["summary"],
    "statements": [{"id": s["id"], "side": s["side"], "verdict": s["verdict"], "reason": s["reason"]}
                   for s in d["statements"]],
    "note": "summarised after the run: id, side, verdict and reason per statement; the full record "
            "(SQL text and rows) was over 300 KB and is not kept",
}
json.dump(slim, open(p, "w"), indent=0, ensure_ascii=False)
print(p, os.path.getsize(p))
