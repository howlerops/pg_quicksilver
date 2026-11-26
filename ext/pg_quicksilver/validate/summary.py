#!/usr/bin/env python3
"""Prints the tables of docs/40 from results/equality.json.

    summary.py [results/equality.json]
"""
import collections, json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    d = json.load(open(sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "results", "equality.json")))
    s = d["summary"]
    print(s["server"])
    print(f"{s['statements']} statements: {s['routed_to_mirror']} routed to the mirror "
          f"({s['routed_identical']} identical, {s['routed_same_error']} the same error, {s['routed_different']} different), "
          f"{s['stayed_on_heap']} stayed on the heap ({s['heap_different']} different)")
    by = collections.Counter()
    for r in d["statements"]:
        by[(r["source"], r["side"])] += 1
    print("\n| corpus | statements | mirror | heap |\n|---|---|---|---|")
    for src in ("exactness", "own", "seed"):
        m, h = by[(src, "mirror")], by[(src, "heap")]
        print(f"| {src} | {m + h} | {m} | {h} |")
    print("\n| why it stayed on the heap | statements |\n|---|---|")
    for reason, n in s["heap_reasons"]:
        print(f"| {reason} | {n} |")
    first = collections.Counter()
    for r in d["statements"]:
        if r["side"] == "heap" and r["reason"] == "construct not on the allowlist":
            first[r["detail"].split(" and ")[0].split(" (")[0]] += 1
    print("\n| first construct not on the allowlist | statements |\n|---|---|")
    for k, n in first.most_common(15):
        print(f"| `{k}` | {n} |")


if __name__ == "__main__":
    main()
