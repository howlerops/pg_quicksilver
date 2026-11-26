#!/usr/bin/env python3
"""Builds the Part 3 comparison tables (best / median, ms) from the raw outputs.
Earlier runs: bench/engine_compare/four_engines/results (pg = heap, qs = Quicksilver, mason = Mason) and
bench/engine_compare/merged_build/out/p2_summary.txt (merged-build Quicksilver). This run: out/bench_*.json.
Prints markdown to stdout."""
import json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
FV = os.path.abspath(os.path.join(HERE, ".."))
REPO = os.path.abspath(os.path.join(FV, "..", ".."))
FOUR = os.path.join(REPO, "bench/engine_compare/four_engines/results")
QUERIES = ["count_by_status", "revenue_by_month_join", "pk_lookup", "count_star", "sum_amount", "month_filter_agg"]


def earlier(engine, state, q):
    path = os.path.join(FOUR, f"{engine}_{state}_warm.json")
    if not os.path.exists(path):
        return None          # no such run in the earlier results (Mason was not run in state C)
    d = json.load(open(path))
    v = d["queries"][q]
    return v["best_ms"], v["median_ms"]


def fmt(p):
    return "n/a" if p is None else f"{p[0]:.1f} / {p[1]:.1f}"


def new(state_file, q, eng):
    d = json.load(open(os.path.join(FV, "out", state_file)))
    v = d["queries"][q]
    e = v[eng]
    return e["best_s"] * 1000, e["median_s"] * 1000


if __name__ == "__main__":
    print("| query | state | heap, earlier (pg) | Quicksilver, earlier (qs) | Mason, earlier | heap, this run | routed, this run | DuckDB direct, this run |")
    print("|---|---|---:|---:|---:|---:|---:|---:|")
    states = [("A", "A", "bench_A_fresh.json", "fresh after load"),
              ("B", "B", "bench_B_after_changes.json", "after the change workload"),
              ("C", "C", "bench_C_after_compaction.json", "after the mirror compacted")]
    for st, est, fname, label in states:
        for q in QUERIES:
            h = new(fname, q, "heap")
            r = new(fname, q, "routed")
            dk = new(fname, q, "duck")
            d = json.load(open(os.path.join(FV, "out", fname)))["queries"][q]
            routed_tag = "" if d["routed_to_mirror"] else " (heap)"
            print(f"| {q} | {st}: {label} | {fmt(earlier('pg', est, q))} | {fmt(earlier('qs', est, q))} | "
                  f"{fmt(earlier('mason', est, q))} | {h[0]:.1f} / {h[1]:.1f} | {r[0]:.1f} / {r[1]:.1f}{routed_tag} | "
                  f"{dk[0]:.1f} / {dk[1]:.1f} |")
