#!/usr/bin/env python3
"""Which allowlist entries did the equality harness actually exercise?

An entry is "verified" only if some statement that was routed to the mirror
and came back identical needed it. For every row of quicksilver.allowlist this
removes the row (in a transaction that is rolled back), asks
quicksilver.unsafe_constructs() about each routed statement of the last
equality run, and counts the statements that stop being covered.

An entry with a count of 0 has no evidence behind it and does not belong in
the seed.

    coverage.py [--port 5541] [--results results/equality.json]
"""
import argparse, json, os, sys
import psycopg

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=int(os.environ.get("PPORT", 5541)))
    ap.add_argument("--results", default=os.path.join(HERE, "results", "equality.json"))
    ap.add_argument("--out", default=os.path.join(HERE, "results", "coverage.tsv"))
    ap.add_argument("--set", action="append", default=[], metavar="NAME=VALUE", help="a setting the equality run was made with")
    args = ap.parse_args()
    routed = [r["sql"] for r in json.load(open(args.results))["statements"]
              if r["side"] == "mirror" and r["verdict"] == "identical"]
    opts = "-c search_path=ex,public" + "".join(f" -c {x}" for x in args.set)
    c = psycopg.connect(f"host=/tmp port={args.port} dbname=app user=postgres options='{opts}'")
    entries = c.execute("SELECT kind, ident FROM quicksilver.allowlist ORDER BY 1, 2").fetchall()
    c.rollback()
    zero, lines = [], []
    for kind, ident in entries:
        c.execute("DELETE FROM quicksilver.allowlist WHERE kind = %s AND ident = %s", (kind, ident))
        n = 0
        for sql in routed:
            if c.execute("SELECT count(*) FROM quicksilver.unsafe_constructs(%s)", (sql,)).fetchone()[0]:
                n += 1
        c.rollback()
        lines.append(f"{n}\t{kind}\t{ident}")
        if n == 0:
            zero.append((kind, ident))
    open(args.out, "w").write("statements\tkind\tident\n" + "\n".join(lines) + "\n")
    print(f"{len(entries)} entries, {len(routed)} routed statements; {len(zero)} entries exercised by none:")
    for z in zero:
        print("  ", *z)
    sys.exit(1 if zero else 0)


if __name__ == "__main__":
    main()
