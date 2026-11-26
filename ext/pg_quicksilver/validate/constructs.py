#!/usr/bin/env python3
"""What keeps the corpus on the heap, construct by construct.

Runs quicksilver.unsafe_constructs() over every statement of the two corpora
(nothing is executed) and prints, for each construct that is not on the
allowlist, how many statements it appears in and how many it alone blocks.
This is the worklist for whoever grows the allowlist: the second column is
what one more verified entry would buy.

    constructs.py [--port 5541] [--top 40]
"""
import argparse, collections, os, sys
import psycopg
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from equality import load, HERE, REPO


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=int(os.environ.get("PPORT", 5541)))
    ap.add_argument("--top", type=int, default=40)
    args = ap.parse_args()
    corpus = []
    cdir = os.path.join(REPO, "bench", "exactness", "corpus")
    for f in sorted(os.listdir(cdir)):
        if f.endswith(".sql"):
            corpus += load(os.path.join(cdir, f))
    corpus += load(os.path.join(HERE, "corpus_own.sql"))
    corpus += load(os.path.join(HERE, "corpus_seed.sql"))
    c = psycopg.connect(f"host=/tmp port={args.port} dbname=app user=postgres", autocommit=True)
    c.execute("SET search_path = ex, public")
    for x in os.environ.get("QS_SET", "").split():
        c.execute("SET %s = '%s'" % tuple(x.split("=", 1)))
    seen, alone, safe, failed = collections.Counter(), collections.Counter(), 0, 0
    for sid, sql in corpus:
        try:
            rows = c.execute("SELECT kind, ident FROM quicksilver.unsafe_constructs(%s)", (sql,)).fetchall()
        except psycopg.Error:
            failed += 1
            continue
        if not rows:
            safe += 1
        for r in rows:
            seen[r] += 1
        if len(rows) == 1:
            alone[rows[0]] += 1
    print(f"{len(corpus)} statements: {safe} covered by the allowlist, {failed} do not analyse")
    print(f"{'in':>5} {'alone':>5}  construct")
    for k, v in seen.most_common(args.top):
        print(f"{v:5d} {alone[k]:5d}  {k[0]} {k[1]}")


if __name__ == "__main__":
    main()
