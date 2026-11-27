#!/usr/bin/env python3
"""Equality harness for pg_quicksilver.

Every statement of bench/exactness's corpus and of corpus_own.sql is run twice
against the same server: once with quicksilver.route = heap and once with
quicksilver.route = auto. The two answers must be byte-identical (the text the
server sent, no client adaptation) and so must the RowDescription: column
names, type OIDs and typmods. What the second run did is the extension's own
report (quicksilver.last_decision()).

The exactness tables hold 2,000 rows each, so for them the cost gate is opened
(min_cost = min_rows = 0); statements over the 1M-row tables run with the
default thresholds.

    equality.py [--port 5541] [--user postgres] [--out results.json] [--fuzz]

Exit status 1 if any routed statement differs from the heap.
"""
import argparse, collections, json, os, re, sys
import psycopg

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))


def load(path):
    out, cur, buf = [], None, []
    for line in open(path, encoding="utf-8"):
        m = re.match(r"\s*--\s*@id\s+(\S+)", line)
        if m:
            if cur and buf:
                out.append((cur, "".join(buf).strip().rstrip(";")))
            cur, buf = m.group(1), []
        elif cur is not None and not line.lstrip().startswith("--"):
            buf.append(line)
    if cur and buf:
        out.append((cur, "".join(buf).strip().rstrip(";")))
    return [(i, s) for i, s in out if s]


def ordered(sql):
    """Does the outermost query have an ORDER BY? (parenthesis depth 0)"""
    depth, s = 0, re.sub(r"'(?:[^']|'')*'", "''", sql)
    for m in re.finditer(r"[()]|\border\s+by\b", s, re.I):
        t = m.group(0)
        if t == "(":
            depth += 1
        elif t == ")":
            depth -= 1
        elif depth == 0:
            return True
    return False


def run(conn, sql):
    try:
        cur = conn.execute(sql.encode())      # no parameters: simple protocol
        r = cur.pgresult
        desc = [(r.fname(i).decode(), r.ftype(i), r.fmod(i)) for i in range(r.nfields)]
        rows = [tuple(r.get_value(j, i) for i in range(r.nfields)) for j in range(r.ntuples)]
        return {"desc": desc, "rows": rows}
    except psycopg.Error as e:
        return {"error": e.sqlstate, "message": (e.diag.message_primary or "")[:200]}


def connect(port, settings, user="postgres"):
    c = psycopg.connect(f"host=/tmp port={port} dbname=app user={user}", autocommit=True)
    c.execute("SET search_path = ex, public")
    c.execute("SET TimeZone = 'America/New_York'")
    for s in settings:
        c.execute(s)
    return c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=int(os.environ.get("PPORT", 5541)))
    ap.add_argument("--user", default="postgres", help="the role both sessions run as (an ordinary role routes only on the confined build, docs/40)")
    ap.add_argument("--out", default=os.path.join(HERE, "results", "equality.json"))
    ap.add_argument("--extra", action="append", default=[], help="another corpus file")
    ap.add_argument("--set", action="append", default=[], metavar="NAME=VALUE", help="a setting for the routing session")
    ap.add_argument("--zone", default="", help="the session TimeZone for both sessions (default America/New_York)")
    args = ap.parse_args()

    corpus = []
    cdir = os.path.join(REPO, "bench", "exactness", "corpus")
    for f in sorted(os.listdir(cdir)):
        if f.endswith(".sql"):
            corpus += [(i, s, "exactness") for i, s in load(os.path.join(cdir, f))]
    corpus += [(i, s, "own") for i, s in load(os.path.join(HERE, "corpus_own.sql"))]
    corpus += [(i, s, "seed") for i, s in load(os.path.join(HERE, "corpus_seed.sql"))]
    for f in args.extra:
        corpus += [(i, s, os.path.basename(f)) for i, s in load(f)]

    zone = ["SET TimeZone = '%s'" % args.zone] if args.zone else []
    heap = connect(args.port, zone + ["SET quicksilver.route = heap"], args.user)
    auto = connect(args.port, zone + ["SET quicksilver.route = auto"] + ["SET %s = '%s'" % tuple(x.split("=", 1)) for x in args.set], args.user)
    results, reasons, bad = [], collections.Counter(), []
    n = collections.Counter()
    for sid, sql, src in corpus:
        small = not re.search(r"\bpublic\.orders\b", sql)
        auto.execute("SET quicksilver.min_cost = %s" % (0 if small else 10000))
        auto.execute("SET quicksilver.min_rows = %s" % (0 if small else 50000))
        auto.execute("SELECT quicksilver.forget_last()")
        h = run(heap, sql)
        a = run(auto, sql)
        d = auto.execute("SELECT side, reason, detail FROM quicksilver.last_decision()").fetchone()
        side, reason, detail = d if d else ("heap", "reads no mirrored table", "")
        if "error" in h or "error" in a:
            same = h.get("error") == a.get("error") and h.get("message") == a.get("message")
            verdict = "same error" if same else "DIFFERENT"
        else:
            hr, ar = h["rows"], a["rows"]
            if not ordered(sql):
                key = lambda row: tuple((x is None, x or b"") for x in row)
                hr, ar = sorted(hr, key=key), sorted(ar, key=key)
            if h["desc"] != a["desc"]:
                verdict = "DIFFERENT RowDescription"
            elif hr != ar:
                verdict = "DIFFERENT rows"
            else:
                verdict = "identical"
        n[side] += 1
        n[(side, verdict.split()[0])] += 1
        if side == "heap":
            reasons[reason] += 1
        if verdict.startswith("DIFFERENT"):
            bad.append((sid, side, verdict))
        rec = {"id": sid, "source": src, "sql": sql, "side": side, "reason": reason, "detail": detail,
               "verdict": verdict, "rows": len(h.get("rows", [])), "heap_error": h.get("message"),
               "auto_error": a.get("message")}
        if verdict.startswith("DIFFERENT") and "rows" in h and "rows" in a:
            rec["heap_desc"], rec["auto_desc"] = h["desc"], a["desc"]
            for x, y in zip(hr, ar):
                if x != y:
                    rec["first_diff"] = [[v.decode(errors="replace") if v is not None else None for v in x],
                                         [v.decode(errors="replace") if v is not None else None for v in y]]
                    break
        results.append(rec)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    summary = {
        "statements": len(corpus),
        "routed_to_mirror": n["mirror"],
        "stayed_on_heap": n["heap"],
        "routed_identical": n[("mirror", "identical")],
        "routed_same_error": n[("mirror", "same")],
        "routed_different": n[("mirror", "DIFFERENT")],
        "heap_different": n[("heap", "DIFFERENT")],
        "heap_reasons": reasons.most_common(),
        "server": heap.execute("SELECT version()").fetchone()[0],
        "settings": args.set,
        "zone": args.zone or "America/New_York",
    }
    json.dump({"summary": summary, "statements": results}, open(args.out, "w"), indent=1, ensure_ascii=False)
    print(json.dumps(summary, indent=1))
    for b in bad:
        print("DIFFERENT:", *b)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
