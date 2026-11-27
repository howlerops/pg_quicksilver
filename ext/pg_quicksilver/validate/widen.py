#!/usr/bin/env python3
"""Grow quicksilver.allowlist by evidence, and only by evidence.

The seed allowlist is small on purpose (docs/40). This script widens it through
the real path: PostgreSQL 17 + pg_duckdb + pg_quicksilver + mirrors the sidecar
built and published as DuckLake tables (validate/real/setup.sh), read on the
hot standby. It is re-runnable and ends at a fixed point:

  1. Every statement of bench/exactness/corpus, corpus_own.sql and
     corpus_seed.sql is analysed with quicksilver.unsafe_constructs() under the
     allowlist as shipped. What a statement lacks, and could be given, is its
     set of candidates: a type, operator, function, aggregate, window function,
     cast, node or statement feature that is simply not on the list. What
     breaks a rule of the list (a text ordering under a non-C collation, a
     numeric literal without a decimal point, ...) is not a candidate.
  2. Candidates are proposed in rounds: first the ones that are the ONLY thing
     a statement lacks, so that a difference names its culprit; when there are
     none left, the rest together.
  3. After each round every statement is run twice on the standby, with
     quicksilver.route = heap and = auto, and the bytes compared (equality.py's
     comparison: rows and RowDescription). For every routed statement that
     differs, the entries it exercises that were proposed last are removed
     (the statement was not routed before them and is not after), with the
     statement and the first differing row as the reason, and the run is
     repeated until no routed statement differs. So at the end every routed
     statement that exercises a kept entry is identical.
  4. An entry is kept only if at least --min-evidence routed statements that
     exercise it came back identical. The statements are recorded per entry.

    widen.py --primary 5580 --standby 5581 --out results/widen-icu-en-US.json
    widen.py ... --apply results/widen-icu-en-US.json   # another database: prune only
    widen.py --splice results/widen-C.json              # write the list into the extension's SQL

Exit status 1 if, at the end, any routed statement differs.
"""
import argparse, collections, json, os, re, sys, time
import psycopg

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from equality import load, ordered, run, REPO  # noqa: E402

MARK = "widen.py"
LOST = "PostgreSQL can raise an error here"
SQLFILE = os.path.join(HERE, "..", "pg_quicksilver--0.1.sql")
BEGIN, END = "-- BEGIN widened by validate/widen.py", "-- END widened by validate/widen.py"
CMP = {"=", "<>", "<", "<=", ">", ">="}
ORD = {"<", "<=", ">", ">="}
ARITH = {"+", "-", "*", "/", "%", "^", "|/", "||/", "@", "<<", ">>", "&", "|", "#", "~"}
INTS = {"smallint", "integer", "bigint"}


def corpus_all():
    out = []
    cdir = os.path.join(REPO, "bench", "exactness", "corpus")
    for f in sorted(os.listdir(cdir)):
        if f.endswith(".sql"):
            out += [(i, s, "exactness") for i, s in load(os.path.join(cdir, f))]
    out += [(i, s, "own") for i, s in load(os.path.join(HERE, "corpus_own.sql"))]
    out += [(i, s, "seed") for i, s in load(os.path.join(HERE, "corpus_seed.sql"))]
    return out


def connect(port, settings=()):
    c = psycopg.connect(f"host=/tmp port={port} dbname=app user=postgres", autocommit=True)
    c.execute("SET search_path = ex, public")
    c.execute("SET TimeZone = 'America/New_York'")
    for s in settings:
        c.execute(s)
    return c


def norm(ident):
    return ident.replace("pg_catalog.", "")


class Widen:
    def __init__(self, args):
        self.a = args
        self.prim = psycopg.connect(f"host=/tmp port={args.primary} dbname=app user=postgres", autocommit=True)
        self.corpus = corpus_all()
        self.rejected = {}          # (kind, ident) -> reason
        self.proposed = {}          # (kind, ident) -> row dict
        self.rounds = []
        self.round_of = {}          # (kind, ident) -> the round it was proposed in

    # ---- the allowlist, on the primary; read on the standby
    def sync(self):
        lsn = self.prim.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
        if self.a.standby == self.a.primary:
            return
        with connect(self.a.standby) as c:
            for _ in range(600):
                if c.execute("SELECT pg_last_wal_replay_lsn() >= %s::pg_lsn", (lsn,)).fetchone()[0]:
                    return
                time.sleep(0.05)
        raise SystemExit("the standby never replayed the allowlist change")

    def reset(self):
        self.prim.execute("DELETE FROM quicksilver.allowlist WHERE verified_by LIKE %s", (MARK + "%",))
        self.sync()

    def insert(self, rows):
        for r in rows:
            self.prim.execute(
                "INSERT INTO quicksilver.allowlist (kind, ident, collation_rule, volatility, options, verified_by) "
                "VALUES (%s, %s, %s, 'immutable', %s, %s) ON CONFLICT DO NOTHING",
                (r["kind"], r["ident"], r["collation_rule"], r["options"], MARK + ": proposed"))
        self.sync()
        # a row that does not load is not an entry
        with connect(self.a.standby) as c:
            problems = [p for (p,) in c.execute("SELECT * FROM quicksilver.allowlist_problems()").fetchall()]
        bad = 0
        for p in problems:
            for r in rows:
                if (r["kind"], r["ident"]) in self.proposed and p.startswith("%s %s: " % (r["kind"], r["ident"])):
                    self.reject((r["kind"], r["ident"]), "does not load: " + p)
                    bad += 1
        if bad:
            self.sync()

    def reject(self, key, why):
        if key in self.rejected:
            return
        self.rejected[key] = why
        self.proposed.pop(key, None)
        self.prim.execute("DELETE FROM quicksilver.allowlist WHERE kind = %s AND ident = %s AND verified_by LIKE %s",
                          (key[0], key[1], MARK + "%"))

    # ---- what a candidate would be, from the catalog
    def describe(self, kind, ident, detail):
        """An allowlist row for a construct, or (None, why) if it is not to be proposed."""
        row = {"kind": kind, "ident": ident, "collation_rule": "none", "options": []}
        cur = self.prim.cursor()
        cur.execute("SET search_path = pg_catalog")
        try:
            if kind in ("node", "feature", "window"):
                if kind == "window":
                    cur.execute("SELECT 1 FROM pg_proc WHERE oid = %s::regprocedure", (ident,))
                return row, None
            if kind == "type":
                cur.execute("SELECT typnamespace = 'pg_catalog'::regnamespace FROM pg_type WHERE oid = %s::regtype", (ident,))
                if not cur.fetchone()[0]:
                    return None, "not a pg_catalog type"
                return row, None
            if kind == "cast":
                if "I/O" in (detail or ""):
                    row["options"] = ["io"]
                return row, None
            if kind == "operator":
                cur.execute("""SELECT o.oprname, o.oprleft::regtype::text, o.oprright::regtype::text, o.oprresult::regtype::text,
                                      p.provolatile,
                                      EXISTS (SELECT 1 FROM pg_type t WHERE t.oid IN (o.oprleft, o.oprright) AND t.typcollation <> 0)
                                 FROM pg_operator o JOIN pg_proc p ON p.oid = o.oprcode WHERE o.oid = %s::regoperator""", (ident,))
                name, left, right, res, vol, coll = cur.fetchone()
                if vol != "i":
                    return None, "not immutable (%s): its result depends on session settings the corpus fixes" % vol
                if res == "boolean" and name in CMP:
                    row["options"] = ["cmp"]
                elif name in ARITH and not (name in {"+", "-", "*"} and left == right == "numeric"):
                    # the value may be exact; the error PostgreSQL raises (overflow, division by zero) may be lost
                    row["options"] = ["errors"]
                if coll:
                    row["collation_rule"] = "c" if name in ORD else "deterministic"
                return row, None
            if kind in ("function", "aggregate"):
                cur.execute("""SELECT p.proname, p.provolatile, p.prorettype::regtype::text,
                                      EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = ANY(p.proargtypes) AND t.typcollation <> 0),
                                      (SELECT string_agg(t::regtype::text, ',') FROM unnest(p.proargtypes) t)
                                 FROM pg_proc p WHERE p.oid = %s::regprocedure""", (ident,))
                name, vol, ret, coll, argt = cur.fetchone()
                if vol != "i":
                    return None, "not immutable (%s): its result depends on session settings the corpus fixes" % vol
                if coll:
                    row["collation_rule"] = "c" if (kind == "aggregate" and name in ("min", "max")) else "deterministic"
                if kind == "function" and ret == "numeric" and name == "numeric":
                    # a conversion to numeric: the mirror chooses a scale of its own (docs/40)
                    row["options"] = ["cmp_only"]
                return row, None
        except psycopg.Error as e:
            return None, "does not resolve in pg_catalog: " + (e.diag.message_primary or str(e))[:120]
        finally:
            cur.execute("RESET search_path")
        return None, "unknown kind"

    def missing(self, conn):
        """Per statement: (candidates, blockers, has an 'errors' operator)."""
        out = {}
        for sid, sql, _ in self.corpus:
            try:
                rows = conn.execute("SELECT kind, ident, detail FROM quicksilver.unsafe_constructs(%s)", (sql,)).fetchall()
            except psycopg.Error:
                out[sid] = (set(), {("statement", "does not analyse")}, False)
                continue
            cand, block, lost = set(), set(), False
            for kind, ident, detail in rows:
                detail = detail or ""
                key = (kind, norm(ident))
                if detail.startswith(LOST):
                    lost = True       # routed only under tolerate_lost_errors; on the list already
                    continue
                plain = (kind in ("function", "operator", "aggregate", "window", "node", "feature") and detail == "") or \
                        (kind == "type" and detail in ("column", "output column", "constant", "parameter", "")) or \
                        (kind == "cast" and detail in ("I/O conversion", "binary coercion"))
                if plain:
                    cand.add(key + (detail,))
                else:
                    block.add((kind, norm(ident) + (": " + detail if detail else "")))
            out[sid] = (cand, block, lost)
        return out

    # ---- one comparison of the whole corpus
    def compare(self, settings=()):
        heap = connect(self.a.standby, ["SET quicksilver.route = heap"])
        auto = connect(self.a.standby, ["SET quicksilver.route = auto", "SET quicksilver.freshness_wait = '5s'"] + list(settings))
        recs = []
        for sid, sql, src in self.corpus:
            small = not re.search(r"\bpublic\.orders\b", sql)
            auto.execute("SET quicksilver.min_cost = %s" % (0 if small else 10000))
            auto.execute("SET quicksilver.min_rows = %s" % (0 if small else 50000))
            auto.execute("SELECT quicksilver.forget_last()")
            h, a = run(heap, sql), run(auto, sql)
            d = auto.execute("SELECT side, reason, detail FROM quicksilver.last_decision()").fetchone()
            side, reason, detail = d if d else ("heap", "reads no mirrored table", "")
            rec = {"id": sid, "source": src, "side": side, "reason": reason, "detail": detail or ""}
            if "error" in h or "error" in a:
                same = h.get("error") == a.get("error") and h.get("message") == a.get("message")
                rec["verdict"] = "same error" if same else "DIFFERENT"
                rec["diff"] = "heap: %s | routed: %s" % (h.get("message", "%d rows" % len(h.get("rows", []))),
                                                         a.get("message", "%d rows" % len(a.get("rows", []))))
                rec["lost_error"] = "error" in h and "error" not in a
            else:
                hr, ar = h["rows"], a["rows"]
                if not ordered(sql):
                    key = lambda row: tuple((x is None, x or b"") for x in row)
                    hr, ar = sorted(hr, key=key), sorted(ar, key=key)
                if h["desc"] != a["desc"]:
                    rec["verdict"] = "DIFFERENT RowDescription"
                    rec["diff"] = "heap %s | routed %s" % (h["desc"], a["desc"])
                elif hr != ar:
                    rec["verdict"] = "DIFFERENT rows"
                    rec["diff"] = "%d vs %d rows" % (len(hr), len(ar))
                    for x, y in zip(hr, ar):
                        if x != y:
                            f = lambda r: [v.decode(errors="replace") if v is not None else None for v in r]
                            rec["diff"] = "heap %s | routed %s" % (f(x), f(y))
                            break
                else:
                    rec["verdict"] = "identical"
            recs.append(rec)
        heap.close(); auto.close()
        return recs

    @staticmethod
    def summary(recs):
        n = collections.Counter()
        reasons = collections.Counter()
        for r in recs:
            n[r["side"]] += 1
            n[(r["side"], r["verdict"].split()[0])] += 1
            if r["side"] == "heap":
                reasons[r["reason"]] += 1
        return {"statements": len(recs), "routed_to_mirror": n["mirror"], "stayed_on_heap": n["heap"],
                "routed_identical": n[("mirror", "identical")], "routed_same_error": n[("mirror", "same")],
                "routed_different": n[("mirror", "DIFFERENT")], "heap_different": n[("heap", "DIFFERENT")],
                "heap_reasons": reasons.most_common()}

    # ---- to a fixed point: nothing routed differs
    def settle(self, need, lost, tolerant=True):
        settings = ["SET quicksilver.tolerate_lost_errors = on"] if tolerant else []
        while True:
            recs = self.compare(settings)
            removed = 0
            live = dict(self.proposed)      # what this run was made under
            for r in recs:
                if r["side"] != "mirror" or not r["verdict"].startswith("DIFFERENT"):
                    continue
                mine = [k for k in need[r["id"]] if k in live]
                errs = lost[r["id"]] or any("errors" in live[k]["options"] for k in mine)
                if r.get("lost_error") and errs and tolerant:
                    r["verdict"] = "lost error (tolerated)"   # what tolerate_lost_errors says it tolerates
                    continue
                if not mine:
                    raise SystemExit("DIFFERENT with nothing but shipped entries: %s %s" % (r["id"], r["diff"]))
                # The statement was not routed before the newest of its entries
                # was proposed, and is not once that is gone: the older ones
                # keep the statements that verified them. Entries proposed in
                # the same round cannot be told apart, and all go.
                newest = max(self.round_of.get(k, 0) for k in mine)
                for k in mine:
                    if self.round_of.get(k, 0) == newest:
                        self.reject(k, "%s: %s; %s" % (r["id"], r["verdict"], r["diff"][:300]))
                        removed += 1
            if removed == 0:
                return recs
            self.sync()

    def main(self):
        a = self.a
        self.reset()
        std = connect(a.standby)
        base = self.missing(std)          # under the allowlist as shipped
        need = {sid: {c[:2] for c in base[sid][0]} for sid in base}
        lost = {sid: base[sid][2] for sid in base}
        detail = {c[:2]: c[2] for sid in base for c in base[sid][0]}
        before = self.summary(self.compare())
        before_tol = self.summary(self.compare(["SET quicksilver.tolerate_lost_errors = on"]))
        print("before: routed %d of %d (%d with tolerate_lost_errors), different %d" % (
            before["routed_to_mirror"], before["statements"], before_tol["routed_to_mirror"], before["routed_different"]), flush=True)

        if a.apply:
            wanted = json.load(open(a.apply))["entries"]
            rows = []
            for e in wanted:
                key = (e["kind"], e["ident"])
                self.proposed[key] = {"kind": e["kind"], "ident": e["ident"], "collation_rule": e["collation_rule"], "options": e["options"]}
                self.round_of[key] = e.get("round", 0)
                rows.append(self.proposed[key])
            self.insert(rows)
            recs = self.settle(need, lost)
        else:
            tried, rnd, recs = set(), 0, None
            while True:
                rnd += 1
                now = self.missing(std)
                single, joint = set(), set()
                for sid, (cand, block, _) in now.items():
                    keys = {c[:2] for c in cand} - tried
                    if block or not cand or any(c[:2] in self.rejected for c in cand):
                        continue
                    if len(cand) == 1:
                        single |= keys
                    else:
                        joint |= keys
                batch = single or joint
                if not batch:
                    break
                rows = []
                for key in sorted(batch):
                    tried.add(key)
                    row, why = self.describe(key[0], key[1], detail.get(key, ""))
                    if row is None:
                        self.rejected[key] = why
                        continue
                    self.proposed[key] = row
                    self.round_of[key] = rnd
                    rows.append(row)
                self.insert(rows)
                n0 = len(self.rejected)
                recs = self.settle(need, lost)
                s = self.summary(recs)
                self.rounds.append({"round": rnd, "how": "alone" if single else "together", "proposed": len(rows),
                                    "rejected": len(self.rejected) - n0, "routed": s["routed_to_mirror"]})
                print("round %d (%s): proposed %d, rejected %d, routed %d" % (
                    rnd, "alone" if single else "together", len(rows), len(self.rejected) - n0, s["routed_to_mirror"]), flush=True)
            if recs is None:
                recs = self.settle(need, lost)

        # evidence, and the entries that have too little of it
        while True:
            ev = collections.defaultdict(list)
            for r in recs:
                if r["side"] == "mirror" and r["verdict"] == "identical":
                    for k in need[r["id"]]:
                        if k in self.proposed:
                            ev[k].append(r["id"])
            thin = [k for k in list(self.proposed) if len(ev[k]) < a.min_evidence]
            if not thin:
                break
            for k in thin:
                self.reject(k, "exercised by %d routed statement(s), fewer than %d: that is not verification" % (len(ev[k]), a.min_evidence))
            self.sync()
            recs = self.settle(need, lost)

        for k, row in self.proposed.items():
            self.prim.execute("UPDATE quicksilver.allowlist SET verified_by = %s WHERE kind = %s AND ident = %s",
                              ("%s: %d statements routed and identical (%s)" % (MARK, len(ev[k]), a.label), k[0], k[1]))
        self.sync()
        final_tol = self.settle(need, lost)
        final = self.settle(need, lost, tolerant=False)
        after, after_tol = self.summary(final), self.summary(final_tol)
        first = collections.Counter()
        left = self.missing(std)
        for r in final:
            if r["side"] == "heap" and r["reason"] == "construct not on the allowlist":
                cand, block, _ = left[r["id"]]
                names = sorted(block) + sorted((c[0], c[1]) for c in cand)
                if names:
                    first["%s %s" % names[0]] += 1
                elif lost[r["id"]]:
                    first["operator integer + - * (an overflow error may be lost: quicksilver.tolerate_lost_errors)"] += 1
        out = {
            "label": a.label, "server": std.execute("SELECT version()").fetchone()[0],
            "locale": std.execute("SELECT datlocprovider::text || ':' || coalesce(datlocale, datcollate) FROM pg_database WHERE datname = current_database()").fetchone()[0],
            "min_evidence": a.min_evidence, "before": before, "before_tolerate_lost_errors": before_tol,
            "after": after, "after_tolerate_lost_errors": after_tol, "rounds": self.rounds,
            "entries": [dict(row, round=self.round_of.get(k, 0), verified_by=sorted(ev[k]), statements=len(ev[k])) for k, row in sorted(self.proposed.items())],
            "rejected": [{"kind": k[0], "ident": k[1], "why": why} for k, why in sorted(self.rejected.items())],
            "first_blocker": first.most_common(40),
            "lost_errors_tolerated": [r["id"] for r in final_tol if r["verdict"].startswith("lost error")],
            "statements": final,
        }
        os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
        json.dump(out, open(a.out, "w"), indent=1, ensure_ascii=False)
        print(json.dumps({"before": {k: before[k] for k in ("statements", "routed_to_mirror", "routed_different")},
                          "after": {k: v for k, v in after.items() if k != "heap_reasons"},
                          "after_tolerate_lost_errors": {k: after_tol[k] for k in ("routed_to_mirror", "routed_identical", "routed_different")},
                          "entries_added": len(self.proposed), "rejected": len(self.rejected),
                          "heap_reasons": after["heap_reasons"]}, indent=1))
        bad = [r for r in final + final_tol if r["side"] == "mirror" and r["verdict"].startswith("DIFFERENT")]
        for r in bad:
            print("DIFFERENT:", r["id"], r["diff"])
        return 1 if bad else 0


def splice(path):
    """Write the verified entries into the extension's SQL, between the markers."""
    d = json.load(open(path))
    lines = [BEGIN,
             "-- Generated: do not edit by hand. Each row was proposed from quicksilver.unsafe_constructs(),",
             "-- routed through the DuckLake mirror on a hot standby, and kept only because every routed",
             "-- statement that exercises it returned the heap's bytes and RowDescription, in the ICU en-US",
             "-- and in the C locale database. Which statements: validate/results/widen-*.json.",
             "INSERT INTO quicksilver.allowlist (kind, ident, collation_rule, options, verified_by) VALUES"]
    q = lambda s: "'" + s.replace("'", "''") + "'"
    vals = []
    for e in d["entries"]:
        vals.append("  (%s, %s, %s, '{%s}', %s)" % (q(e["kind"]), q(e["ident"]), q(e["collation_rule"]), ",".join(e["options"]),
                                                    q("validate/widen.py: %d statements routed and identical, both locales" % e["statements"])))
    lines.append(",\n".join(vals) + ";")
    lines.append(END)
    s = open(SQLFILE).read()
    block = "\n".join(lines) + "\n"
    if BEGIN in s:
        s = s[:s.index(BEGIN)] + block + s[s.index(END) + len(END) + 1:]
    else:
        s = s.rstrip("\n") + "\n\n" + block
    open(SQLFILE, "w").write(s)
    print("%d entries written to %s" % (len(d["entries"]), os.path.relpath(SQLFILE)))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--primary", type=int, default=5580)
    ap.add_argument("--standby", type=int, default=5581)
    ap.add_argument("--out", default=os.path.join(HERE, "results", "widen.json"))
    ap.add_argument("--apply", help="a previous result: propose exactly its entries, and only prune")
    ap.add_argument("--min-evidence", type=int, default=3)
    ap.add_argument("--label", default="real path, hot standby")
    ap.add_argument("--splice", help="write a result's entries into pg_quicksilver--0.1.sql and exit")
    args = ap.parse_args()
    if args.splice:
        splice(args.splice)
        sys.exit(0)
    sys.exit(Widen(args).main())
