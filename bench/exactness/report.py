"""Turn per-statement comparisons into results.json and REPORT.md.

Attribution has two layers, and only the second involves anyone's judgement.

1. FAMILY is decided by experiment. Every statement also runs on a CONTROL: the
   same DuckDB, the same session settings, but over tables whose columns have
   the source's own types (loaded with DuckDB's postgres scanner) instead of the
   mirror's. If the control is exact and the mirror is not, the mirror's type
   mapping is the whole cause. If the control fails the same way, faithful types
   would not have helped: it is DuckDB. Anything else is both.

2. CAUSE is the specific mechanism. It is derived from the error text or the
   type difference where that is unambiguous, and read from root_causes.tsv
   (statement id, cause, note) where a person had to look. A wrong value with
   no entry in that file is reported as `unreviewed`, never guessed.
"""
from __future__ import annotations

import collections
import csv
import os
import re

import compare as C

FAM_TYPES = "type mapping"
FAM_DUCK = "DuckDB"
FAM_BOTH = "both"

# Cause -> (group, can the project remove it?, how)
GROUPS = ["type mapping", "function or syntax missing in DuckDB", "DuckDB semantics",
          "ordering / collation", "column naming", "environment", "both engines refuse", "unreviewed"]


def load_causes(path: str) -> dict:
    out = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for row in csv.reader((l for l in f if l.strip() and not l.startswith("#")), delimiter="\t"):
                out[row[0]] = {"group": row[1], "cause": row[2], "note": row[3] if len(row) > 3 else ""}
    return out


def auto_cause(s: dict) -> tuple:
    """(group, cause) where the evidence in the record is enough on its own."""
    cls, fam = s["class"], s.get("family")
    if cls == C.ERR_MIRROR:
        e = s.get("error_mirror", "")
        if fam == FAM_TYPES:
            return "type mapping", "statement fails on the mirrored type, runs on the source type"
        if re.search(r"Catalog Error.*(Function|function|Macro).*does not exist", e):
            return GROUPS[1], "function does not exist in DuckDB"
        if "Parser Error" in e:
            return GROUPS[1], "syntax DuckDB does not parse"
        if re.search(r"No function matches|Could not choose a best candidate|No operator", e):
            return GROUPS[1], "no such function/operator signature in DuckDB"
        if "Not implemented" in e or "not supported" in e.lower():
            return GROUPS[1], "not implemented in DuckDB"
        if "Conversion Error" in e:
            return "DuckDB semantics", "DuckDB rejects an input or cast PostgreSQL accepts"
        if "Type with name" in e:
            return GROUPS[1], "type does not exist in DuckDB"
        return GROUPS[1], "other construct DuckDB rejects"
    if cls == C.ERR_BOTH:
        return "both engines refuse", "both raise an error (not a divergence)"
    if cls == C.ERR_SOURCE:
        if fam == FAM_TYPES:
            return "type mapping", "PostgreSQL raises on the source type; the mirrored type has room"
        return "DuckDB semantics", "PostgreSQL raises an error, DuckDB returns a value"
    if cls == C.DIFF_NAME:
        return "column naming", "DuckDB names an unaliased column differently"
    if cls == C.DIFF_TYPE:
        labels = sorted({d["label"] for d in s.get("type_diffs", [])})
        if fam == FAM_TYPES:
            return "type mapping", "result type follows the mirrored type: " + (", ".join(labels) or "rendering")
        if labels:
            return "DuckDB semantics", "DuckDB derives a different result type: " + ", ".join(labels)
        return "DuckDB semantics", "same value, rendered at a different scale/format"
    return None, None


def attribute(stmts: list, causes: dict) -> None:
    for s in stmts:
        if s["class"] == C.EXACT:
            continue
        cc = s.get("control_class")
        if cc is None:
            s["family"] = None
        elif cc == C.EXACT:
            s["family"] = FAM_TYPES
        elif cc == s["class"] and s.get("control_example") == s.get("example") \
                and s.get("control_error") == s.get("error_mirror"):
            s["family"] = FAM_DUCK
        else:
            s["family"] = FAM_BOTH if cc != s["class"] else FAM_DUCK
        if s["id"] in causes:
            s["group"], s["cause"] = causes[s["id"]]["group"], causes[s["id"]]["cause"]
            if causes[s["id"]]["note"]:
                s["note"] = causes[s["id"]]["note"]
            s["cause_source"] = "reviewed"
        else:
            g, c = auto_cause(s)
            s["group"], s["cause"] = g or "unreviewed", c or "unreviewed"
            s["cause_source"] = "derived" if g else "none"


def width_only(s: dict) -> bool:
    """Same values, and the ONLY difference is which integer type carries them."""
    return s["class"] == C.DIFF_TYPE and s.get("sub") is None and bool(s["type_diffs"]) \
        and all(d["label"] == "integer width" for d in s["type_diffs"])


def build(meta: dict, check: dict, probes: list, stmts: list, causes_path: str) -> dict:
    causes = load_causes(causes_path)
    attribute(stmts, causes)
    n = len(stmts)
    by_class = collections.Counter(s["class"] for s in stmts)
    cats = collections.OrderedDict()
    for s in stmts:
        cats.setdefault(s["category"], collections.Counter())[s["class"]] += 1
    control = collections.Counter(s["control_class"] for s in stmts if s.get("control_class"))
    stale = sorted(set(causes) - {s["id"] for s in stmts if s["class"] != C.EXACT})
    return {
        "meta": meta,
        "selfcheck": check,
        "probes": probes,
        "summary": {
            "statements": n,
            "by_class": {c: by_class.get(c, 0) for c in C.CLASSES},
            "by_class_pct": {c: round(100.0 * by_class.get(c, 0) / n, 1) for c in C.CLASSES},
            "control_by_class": {c: control.get(c, 0) for c in C.CLASSES} if control else None,
            "integer_width_only": sum(1 for s in stmts if width_only(s)),
            "tuned_by_class": {c: sum(1 for s in stmts if s.get("tuned_class") == c) for c in C.CLASSES},
            "names_differ": sum(1 for s in stmts if s.get("names_differ")),
            "float_noise_only": sum(1 for s in stmts if s.get("float_noise") and s["class"] == C.EXACT),
            "by_family": dict(collections.Counter(s.get("family") or "n/a" for s in stmts if s["class"] != C.EXACT)),
            "by_group": dict(collections.Counter(s["group"] for s in stmts if s["class"] != C.EXACT)),
            "stale_root_cause_ids": stale,
        },
        "by_category": {k: {c: v.get(c, 0) for c in C.CLASSES} | {"total": sum(v.values())} for k, v in cats.items()},
        "statements": stmts,
    }


def headline(doc: dict) -> str:
    s = doc["summary"]
    parts = [f"{C.CLASS_LABEL[c]} {s['by_class'][c]} ({s['by_class_pct'][c]}%)" for c in C.CLASSES if s["by_class"][c]]
    return f"{s['statements']} statements: " + "; ".join(parts)


def _cell(v) -> str:
    v = "NULL" if v is None else str(v)
    v = v.replace("|", "\\|").replace("\n", "⏎")
    return "`" + (v if len(v) <= 60 else v[:57] + "...") + "`"


def markdown(doc: dict) -> str:
    m, s = doc["meta"], doc["summary"]
    n = s["statements"]
    L = []
    w = L.append
    w("# Exactness of the mirror read through DuckDB — generated report\n")
    w("Generated by `bench/exactness/run.py` from `results.json`. Do not edit by hand.\n")
    w(f"- PostgreSQL {m['postgres']} (database collation `{m['collation']}`), DuckDB {m['duckdb']} (Python)")
    w(f"- both sessions: `TimeZone = {m['session_timezone']}`; DuckDB also: " + " ".join(f"`{x}`" for x in m["duckdb_settings"][:-1]))
    w("- mirror built by `qs-mirror`; `qs-verify` said MATCH on: " + ", ".join(f"{t} ({r} rows)" for t, r in m["tables"].items()))
    c = doc["selfcheck"]
    w(f"- self-check: {'PASS' if c['passed'] else 'FAIL'} — corpus against itself {c['corpus_exact_vs_self']} exact + "
      f"{c['corpus_same_error_vs_self']} identical errors of {c['corpus_statements']}; {c['wrong_pairs']} deliberately wrong pairs "
      f"flagged as expected; {c['render_pairs']} literal render pairs agree\n")

    w("## Headline\n")
    tuned = " + ".join(x.rstrip(";") for x in m.get("extra_settings", []))
    w(f"| outcome | statements | share | with `{tuned}` |" + (" typed control |" if s["control_by_class"] else ""))
    w("|---|---:|---:|---:|" + ("---:|" if s["control_by_class"] else ""))
    for k in C.CLASSES:
        row = f"| {C.CLASS_LABEL[k]} | {s['by_class'][k]} | {s['by_class_pct'][k]}% | {s['tuned_by_class'][k]} |"
        if s["control_by_class"]:
            row += f" {s['control_by_class'][k]} |"
        w(row)
        if k == C.DIFF_TYPE:
            w(f"| &nbsp;&nbsp;of which only the integer width differs | {s['integer_width_only']} | "
              f"{round(100.0 * s['integer_width_only'] / n, 1)}% | |" + (" |" if s["control_by_class"] else ""))
    w(f"| **total** | **{n}** | | {n} |" + (f" {sum(s['control_by_class'].values())} |" if s["control_by_class"] else ""))
    w("")
    if s["control_by_class"]:
        w("The *typed control* column is the same statements on the same DuckDB over tables that keep the "
          "source's own column types. The gap between the two columns is what the mirror's type mapping costs.\n")
    w(f"{s['float_noise_only']} statements counted as exact differ only in float digits beyond a relative "
      f"{m['float_rtol']:g}. {s['names_differ']} statements return at least one column under a different name.\n")

    w("## By category\n")
    w("| category | n | " + " | ".join(C.CLASS_LABEL[k] for k in C.CLASSES) + " |")
    w("|---|---:|" + "---:|" * len(C.CLASSES))
    for cat, v in doc["by_category"].items():
        w(f"| {cat.replace('_', ' ')} | {v['total']} | " + " | ".join(str(v[k]) for k in C.CLASSES) + " |")
    w("")

    bad = [x for x in doc["statements"] if x["class"] != C.EXACT]
    w("## Root causes\n")
    w(f"{len(bad)} statements are not exact. By family (decided by the typed control):\n")
    w("| family | statements |\n|---|---:|")
    for k, v in sorted(s["by_family"].items(), key=lambda kv: -kv[1]):
        w(f"| {k} | {v} |")
    w("\nBy group:\n")
    w("| group | statements | wrong value | error on mirror only | same values, other type or name |\n|---|---:|---:|---:|---:|")
    gro = collections.OrderedDict()
    for x in bad:
        gro.setdefault(x["group"], []).append(x)
    for g, xs in sorted(gro.items(), key=lambda kv: -len(kv[1])):
        k = collections.Counter(x["class"] for x in xs)
        w(f"| {g} | {len(xs)} | {k[C.DIFF_VALUE]} | {k[C.ERR_MIRROR]} | {k[C.DIFF_TYPE] + k[C.DIFF_NAME]} |")
    w("\nBy cause:\n")
    w("| group | cause | statements | of which wrong value | example ids |\n|---|---|---:|---:|---|")
    agg = collections.OrderedDict()
    for x in bad:
        agg.setdefault((x["group"], x["cause"]), []).append(x)
    for (g, cause), xs in sorted(agg.items(), key=lambda kv: -len(kv[1])):
        wrong = sum(1 for x in xs if x["class"] == C.DIFF_VALUE)
        w(f"| {g} | {cause} | {len(xs)} | {wrong} | {', '.join(x['id'] for x in xs[:4])}{' …' if len(xs) > 4 else ''} |")
    w("")

    w("## Values the mirror refuses, or stores differently by path\n")
    w("Each probe is one table holding one value, mirrored on its own (`probes.sql`): row 1 arrives by "
      "snapshot, row 2 is the same value arriving over the stream. *reads* is what DuckDB returns, "
      "rendered as PostgreSQL would print it; the *value* column is what PostgreSQL prints.\n")
    w("| column type | value | outcome | qs-verify | mirror type | snapshot row reads | stream row reads |\n|---|---|---|---|---|---|---|")
    for h in doc["probes"]:
        ok = h["outcome"].startswith("mirrored")
        w(f"| `{h['type']}` | {_cell(h['value'])} | {h['outcome']} | {h['verify'] or ''} | {h['mirror_type'] or ''} | "
          f"{_cell(h['snapshot_row']) if ok else ''} | {_cell(h['stream_row']) if ok and h['stream_row'] is not None else ''} |")
    w("")

    for cls in (C.DIFF_VALUE, C.ERR_MIRROR, C.ERR_SOURCE, C.DIFF_TYPE, C.DIFF_NAME, C.ERR_BOTH):
        xs = [x for x in doc["statements"] if x["class"] == cls]
        if not xs:
            continue
        w(f"## {C.CLASS_LABEL[cls]} ({len(xs)})\n")
        if cls == C.DIFF_VALUE:
            w("| id | what differs | source | mirror | family | cause |\n|---|---|---|---|---|---|")
            for x in xs:
                ex = x.get("example") or {}
                what = x.get("sub") or ""
                if "col" in ex:
                    what += f" (`{ex['col']}`, row {ex['row']}, {x.get('rows_differing')} rows)"
                w(f"| {x['id']} | {what} | {_cell(ex.get('source'))} | {_cell(ex.get('mirror'))} | {x.get('family') or ''} | {x['cause']} |")
        elif cls in (C.ERR_MIRROR, C.ERR_BOTH):
            w("| id | mirror error | family | cause |\n|---|---|---|---|")
            for x in xs:
                w(f"| {x['id']} | {_cell(x.get('error_mirror', ''))[:140]} | {x.get('family') or ''} | {x.get('cause', '')} |")
        elif cls == C.ERR_SOURCE:
            w("| id | PostgreSQL error | family | cause |\n|---|---|---|---|")
            for x in xs:
                w(f"| {x['id']} | {_cell(x.get('error_source', ''))} | {x.get('family') or ''} | {x['cause']} |")
        elif cls == C.DIFF_TYPE:
            w("| id | columns (source → mirror) | family | cause |\n|---|---|---|---|")
            for x in xs:
                td = "; ".join(f"`{d['col']}` {d['source']} → {d['mirror']}" for d in x["type_diffs"][:3]) or "(rendering only)"
                w(f"| {x['id']} | {td} | {x.get('family') or ''} | {x['cause']} |")
        else:
            w("| id | names (source → mirror) |\n|---|---|")
            for x in xs:
                w(f"| {x['id']} | " + "; ".join(f"`{a}` → `{b}`" for a, b in x.get("names", [])[:3]) + " |")
        w("")
    return "\n".join(L) + "\n"
