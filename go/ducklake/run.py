"""Drive the real mirror package and the publisher side by side.

  python3 run.py functional --work DIR
      small table; checks every publish against the mirror's own view SQL, then
      schema evolution, time travel across a merge, and a patch file.

  python3 run.py bench --work DIR [--rows 2000000] [--applies 50] [--publish catalog|sql]
      the measurement in README.md. Writes DIR/result-<publish>.json.

Needs: go (to build ./gen and ../cmd/qs-query), python duckdb with ducklake.
"""
import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import time

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from publish import MD, Publisher, Unpublishable  # noqa: E402

TR = duckdb.connect()
COLS = [("id", "uuid"), ("amount", "numeric(12,2)"), ("created_at", "timestamptz"),
        ("status", "text"), ("note", "text")]


def build(work):
    os.makedirs(work, exist_ok=True)
    subprocess.run(["go", "build", "-o", work + "/gen", "./gen"], cwd=HERE, check=True)
    subprocess.run(["go", "build", "-o", work + "/qs-query", "./cmd/qs-query"],
                   cwd=os.path.dirname(HERE), check=True)


class Gen:
    def __init__(self, work, env=None):
        e = dict(os.environ, QS_COMPACT_IDLE_SECONDS="1000000000")
        e.update(env or {})
        self.p = subprocess.Popen([work + "/gen", work + "/mirror"], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, text=True, env=e)

    def do(self, cmd):
        self.p.stdin.write(cmd + "\n")
        self.p.stdin.flush()
        line = self.p.stdout.readline()
        if not line:
            raise RuntimeError("gen died on: " + cmd)
        return json.loads(line)

    def close(self):
        self.p.stdin.close()
        self.p.wait()


def view_sql(work):
    out = subprocess.run([work + "/qs-query", "-mirror", work + "/mirror", "-table", "public.orders"],
                         capture_output=True, text=True, check=True).stdout
    return "\n".join(l for l in out.splitlines() if not l.startswith("--"))


def fp_sql(src, cols):
    h = ", ".join('"%s"' % c for c in cols)
    return f"select count(*), sum(amount), bit_xor(hash({h})) from {src}"


def manifest_sql(work, cols):
    """The same reconstruction ViewSQL does, from the manifest, for the one case
    qs-query cannot serve without a source connection: a column no base file
    carries yet. A file lacking a column reads it as MissingVals."""
    d = work + "/mirror/public.orders"
    st = json.load(open(d + "/state.json"))
    parts = []
    for sub, key in (("base", "base_files"), ("delta", "delta_files")):
        for f in st.get(key) or []:
            p = f"{d}/{sub}/{f}"
            has = {r[0] for r in TR.execute(f"select name from parquet_schema('{p}')").fetchall()}
            sel = ", ".join('"%s"' % c if c in has else "'%s' as \"%s\"" % (st["missing_vals"][c], c) for c in cols)
            q = f"select {sel} from read_parquet('{p}', file_row_number = true)"
            gen = (st.get("dv_gen") or {}).get(f[:-8], 0)
            if gen:
                q += f" where file_row_number not in (select p from '{d}/dv/{f[:-8]}.{gen:09d}.dv.parquet')"
            parts.append(q)
    return " union all ".join(parts)


def truth(work, cols):
    if len(cols) > len(COLS):
        return TR.execute(fp_sql("(" + manifest_sql(work, cols) + ")", cols)).fetchone()
    return TR.execute(fp_sql("(" + view_sql(work) + ")", cols)).fetchone()


def fresh(work):
    for d in ("mirror", "lake"):
        shutil.rmtree(os.path.join(work, d), ignore_errors=True)
    for f in ("catalog.db", "catalog.db.wal"):
        if os.path.exists(os.path.join(work, f)):
            os.remove(os.path.join(work, f))


def functional(a):
    work = a.work
    for mode in ("sql", "catalog"):
        print(f"=== mode={mode}: publish after every apply")
        fresh(work)
        g = Gen(work)
        cols = [n for n, _ in COLS]
        g.do("load 20000")
        pub = Publisher(work + "/mirror/public.orders", work + "/catalog.db", work + "/lake", "orders",
                        list(COLS), mode)
        truths = {}

        def check(label):
            r = pub.publish()
            want = truth(work, cols)
            got = pub.con.execute(fp_sql("lake.orders", cols)).fetchone()
            cnt = pub.con.execute("select count(*) from lake.orders").fetchone()[0]
            ok = want == got and cnt == want[0]
            print(f"  {label:34s} snap={r['snapshot']} +{r['added']} -{r['dropped']} dv={r['dv_changed']} "
                  f"{r['ms']:.1f}ms rows={got[0]} {'OK' if ok else 'MISMATCH want=%s got=%s cnt=%s' % (want, got, cnt)}")
            assert ok
            truths[r["snapshot"]] = (want, list(cols))
            return r

        check("initial load")
        merged = False
        for i in range(20):
            s = g.do("step 200 40 40")
            check(f"apply {i + 1} {s.get('maintain', '')}")
            merged |= s.get("maintain") == "merge_deltas"
        assert merged, "no merge happened; the drop path is untested"
        r = pub.publish()
        assert r["snapshot"] is None, "an unchanged manifest made a snapshot"
        print("  unchanged manifest: no snapshot")
        listed = pub.con.execute("select count(*) from ducklake_list_files('lake', 'orders')").fetchone()[0]
        print(f"  files a reader is given: {listed}; files the manifest names: {r['files']}")
        if mode == "catalog":
            # a restarted publisher must pick up where the catalog says it is
            pub.con.close()
            pub = Publisher(work + "/mirror/public.orders", work + "/catalog.db", work + "/lake", "orders",
                            list(COLS), mode)
            assert pub.publish()["snapshot"] is None

        g.do("evolve region text emea")
        cols.append("region")
        pub.columns.append(("region", "text"))
        # No file carries the column yet, and qs-query without a source
        # connection learns columns from files, so the view cannot be asked
        # until the next apply. The lake can: every row reads the default.
        r = pub.publish()
        print(f"  ADD COLUMN region DEFAULT 'emea'    snap={r['snapshot']}")
        print("    region values:", pub.con.execute(
            "select region, count(*) from lake.orders group by 1 order by 1").fetchall())
        s = g.do("step 200 40 40")
        check("apply after evolve")
        print("    region values:", pub.con.execute(
            "select region, count(*) from lake.orders group by 1 order by 1").fetchall())
        g.do("compact")
        check("base rewrite (compact)")

        bad = 0
        for snap, (want, cs) in truths.items():
            got = pub.con.execute(fp_sql(f"lake.orders at (version => {snap})", cs)).fetchone()
            bad += got != want
        print(f"  time travel: {len(truths) - bad}/{len(truths)} snapshots reproduce the manifest they were published from")
        assert bad == 0
        snaps = pub.con.execute(f"select count(*) from {MD}.ducklake_snapshot").fetchone()[0]
        print(f"  snapshots in catalog: {snaps} for {len(truths)} published manifests (+ create schema/table)")

        s = g.do("patch 50")
        assert s["partial"] > 0, "the mirror did not write a patch file"
        try:
            pub.publish()
            raise AssertionError("a patch file was published")
        except Unpublishable as e:
            print("  patch file:", e)
        g.close()
        pub.con.close()

    print("=== with QS_PARTIAL_DELTAS=0 the same change is a whole row and publishes")
    fresh(work)
    g = Gen(work, {"QS_PARTIAL_DELTAS": "0"})
    cols = [n for n, _ in COLS]
    g.do("load 20000")
    pub = Publisher(work + "/mirror/public.orders", work + "/catalog.db", work + "/lake", "orders", list(COLS), a.mode2)
    pub.publish()
    s = g.do("patch 50")
    assert s["partial"] == 0
    pub.publish()
    assert truth(work, cols) == pub.con.execute(fp_sql("lake.orders", cols)).fetchone()
    print("  OK", pub.con.execute("select status, count(*) from lake.orders where status='patched' group by 1").fetchall())
    g.close()


def du(path):
    return sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(path) for f in fs)


def bench(a):
    work = a.work
    fresh(work)
    cols = [n for n, _ in COLS]
    g = Gen(work)
    t0 = time.time()
    print("load", g.do(f"load {a.rows}"), f"{time.time() - t0:.0f}s", flush=True)
    cat = work + "/catalog.db"
    pub = Publisher(work + "/mirror/public.orders", cat, work + "/lake", "orders", list(COLS), a.mode2)
    out = {"rows": a.rows, "mode": a.mode2, "applies": []}
    r = pub.publish()
    pub.con.execute("CHECKPOINT " + MD)
    out["initial"] = dict(r, catalog_bytes=os.path.getsize(cat))
    print("initial publish", out["initial"], flush=True)
    truths = {r["snapshot"]: truth(work, cols)}
    u, d, n = (int(a.rows * f / a.applies) for f in (0.05, 0.01, 0.01))
    for i in range(a.applies):
        s = g.do(f"step {u} {d} {n}")
        r = pub.publish()
        want = truth(work, cols)
        got = pub.con.execute(fp_sql("lake.orders", cols)).fetchone()
        assert want == got, (i, want, got)
        truths[r["snapshot"]] = want
        st = json.load(open(work + "/mirror/public.orders/state.json"))
        r.update(apply=i + 1, apply_ms=s["apply_ms"], maintain=s.get("maintain", ""),
                 catalog_bytes=os.path.getsize(cat) + (os.path.getsize(cat + ".wal") if os.path.exists(cat + ".wal") else 0),
                 dead=pub.con.execute(f"select coalesce(sum(delete_count),0) from {MD}.ducklake_delete_file "
                                      "where end_snapshot is null").fetchone()[0],
                 delta_rows=st["delta_rows"])
        out["applies"].append(r)
        print({k: (round(v, 1) if isinstance(v, float) else v) for k, v in r.items()}, flush=True)

    bad = [s for s, want in truths.items()
           if pub.con.execute(fp_sql(f"lake.orders at (version => {s})", cols)).fetchone() != want]
    out["time_travel"] = {"snapshots": len(truths), "wrong": len(bad)}
    print("time travel", out["time_travel"], flush=True)

    pub.con.execute("CHECKPOINT " + MD)
    out["catalog_bytes_after_checkpoint"] = os.path.getsize(cat)
    out["catalog_rows"] = {t: pub.con.execute(f"select count(*) from {MD}.{t}").fetchone()[0] for t in (
        "ducklake_snapshot", "ducklake_data_file", "ducklake_delete_file", "ducklake_file_column_stats")}
    try:
        out["catalog_rows"]["inlined_deletes"] = pub.con.execute(
            f"select count(*) from {MD}.ducklake_inlined_delete_1").fetchone()[0]
    except duckdb.Error:
        pass
    out["mirror_bytes"] = du(work + "/mirror")
    out["lake_dir_files"] = len(os.listdir(work + "/lake"))
    out["lake_delete_file_bytes"] = sum(
        os.path.getsize(os.path.join(dp, f)) for dp, _, fs in os.walk(work + "/lake") for f in fs if "delete" in f)
    pub.con.close()

    with open(work + "/view.sql", "w") as f:
        f.write(view_sql(work))
    rd = subprocess.run(["taskset", "-c", "1", sys.executable, HERE + "/read.py", work + "/view.sql", cat],
                        capture_output=True, text=True, check=True).stdout
    out["read"] = json.loads(rd)

    # A base rewrite: the manifest swaps one file for every file.
    pub = Publisher(work + "/mirror/public.orders", cat, work + "/lake", "orders", list(COLS), a.mode2)
    s = g.do("compact")
    r = pub.publish()
    want = truth(work, cols)
    assert want == pub.con.execute(fp_sql("lake.orders", cols)).fetchone()
    prev = max(truths)
    assert truths[prev] == pub.con.execute(fp_sql(f"lake.orders at (version => {prev})", cols)).fetchone()
    out["compact"] = dict(r, rewrite_ms=s["ms"])
    print("compact", out["compact"], flush=True)
    g.close()

    ms = [x["ms"] for x in out["applies"]]
    q = statistics.quantiles(ms, n=20)
    out["publish_ms"] = {"median": statistics.median(ms), "p95": q[18], "max": max(ms)}
    print(json.dumps(out["publish_ms"]), json.dumps(out["read"], indent=1))
    with open(f"{work}/result-{a.mode2}.json", "w") as f:
        json.dump(out, f, indent=1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["functional", "bench"])
    ap.add_argument("--work", required=True)
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--applies", type=int, default=50)
    ap.add_argument("--publish", dest="mode2", default="catalog", choices=["sql", "catalog"])
    a = ap.parse_args()
    a.work = os.path.abspath(a.work)
    build(a.work)
    {"functional": functional, "bench": bench}[a.mode](a)
