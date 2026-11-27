#!/usr/bin/env python3
"""Crash-safety fuzz for pg_quicksilver. Meant for an --enable-cassert server.

Takes the three corpora and, with a fixed seed, sends mutations of their
statements under randomly chosen settings and wrappers:

  mutations  tokens dropped, doubled, swapped; the statement cut short;
             literals replaced by parameters, NULLs, huge or odd values;
             keywords swapped for their neighbours (LEFT/FULL, ASC/DESC, ...)
  wrappers   EXPLAIN, EXPLAIN ANALYZE, EXPLAIN (FORMAT JSON), PREPARE/EXECUTE
             (several times, so the plan cache is exercised), DECLARE/FETCH in
             a transaction, a subquery, a CTE, UNION ALL with itself,
             BEGIN/SAVEPOINT, a PL/pgSQL DO block, the extended protocol
  settings   every quicksilver.route mode, route_in_transaction,
             allow_unsafe, fallback, min_cost, work_mem, plan_cache_mode,
             debug_parallel_query, jit, isolation level
  meanwhile  a second connection disables and re-enables mirrors, reports old
             and new LSNs, removes and restores allowlist rows, republishes a
             mirror view

Most of what it sends is an error, and that is the point: the only failures
are a lost connection, or a TRAP / signal / PANIC line in the server log.

When a mutated statement was answered by the mirror (and allow_unsafe was off)
its rows are also compared, as a multiset, with the heap's.

    fuzz.py [--port 5544] [--n 6000] [--seed 1] --log /path/to/primary.log
"""
import argparse, collections, os, random, re, sys
import psycopg
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from equality import load, HERE, REPO

TOKEN = re.compile(r"'(?:[^']|'')*'|\"[^\"]*\"|\d+\.\d+|\d+|[A-Za-z_][A-Za-z_0-9]*|::|<>|<=|>=|\S")
SWAP = {"LEFT": "FULL", "FULL": "RIGHT", "JOIN": "CROSS JOIN", "ASC": "DESC", "DESC": "ASC", "AND": "OR", "OR": "AND",
        "sum": "avg", "min": "max", "count": "sum", "=": "<>", "<": ">=", "ORDER": "GROUP", "WHERE": "HAVING",
        "FIRST": "LAST", "LIMIT": "OFFSET", "SELECT": "SELECT DISTINCT", "NOT": "", "NULL": "0", "BY": "BY 1,",
        "public": "pg_catalog", "orders": "customers", "id": "ctid", "t_num": "t_doc"}
ODD = ["NULL", "$1", "0", "-1", "2147483648", "9223372036854775807", "1e400", "'NaN'", "'infinity'", "''", "'\\x00'",
       "1.0000000000000000000000000000000000000000001", "(SELECT 1)", "now()", "random()", "'é😀'", "ARRAY[1,2]",
       "id", "*", "DEFAULT", "current_user", "1/0", "pg_sleep(0)", "(SELECT count(*) FROM public.orders)"]
SETTINGS = [
    ("quicksilver.route", ["off", "shadow", "auto", "heap", "mirror", "mirror", "auto", "auto", "mirror"]),
    ("quicksilver.route_in_transaction", ["on", "off"]),
    ("quicksilver.allow_unsafe", ["off", "off", "on"]),
    ("quicksilver.fallback", ["on", "on", "off"]),
    ("quicksilver.tolerate_lost_errors", ["off", "on"]),
    ("quicksilver.min_cost", ["0", "0", "0", "10000", "1e9"]),
    ("quicksilver.min_rows", ["0", "0", "50000"]),
    ("quicksilver.negative_cache_ttl", ["0", "60"]),
    ("quicksilver.prefer_custom_plans", ["on", "off"]),
    ("work_mem", ["64kB", "4MB"]),
    ("plan_cache_mode", ["auto", "force_generic_plan", "force_custom_plan"]),
    ("debug_parallel_query", ["off", "off", "on"]),
    ("jit", ["off", "on"]),
    ("max_parallel_workers_per_gather", ["0", "2"]),
    ("default_transaction_isolation", ["'read committed'", "'read committed'", "'repeatable read'"]),
]


def mutate(rng, sql):
    toks = TOKEN.findall(sql)
    for _ in range(rng.choice([0, 1, 1, 2, 3])):
        if not toks:
            break
        i = rng.randrange(len(toks))
        k = rng.randrange(8)
        if k == 0:
            del toks[i]
        elif k == 1:
            toks.insert(i, toks[i])
        elif k == 2:
            j = rng.randrange(len(toks))
            toks[i], toks[j] = toks[j], toks[i]
        elif k == 3:
            toks = toks[:i]
        elif k == 4:
            toks[i] = rng.choice(ODD)
        elif k == 5 and toks[i] in SWAP:
            toks[i] = SWAP[toks[i]]
        elif k == 6:
            lits = [n for n, t in enumerate(toks) if t[0].isdigit() or t[0] == "'"]
            if lits:
                toks[rng.choice(lits)] = rng.choice(ODD)
        else:
            names = [n for n, t in enumerate(toks) if t in SWAP]
            if names:
                n = rng.choice(names)
                toks[n] = SWAP[toks[n]]
    return " ".join(toks)


def wrap(rng, sql):
    k = rng.randrange(16)
    if k == 0:
        return [f"EXPLAIN {sql}"]
    if k == 1:
        return [f"EXPLAIN (ANALYZE, BUFFERS) {sql}"]
    if k == 2:
        return [f"EXPLAIN (VERBOSE, FORMAT JSON) {sql}"]
    if k == 3:
        return [f"PREPARE fz AS {sql}"] + ["EXECUTE fz"] * rng.choice([1, 2, 7]) + ["EXPLAIN EXECUTE fz", "DEALLOCATE fz"]
    if k == 4:
        return ["BEGIN", f"DECLARE fzc {rng.choice(['', 'SCROLL', 'NO SCROLL'])} CURSOR {rng.choice(['', 'WITH HOLD'])} FOR {sql}",
                "FETCH 3 FROM fzc", rng.choice(["FETCH ALL FROM fzc", "MOVE BACKWARD 1 FROM fzc", "CLOSE fzc", "SELECT 1/0"]),
                rng.choice(["COMMIT", "ROLLBACK"]), "ROLLBACK"]
    if k == 5:
        return [f"SELECT count(*) FROM ({sql}) fzq"]
    if k == 6:
        return [f"WITH fzw AS ({sql}) SELECT * FROM fzw"]
    if k == 7:
        return [f"({sql}) UNION ALL ({sql})"]
    if k == 8:
        return ["BEGIN", "SAVEPOINT fzs", sql, rng.choice(["ROLLBACK TO fzs", "RELEASE fzs"]), sql, "SAVEPOINT fzs2", "COMMIT", "ROLLBACK"]
    if k == 9:
        return ["DO $fz$ BEGIN PERFORM 1 FROM (" + sql + ") q; EXCEPTION WHEN OTHERS THEN NULL; END $fz$"]
    if k == 10:
        return ["BEGIN", sql, sql, rng.choice(["COMMIT", "ROLLBACK"])]
    if k == 11:
        return ["BEGIN", "EXPLAIN SELECT 1", sql, "COMMIT", "ROLLBACK"]
    if k == 12:
        return [f"CREATE TEMP TABLE fzt AS {sql}", "DROP TABLE IF EXISTS fzt"]
    if k == 13:
        return [f"COPY ({sql}) TO STDOUT"]
    return [sql]


def chaos(rng, c):
    """What a sidecar, a DBA and a deploy do to the registry while statements run."""
    k = rng.randrange(9)
    try:
        if k == 0:
            c.execute("UPDATE quicksilver.mirrors SET enabled = NOT enabled WHERE heap = 'public.customers'::regclass")
        elif k == 1:
            c.execute("SELECT quicksilver.report_applied_lsn(heap, '0/10') FROM quicksilver.mirrors WHERE heap = 'ex.orders'::regclass")
        elif k == 2:
            c.execute("SELECT quicksilver.report_applied_lsn(heap, pg_current_wal_lsn()) FROM quicksilver.mirrors")
        elif k == 3:
            c.execute("DELETE FROM quicksilver.allowlist WHERE kind = 'operator' AND ident = '=(integer,integer)'")
        elif k == 4:
            c.execute("INSERT INTO quicksilver.allowlist (kind, ident, options, verified_by) VALUES "
                      "('operator', '=(integer,integer)', '{cmp}', 'docs/40 equality harness, pg_duckdb 1.2.0') ON CONFLICT DO NOTHING")
        elif k == 5:
            c.execute("ANALYZE ex.t_num")
        elif k == 6:
            c.execute("ALTER TABLE ex.t_doc ADD COLUMN IF NOT EXISTS fz_extra int")
        elif k == 7:
            c.execute("ALTER TABLE ex.t_doc DROP COLUMN IF EXISTS fz_extra")
        else:
            c.execute("SELECT quicksilver.reset()")
    except psycopg.Error:
        pass


def connect(port):
    c = psycopg.connect(f"host=/tmp port={port} dbname=app user=postgres", autocommit=True)
    c.execute("SET search_path = ex, public")
    c.execute("SET statement_timeout = '20s'")
    return c


def rows_of(cur):
    r = cur.pgresult
    if r is None or r.nfields == 0 and r.ntuples == 0:
        return None
    return sorted(tuple((r.get_value(j, i) is None, r.get_value(j, i) or b"") for i in range(r.nfields)) for j in range(r.ntuples))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=int(os.environ.get("PPORT", 5541)))
    ap.add_argument("--n", type=int, default=6000)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--log", required=True)
    args = ap.parse_args()
    rng = random.Random(args.seed)
    corpus = []
    cdir = os.path.join(REPO, "bench", "exactness", "corpus")
    for f in sorted(os.listdir(cdir)):
        if f.endswith(".sql"):
            corpus += [s for _, s in load(os.path.join(cdir, f))]
    for f in ("corpus_own.sql", "corpus_seed.sql"):
        corpus += [s for _, s in load(os.path.join(HERE, f))]
    log_start = os.path.getsize(args.log)

    fz, heap, admin = connect(args.port), connect(args.port), connect(args.port)
    heap.execute("SET quicksilver.route = heap")
    for s0 in ("SET quicksilver.route = auto", "SET quicksilver.min_cost = 0", "SET quicksilver.min_rows = 0"):
        fz.execute(s0)      # the tables are small: without this nearly everything is "cheap on the heap"
    n = {"sent": 0, "errors": 0, "ok": 0, "mirror": 0, "compared": 0, "different": 0, "lost": 0}
    diffs = []
    errs = collections.Counter()
    unsafe = lost = False
    for it in range(args.n):
        if rng.random() < 0.25:
            name, vals = rng.choice(SETTINGS)
            v = rng.choice(vals)
            try:
                fz.execute(f"SET {name} = {v}")
                if name == "quicksilver.allow_unsafe":
                    unsafe = v == "on"
                if name == "quicksilver.tolerate_lost_errors":
                    lost = v == "on"
            except psycopg.Error:
                pass
        if rng.random() < 0.05:
            chaos(rng, admin)
        base = rng.choice(corpus)
        stmts = wrap(rng, mutate(rng, base) if rng.random() < 0.7 else base)
        for sql in stmts:
            n["sent"] += 1
            params = [rng.choice([1, None, "x", 2 ** 40, 0.5])] * sql.count("$1") if "$1" in sql else None
            try:
                fz.execute("SELECT quicksilver.forget_last()")
                if sql.startswith("COPY "):
                    with fz.cursor().copy(sql.encode()) as cp:
                        for _ in cp:
                            pass
                    got = None
                else:
                    cur = fz.execute(sql.encode(), params, prepare=rng.choice([None, True, False]) if params else None)
                    got = rows_of(cur)
                n["ok"] += 1
                d = fz.execute("SELECT side FROM quicksilver.last_decision()").fetchone()
                if d and d[0] == "mirror":
                    n["mirror"] += 1
                    plain = len(stmts) == 1 and got is not None and not unsafe and not lost and not sql.lstrip().upper().startswith(("EXPLAIN", "COPY"))
                    if plain:
                        try:
                            want = rows_of(heap.execute(sql.encode(), params))
                            n["compared"] += 1
                            if want != got:
                                n["different"] += 1
                                diffs.append(sql)
                        except psycopg.Error:
                            pass
            except (UnicodeError, ValueError, TypeError):
                n["errors"] += 1            # psycopg's own parsing of a mangled statement
            except psycopg.OperationalError as e:
                if fz.closed or fz.broken:
                    n["lost"] += 1
                    print("CONNECTION LOST after:", sql[:300], "|", str(e)[:200], flush=True)
                    try:
                        fz = connect(args.port)
                    except psycopg.Error:
                        print("server is not accepting connections", flush=True)
                        break
                else:
                    n["errors"] += 1
                    errs["operational: " + str(e)[:70]] += 1
            except psycopg.Error as e:
                n["errors"] += 1
                errs[(e.sqlstate or "?") + " " + re.sub(r"[0-9]+|\"[^\"]*\"", "N", (e.diag.message_primary or str(e)))[:70]] += 1
                try:
                    if fz.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                        fz.execute("ROLLBACK")
                except psycopg.Error:
                    pass
        try:
            if fz.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                fz.execute("ROLLBACK")
        except psycopg.Error:
            pass
        try:
            fz.execute("DEALLOCATE fz")     # a PREPARE wrapper that failed half-way
        except psycopg.Error:
            pass
    for c in (admin,):
        try:
            c.execute("UPDATE quicksilver.mirrors SET enabled = true WHERE NOT enabled")
            c.execute("ALTER TABLE ex.t_doc DROP COLUMN IF EXISTS fz_extra")
            c.execute("INSERT INTO quicksilver.allowlist (kind, ident, options, verified_by) VALUES "
                      "('operator', '=(integer,integer)', '{cmp}', 'docs/40 equality harness, pg_duckdb 1.2.0') ON CONFLICT DO NOTHING")
            c.execute("SELECT quicksilver.report_applied_lsn(heap, pg_current_wal_lsn()) FROM quicksilver.mirrors")
        except psycopg.Error as e:
            print("cleanup:", e)
    with open(args.log, errors="replace") as f:
        f.seek(log_start)
        tail = f.read()
    bad = [l for l in tail.splitlines() if re.search(r"TRAP:|terminated by signal|PANIC:|was terminated|server process .* exited with exit code [^0]", l)]
    warn = [l for l in tail.splitlines() if "WARNING:" in l]
    print(n)
    print("most frequent errors:")
    for k, v in errs.most_common(8):
        print(f"  {v:6d}  {k}")
    print(f"server log: {len(bad)} crash lines, {len(warn)} WARNING lines")
    for l in bad[:10] + warn[:10]:
        print("  ", l[:300])
    for s in diffs[:20]:
        print("DIFFERENT:", s[:300])
    sys.exit(1 if bad or n["lost"] or diffs else 0)


if __name__ == "__main__":
    main()
