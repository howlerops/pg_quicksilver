#!/usr/bin/env python3
"""Random transaction scripts, each run twice: with the router off and with it on.

    python3 validate/real/txfuzz.py [--port 5580] [--scripts 2000] [--seed 1]

A script is a sequence of statements over one connection: reads of mirrored
tables that the router may answer from the mirror, reads it never will,
writes, BEGIN in every isolation level and READ ONLY, SAVEPOINT / RELEASE /
ROLLBACK TO (existing or not), EXPLAIN and EXPLAIN ANALYZE, cursors (WITH HOLD
too), SQL-level and protocol-level prepared statements, PL/pgSQL with an
exception block, statements that fail. It is run on a connection with
quicksilver.route = off and then, statement by statement the same, on one with
quicksilver.route = auto (the defaults otherwise; the cost gate is opened so
that the small statements are candidates too).

Every statement must end the same way on both: the same rows, or the same
SQLSTATE and message, and the same transaction status after it. EXPLAIN is
compared by outcome only (its text names the side). The scripts' writes go to
a table of their own per side, and the two tables must be equal at the end.

It runs on the primary, where validate/real/primary_report.sh has vouched for
the lake (nothing writes to the mirrored tables here), so every routable read
is routed unless the session's state forbids it: which is what is under test.
Exit status 0 only if no statement differed and the mirror answered some.
"""
import argparse
import random
import sys

import psycopg
from psycopg import pq

ap = argparse.ArgumentParser()
ap.add_argument("--port", type=int, default=5580)
ap.add_argument("--host", default="/tmp")
ap.add_argument("--db", default="app")
ap.add_argument("--user", default="postgres")
ap.add_argument("--scripts", type=int, default=2000)
ap.add_argument("--seed", type=int, default=1)
ap.add_argument("--verbose", action="store_true")
args = ap.parse_args()
DSN = f"host={args.host} port={args.port} dbname={args.db} user={args.user}"


def reads(rng):
    """(sql, params or None): statements over mirrored tables that the allowlist admits."""
    k = rng.randint(1, 60)
    return rng.choice([
        ("SELECT count(*), sum(amount) FROM public.orders WHERE customer_id = %s", (k,)),
        (f"SELECT count(*), sum(amount) FROM public.orders WHERE customer_id = {k}", None),
        (f"SELECT customer_id, count(*) FROM public.orders WHERE customer_id > 1 AND customer_id <= {k} GROUP BY customer_id ORDER BY customer_id", None),
        (f"SELECT id, amount FROM public.orders WHERE customer_id = {k + 1} ORDER BY id LIMIT {rng.randint(1, 80)}", None),
        ("SELECT count(*), min(id), max(id) FROM public.customers WHERE id <= %s", (k * 100,)),
        (f"SELECT c.region, count(*), sum(o.amount) FROM public.orders o JOIN public.customers c ON c.id = o.customer_id WHERE o.customer_id BETWEEN {k + 1} AND {k + 9} GROUP BY c.region ORDER BY 2, 3", None),
        (f"SELECT id, customer_id, amount FROM public.orders WHERE customer_id BETWEEN 2 AND {k + 2} ORDER BY id", None),
    ] + ([("SELECT count(*), min(id), max(id), sum(amount) FROM public.orders", None)] if rng.random() < 0.15 else []))


def gen(rng, w):
    """One script: a list of (kind, sql, params, prepare)."""
    out = []
    add = lambda kind, sql, params=None, prepare=None: out.append((kind, sql, params, prepare))
    sp = 0
    cur = 0
    prep = 0
    for _ in range(rng.randint(4, 22)):
        r = rng.random()
        if r < 0.30:
            sql, params = reads(rng)
            add("q", sql, params, rng.choice([None, None, True]) if params else None)
        elif r < 0.36:
            add("q", rng.choice([
                f"SELECT count(*), coalesce(sum(i), 0) FROM {w}",
                f"SELECT i FROM {w} ORDER BY i LIMIT 5",
                f"SELECT count(*) FROM public.customers c JOIN {w} x ON x.i = c.id",
                "SELECT 1",
                f"SELECT fz.total({rng.randint(2, 40)})",
                f"SELECT fz.guarded({rng.randint(2, 40)})",
            ]))
        elif r < 0.46:
            k = rng.randint(1, 30)
            add("x", rng.choice([
                f"INSERT INTO {w} VALUES ({k})",
                f"INSERT INTO {w} SELECT g FROM generate_series({k}, {k + 3}) g",
                f"UPDATE {w} SET i = i + 100 WHERE i = {k}",
                f"DELETE FROM {w} WHERE i = {k}",
                f"SELECT i FROM {w} WHERE i = {k} FOR UPDATE",
            ]))
        elif r < 0.56:
            add("x", rng.choice([
                "BEGIN", "BEGIN", "BEGIN", "BEGIN READ ONLY", "BEGIN ISOLATION LEVEL REPEATABLE READ",
                "BEGIN ISOLATION LEVEL SERIALIZABLE", "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
                "START TRANSACTION ISOLATION LEVEL READ COMMITTED",
            ]))
        elif r < 0.63:
            add("x", rng.choice(["COMMIT", "COMMIT", "ROLLBACK", "END"]))
        elif r < 0.71:
            sp += 1
            add("x", f"SAVEPOINT s{rng.randint(1, 3)}")
        elif r < 0.77:
            add("x", rng.choice([f"RELEASE s{rng.randint(1, 3)}", f"ROLLBACK TO s{rng.randint(1, 3)}",
                                 f"ROLLBACK TO SAVEPOINT s{rng.randint(1, 3)}"]))
        elif r < 0.83:
            sql, params = reads(rng)
            if params:
                sql = sql.replace("%s", str(params[0]))
            add("e", rng.choice(["EXPLAIN ", "EXPLAIN (COSTS OFF) ", "EXPLAIN ANALYZE ", "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ",
                                 "EXPLAIN (ANALYZE, VERBOSE) "]) + sql)
        elif r < 0.90:
            c = rng.randint(1, 2)
            sql, params = reads(rng)
            if params:
                sql = sql.replace("%s", str(params[0]))
            add("x", rng.choice([
                f"DECLARE c{c} CURSOR FOR {sql}", f"DECLARE c{c} CURSOR FOR {sql}",
                f"DECLARE c{c} CURSOR WITH HOLD FOR {sql}", f"DECLARE c{c} NO SCROLL CURSOR FOR {sql}",
                f"DECLARE c{c} SCROLL CURSOR FOR {sql}",
            ]))
            cur += 1
        elif r < 0.94:
            c = rng.randint(1, 2)
            add("q", rng.choice([f"FETCH {rng.randint(1, 30)} FROM c{c}", f"FETCH ALL FROM c{c}", f"FETCH NEXT FROM c{c}",
                                 f"FETCH BACKWARD 2 FROM c{c}"]))
            if rng.random() < 0.3:
                add("x", f"CLOSE c{c}")
        elif r < 0.97:
            p = rng.randint(1, 2)
            add("x", rng.choice([
                f"PREPARE p{p}(int) AS SELECT count(*), sum(amount) FROM public.orders WHERE customer_id = $1",
                f"PREPARE p{p}(int) AS SELECT id, amount FROM public.orders WHERE customer_id = $1 ORDER BY id LIMIT 7",
                f"PREPARE p{p} AS SELECT count(*), sum(amount) FROM public.orders WHERE customer_id = 7",
            ]))
            add("q", f"EXECUTE p{p}" + ("" if rng.random() < 0.34 else f"({rng.randint(1, 50)})"))
            prep += 1
        else:
            add("q", rng.choice([
                "SELECT 1/0", "SELECT * FROM fz.no_such_table",
                "DO $$ BEGIN BEGIN PERFORM 1/0; EXCEPTION WHEN OTHERS THEN NULL; END; END $$",
                "SET LOCAL work_mem = '64kB'", "SELECT count(*) FROM public.orders WHERE customer_id = 'x'",
            ]))
    return out


def run(conn, script):
    """Run a script; return one comparable outcome per statement."""
    res = []
    for kind, sql, params, prepare in script:
        try:
            cur = conn.execute(sql, params, prepare=prepare)
            if kind == "e":
                rows = ("explained",)
            elif cur.description is None:
                rows = (cur.statusmessage,)
            else:
                rows = cur.fetchall()
                if " ORDER BY " not in sql.upper() and not sql.startswith(("FETCH", "EXECUTE")):
                    rows = sorted(rows, key=repr)
                rows = (tuple(d.name for d in cur.description), tuple(d.type_code for d in cur.description), rows)
            res.append(("ok", rows, conn.info.transaction_status.name))
        except psycopg.Error as e:
            res.append(("error", e.sqlstate, (e.diag.message_primary or "")[:200], conn.info.transaction_status.name))
            if conn.broken:
                res.append(("CONNECTION LOST",))
                return res
    return res


def finish(conn, w):
    """End whatever the script left open; return its table, then empty it."""
    if conn.broken:
        return None
    for s in ("ROLLBACK", "CLOSE ALL", "DEALLOCATE PREPARE p1", "DEALLOCATE PREPARE p2"):
        try:
            conn.execute(s)
        except psycopg.Error:
            pass
    rows = conn.execute(f"SELECT i FROM {w} ORDER BY i").fetchall()
    conn.execute(f"TRUNCATE {w}")
    return rows


def routed(conn):
    return conn.execute("SELECT coalesce(sum(statements), 0) FROM quicksilver.reason_stats WHERE reason = 'routed'").fetchone()[0]


setup = psycopg.connect(DSN, autocommit=True)
setup.execute("SET client_min_messages = warning")
setup.execute("DROP SCHEMA IF EXISTS fz CASCADE")
setup.execute("CREATE SCHEMA fz")
setup.execute("CREATE TABLE fz.w_off (i int)")
setup.execute("CREATE TABLE fz.w_on (i int)")
setup.execute("CREATE FUNCTION fz.total(int) RETURNS numeric LANGUAGE sql STABLE AS "
              "$$ SELECT sum(amount) FROM public.orders WHERE customer_id = $1 $$")
setup.execute("CREATE FUNCTION fz.guarded(int) RETURNS bigint LANGUAGE plpgsql AS $$ DECLARE n bigint; BEGIN "
              "BEGIN SELECT count(*) INTO n FROM public.orders WHERE customer_id = $1; PERFORM 1/(n - n); "
              "EXCEPTION WHEN division_by_zero THEN RETURN n; END; RETURN -1; END $$")
cols = [r[0] for r in setup.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'quicksilver' AND table_name = 'reason_stats'")]
if "statements" not in cols:
    sys.exit(f"quicksilver.reason_stats has columns {cols}")

off = psycopg.connect(DSN, autocommit=True)
on = psycopg.connect(DSN, autocommit=True)
off.execute("SET quicksilver.route = off")
for s in ("SET quicksilver.route = auto", "SET quicksilver.min_cost = 0", "SET quicksilver.min_rows = 0"):
    on.execute(s)
for c in (off, on):
    c.execute("SET client_min_messages = warning")
    c.execute("SET max_parallel_workers_per_gather = 0")

rng = random.Random(args.seed)
before = routed(setup)
n_stmt = n_diff = n_err = n_lost = 0
kinds = {}
for i in range(args.scripts):
    seed = rng.getrandbits(48)
    a = run(off, gen(random.Random(seed), "fz.w_off"))
    b = run(on, gen(random.Random(seed), "fz.w_on"))
    script = gen(random.Random(seed), "fz.w")
    n_stmt += len(script)
    n_err += sum(1 for r in a if r[0] == "error")
    if a != b or ("CONNECTION LOST",) in a + b:
        n_diff += 1
        n_lost += ("CONNECTION LOST",) in a + b
        if n_diff <= 5 or args.verbose:
            print(f"DIFFERENT: script {i}, seed {seed}")
            for (kind, sql, params, prepare), ra, rb in zip(script, a + [None] * 99, b + [None] * 99):
                mark = "   " if ra == rb else ">>>"
                print(f"  {mark} {sql} {params or ''}")
                if ra != rb:
                    print(f"        off: {str(ra)[:300]}\n        on:  {str(rb)[:300]}")
        if off.broken or on.broken:
            break
    ta = finish(off, "fz.w_off")
    tb = finish(on, "fz.w_on")
    if ta != tb:
        n_diff += 1
        print(f"DIFFERENT TABLE after script {i}, seed {seed}")
    for kind, sql, _, _ in script:
        word = sql.split()[0] + (" ANALYZE" if "ANALYZE" in sql.split("SELECT")[0] else "")
        kinds[word] = kinds.get(word, 0) + 1
    if (i + 1) % 500 == 0:
        print(f"  {i + 1} scripts, {n_stmt} statements, {routed(setup) - before} answered by the mirror, {n_diff} different", flush=True)

answered = routed(setup) - before
print("statements by first word: " + ", ".join(f"{k} {v}" for k, v in sorted(kinds.items(), key=lambda kv: -kv[1])))
print(f"scripts: {args.scripts}   statements: {n_stmt}   errors (the same on both sides): {n_err}   "
      f"answered by the mirror with the router on: {answered}")
print(f"scripts that differed: {n_diff}   connections lost: {n_lost}")
setup.execute("DROP SCHEMA fz CASCADE")
sys.exit(0 if n_diff == 0 and answered > 0 else 1)
