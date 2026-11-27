#!/usr/bin/env python3
"""How exactly does the mirror, read through DuckDB, reproduce PostgreSQL?

    sudo python3 bench/exactness/run.py            # everything, then clean up

builds a PostgreSQL 17 primary and standby under /var/lib/postgresql/qsexact
(ports 5500-5509), loads schema.sql + data.sql, mirrors the tables with the real
sidecar (go/cmd/qs-mirror), loads data_special.sql through the replication
stream, waits for go/cmd/qs-verify to say MATCH on every table, creates DuckDB
views from go/cmd/qs-query, runs every statement in corpus/*.sql on both
engines and writes results/results.json and results/REPORT.md.

    --selfcheck   only test the comparison itself (still needs the cluster)
    --keep        leave the cluster running afterwards
    --reuse       use a cluster a previous --keep left behind
    --locale C    initdb with the C locale instead of ICU en-US (CloudNativePG's
                  default); results go to results/locale-C/
    --only ID     run statements whose id starts with ID, print them, write nothing

Must run as root: the servers run as the `postgres` OS user.
"""
from __future__ import annotations

import argparse
import collections
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
import warnings

import duckdb
import psycopg
from psycopg.adapt import Loader

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import compare as C  # noqa: E402
import report  # noqa: E402

PG = "/usr/lib/postgresql/17/bin"
BASE = "/var/lib/postgresql/qsexact"
P_PRIMARY, P_STANDBY, P_HEALTH, P_PROBE = 5500, 5501, 5502, 5503
DB = "app"
DSN = f"postgres://postgres@localhost:{P_PRIMARY}/{DB}"
GO = os.path.abspath(os.path.join(HERE, "..", "..", "go"))
TABLES = ["t_num", "t_str", "t_time", "t_doc", "customers", "orders"]
# Both engines are given the same, deliberately non-UTC, session time zone. A
# UTC-only comparison cannot see a single daylight-saving difference.
TZ = "America/New_York"

warnings.filterwarnings("ignore", category=UserWarning)


# ------------------------------------------------------------------ cluster

def sh(cmd: str, check: bool = True, as_pg: bool = True, quiet: bool = False) -> str:
    argv = ["su", "postgres", "-c", cmd] if as_pg else ["bash", "-c", cmd]
    p = subprocess.run(argv, capture_output=True, text=True)
    if check and p.returncode != 0:
        raise SystemExit(f"FAILED ({p.returncode}): {cmd}\n{p.stdout}\n{p.stderr}")
    if not quiet and p.returncode != 0 and check:
        print(p.stderr, file=sys.stderr)
    return p.stdout


def psql(sql: str = "", db: str = DB, file: str | None = None, port: int = P_PRIMARY) -> str:
    src = f"-f {file}" if file else f"-Atc \"{sql}\""
    return sh(f"{PG}/psql -X -v ON_ERROR_STOP=1 -h {BASE}/sock -p {port} -U postgres -d {db} {src}")


def teardown() -> None:
    kill_sidecars()
    for d in ("standby", "primary"):
        if os.path.isdir(f"{BASE}/{d}"):
            sh(f"{PG}/pg_ctl -D {BASE}/{d} stop -m immediate", check=False)
    shutil.rmtree(BASE, ignore_errors=True)


def build_binaries() -> None:
    os.makedirs(f"{BASE}/bin", exist_ok=True)
    for b in ("qs-mirror", "qs-verify", "qs-query"):
        subprocess.run(["go", "build", "-o", f"{BASE}/bin/{b}", f"./cmd/{b}"], cwd=GO, check=True)
    subprocess.run(["chmod", "-R", "a+rX", f"{BASE}/bin"], check=True)


def start_sidecar(tables: list, slot: str, mirror: str, health: int, log: str) -> subprocess.Popen:
    """The sidecar as the plugin launches it: on the standby, streaming from the
    primary, configured only by environment (see bench/scripts/e2e_mirror.sh)."""
    env = (f"QS_CLUSTER=exact QS_MODE=shadow QS_INGEST=logical "
           f"QS_TABLES={','.join('public.' + t for t in tables)} QS_SLOT={slot} QS_PUBLICATION={slot} "
           f"QS_MIRROR_PATH={mirror} QS_FRESHNESS_SLO=15s "
           f"QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT={P_PRIMARY} "
           f"QS_LOCAL_SOCKET_DIR={BASE}/sock QS_LOCAL_PORT={P_STANDBY} "
           f"QS_DATABASE={DB} QS_PGUSER=postgres QS_POD_NAME=exact-2 "
           f"QS_HEALTH_ADDR=127.0.0.1:{health}")
    sh(f"mkdir -p {mirror}")
    # Detached from our stdio: a sidecar left running by --keep must not hold the
    # caller's pipe open after this script has exited.
    return subprocess.Popen(["su", "postgres", "-c", f"{env} exec {BASE}/bin/qs-mirror >> {log} 2>&1"],
                            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            start_new_session=True)


def ready(port: int) -> bool:
    try:
        return urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=2).status == 200
    except Exception:
        return False


def verify(mirror: str, table: str) -> str:
    return sh(f"{BASE}/bin/qs-verify -dsn {DSN} -mirror {mirror} -table public.{table}", check=False).strip()


def setup(locale: str) -> dict:
    teardown()
    os.makedirs(BASE)
    build_binaries()
    os.makedirs(f"{BASE}/sql")
    for f in ("schema.sql", "data.sql", "data_special.sql", "probes.sql"):
        shutil.copy(os.path.join(HERE, f), f"{BASE}/sql/{f}")
    subprocess.run(["chown", "-R", "postgres:postgres", BASE], check=True)
    sh(f"mkdir -p {BASE}/sock")
    loc = "--locale=C.UTF-8 --locale-provider=icu --icu-locale=en-US" if locale == "icu-en-US" \
        else "--locale=C.UTF-8 --locale-provider=builtin --builtin-locale=C" if locale == "C" \
        else None
    if loc is None:
        raise SystemExit(f"unknown --locale {locale}")
    sh(f"{PG}/initdb -D {BASE}/primary -U postgres -A trust -E UTF8 {loc}")
    with open(f"{BASE}/primary/postgresql.conf", "a") as f:
        f.write(f"port = {P_PRIMARY}\nlisten_addresses = 'localhost'\n"
                f"unix_socket_directories = '{BASE}/sock'\nwal_level = logical\n"
                "max_wal_senders = 10\nmax_replication_slots = 10\nfsync = off\n"
                "timezone = 'UTC'\nshared_buffers = 128MB\n")
    sh(f"{PG}/pg_ctl -D {BASE}/primary -l {BASE}/primary.log -w start")
    psql(f"CREATE DATABASE {DB}", db="postgres")
    psql(file=f"{BASE}/sql/schema.sql")
    psql(file=f"{BASE}/sql/data.sql")
    psql(file=f"{BASE}/sql/probes.sql")
    psql("SELECT pg_create_physical_replication_slot('exact_standby', true)", db="postgres")
    psql("ALTER SYSTEM SET synchronized_standby_slots = 'exact_standby'", db="postgres")
    psql("SELECT pg_reload_conf()", db="postgres")
    sh(f"{PG}/pg_basebackup -D {BASE}/standby -R -X stream -S exact_standby -c fast "
       f"-d 'host={BASE}/sock port={P_PRIMARY} user=postgres dbname=postgres'")
    with open(f"{BASE}/standby/postgresql.auto.conf", "a") as f:
        f.write(f"port = {P_STANDBY}\nsync_replication_slots = on\nhot_standby_feedback = on\n")
    sh(f"{PG}/pg_ctl -D {BASE}/standby -l {BASE}/standby.log -w start")
    if psql("SELECT pg_is_in_recovery()", db="postgres", port=P_STANDBY).strip() != "t":
        raise SystemExit("standby is not in recovery")

    probes = probe_values()

    mirror = f"{BASE}/mirror"
    start_sidecar(TABLES, "qs_exact", mirror, P_HEALTH, f"{BASE}/mirror.log")
    t0 = time.time()
    while not ready(P_HEALTH):
        if time.time() - t0 > 180:
            raise SystemExit("sidecar never became ready:\n" + open(f"{BASE}/mirror.log").read()[-3000:])
        time.sleep(0.5)
    # The special rows go in only now, so they arrive over the stream.
    psql(file=f"{BASE}/sql/data_special.sql")
    verdict, t0 = {}, time.time()
    while True:
        verdict = {t: verify(mirror, t) for t in TABLES}
        if all(v.splitlines()[-1:] == ["MATCH"] or v.startswith("MATCH") for v in verdict.values()):
            break
        if time.time() - t0 > 240:
            raise SystemExit("qs-verify never said MATCH:\n" + json.dumps(verdict, indent=1)
                             + open(f"{BASE}/mirror.log").read()[-3000:])
        time.sleep(1)
    counts = {t: int(psql(f"SELECT count(*) FROM {t}")) for t in TABLES}
    return {"probes": probes, "verify": {t: "MATCH" for t in TABLES}, "rows": counts}


def kill_sidecars() -> None:
    # Anchored: the sidecar is exec'd, so its command line is exactly this path.
    # Other qs-mirror processes on the machine are someone else's.
    subprocess.run(["pkill", "-f", f"^{BASE}/bin/qs-mirror$"], capture_output=True)


def probe_values() -> list:
    """Mirror each probe table on its own; record what the sidecar, qs-verify and
    DuckDB each make of the one value in it, by snapshot and then by stream.

    One sidecar per table: a halt takes the whole mirror out of service, so two
    probes in one sidecar would hide each other."""
    out = []
    pg = Postgres()
    names = [r[0] for r in pg.conn.execute(
        "SELECT relname::text FROM pg_class WHERE relname LIKE 'probe%' AND relkind = 'r' ORDER BY 1").fetchall()]
    for n, name in enumerate(names):
        src = pg.run(f"SELECT v FROM {name} WHERE id = 1")
        rec = {"probe": name, "type": src.raw_types[0], "value": src.rows[0][0], "outcome": "timeout",
               "reason": "", "verify": None, "mirror_type": None, "snapshot_row": None, "stream_row": None}
        mirror, log = f"{BASE}/probe/{name}", f"{BASE}/probe-{name}.log"
        proc = start_sidecar([name], f"qs_p{n}", mirror, P_PROBE, log)

        def wait(nrows: int, secs: float):
            t0 = time.time()
            while time.time() - t0 < secs:
                time.sleep(0.25)
                for st in glob.glob(f"{mirror}/**/state.json", recursive=True):
                    try:
                        if json.load(open(st)).get("halted"):
                            rec["reason"] = json.load(open(st))["halted"]
                            return "halted"
                    except (ValueError, OSError):
                        pass
                text = open(log, errors="replace").read() if os.path.exists(log) else ""
                if "panic:" in text or proc.poll() is not None:
                    m = re.search(r"panic: .*", text)
                    rec["reason"] = (m.group(0) if m else text[-300:]) + " (the sidecar process exited)"
                    return "crashed"
                v = verify(mirror, name)
                if f"mirror {nrows} rows" in v and ("MATCH" in v or "DIVERGED" in v):
                    return v
            return None

        r = wait(1, 45)
        if r in ("halted", "crashed"):
            rec["outcome"] = r
        elif r is not None:
            rec["outcome"] = "mirrored"
            pg.conn.execute(f"INSERT INTO {name} SELECT 2, v FROM {name} WHERE id = 1")
            r2 = wait(2, 45)
            if r2 in ("halted", "crashed"):
                rec["outcome"] = f"mirrored by snapshot, {r2} on the streamed row"
            elif r2 is None:
                rec["outcome"] = "mirrored by snapshot, streamed row never applied"
            else:
                rec["verify"] = "MATCH" if "\nMATCH" in r2 else "DIVERGED"
            try:
                con = duckdb.connect()
                con.execute(sh(f"{BASE}/bin/qs-query -mirror {mirror} -table public.{name} -view {name} -dsn {DSN}"))
                con.execute(f"SET TimeZone = '{TZ}'")
                rel = con.sql(f"SELECT id, v FROM {name} ORDER BY id")
                rec["mirror_type"] = str(rel.types[1])
                rows = dict(C.render_arrow(rel.to_arrow_table(), TZ))
                rec["snapshot_row"], rec["stream_row"] = rows.get("1"), rows.get("2")
            except Exception as e:
                rec["reason"] = f"reading the mirror failed: {type(e).__name__}: {e}"[:300]
        kill_sidecars()
        proc.wait()
        for _ in range(40):     # a halted mirror leaves its slot behind on purpose
            psql("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                 "WHERE slot_name LIKE 'qs_p%' AND NOT active", db="postgres")
            if psql("SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'qs_p%'", db="postgres").strip() == "0":
                break
            time.sleep(0.25)
        out.append(rec)
        print(f"  probe {name:24s} {rec['type']:28s} {str(rec['value'])[:30]:30s} -> {rec['outcome']}"
              f"{' / verify ' + rec['verify'] if rec['verify'] else ''}"
              f"{' / reads ' + repr(rec['snapshot_row']) + ' | ' + repr(rec['stream_row']) if rec['outcome'].startswith('mirrored') else ''}", flush=True)
    pg.conn.close()
    return out


# ------------------------------------------------------------------ engines

class RawText(Loader):
    """Every PostgreSQL value as the text the server sent. No adaptation."""
    def load(self, data):
        return bytes(data).decode("utf-8")


class Postgres:
    def __init__(self):
        self.conn = psycopg.connect(DSN, autocommit=True)
        for (oid,) in self.conn.execute("SELECT oid FROM pg_type").fetchall():
            self.conn.adapters.register_loader(oid, RawText)
        self.conn.execute(f"SET TimeZone = '{TZ}'")
        self.conn.execute("SET statement_timeout = '60s'")
        self._types = {}

    def _type(self, oid: int, mod: int):
        k = (oid, mod)
        if k not in self._types:
            self._types[k] = self.conn.execute(
                "SELECT format_type(%s, %s), coalesce((SELECT e.typtype FROM pg_type t "
                "LEFT JOIN pg_type e ON e.oid = CASE WHEN t.typelem <> 0 AND t.typlen = -1 "
                "THEN t.typelem ELSE t.oid END WHERE t.oid = %s), 'b')",
                (str(oid), str(mod), str(oid))).fetchone()
        return self._types[k]

    def run(self, sql: str) -> C.Result:
        try:
            cur = self.conn.execute(sql.encode())
            cols = [(d.name, d.type_code, cur.pgresult.fmod(i)) for i, d in enumerate(cur.description)]
            rows = cur.fetchall()
        except psycopg.Error as e:
            return C.Result(error=str(e).strip().splitlines()[0])
        r = C.Result(names=[c[0] for c in cols], rows=rows)
        for _, oid, mod in cols:
            fmt, typtype = self._type(oid, mod)
            r.raw_types.append(fmt)
            r.types.append(C.pg_token(fmt, typtype))
        return r


class Mirror:
    """The mirror, as qs-query publishes it to DuckDB."""
    def __init__(self):
        self.con = duckdb.connect()
        self.preamble = []
        for t in TABLES:
            sql = sh(f"{BASE}/bin/qs-query -mirror {BASE}/mirror -table public.{t} -view {t} -dsn {DSN}")
            self.con.execute(sql)
            self.preamble += [l for l in sql.splitlines() if l.startswith("SET ")]
        self.preamble = sorted(set(self.preamble))
        self.con.execute(f"SET TimeZone = '{TZ}'")

    def run(self, sql: str) -> C.Result:
        try:
            rel = self.con.sql(sql)
            names, raw = list(rel.columns), [str(t) for t in rel.types]
            rows = C.render_arrow(rel.to_arrow_table(), TZ)
        except Exception as e:  # duckdb raises a family of unrelated classes
            return C.Result(error=f"{type(e).__name__}: {str(e).strip().splitlines()[0]}"[:400])
        return C.Result(names=names, raw_types=raw, types=[C.duck_token(t) for t in raw], rows=rows)


class Control(Mirror):
    """The same DuckDB over tables that keep the SOURCE's column types, copied
    with DuckDB's postgres scanner. Not something the project ships: it exists
    so that 'the mirror's type mapping did this' is measured, not argued."""
    def __init__(self, preamble: list):
        self.con = duckdb.connect()
        self.con.execute("LOAD postgres_scanner")
        self.con.execute(f"ATTACH 'host=localhost port={P_PRIMARY} user=postgres dbname={DB}' "
                         "AS src (TYPE postgres, READ_ONLY)")
        self.types = {}
        for t in TABLES:
            self.con.execute(f"CREATE TABLE {t} AS SELECT * FROM src.public.{t}")
            if t == "t_doc":    # the scanner hands json and jsonb over as VARCHAR
                self.con.execute("ALTER TABLE t_doc ALTER j TYPE JSON")
                self.con.execute("ALTER TABLE t_doc ALTER jb TYPE JSON")
            self.types[t] = {r[0]: r[1] for r in self.con.execute(f"DESCRIBE {t}").fetchall()}
        self.con.execute("DETACH src")
        for stmt in preamble:
            self.con.execute(stmt)
        self.con.execute(f"SET TimeZone = '{TZ}'")


# ------------------------------------------------------------------- corpus

def is_ordered(sql: str) -> bool:
    """True when the statement's OUTERMOST query block has an ORDER BY."""
    s = re.sub(r"'(?:[^']|'')*'", "''", sql)
    prev = None
    while prev != s:
        prev, s = s, re.sub(r"\([^()]*\)", "()", s)
    return re.search(r"\border\s+by\b", s, re.I) is not None


def load_corpus() -> list:
    out, seen = [], set()
    for path in sorted(glob.glob(os.path.join(HERE, "corpus", "*.sql"))):
        cat = re.sub(r"^\d+_", "", os.path.basename(path)[:-4])
        cur = None
        for line in open(path, encoding="utf-8"):
            m = re.match(r"--\s*@id\s+(\S+)", line)
            if m:
                cur = {"id": m.group(1), "category": cat, "sql": "", "unordered": False}
                if cur["id"] in seen:
                    raise SystemExit(f"duplicate statement id {cur['id']}")
                seen.add(cur["id"])
                out.append(cur)
            elif cur is not None and re.match(r"--\s*@unordered", line):
                cur["unordered"] = True
            elif cur is not None and not line.lstrip().startswith("--"):
                cur["sql"] += line
        for s in out:
            s["sql"] = s["sql"].strip().rstrip(";").strip()
    for s in out:
        if not s["sql"]:
            raise SystemExit(f"statement {s['id']} is empty")
        s["ordered"] = is_ordered(s["sql"]) and not s["unordered"]
    return out


# ---------------------------------------------------------------- self-check

# The same literal, written so both engines parse it. If the DuckDB renderer and
# PostgreSQL's own text output disagree about any of these, every result this
# suite reports is suspect.
RENDER_PAIRS = [
    "SELECT 1::bigint AS a, -7::bigint AS b, NULL::bigint AS c",
    "SELECT 1.50::numeric(10,2) AS a, -0.05::numeric(10,2) AS b, 0::numeric(10,2) AS c",
    "SELECT 12345678901234567890.123456789::numeric(38,9) AS a",
    "SELECT 0.1::double precision AS a, 1e15::double precision AS b, 1e14::double precision AS c, 123456789.12345679::double precision AS d",
    "SELECT 1e-5::double precision AS a, 0.0001::double precision AS b, 1e300::double precision AS c, 5e-324::double precision AS d",
    "SELECT 'NaN'::double precision AS a, 'Infinity'::double precision AS b, '-Infinity'::double precision AS c, '-0'::double precision AS d",
    "SELECT 0.1::real AS a, 1e6::real AS b, 123456.7::real AS c, 1e-5::real AS d, 16777216::real AS e, 3.4028235e38::real AS f",
    "SELECT true AS a, false AS b, NULL::boolean AS c",
    "SELECT 'héllo'::varchar AS a, ''::varchar AS b, ' x '::varchar AS c",
    "SELECT DATE '2024-02-29' AS a, DATE '0001-01-01' AS b, DATE '9999-12-31' AS c, 'infinity'::date AS d, '-infinity'::date AS e",
    "SELECT TIME '00:00:00' AS a, TIME '23:59:59.999999' AS b, TIME '12:00:00.5' AS c",
    "SELECT TIMESTAMP '2024-03-10 02:30:00' AS a, TIMESTAMP '1969-12-31 23:59:59.999999' AS b, TIMESTAMP '0001-01-01 00:00:00' AS c, 'infinity'::timestamp AS d, '-infinity'::timestamp AS e",
    "SELECT TIMESTAMPTZ '2024-03-10 06:59:59+00' AS a, TIMESTAMPTZ '2024-03-10 07:00:00+00' AS b, TIMESTAMPTZ '2024-11-03 05:30:00+00' AS c, TIMESTAMPTZ '2024-11-03 06:30:00+00' AS d",
    "SELECT TIMESTAMPTZ '1800-01-01 00:00:00+00' AS a, TIMESTAMPTZ '2024-07-01 12:00:00.123+00' AS b, 'infinity'::timestamptz AS c",
    "SELECT INTERVAL '1 year 2 months 3 days 04:05:06.789' AS a, INTERVAL '-1 days 2 hours' AS b, INTERVAL '0 seconds' AS c",
    "SELECT INTERVAL '14 months' AS a, INTERVAL '-14 months' AS b, INTERVAL '1 day' AS c, INTERVAL '36 hours' AS d, INTERVAL '-1 second' AS e, INTERVAL '1 month -1 day' AS f",
    "SELECT '00000000-0000-0000-0000-00000000000a'::uuid AS a",
    "SELECT 'abc'::bytea AS a, ''::bytea AS b",
    "SELECT ARRAY[1,2,NULL]::bigint[] AS a, ARRAY['a','b c','', NULL, 'NULL', 'q\"x', 'a,b']::varchar[] AS b",
    "SELECT ARRAY[1.5, 2.25]::numeric(8,2)[] AS a, ARRAY[true,false] AS b, ARRAY[DATE '2024-01-01'] AS c",
    "SELECT ARRAY[TIMESTAMP '2024-01-01 10:00:00'] AS a, ARRAY[1.5::double precision] AS b",
    "SELECT * FROM (VALUES (1::bigint, 'a'::varchar), (2, NULL), (NULL, 'c')) AS v(x, y) ORDER BY x",
]

# (reference, other, ordered, class the comparison MUST return). Both statements
# run on PostgreSQL: these test that a difference is flagged, and as what.
WRONG_PAIRS = [
    ("SELECT 1 AS a", "SELECT 2 AS a", False, C.DIFF_VALUE),
    ("SELECT 1 AS a", "SELECT 1::bigint AS a", False, C.DIFF_TYPE),
    ("SELECT 1 AS a", "SELECT 1 AS b", False, C.DIFF_NAME),
    ("SELECT 1.50::numeric AS a", "SELECT 1.5::numeric AS a", False, C.DIFF_TYPE),
    ("SELECT 1.50::numeric AS a", "SELECT 1.51::numeric AS a", False, C.DIFF_VALUE),
    ("SELECT 2.5::numeric AS a", "SELECT 2.5::float8 AS a", False, C.DIFF_TYPE),
    ("SELECT 0.1::float8 + 0.2::float8 AS a", "SELECT 0.3::float8 AS a", False, C.EXACT),   # noise
    ("SELECT 0.3::float8 AS a", "SELECT 0.3000001::float8 AS a", False, C.DIFF_VALUE),
    ("SELECT 'a'::text AS a", "SELECT 'a '::text AS a", False, C.DIFF_VALUE),
    ("SELECT ''::text AS a", "SELECT NULL::text AS a", False, C.DIFF_VALUE),
    ("SELECT 'a'::text AS a", "SELECT 'A'::text AS a", False, C.DIFF_VALUE),
    ("SELECT x FROM generate_series(1,3) x ORDER BY x", "SELECT x FROM generate_series(1,3) x ORDER BY x DESC", True, C.DIFF_VALUE),
    ("SELECT x FROM generate_series(1,3) x", "SELECT x FROM generate_series(1,3) x ORDER BY x DESC", False, C.EXACT),
    ("SELECT x FROM (VALUES (1),(1),(2)) v(x)", "SELECT x FROM (VALUES (1),(2),(2)) v(x)", False, C.DIFF_VALUE),
    ("SELECT x FROM (VALUES (1),(2)) v(x)", "SELECT x FROM (VALUES (1),(2),(2)) v(x)", False, C.DIFF_VALUE),
    ("SELECT 1 AS a, 2 AS b", "SELECT 1 AS a", False, C.DIFF_VALUE),
    ("SELECT 1 AS a", "SELECT 1/0 AS a", False, C.ERR_MIRROR),
    ("SELECT 1/0 AS a", "SELECT 1 AS a", False, C.ERR_SOURCE),
    ("SELECT 1/0 AS a", "SELECT no_such_fn() AS a", False, C.ERR_BOTH),
    ("SELECT '{\"a\": 1, \"b\": 2}'::jsonb AS a", "SELECT '{\"b\":2,\"a\":1}'::json AS a", False, C.DIFF_TYPE),
    ("SELECT '{\"a\": 1}'::jsonb AS a", "SELECT '{\"a\": 2}'::jsonb AS a", False, C.DIFF_VALUE),
    ("SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid AS a", "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::text AS a", False, C.DIFF_TYPE),
    ("SELECT timestamptz '2024-03-10 07:00:00+00' AS a", "SELECT timestamptz '2024-03-10 06:00:00+00' AS a", False, C.DIFF_VALUE),
    ("SELECT date '2024-03-10' AS a", "SELECT timestamp '2024-03-10 00:00:00' AS a", False, C.DIFF_TYPE),
    ("SELECT interval '1 month' AS a", "SELECT interval '30 days' AS a", False, C.DIFF_VALUE),
    ("SELECT NULL::int AS a", "SELECT 0 AS a", False, C.DIFF_VALUE),
    ("SELECT ARRAY[1,2] AS a", "SELECT ARRAY[2,1] AS a", False, C.DIFF_VALUE),
    ("SELECT true AS a", "SELECT false AS a", False, C.DIFF_VALUE),
]


def selfcheck(pg: Postgres, mirror: Mirror, corpus: list) -> dict:
    fails = []
    # 1. The corpus against itself must be 100% exact. Statements that error are
    #    expected to error identically, which is `error on both`.
    n_exact = n_err = 0
    for s in corpus:
        a, b = pg.run(s["sql"]), pg.run(s["sql"])
        c = C.compare(a, b, s["ordered"])
        if c["class"] == C.EXACT and not c["float_noise"]:
            n_exact += 1
        elif c["class"] == C.ERR_BOTH and a.error == b.error:
            n_err += 1
        else:
            fails.append(f"corpus {s['id']}: PostgreSQL vs itself is {c['class']} {c.get('example')}")
    # 2. Deliberately wrong pairs must be flagged, as the right class.
    for ref, other, ordered, want in WRONG_PAIRS:
        got = C.compare(pg.run(ref), pg.run(other), ordered)["class"]
        if got != want:
            fails.append(f"wrong-pair: {ref!r} vs {other!r}: wanted {want}, got {got}")
    # 3. The DuckDB renderer must agree with PostgreSQL's text output on literals.
    for sql in RENDER_PAIRS:
        a, b = pg.run(sql), mirror.run(sql)
        c = C.compare(a, b, is_ordered(sql))
        strict = (a.rows == b.rows) if not (a.error or b.error) else False
        if c["class"] in (C.DIFF_VALUE, C.ERR_BOTH, C.ERR_MIRROR, C.ERR_SOURCE) or not strict:
            fails.append(f"render: {sql!r}: {c['class']} source={a.rows or a.error} mirror={b.rows or b.error}")
    return {"corpus_statements": len(corpus), "corpus_exact_vs_self": n_exact,
            "corpus_same_error_vs_self": n_err, "wrong_pairs": len(WRONG_PAIRS),
            "render_pairs": len(RENDER_PAIRS), "failures": fails, "passed": not fails}


# ---------------------------------------------------------------------- main

# Settings DuckDB already has that move it towards PostgreSQL, beyond the one
# qs-query recommends. Measured as a second pass so the report can say what
# recommending them would buy; the headline is WITHOUT them.
EXTRA_SETTINGS = ["SET integer_division = true;"]


def run_corpus(pg: Postgres, mirror: Mirror, control, tuned, corpus: list) -> list:
    out = []
    for s in corpus:
        a, b = pg.run(s["sql"]), mirror.run(s["sql"])
        c = C.compare(a, b, s["ordered"])
        rec = {"id": s["id"], "category": s["category"], "sql": s["sql"], **c}
        rec["tuned_class"] = C.compare(a, tuned.run(s["sql"]), s["ordered"])["class"]
        if control is not None:
            k = control.run(s["sql"])
            kc = C.compare(a, k, s["ordered"])
            rec["control_class"] = kc["class"]
            if kc.get("example"):
                rec["control_example"] = kc["example"]
            if k.error:
                rec["control_error"] = k.error
        if a.error:
            rec["error_source"] = a.error
        if b.error:
            rec["error_mirror"] = b.error
        if not a.error:
            rec["source_types"] = a.raw_types
        if not b.error:
            rec["mirror_types"] = b.raw_types
        out.append(rec)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--selfcheck", action="store_true")
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--reuse", action="store_true")
    ap.add_argument("--locale", default="icu-en-US", choices=["icu-en-US", "C"])
    ap.add_argument("--only")
    args = ap.parse_args()
    if os.geteuid() != 0:
        raise SystemExit("run as root (the servers run as the postgres OS user)")
    if hasattr(os, "sched_setaffinity") and os.environ.get("QS_EXACT_CPU", "3") != "":
        try:
            os.sched_setaffinity(0, {int(os.environ.get("QS_EXACT_CPU", "3"))})
        except (OSError, ValueError):
            pass

    corpus = load_corpus()
    outdir = os.path.join(HERE, "results") if args.locale == "icu-en-US" \
        else os.path.join(HERE, "results", "locale-" + args.locale)
    state_file = f"{BASE}/setup.json"
    rc = 0
    try:
        if args.reuse:
            info = json.load(open(state_file))
        else:
            print(f"building the cluster and the mirror under {BASE} (locale {args.locale}) ...", flush=True)
            info = setup(args.locale)
            info["locale"] = args.locale
            json.dump(info, open(state_file, "w"))
        pg, mirror = Postgres(), Mirror()
        try:
            control = Control(mirror.preamble)
        except Exception as e:  # no scanner extension, no network to fetch it
            print(f"typed control unavailable ({type(e).__name__}: {e}); families will not be attributed")
            control = None

        if args.only:
            for s in corpus:
                if s["id"].startswith(args.only):
                    a, b = pg.run(s["sql"]), mirror.run(s["sql"])
                    c = C.compare(a, b, s["ordered"])
                    print(f"\n== {s['id']}  [{c['class']}{' / ' + c['sub'] if c.get('sub') else ''}]\n{s['sql']}")
                    print("  source:", a.error or list(zip(a.names, a.raw_types)))
                    print("  mirror:", b.error or list(zip(b.names, b.raw_types)))
                    if control is not None:
                        k = control.run(s["sql"])
                        print("  control:", C.compare(a, k, s["ordered"])["class"], k.error or list(zip(k.names, k.raw_types)))
                        for row in k.rows[:4]:
                            print(f"    control: {row}")
                    for label, r in (("source", a), ("mirror", b)):
                        for row in r.rows[:8]:
                            print(f"    {label}: {row}")
                    print("  example:", c.get("example"))
            return 0

        check = selfcheck(pg, mirror, corpus)
        print(f"self-check: corpus vs itself {check['corpus_exact_vs_self']} exact + "
              f"{check['corpus_same_error_vs_self']} identical errors of {check['corpus_statements']}; "
              f"{check['wrong_pairs']} wrong pairs; {check['render_pairs']} render pairs; "
              f"{'PASS' if check['passed'] else 'FAIL'}", flush=True)
        for f in check["failures"]:
            print("  FAIL:", f)
        if not check["passed"]:
            rc = 1
        if args.selfcheck:
            return rc

        tuned = Mirror()
        for stmt in EXTRA_SETTINGS:
            tuned.con.execute(stmt)
        stmts = run_corpus(pg, mirror, control, tuned, corpus)
        meta = {
            "postgres": pg.conn.execute("SHOW server_version").fetchone()[0],
            "duckdb": duckdb.__version__,
            "locale": info["locale"],
            "collation": pg.conn.execute(
                "SELECT datlocprovider::text || ':' || coalesce(datlocale, datcollate) FROM pg_database "
                "WHERE datname = current_database()").fetchone()[0],
            "session_timezone": TZ,
            "duckdb_settings": mirror.preamble + [f"SET TimeZone = '{TZ}';"],
            "float_rtol": C.FLOAT_RTOL,
            "extra_settings": EXTRA_SETTINGS,
            "tables": info["rows"], "verify": info["verify"],
            "mirror_types": {t: {r[0]: r[1] for r in mirror.con.execute(f"DESCRIBE {t}").fetchall()} for t in TABLES},
            "control_types": control.types if control is not None else None,
            "statements": len(stmts),
        }
        os.makedirs(outdir, exist_ok=True)
        doc = report.build(meta, check, info["probes"], stmts, os.path.join(HERE, "root_causes.tsv"))
        with open(os.path.join(outdir, "results.json"), "w") as f:
            json.dump(doc, f, indent=1, ensure_ascii=False)
            f.write("\n")
        with open(os.path.join(outdir, "probes.json"), "w") as f:
            json.dump(info["probes"], f, indent=1, ensure_ascii=False)
            f.write("\n")
        with open(os.path.join(outdir, "REPORT.md"), "w") as f:
            f.write(report.markdown(doc))
        print(report.headline(doc))
        print(f"wrote {outdir}/results.json, probes.json, REPORT.md")
        return rc
    finally:
        if not (args.keep or args.reuse):
            teardown()


if __name__ == "__main__":
    sys.exit(main())
