#!/usr/bin/env python3
"""Spike S3 — is Postgres access control enforced when DuckDB executes the scan?

docs/06 section 4 set a hard gate: until RLS and column-privilege enforcement
over pg_duckdb-executed scans is verified by test, refuse to mirror any table
with RLS or non-trivial column grants.

Three phases, because the first result is a trap:

  Phase 1  default config — pg_duckdb refuses non-superusers outright, so every
           "pass" here is vacuous. Recorded to show why the naive test is useless.
  Phase 2  duckdb.postgres_role granted — the real enforcement test.
  Phase 3  can an ordinary role reach the Parquet mirror at all, and at what
           privilege cost? This is the one that decides Architecture A.

Requires: duckdb.postgres_role = 'duckdb_users' in postgresql.conf (postmaster
context, so a restart is needed), and an events_pq view over the mirror.
"""
import json
from pathlib import Path

import psycopg

SUPER = "host=/tmp port=5433 user=postgres dbname=postgres"
PARQUET = "/var/lib/postgresql/qsbench/parquet/events.parquet"
OUT = []


def note(phase, name, observed, verdict, detail=""):
    OUT.append({"phase": phase, "test": name, "observed": observed,
                "verdict": verdict, "detail": detail})
    print(f"  {name:<46}{verdict:<9}{observed[:58]}")
    if detail:
        print(f"      -> {detail}")


def analyst():
    c = psycopg.connect(SUPER.replace("user=postgres", "user=analyst"), autocommit=True)
    return c, c.cursor()


def run(cur, sql, force_duckdb=False):
    try:
        cur.execute(f"SET duckdb.force_execution={'true' if force_duckdb else 'false'}")
        cur.execute(sql)
        return True, cur.fetchall()
    except Exception as e:
        return False, str(e).splitlines()[0][:120]


def setup(s):
    s.execute("DROP TABLE IF EXISTS secure_events CASCADE")
    s.execute("SELECT 1 FROM pg_roles WHERE rolname='analyst'")
    if s.fetchone():
        s.execute("DROP OWNED BY analyst CASCADE")
        s.execute("DROP ROLE analyst")
    s.execute("CREATE ROLE analyst LOGIN PASSWORD 'x'")
    s.execute("CREATE TABLE secure_events(id int primary key, tenant_id int,"
              " amount numeric(10,2), secret text)")
    s.execute("INSERT INTO secure_events SELECT g, 1 + (g % 3), (g*1.5)::numeric(10,2),"
              " 'SECRET-' || g FROM generate_series(1,3000) g")
    s.execute("ALTER TABLE secure_events ENABLE ROW LEVEL SECURITY")
    s.execute("ALTER TABLE secure_events FORCE ROW LEVEL SECURITY")
    s.execute("CREATE POLICY tenant_isolation ON secure_events FOR SELECT TO analyst"
              " USING (tenant_id = 1)")
    s.execute("GRANT SELECT (id, tenant_id, amount) ON secure_events TO analyst")
    s.execute("GRANT USAGE ON SCHEMA public TO analyst")
    s.execute("GRANT SELECT ON events_pq TO analyst")


def main():
    sup = psycopg.connect(SUPER, autocommit=True)
    s = sup.cursor()
    setup(s)
    s.execute("SELECT current_setting('duckdb.postgres_role')")
    role_gate = s.fetchone()[0]
    print(f"\n3000 rows; analyst's RLS policy permits 1000 (tenant_id=1)")
    print(f"duckdb.postgres_role = {role_gate!r}\n")

    # ---- Phase 1: default deny -------------------------------------------
    print("PHASE 1 — before granting duckdb.postgres_role (every pass is vacuous)")
    s.execute("REVOKE duckdb_users FROM analyst")
    c, a = analyst()
    ok, v = run(a, "SELECT count(*) FROM secure_events", force_duckdb=True)
    note("1", "RLS via DuckDB executor", "denied" if not ok else str(v),
         "VACUOUS", "denied because the role gate blocks pg_duckdb entirely, "
                    "not because RLS was enforced")
    c.close()

    # ---- Phase 2: real enforcement test ----------------------------------
    print("\nPHASE 2 — duckdb.postgres_role granted: the real test")
    s.execute("GRANT duckdb_users TO analyst")
    c, a = analyst()
    ok, v = run(a, "SELECT count(*), min(tenant_id), max(tenant_id) FROM secure_events",
                force_duckdb=True)
    if ok:
        n, lo, hi = v[0]
        good = (n == 1000 and lo == 1 and hi == 1)
        note("2", "RLS enforced under DuckDB execution", f"{n} rows, tenant {lo}..{hi}",
             "PASS" if good else "FAIL",
             "policy honoured (superuser sees 3000, tenants 1..3)" if good
             else f"RLS BYPASSED: saw {n} of 3000")
    else:
        note("2", "RLS enforced under DuckDB execution", str(v), "ERROR")

    ok, v = run(a, "SELECT secret FROM secure_events LIMIT 1", force_duckdb=True)
    note("2", "denied column under DuckDB execution",
         f"READ IT: {v}" if ok else "denied", "FAIL" if ok else "PASS")

    # ---- Phase 3: reaching the mirror ------------------------------------
    print("\nPHASE 3 — can an ordinary role reach the Parquet mirror?")
    ok, v = run(a, "SELECT count(*) FROM events_pq")
    note("3", "mirror view (owned by superuser)", f"{v}" if ok else "denied",
         "BLOCKED" if not ok else "ALLOWED",
         "view ownership does NOT help — the file check is on the CURRENT user")
    ok, v = run(a, f"SELECT count(*) FROM read_parquet('{PARQUET}')")
    note("3", "direct read_parquet", f"{v}" if ok else "denied",
         "BLOCKED" if not ok else "ALLOWED")
    c.close()

    # W1 — the grant that makes it work, and what else it buys
    print("\n  workaround W1: grant pg_read_server_files + pg_write_server_files")
    s.execute("GRANT pg_read_server_files, pg_write_server_files TO analyst")
    c, a = analyst()
    ok, v = run(a, "SELECT count(*) FROM events_pq")
    note("3", "W1: mirror view after the grant", f"{v}" if ok else "denied",
         "ALLOWED" if ok else "BLOCKED")
    try:
        a.execute("CREATE TEMP TABLE leak(l text)")
        a.execute("COPY leak FROM '/etc/passwd'")
        a.execute("SELECT count(*) FROM leak")
        note("3", "W1: COPY FROM /etc/passwd", f"{a.fetchone()[0]} lines read", "DANGER",
             "the same grant hands the app role arbitrary server-file read/write")
    except Exception as e:
        note("3", "W1: COPY FROM /etc/passwd", str(e).splitlines()[0][:60], "PASS")
    c.close()
    s.execute("REVOKE pg_read_server_files, pg_write_server_files FROM analyst")

    # W2 — SECURITY DEFINER wrapper
    print("\n  workaround W2: SECURITY DEFINER wrapper function")
    s.execute("ALTER SYSTEM SET duckdb.unsafe_allow_execution_inside_functions=true")
    s.execute("SELECT pg_reload_conf()")
    s.execute("CREATE OR REPLACE FUNCTION mirror_count() RETURNS bigint LANGUAGE sql"
              " SECURITY DEFINER AS $$ SELECT count(*) FROM events_pq $$")
    s.execute("GRANT EXECUTE ON FUNCTION mirror_count() TO analyst")
    c, a = analyst()
    ok, v = run(a, "SELECT mirror_count()")
    note("3", "W2: SECURITY DEFINER wrapper", f"{v}" if ok else "denied",
         "ALLOWED" if ok else "BLOCKED",
         "works, but needs duckdb.unsafe_allow_execution_inside_functions and only "
         "serves FIXED queries — arbitrary SQL (goal G1) is not possible this way")
    ok, v = run(a, "SELECT count(*) FROM events_pq")
    note("3", "W2: direct view still blocked (control)", "denied" if not ok else str(v),
         "PASS" if not ok else "FAIL")
    c.close()

    Path("results").mkdir(exist_ok=True)
    Path("results/s3_security.json").write_text(json.dumps(OUT, indent=2))
    print("\nwritten: results/s3_security.json")
    print("\nRoot cause (pg_duckdb src/pg/permissions.cpp):")
    print("  AllowRawFileAccess() == is_member_of_role(pg_write_server_files)")
    print("                       && is_member_of_role(pg_read_server_files)")


if __name__ == "__main__":
    main()
