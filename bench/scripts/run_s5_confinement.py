#!/usr/bin/env python3
"""Spike S5 — can an ordinary role read the mirror WITHOUT dangerous privileges?

S3 (docs/11 Result 7) found the blocker: stock pg_duckdb gates local file reads
on membership of pg_read_server_files AND pg_write_server_files, and granting
those lets an application role COPY FROM /etc/passwd. A superuser-owned view
does not help — the check is on the current user at plan time.

This validates the proposed fix: a `duckdb.allowed_directories` GUC that
CONFINES unprivileged backends to named directories instead of disabling the
local filesystem outright. Patch in bench/patches/.

The security property under test has three parts, and all three must hold:
  1. the mirror IS readable by a role with only duckdb.postgres_role
  2. nothing OUTSIDE the allowed directory is readable (incl. path traversal)
  3. the confinement CANNOT be widened from SQL afterwards

Requires in postgresql.conf:
    shared_preload_libraries = 'pg_duckdb'
    duckdb.postgres_role     = 'duckdb_users'
    duckdb.allowed_directories = '/var/lib/postgresql/qsbench/parquet'
and a role `analyst` in duckdb_users with SELECT on the events_pq view.
"""
import json
import sys
from pathlib import Path

import psycopg

SUPER = "host=/tmp port=5433 user=postgres dbname=postgres"
ALLOWED = "/var/lib/postgresql/qsbench/parquet"
PQ = f"{ALLOWED}/events.parquet"
PGDATA = "/var/lib/postgresql/qsbench/pgdata"

# (label, sql, expectation)
CASES = [
    ("allow", "the mirror view", "SELECT count(*) FROM events_pq"),
    ("allow", "aggregate over the mirror", "SELECT sum(amount)::text FROM events_pq"),
    ("allow", "repeat query (lock is once-only)", "SELECT count(*) FROM events_pq"),
    ("allow", "direct read_parquet in allowed dir", f"SELECT count(*) FROM read_parquet('{PQ}')"),

    ("deny", "/etc/passwd", "SELECT count(*) FROM read_csv('/etc/passwd')"),
    ("deny", "pg_hba.conf", f"SELECT count(*) FROM read_csv('{PGDATA}/pg_hba.conf')"),
    ("deny", "PG_VERSION", f"SELECT count(*) FROM read_csv('{PGDATA}/PG_VERSION')"),
    ("deny", "path traversal ../pgdata", f"SELECT count(*) FROM read_csv('{ALLOWED}/../pgdata/PG_VERSION')"),
    ("deny", "absolute path outside dir", "SELECT count(*) FROM read_csv('/tmp/../etc/passwd')"),
    ("deny", "http exfiltration", "SELECT count(*) FROM read_csv('https://example.com/x.csv')"),

    # NOTE: `SELECT * FROM duckdb.query($$ SET ... $$)` is NOT a valid widening
    # test — duckdb.query only accepts a single SELECT, so the SET dies in the
    # parser and never reaches the lock. It looks like a pass and proves
    # nothing. Test only vectors a restricted role can actually reach: the
    # Postgres GUCs.
    ("deny", "widen: duckdb.disabled_filesystems", "SET duckdb.disabled_filesystems=''"),
    ("deny", "widen: duckdb.allowed_directories", "SET duckdb.allowed_directories='/'"),
    ("deny", "widen: duckdb.enable_external_access", "SET duckdb.enable_external_access=true"),
]


def main():
    sup = psycopg.connect(SUPER, autocommit=True).cursor()
    sup.execute("GRANT SELECT ON events_pq TO analyst")
    sup.execute("SELECT pg_has_role('analyst','pg_read_server_files','member'),"
                " pg_has_role('analyst','duckdb_users','member')")
    reads_files, duckdb_member = sup.fetchone()
    print(f"\nanalyst: duckdb_users={duckdb_member}  pg_read_server_files={reads_files}")
    print(f"duckdb.allowed_directories = {ALLOWED}\n")
    if reads_files or not duckdb_member:
        sys.exit("precondition failed: analyst must be in duckdb_users and NOT in "
                 "pg_read_server_files, or the test proves nothing")

    conn = psycopg.connect(SUPER.replace("user=postgres", "user=analyst"), autocommit=True)
    cur = conn.cursor()
    results, failures = [], []
    section = None
    for want, label, sql in CASES:
        if want != section:
            print(f"-- must be {want.upper()} --")
            section = want
        try:
            cur.execute(sql)
            rows = cur.fetchall()[:1] if cur.description else "ok"
            got, detail = "ALLOWED", str(rows)[:40]
        except Exception as e:
            got, detail = "DENIED", str(e).splitlines()[0][:60]
        ok = (got == "ALLOWED") == (want == "allow")
        if not ok:
            failures.append(label)
        results.append({"expect": want, "case": label, "got": got, "detail": detail, "pass": ok})
        print(f"  {'OK ' if ok else '!! '}{label:<36}{got:<8}{detail}")

    sup.execute("SELECT count(*) FROM read_csv('/etc/passwd')")
    n = sup.fetchone()[0]
    print(f"\n-- control --\n  OK  superuser unrestricted           ALLOWED ({n},)")

    Path("results").mkdir(exist_ok=True)
    Path("results/s5_confinement.json").write_text(json.dumps(results, indent=2))
    print(f"\n{'ALL PASS — confinement holds' if not failures else 'FAILURES: ' + ', '.join(failures)}")
    print("written: results/s5_confinement.json")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
