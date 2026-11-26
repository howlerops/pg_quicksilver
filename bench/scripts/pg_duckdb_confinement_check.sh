#!/usr/bin/env bash
# Check, case by case, what an unprivileged role can reach through pg_duckdb.
#
# Usage:  pg_duckdb_confinement_check.sh <pg_bin_dir> <port>
#
#   <pg_bin_dir>  directory holding psql (for example .../17/bin)
#   <port>        port of a running server with pg_duckdb loaded
#
# Environment (optional):
#   APP_ROLE       ordinary role under test (default: app)
#   DUCKDB_ROLE    role named by duckdb.postgres_role (default: duckdb_users)
#   LAKE_DIR       directory listed in duckdb.allowed_directories (default:
#                  /var/lib/postgresql/qspgd/lake)
#   OUTSIDE_DIR    directory NOT listed (default: /var/lib/postgresql/qspgd/outside)
#
# The server must already have, set before it started:
#   shared_preload_libraries = 'pg_duckdb'
#   duckdb.postgres_role     = '<DUCKDB_ROLE>'
#   duckdb.allowed_directories = '<LAKE_DIR>/'
# This script creates the roles and the test files, and checks those settings.
# It does not change any setting.
#
# Each case prints one line:  CASE <id> | expect | observed | first line of the error
# The script exits 0 only when every case matches its expectation. Run against
# stock pg_duckdb 1.2.2 it exits 1: case a is then DENIED, which is the reason
# the patch exists.
#
# Connections use the server's authentication (trust on loopback in the private
# cluster). The superuser for setup is the role 'postgres'.
set -uo pipefail

if [ $# -ne 2 ]; then
	echo "usage: $0 <pg_bin_dir> <port>" >&2
	exit 2
fi
PG_BIN=$1
PORT=$2
APP_ROLE=${APP_ROLE:-app}
DUCKDB_ROLE=${DUCKDB_ROLE:-duckdb_users}
LAKE_DIR=${LAKE_DIR:-/var/lib/postgresql/qspgd/lake}
OUTSIDE_DIR=${OUTSIDE_DIR:-/var/lib/postgresql/qspgd/outside}
PSQL="$PG_BIN/psql -X -q -h 127.0.0.1 -p $PORT -v ON_ERROR_STOP=1 -d postgres"

fail=0
note() { printf '%s\n' "$*"; }

# Setup: roles, then the files. Idempotent.
$PSQL -U postgres -c "DO \$\$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '$DUCKDB_ROLE') THEN
    CREATE ROLE $DUCKDB_ROLE NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '$APP_ROLE') THEN
    CREATE ROLE $APP_ROLE LOGIN NOSUPERUSER;
  END IF;
END \$\$;
GRANT $DUCKDB_ROLE TO $APP_ROLE;" || { note "SETUP FAILED: roles"; exit 2; }

# The settings the check depends on, read back from the server.
settings=$($PSQL -U postgres -Atc "SELECT name || '=' || setting FROM pg_settings WHERE name IN ('duckdb.postgres_role','duckdb.allowed_directories')" 2>&1)
case "$settings" in
*"duckdb.postgres_role=$DUCKDB_ROLE"*) ;;
*) note "SETUP FAILED: duckdb.postgres_role is not '$DUCKDB_ROLE' (got: $settings)"; exit 2 ;;
esac
case "$settings" in
*"duckdb.allowed_directories=$LAKE_DIR/"*|*"duckdb.allowed_directories=$LAKE_DIR"*) ;;
*) note "SETUP FAILED: duckdb.allowed_directories is not '$LAKE_DIR/' (got: $settings)"; exit 2 ;;
esac

mkdir -p "$LAKE_DIR" "$OUTSIDE_DIR"
# The lake file, a file outside, a DuckDB database outside, and a symlink from
# inside the lake to the outside file. Written by python3 duckdb, as the test
# depends on the file format and not on pg_duckdb.
python3 -I - "$LAKE_DIR" "$OUTSIDE_DIR" <<'PY' || { note "SETUP FAILED: test files"; exit 2; }
import os, sys
import duckdb
lake, outside = sys.argv[1], sys.argv[2]
duckdb.sql(f"COPY (SELECT 1 AS a) TO '{lake}/t.parquet' (FORMAT parquet)")
duckdb.sql(f"COPY (SELECT 2 AS a) TO '{outside}/s.parquet' (FORMAT parquet)")
con = duckdb.connect(f"{outside}/x.duckdb")
con.sql("CREATE OR REPLACE TABLE t AS SELECT 3 AS a")
con.close()
link = os.path.join(lake, "link_out.parquet")
if os.path.lexists(link):
    os.unlink(link)
os.symlink(os.path.join(outside, "s.parquet"), link)
for p in (lake + "/t.parquet", outside + "/s.parquet", outside + "/x.duckdb"):
    os.chmod(p, 0o644)
PY

# Run one statement as a role; print SUCCEED or DENIED and the first error line.
# A failure that is not a policy refusal (a connection error, a missing file)
# is reported as ERROR, which never matches an expectation.
POLICY_RE='disabled by configuration|has been locked|permission denied to set|Cannot write|Cannot access file|writes to local files'
run_case() {
	local id=$1 expect=$2 role=$3 sql=$4 out rc observed first
	out=$($PSQL -U "$role" -Atc "$sql" 2>&1)
	rc=$?
	first=$(printf '%s\n' "$out" | grep -m1 -E 'ERROR|FATAL|error' | sed -e 's/^LINE.*//' -e 's/^[[:space:]]*//')
	if [ $rc -eq 0 ]; then
		observed=SUCCEED
		first=$(printf '%s' "$out" | head -1)
		first="rows: $first"
	elif printf '%s' "$out" | grep -q -E "$POLICY_RE"; then
		observed=DENIED
	else
		observed=ERROR
	fi
	local verdict=ok
	if [ "$observed" != "$expect" ]; then
		verdict=MISMATCH
		fail=1
	fi
	printf 'CASE %s | expect %s | observed %s | %s | %s\n' "$id" "$expect" "$observed" "$verdict" "$first"
}

R="SELECT * FROM duckdb.query(\$q\$"
W="SELECT duckdb.raw_query(\$q\$"
note "== pg_duckdb confinement check: role=$APP_ROLE member-of=$DUCKDB_ROLE port=$PORT"
note "== lake=$LAKE_DIR outside=$OUTSIDE_DIR"
run_case a SUCCEED "$APP_ROLE" "$R SELECT a FROM read_parquet('$LAKE_DIR/t.parquet') \$q\$)"
run_case b DENIED "$APP_ROLE" "$R SELECT a FROM read_parquet('$OUTSIDE_DIR/s.parquet') \$q\$)"
run_case c DENIED "$APP_ROLE" "$R SELECT * FROM read_csv('/etc/passwd') \$q\$)"
run_case d DENIED "$APP_ROLE" "$R SELECT a FROM read_parquet('$LAKE_DIR/../outside/s.parquet') \$q\$)"
run_case e DENIED "$APP_ROLE" "$R SELECT a FROM read_parquet('$LAKE_DIR/link_out.parquet') \$q\$)"
run_case f DENIED "$APP_ROLE" "$W COPY (SELECT 1) TO '$LAKE_DIR/new.parquet' \$q\$)"
run_case g DENIED "$APP_ROLE" "$W COPY (SELECT 1) TO '$OUTSIDE_DIR/new.parquet' \$q\$)"
run_case h DENIED "$APP_ROLE" "$W ATTACH '$OUTSIDE_DIR/x.duckdb' AS x \$q\$)"
run_case i DENIED "$APP_ROLE" "$R SELECT * FROM read_parquet('http://127.0.0.1:9/x.parquet') \$q\$)"
run_case j1 DENIED "$APP_ROLE" "SET duckdb.allowed_directories = '/'"
run_case j2 DENIED "$APP_ROLE" "$W SET allowed_directories=['/'] \$q\$)"
run_case j3 DENIED "$APP_ROLE" "$W SET enable_external_access=true \$q\$)"
run_case j4 DENIED "$APP_ROLE" "$W SET lock_configuration=false \$q\$)"
run_case k SUCCEED postgres "$R SELECT a FROM read_parquet('$LAKE_DIR/t.parquet') \$q\$)"

if [ $fail -eq 0 ]; then
	note "== RESULT: PASS (every case matches its expectation)"
else
	note "== RESULT: FAIL (at least one case does not match its expectation)"
fi
exit $fail
