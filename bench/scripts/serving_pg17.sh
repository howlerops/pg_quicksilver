#!/usr/bin/env bash
# Answer a SELECT against the mirror THROUGH PostgreSQL 17, as an ordinary role.
#
# Everything else in this repository queries the mirror with DuckDB directly.
# That proves the Parquet is right; it does not prove anyone can reach it from
# the database they already have a connection to. This closes the last item in
# docs/12, and it is the first time the serving path has run on the major
# version the plugin actually requires — the prototype was built against 16.
#
# The topology is the deployed one, not a convenience: the mirror is built by
# the production sidecar on a STANDBY, and the standby is where the query is
# answered, because that is the node the files are on. The view is created on
# the primary and replicates, which is also what would happen in a Cluster.
#
# Two properties, and both must hold or the path is useless:
#
#   1. an UNPRIVILEGED role can read the mirror — no pg_read_server_files, no
#      pg_write_server_files. Stock pg_duckdb gates local reads on holding BOTH,
#      and granting those also grants COPY FROM '/etc/passwd' (docs/11 Result 7).
#   2. that role can read NOTHING ELSE — not PGDATA, not /etc, not by path
#      traversal, not over HTTP, and it cannot widen the confinement from SQL.
#
# Property 1 without property 2 is the privilege escalation the patch exists to
# avoid. Property 2 without property 1 is a mirror nobody can query.
#
# The extension is pg_duckdb v1.1.1 plus
# bench/patches/pg_duckdb-allowed-directories.patch, built against PostgreSQL 17.
# It is NOT upstream; a run that cannot find it exits 2 as INCOMPLETE rather
# than reporting success.
set -uo pipefail

PG=/usr/lib/postgresql/17/bin
BASE=/var/lib/postgresql/qs-serve
PRIMARY=$BASE/primary
STANDBY=$BASE/standby
MIRROR=$BASE/mirror
PPORT=5455
SPORT=5456
DB=app
# 200k is enough to prove correctness and far too small to say anything about
# speed: at that size everything is in shared_buffers and every query is a
# rounding error. Section 9 is skipped below 1M for exactly that reason.
ROWS=${ROWS:-200000}
GO=$(cd "$(dirname "$0")/../../go" && pwd)
HEALTH=127.0.0.1:9201
FAIL=0

say()  { printf '\n== %s ==\n' "$*"; }
bad()  { printf 'FAIL: %s\n' "$*"; FAIL=1; }
skip() { printf '\nINCOMPLETE — section skipped, which is NOT a pass: %s\n' "$*"; exit 2; }
# psq <port> <sql> [user] [db]
psq()  { su postgres -c "$PG/psql -h /tmp -p $1 -U ${3:-postgres} -d ${4:-$DB} -Atc \"$2\"" 2>&1; }

cleanup() {
  pkill -x qs-mirror 2>/dev/null
  su postgres -c "$PG/pg_ctl -D $STANDBY -m immediate stop" >/dev/null 2>&1
  su postgres -c "$PG/pg_ctl -D $PRIMARY -m immediate stop" >/dev/null 2>&1
  return 0
}
trap cleanup EXIT

# ---------------------------------------------------------------- preconditions
say "0. the patched extension, for PostgreSQL 17"
# Section 9 is a timing comparison, so the machine is part of the result.
printf 'host: %s vCPU, %s\n' "$(nproc)" "$(awk '/MemTotal/{print $2, $3}' /proc/meminfo)"
printf 'rows: %s\n' "$ROWS"
LIBDIR=$($PG/pg_config --pkglibdir 2>/dev/null) || skip "no PostgreSQL 17 pg_config"
[ -f "$LIBDIR/pg_duckdb.so" ] || skip "no pg_duckdb.so in $LIBDIR — see bench/patches/README.md"

# Checking for the GUC rather than for the file. Stock pg_duckdb has no
# duckdb.allowed_directories at all, so the wrong binary would not fail here —
# it would fail much later as "permission denied" on the mirror, which reads
# exactly like the bug this patch exists to fix.
#
# grep -a on the binary, NOT `strings ... | grep -q`. Under `set -o pipefail`
# that pipeline reports FAILURE on success: grep -q exits at the first match,
# strings takes SIGPIPE, and pipefail returns strings' death as the pipeline's
# status. The check then skips precisely when the patched build IS present,
# which is how this was found.
if ! grep -qa "duckdb.allowed_directories" "$LIBDIR/pg_duckdb.so"; then
  skip "pg_duckdb.so has no duckdb.allowed_directories: stock build, not the patched one"
fi
echo "  $LIBDIR/pg_duckdb.so carries duckdb.allowed_directories"
$PG/pg_config --version

# ---------------------------------------------------------------- primary
say "1. primary on $PPORT, with pg_duckdb preloaded"
cleanup
rm -rf "$BASE"; mkdir -p "$BASE"; chown postgres:postgres "$BASE"
su postgres -c "$PG/initdb -D $PRIMARY -A trust" >/dev/null 2>&1 || skip "initdb failed"

# The confinement directory is named in the config, not discovered, and it is
# PGC_POSTMASTER: it cannot be widened later by anything, including a superuser
# session. That is the property the whole patch rests on.
duck_conf() {
  cat <<EOF
shared_preload_libraries = 'pg_duckdb'
duckdb.postgres_role = 'duckdb_users'
duckdb.allowed_directories = '$MIRROR'
EOF
}
{ echo "port = $PPORT"; echo "listen_addresses = 'localhost'";
  echo "unix_socket_directories = '/tmp'"; echo "wal_level = logical";
  duck_conf; } >> "$PRIMARY/postgresql.conf"

su postgres -c "$PG/pg_ctl -D $PRIMARY -l $BASE/primary.log -w start" >/dev/null 2>&1 \
  || { tail -20 "$BASE/primary.log"; skip "primary would not start with pg_duckdb preloaded"; }

psq $PPORT "CREATE DATABASE $DB" postgres postgres >/dev/null
psq $PPORT "CREATE EXTENSION pg_duckdb" >/dev/null 2>&1 \
  || skip "CREATE EXTENSION pg_duckdb failed: $(psq $PPORT 'CREATE EXTENSION pg_duckdb')"

# note is NULLABLE on purpose, and NULL in exactly THREE rows. Section 8 takes
# the top 5 of an ORDER BY over it, so the two engines cannot both be right:
# PostgreSQL sorts NULLs FIRST on DESC and would return 3 NULLs then 2 values,
# DuckDB sorts them last on both and would return 5 values. Making a third of
# the column NULL, as this did first, returns 5 NULLs under either rule and
# proves nothing.
psq $PPORT "CREATE TABLE events(
              id bigint PRIMARY KEY,
              sku text NOT NULL,
              amount numeric(12,2) NOT NULL,
              note text,
              ts timestamptz NOT NULL)" >/dev/null
psq $PPORT "INSERT INTO events
              SELECT g, 'SKU-'||(g%997), (g%10000)/100.0,
                     CASE WHEN g <= 3 THEN NULL ELSE 'n-'||lpad(g::text,6,'0') END,
                     now() - (g||' seconds')::interval
              FROM generate_series(1,$ROWS) g" >/dev/null

# Deletes and updates before the mirror is built, so the mirror carries a
# deletion vector and column-partial deltas and the published view has to
# RECONSTRUCT rather than scan one file. A serving path that only works on a
# freshly compacted mirror is not a serving path.
psq $PPORT "DELETE FROM events WHERE id % 50 = 0" >/dev/null
psq $PPORT "UPDATE events SET amount = amount + 1 WHERE id % 37 = 0" >/dev/null
echo "  seeded $(psq $PPORT 'SELECT count(*) FROM events') live rows"

# ---------------------------------------------------------------- standby
say "2. standby on $SPORT"
psq $PPORT "SELECT pg_create_physical_replication_slot('serve_standby', true)" postgres postgres >/dev/null
# -c fast: without it pg_basebackup waits on a SPREAD checkpoint, which reads as
# a hang and has been mistaken for one twice in this repository.
su postgres -c "$PG/pg_basebackup -D $STANDBY -R -X stream -S serve_standby -c fast \
  -d 'host=/tmp port=$PPORT user=postgres dbname=postgres'" >/dev/null 2>&1 \
  || skip "pg_basebackup failed"
{ echo "port = $SPORT"; echo "unix_socket_directories = '/tmp'";
  echo "hot_standby_feedback = on"; echo "sync_replication_slots = on"; } \
  >> "$STANDBY/postgresql.conf"
su postgres -c "$PG/pg_ctl -D $STANDBY -l $BASE/standby.log -w start" >/dev/null 2>&1 \
  || { tail -20 "$BASE/standby.log"; skip "standby would not start"; }
[ "$(psq $SPORT 'SELECT pg_is_in_recovery()')" = "t" ] || skip "standby is not in recovery"

# ---------------------------------------------------------------- the mirror
say "3. the production sidecar builds the mirror on the standby"
( cd "$GO" && go build -o "$BASE/qs-mirror" ./cmd/qs-mirror ) || skip "qs-mirror did not build"
( cd "$GO" && go build -o "$BASE/qs-query"  ./cmd/qs-query )  || skip "qs-query did not build"
chown -R postgres:postgres "$BASE"

su postgres -c "QS_CLUSTER=serve QS_MODE=shadow QS_INGEST=logical \
  QS_TABLES=public.events QS_SLOT=qs_serve QS_PUBLICATION=qs_serve \
  QS_MIRROR_PATH=$MIRROR QS_FRESHNESS_SLO=30s \
  QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$PPORT \
  QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=$SPORT \
  QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=serve-2 \
  QS_HEALTH_ADDR=$HEALTH $BASE/qs-mirror" >> "$BASE/mirror.log" 2>&1 &

for _ in $(seq 1 120); do
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://$HEALTH/readyz" 2>/dev/null)
  [ "$code" = "200" ] && break
  sleep 1
done
if [ "${code:-}" != "200" ]; then
  tail -30 "$BASE/mirror.log"
  skip "the sidecar never became ready, so there is no mirror to serve"
fi
echo "  mirror ready: $(ls "$MIRROR" 2>/dev/null | tr '\n' ' ')"

# ---------------------------------------------------------------- the view
say "4. publish the mirror as a view on the primary; it replicates"
# -engine postgres, because the DuckDB view is not valid here: its
# SET default_null_order is not a PostgreSQL GUC, and a bare column reference
# inside read_parquet fails as `column "id" does not exist` through pg_duckdb.
VIEW_SQL=$(su postgres -c "$BASE/qs-query -mirror $MIRROR -table public.events -view events_pq \
  -engine postgres -dsn 'host=/tmp port=$PPORT user=postgres dbname=$DB'" 2>/dev/null) \
  || skip "qs-query produced no view"
printf '%s\n' "$VIEW_SQL" | su postgres -c "$PG/psql -h /tmp -p $PPORT -U postgres -d $DB -q" \
  || skip "the published view would not create"

psq $PPORT "CREATE ROLE duckdb_users" postgres >/dev/null
psq $PPORT "CREATE ROLE analyst LOGIN IN ROLE duckdb_users" postgres >/dev/null
psq $PPORT "GRANT USAGE ON SCHEMA public TO analyst" >/dev/null
psq $PPORT "GRANT SELECT ON events_pq TO analyst" >/dev/null
psq $PPORT "SELECT pg_switch_wal()" postgres postgres >/dev/null
for _ in $(seq 1 60); do
  [ "$(psq $SPORT "SELECT to_regclass('public.events_pq') IS NOT NULL")" = "t" ] && break
  sleep 0.5
done
[ "$(psq $SPORT "SELECT to_regclass('public.events_pq') IS NOT NULL")" = "t" ] \
  || skip "the view never reached the standby"

# ---------------------------------------------------------------- the role
say "5. an ordinary role, with none of the dangerous privileges"
reads=$(psq  $SPORT "SELECT pg_has_role('analyst','pg_read_server_files','member')")
writes=$(psq $SPORT "SELECT pg_has_role('analyst','pg_write_server_files','member')")
member=$(psq $SPORT "SELECT pg_has_role('analyst','duckdb_users','member')")
echo "  analyst: duckdb_users=$member read_server_files=$reads write_server_files=$writes"
[ "$reads" = "f" ] && [ "$writes" = "f" ] && [ "$member" = "t" ] \
  || skip "analyst has the wrong privileges, so nothing below would prove anything"

# ---------------------------------------------------------------- property 1
say "6. the mirror IS readable through PostgreSQL, and AGREES with the source"
src_count=$(psq $PPORT "SELECT count(*) FROM events")
src_sum=$(psq   $PPORT "SELECT sum(amount)::text FROM events")
mir_count=$(psq $SPORT "SELECT count(*) FROM events_pq" analyst)
mir_sum=$(psq   $SPORT "SELECT sum(amount)::text FROM events_pq" analyst)
echo "  source (primary, heap)   : $src_count rows, sum $src_sum"
echo "  mirror (standby, parquet): $mir_count rows, sum $mir_sum"
[ "$src_count" = "$mir_count" ] || bad "row count: mirror $mir_count vs source $src_count"
[ "$src_sum"   = "$mir_sum"   ] || bad "sum: mirror $mir_sum vs source $src_sum"

# Twice on purpose. Confinement LOCKS the DuckDB configuration, and the lock is
# applied once per instance; the second query is the one that used to throw
# "Cannot change configuration option ... the configuration has been locked".
again=$(psq $SPORT "SELECT count(*) FROM events_pq" analyst)
[ "$again" = "$mir_count" ] || bad "second query on a cached instance returned '$again'"

# ---------------------------------------------------------------- property 2
say "7. and NOTHING else is readable"
deny() {
  local label=$1 sql=$2 out
  out=$(psq $SPORT "$sql" analyst)
  if printf '%s' "$out" | grep -qiE "permission|not allowed|denied|cannot access|prohibited|locked|must be superuser|ERROR"; then
    printf '  denied   %s\n' "$label"
  else
    bad "READABLE by an unprivileged role: $label -> $(printf '%s' "$out" | head -1)"
  fi
}
deny "/etc/passwd"                 "SELECT count(*) FROM read_csv('/etc/passwd')"
deny "pg_hba.conf"                 "SELECT count(*) FROM read_csv('$STANDBY/pg_hba.conf')"
deny "PG_VERSION"                  "SELECT count(*) FROM read_csv('$STANDBY/PG_VERSION')"
deny "path traversal out of dir"   "SELECT count(*) FROM read_csv('$MIRROR/../standby/PG_VERSION')"
deny "http exfiltration"           "SELECT count(*) FROM read_csv('https://example.com/x.csv')"
deny "widen disabled_filesystems"  "SET duckdb.disabled_filesystems=''"
deny "widen allowed_directories"   "SET duckdb.allowed_directories='/'"
deny "widen external access"       "SET duckdb.enable_external_access=true"

# Still readable after all of that. Otherwise the denials prove nothing except
# that the connection broke.
after=$(psq $SPORT "SELECT count(*) FROM events_pq" analyst)
[ "$after" = "$mir_count" ] || bad "mirror unreadable after the denial cases: '$after'"

# ---------------------------------------------------------------- null ordering
say "8. ORDER BY over a NULLABLE column — measured, not assumed"
# The DuckDB view fixes this with SET default_null_order. pg_duckdb has no such
# GUC and duckdb.query takes a single SELECT, so there is nowhere to put it.
# Whether it matters depends on where the sort actually runs, which is a
# planner decision and not something to guess at.
src_desc=$(psq $PPORT "SELECT coalesce(string_agg(x,','),'') FROM (
             SELECT coalesce(note,'<NULL>') AS x FROM events ORDER BY note DESC, id LIMIT 5) s")
mir_desc=$(psq $SPORT "SELECT coalesce(string_agg(x,','),'') FROM (
             SELECT coalesce(note,'<NULL>') AS x FROM events_pq ORDER BY note DESC, id LIMIT 5) s" analyst)
echo "  source ORDER BY note DESC: $src_desc"
echo "  mirror ORDER BY note DESC: $mir_desc"
if [ "$src_desc" = "$mir_desc" ]; then
  echo "  AGREES — the sort runs in PostgreSQL over the projected rows"
else
  echo "  DIFFERS — the sort reaches DuckDB, which orders NULLs the other way."
  echo "  This is a DOCUMENTED limit of the PostgreSQL path, not a mirror error:"
  echo "  write an explicit NULLS FIRST / NULLS LAST on any ORDER BY that must agree."
  # Deliberately not a FAIL. The rows are correct; only their order under an
  # unqualified ORDER BY differs, and qs-query prints this caveat when it emits
  # the view. Failing here would be claiming a guarantee that was never made.
fi

# ---------------------------------------------------------------- performance
say "9. query performance: the heap and the mirror, on the SAME node"
if [ "$ROWS" -lt 1000000 ]; then
  echo "  skipped at $ROWS rows — everything fits in shared_buffers and the"
  echo "  numbers would be noise. Re-run with ROWS=3000000 to measure."
else
  # Both sides run on the STANDBY, as the same query, against the same page
  # cache, seconds apart. docs/11 compared a mirror on one box with PostgreSQL
  # on another and had to argue the machines were comparable; here there is
  # nothing to argue about.
  #
  # Minimum of three, not the mean: the thing being measured is how fast the
  # engine CAN answer, and a mean over three runs on a shared box measures the
  # noise as much as the query.
  timed() {
    local port=$1 user=$2 sql=$3 best=999999 t
    for _ in 1 2 3; do
      t=$(su postgres -c "$PG/psql -h /tmp -p $port -U $user -d $DB -q -c '\\timing on' -c \"$sql\" 2>&1" \
          | sed -n 's/^Time: \([0-9.]*\) ms.*/\1/p' | tail -1)
      [ -n "$t" ] || { echo "ERR"; return; }
      awk -v a="$t" -v b="$best" 'BEGIN{exit !(a<b)}' && best=$t
    done
    echo "$best"
  }

  printf '  %-34s %12s %12s %9s %s\n' query heap mirror ratio agree
  perf_row() {
    local label=$1 heap_sql=$2 mir_sql=$3
    local h m hv mv ratio agree
    h=$(timed $SPORT postgres "$heap_sql")
    m=$(timed $SPORT analyst  "$mir_sql")
    # A fast wrong answer is not a result, so every timed query is also
    # compared. This is the check that caught the jsonb shape in docs/32.
    hv=$(psq $SPORT "$heap_sql")
    mv=$(psq $SPORT "$mir_sql" analyst)
    if [ "$hv" = "$mv" ]; then agree="yes"; else agree="NO ($hv vs $mv)"; FAIL=1; fi
    if [ "$h" = "ERR" ] || [ "$m" = "ERR" ]; then
      ratio="-"
    else
      ratio=$(awk -v h="$h" -v m="$m" 'BEGIN{ if (m>0) printf "%.2fx", h/m; else print "-" }')
    fi
    printf '  %-34s %10s ms %10s ms %9s %s\n' "$label" "$h" "$m" "$ratio" "$agree"
  }

  perf_row "count(*)" \
    "SELECT count(*) FROM events" \
    "SELECT count(*) FROM events_pq"
  perf_row "sum one column" \
    "SELECT sum(amount)::text FROM events" \
    "SELECT sum(amount)::text FROM events_pq"
  perf_row "filter + aggregate" \
    "SELECT sum(amount)::text FROM events WHERE amount > 50" \
    "SELECT sum(amount)::text FROM events_pq WHERE amount > 50"
  perf_row "group by sku, top 1" \
    "SELECT sku FROM events GROUP BY sku ORDER BY count(*) DESC, sku LIMIT 1" \
    "SELECT sku FROM events_pq GROUP BY sku ORDER BY count(*) DESC, sku LIMIT 1"
  perf_row "truncate to the hour" \
    "SELECT count(DISTINCT date_trunc('hour', ts))::text FROM events" \
    "SELECT count(DISTINCT date_trunc('hour', ts))::text FROM events_pq"

  # The unflattering one, and the reason mode: takeover is gated. Leaving it out
  # would make this table a sales document.
  perf_row "point lookup by key (OLTP)" \
    "SELECT sku FROM events WHERE id = 123457" \
    "SELECT sku FROM events_pq WHERE id = 123457"

  echo
  echo "  Both columns are the same PostgreSQL on the same node: the heap column"
  echo "  is an ordinary query, the mirror column goes through pg_duckdb to"
  echo "  Parquet. A ratio above 1 means the mirror won."
fi

say "result"
if [ $FAIL -eq 0 ]; then
  echo "PASS — on PostgreSQL 17, an unprivileged role read $mir_count rows from the"
  echo "mirror THROUGH PostgreSQL, agreeing with the source heap, and reached"
  echo "nothing outside $MIRROR."
else
  echo "FAIL"
fi
exit $FAIL
