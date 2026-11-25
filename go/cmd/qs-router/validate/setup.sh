#!/usr/bin/env bash
# Builds what qs-router is validated against: a PostgreSQL 17 primary with
# pg_duckdb, a hot standby, the production sidecar building a real mirror of
# two tables, and the mirror published as views in schema qs_mirror under the
# heap tables' own names (qs-query -engine postgres).
#
# Stock pg_duckdb only lets a superuser read local files; the confinement patch
# in bench/patches is what makes it safe for ordinary roles and is NOT what this
# script installs. Everything here runs as the postgres superuser.
set -uo pipefail
PG=/usr/lib/postgresql/17/bin
BASE=${BASE:-/var/lib/postgresql/qsrouter}
PPORT=${PPORT:-5491}; SPORT=${SPORT:-5492}; HEALTH=127.0.0.1:${HPORT:-5493}
ROWS=${ROWS:-1000000}
DB=app
HERE=$(cd "$(dirname "$0")" && pwd)
GO=$(cd "$HERE/../../.." && pwd)
MIRROR=$BASE/mirror
die() { echo "FAIL: $*" >&2; exit 1; }
psq() { su postgres -c "$PG/psql -h /tmp -p $1 -U postgres -d ${3:-$DB} -v ON_ERROR_STOP=1 -Atc \"$2\"" 2>&1; }

[ -f "$($PG/pg_config --pkglibdir)/pg_duckdb.so" ] || die "pg_duckdb is not installed for PostgreSQL 17"
"$HERE/teardown.sh"
mkdir -p "$BASE"; chown postgres:postgres "$BASE"
su postgres -c "$PG/initdb -D $BASE/primary -A trust" >/dev/null 2>&1 || die initdb
cat >> "$BASE/primary/postgresql.conf" <<CONF
port = $PPORT
listen_addresses = 'localhost'
unix_socket_directories = '/tmp'
wal_level = logical
shared_preload_libraries = 'pg_duckdb'
shared_buffers = 512MB
CONF
su postgres -c "$PG/pg_ctl -D $BASE/primary -l $BASE/primary.log -w start" >/dev/null 2>&1 || { tail "$BASE/primary.log"; die "primary start"; }
psq $PPORT "CREATE DATABASE $DB" postgres >/dev/null
psq $PPORT "CREATE EXTENSION pg_duckdb" || die "CREATE EXTENSION"

cp "$HERE/seed.sql" "$BASE/seed.sql"; chown postgres:postgres "$BASE/seed.sql"
su postgres -c "$PG/psql -h /tmp -p $PPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1 -v rows=$ROWS -f $BASE/seed.sql" || die seed

psq $PPORT "SELECT pg_create_physical_replication_slot('router_standby', true)" postgres >/dev/null
su postgres -c "$PG/pg_basebackup -D $BASE/standby -R -X stream -S router_standby -c fast -d 'host=/tmp port=$PPORT user=postgres dbname=postgres'" >/dev/null 2>&1 || die basebackup
{ echo "port = $SPORT"; echo "hot_standby_feedback = on"; echo "sync_replication_slots = on"; } >> "$BASE/standby/postgresql.conf"
su postgres -c "$PG/pg_ctl -D $BASE/standby -l $BASE/standby.log -w start" >/dev/null 2>&1 || { tail "$BASE/standby.log"; die "standby start"; }

( cd "$GO" && go build -o "$BASE/qs-mirror" ./cmd/qs-mirror && go build -o "$BASE/qs-query" ./cmd/qs-query ) || die build
chown -R postgres:postgres "$BASE"
"$HERE/sidecar.sh" start || die "sidecar never ready"

psq $PPORT "CREATE SCHEMA qs_mirror" >/dev/null
for t in orders customers; do
  su postgres -c "$BASE/qs-query -mirror $MIRROR -table public.$t -view qs_mirror.$t -engine postgres \
     -dsn 'host=/tmp port=$PPORT user=postgres dbname=$DB'" 2>"$BASE/qs-query.$t.err" > "$BASE/view.$t.sql" || { cat "$BASE/qs-query.$t.err"; die "qs-query $t"; }
  su postgres -c "$PG/psql -h /tmp -p $PPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1 -f $BASE/view.$t.sql" || die "view $t"
done
echo "heap   orders: $(psq $PPORT 'SELECT count(*), sum(amount) FROM public.orders')"
echo "mirror orders: $(psq $PPORT 'SELECT count(*), sum(amount) FROM qs_mirror.orders')"
curl -s "http://$HEALTH/readyz"; echo
