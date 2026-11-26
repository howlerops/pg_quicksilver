#!/usr/bin/env bash
# Builds what pg_quicksilver is validated against: a PostgreSQL 17 primary with
# pg_duckdb and pg_quicksilver preloaded, a hot standby, the production sidecar
# building a real mirror, the mirror published as pg_duckdb views, and the
# extension's registry filled in.
#
#   public.orders (ROWS rows), public.customers   the router's tables (docs/38)
#   public.qs_types                               every type the seed allowlist names
#   ex.*                                          the exactness suite's six tables (docs/39)
#
# Stock pg_duckdb only lets a superuser read local files, so the routed side is
# only reachable as a superuser here; see docs/40.
set -uo pipefail
. "$(dirname "$0")/env.sh"
ROWS=${ROWS:-1000000}
[ -f "$($PG/pg_config --pkglibdir)/pg_duckdb.so" ] || die "pg_duckdb is not installed for $PG"
[ -f "$($PG/pg_config --pkglibdir)/pg_quicksilver.so" ] || die "pg_quicksilver is not installed for $PG (make install)"
"$HERE/teardown.sh"
mkdir -p "$BASE"; chown postgres:postgres "$BASE"
su postgres -c "$PG/initdb -D $BASE/primary -U postgres -A trust -E UTF8 --locale=C.UTF-8 --locale-provider=icu --icu-locale=en-US" >/dev/null 2>&1 || die initdb
cat >> "$BASE/primary/postgresql.conf" <<CONF
port = $PPORT
listen_addresses = 'localhost'
unix_socket_directories = '/tmp'
wal_level = logical
shared_preload_libraries = 'pg_duckdb,pg_quicksilver'
shared_buffers = 512MB
timezone = 'UTC'
duckdb.postgres_role = 'qs_duck'
# This path is the stand-in one (docs/40): a view names files and the LSN that
# publish.sh reports is the one at publication, so it cannot keep up with the
# strict default. The real path is validate/real.
quicksilver.max_lag = '16MB'
max_wal_senders = 10
max_replication_slots = 10
CONF
su postgres -c "$PG/pg_ctl -D $BASE/primary -l $BASE/primary.log -w start" >/dev/null 2>&1 || { tail "$BASE/primary.log"; die "primary start"; }
psq $PPORT "CREATE DATABASE $DB" postgres >/dev/null
psq $PPORT "CREATE ROLE qs_duck" postgres >/dev/null    # members may run DuckDB (the permission tests)
psq $PPORT "CREATE EXTENSION pg_duckdb" || die "CREATE EXTENSION pg_duckdb"

mkdir -p "$BASE/sql"
cp "$REPO/go/cmd/qs-router/validate/seed.sql" "$REPO"/bench/exactness/{schema,data,data_special}.sql "$HERE/types.sql" "$BASE/sql/"
chown -R postgres:postgres "$BASE/sql"
psqf -v rows=$ROWS -f "$BASE/sql/seed.sql" || die seed
psqf -f "$BASE/sql/types.sql" || die types
psq $PPORT "CREATE SCHEMA ex" >/dev/null
for f in schema data; do
  su postgres -c "PGOPTIONS='-c search_path=ex' $PG/psql -X -h /tmp -p $PPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1 -f $BASE/sql/$f.sql" || die "ex $f"
done

psq $PPORT "SELECT pg_create_physical_replication_slot('qsext_standby', true)" postgres >/dev/null
su postgres -c "$PG/pg_basebackup -D $BASE/standby -R -X stream -S qsext_standby -c fast -d 'host=/tmp port=$PPORT user=postgres dbname=postgres'" >/dev/null 2>&1 || die basebackup
{ echo "port = $SPORT"; echo "hot_standby_feedback = on"; echo "sync_replication_slots = on"; } >> "$BASE/standby/postgresql.conf"
su postgres -c "$PG/pg_ctl -D $BASE/standby -l $BASE/standby.log -w start" >/dev/null 2>&1 || { tail "$BASE/standby.log"; die "standby start"; }

( cd "$REPO/go" && go build -o "$BASE/qs-mirror" ./cmd/qs-mirror && go build -o "$BASE/qs-query" ./cmd/qs-query ) || die build
chown -R postgres:postgres "$BASE"
"$HERE/sidecar.sh" start || die "sidecar never ready"
# The tricky rows arrive over the replication stream, as in bench/exactness.
su postgres -c "PGOPTIONS='-c search_path=ex' $PG/psql -X -h /tmp -p $PPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1 -f $BASE/sql/data_special.sql" || die "ex special"
psq $PPORT "VACUUM ANALYZE" >/dev/null
psq $PPORT "CREATE EXTENSION pg_quicksilver" || die "CREATE EXTENSION pg_quicksilver"
"$HERE/publish.sh" || die publish
echo "heap   orders: $(psq $PPORT 'SET quicksilver.route=off; SELECT count(*), sum(amount) FROM public.orders' | tail -1)"
echo "mirror orders: $(psq $PPORT 'SELECT count(*), sum(amount) FROM qs_mirror.orders')"
echo "heap   ex.t_num: $(psq $PPORT 'SET quicksilver.route=off; SELECT count(*) FROM ex.t_num' | tail -1)   mirror: $(psq $PPORT 'SELECT count(*) FROM qs_mirror_ex.t_num')"
