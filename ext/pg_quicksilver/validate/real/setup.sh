#!/usr/bin/env bash
# From an empty directory to a routed standby, with nothing but configuration:
#
#   primary  shared_preload_libraries = 'pg_duckdb,pg_quicksilver',
#            CREATE EXTENSION pg_duckdb, pg_quicksilver, the tables
#   standby  a streaming hot standby of it
#   sidecar  the production qs-mirror next to the standby, QS_DUCKLAKE=on
#
# The mirror relations, the registry, the lake attachment and the applied LSN
# are the sidecar's doing. Nothing here creates a view or registers anything.
#
#   LOCALE=icu-en-US (default) | C      the two databases bench/exactness uses
#   ROWS=1000000                        public.orders
#   DATASET=seed (default) | bench      bench: the orders/customers of bench/engine_compare/four_engines
#                                       (uuid keys, jsonb payload; NC customers), and nothing else
#   DB_ROUTE=auto | none                ALTER DATABASE ... SET quicksilver.route
#   DUCK_EXTERNAL=off, DUCK_DIRS=$MIRROR  pg_duckdb confined to the lake's directory, which is what lets
#                                       a role that is not a superuser be routed (docs/40)
set -uo pipefail
. "$(dirname "$0")/env.sh"
ROWS=${ROWS:-1000000}; LOCALE=${LOCALE:-icu-en-US}
for so in pg_duckdb pg_quicksilver; do
  [ -f "$($PG/pg_config --pkglibdir)/$so.so" ] || die "$so is not installed for $PG"
done
"$HERE/teardown.sh"
mkdir -p "$BASE" /tmp/qs_regress; chown postgres:postgres "$BASE" /tmp/qs_regress    # /tmp/qs_regress: where errors.sql keeps a mirror it can break
case "$LOCALE" in
  icu-en-US) loc="--locale=C.UTF-8 --locale-provider=icu --icu-locale=en-US" ;;
  C) loc="--locale=C.UTF-8 --locale-provider=builtin --builtin-locale=C" ;;
  *) die "LOCALE=$LOCALE" ;;
esac
su postgres -c "$PG/initdb -D $BASE/primary -U postgres -A trust -E UTF8 $loc" >/dev/null 2>&1 || die initdb
cat >> "$BASE/primary/postgresql.conf" <<CONF
port = $PPORT
listen_addresses = 'localhost'
unix_socket_directories = '/tmp'
wal_level = logical
shared_preload_libraries = 'pg_duckdb,pg_quicksilver'
shared_buffers = 512MB
max_wal_size = 4GB
timezone = 'UTC'
max_wal_senders = 10
max_replication_slots = 10
max_connections = 200
duckdb.postgres_role = 'qs_duck'
duckdb.enable_external_access = ${DUCK_EXTERNAL:-off}
duckdb.allowed_directories = '${DUCK_DIRS-$MIRROR,/tmp/qs_regress}'
${EXTRA_CONF:-}
CONF
su postgres -c "$PG/pg_ctl -D $BASE/primary -l $BASE/primary.log -w start" >/dev/null 2>&1 || { tail "$BASE/primary.log"; die "primary start"; }
psq $PPORT "CREATE DATABASE $DB" postgres >/dev/null
psq $PPORT "CREATE ROLE qs_duck" postgres >/dev/null    # members may run DuckDB (the permission tests)
psq $PPORT "CREATE EXTENSION pg_duckdb" >/dev/null || die "CREATE EXTENSION pg_duckdb"
psq $PPORT "CREATE EXTENSION pg_quicksilver" >/dev/null || die "CREATE EXTENSION pg_quicksilver"
# What the image has to have done (docs/40): with pg_duckdb confined to the lake's
# directory, DuckDB cannot download or install an extension, on any instance. So
# the two the lake needs are installed here, in a session that lifts the
# confinement for itself (a superuser may), before the base backup copies
# $PGDATA/pg_duckdb/extensions to the standby.
su postgres -c "$PG/psql -X -h /tmp -p $PPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1" >/dev/null <<SQL || die "install DuckDB extensions"
SET duckdb.enable_external_access = on;
SET duckdb.allowed_directories = '';
SET client_min_messages = warning;
SELECT duckdb.install_extension('ducklake');
SELECT duckdb.install_extension('sqlite');
SQL
# The application's choice, per database; replicated to the standby.
# (DB_ROUTE=none leaves it off: the regression tests set it themselves.)
[ "${DB_ROUTE:-auto}" = none ] || psq $PPORT "ALTER DATABASE $DB SET quicksilver.route = ${DB_ROUTE:-auto}" >/dev/null

mkdir -p "$BASE/sql"
cp "$REPO/go/cmd/qs-router/validate/seed.sql" "$REPO"/bench/exactness/{schema,data,data_special}.sql "$HERE/../types.sql" \
   "$REPO/bench/engine_compare/four_engines/scripts/load.sql" "$BASE/sql/"
chown -R postgres:postgres "$BASE/sql"
DATASET=${DATASET:-seed}
if [ "$DATASET" = bench ]; then
  psqf -v rows=$ROWS -v nc=${NC:-100000} -f "$BASE/sql/load.sql" >/dev/null || die load
  export QS_TABLES_OVERRIDE=public.orders,public.customers
else
  psqf -v rows=$ROWS -f "$BASE/sql/seed.sql" || die seed
  psqf -f "$BASE/sql/types.sql" || die types
  psq $PPORT "CREATE SCHEMA ex" >/dev/null
  for f in schema data; do
    su postgres -c "PGOPTIONS='-c search_path=ex' $PG/psql -X -h /tmp -p $PPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1 -f $BASE/sql/$f.sql" || die "ex $f"
  done
fi

psq $PPORT "SELECT pg_create_physical_replication_slot('qsint_standby', true)" postgres >/dev/null
su postgres -c "$PG/pg_basebackup -D $BASE/standby -R -X stream -S qsint_standby -c fast -d 'host=/tmp port=$PPORT user=postgres dbname=postgres'" >/dev/null 2>&1 || die basebackup
{ echo "port = $SPORT"; echo "hot_standby_feedback = on"; echo "sync_replication_slots = on"; } >> "$BASE/standby/postgresql.conf"
su postgres -c "$PG/pg_ctl -D $BASE/standby -l $BASE/standby.log -w start" >/dev/null 2>&1 || { tail "$BASE/standby.log"; die "standby start"; }

( cd "$REPO/go" && go build -o "$BASE/qs-mirror" ./cmd/qs-mirror && go build -o "$BASE/qs-query" ./cmd/qs-query && go build -o "$BASE/qs-verify" ./cmd/qs-verify ) || die build
chown -R postgres:postgres "$BASE"
"$HERE/sidecar.sh" start || die "sidecar never ready"
# The tricky rows arrive over the replication stream, as in bench/exactness.
[ "$DATASET" = bench ] || su postgres -c "PGOPTIONS='-c search_path=ex' $PG/psql -X -h /tmp -p $PPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1 -f $BASE/sql/data_special.sql" || die "ex special"
psq $PPORT "VACUUM ANALYZE" >/dev/null
echo "primary :$PPORT standby :$SPORT locale $LOCALE orders $(psq $PPORT 'SELECT count(*) FROM public.orders')"
