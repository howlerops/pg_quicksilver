#!/usr/bin/env bash
# w2e setup: primary + hot standby + qs-mirror sidecar, bench dataset (uuid keys, 500k customers).
# Env: ROWS (orders), NC (customers, default 500000), JOIN=on|off
set -uo pipefail
WT=/home/user/pg_quicksilver/.claude/worktrees/agent-a63c879ecb16e1405
export PG=/var/lib/postgresql/w2e/pg/usr/lib/postgresql/17/bin   # patched PG17 binaries (copied) + lib/share symlinks + the repo's pg_quicksilver
export BASE=/var/lib/postgresql/w2e PPORT=6610 SPORT=6611 HPORT=6612
export DUCK_EXTERNAL=off DUCK_DIRS=
. "$WT/ext/pg_quicksilver/validate/real/env.sh"
ROWS=${ROWS:-1000000}; NC=${NC:-500000}; JOIN=${JOIN:-off}
die() { echo "FAIL: $*" >&2; exit 1; }
[ -e /tmp/.s.PGSQL.$PPORT ] && die "port busy"
mkdir -p "$BASE"; chown postgres:postgres "$BASE"
su postgres -c "$PG/initdb -D $BASE/primary -U postgres -A trust -E UTF8 --locale=C.UTF-8 --locale-provider=icu --icu-locale=en-US" >/dev/null 2>&1 || die initdb
mkdir -p "$BASE/primary/pg_duckdb"
cp -a /var/lib/postgresql/qs-acc173/int/primary/pg_duckdb/extensions "$BASE/primary/pg_duckdb/" || die "copy duck ext"
chown -R postgres:postgres "$BASE/primary/pg_duckdb"
cat >> "$BASE/primary/postgresql.conf" <<CONF
port = $PPORT
listen_addresses = 'localhost'
unix_socket_directories = '/tmp'
wal_level = logical
shared_preload_libraries = 'pg_duckdb,pg_quicksilver'
shared_buffers = 1GB
max_wal_size = 1GB
min_wal_size = 256MB
timezone = 'UTC'
max_wal_senders = 10
max_replication_slots = 10
max_connections = 200
duckdb.postgres_role = 'qs_duck'
duckdb.enable_external_access = off
duckdb.allowed_directories = '$BASE/mirror'
CONF
su postgres -c "$PG/pg_ctl -D $BASE/primary -l $BASE/primary.log -w start" >/dev/null 2>&1 || { tail "$BASE/primary.log"; die "primary start"; }
psq $PPORT "CREATE DATABASE $DB" postgres >/dev/null
psq $PPORT "CREATE ROLE qs_duck" postgres >/dev/null
psq $PPORT "CREATE EXTENSION pg_duckdb" >/dev/null || die "ext duckdb"
psq $PPORT "CREATE EXTENSION pg_quicksilver" >/dev/null || die "ext qs"
psq $PPORT "GRANT qs_duck TO quicksilver_mirror_reader" >/dev/null || die grant
psq $PPORT "ALTER DATABASE $DB SET quicksilver.route = auto" >/dev/null
su postgres -c "$PG/psql -X -h /tmp -p $PPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1" >/dev/null <<SQL || die "install DuckDB extensions"
SET duckdb.enable_external_access = on;
SET duckdb.allowed_directories = '';
SET client_min_messages = warning;
SELECT duckdb.install_extension('ducklake');
SELECT duckdb.install_extension('sqlite');
SQL
mkdir -p "$BASE/sql"; cp "$WT/bench/engine_compare/four_engines/scripts/load.sql" "$BASE/sql/"; chown -R postgres:postgres "$BASE/sql"
T0=$(date +%s)
su postgres -c "$PG/psql -X -h /tmp -p $PPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1 -v rows=$ROWS -v nc=$NC -f $BASE/sql/load.sql" >"$BASE/load.out" 2>&1 || { tail "$BASE/load.out"; die load; }
echo "load secs $(( $(date +%s) - T0 ))"
psq $PPORT "SELECT pg_create_physical_replication_slot('qsint_standby', true)" postgres >/dev/null
su postgres -c "$PG/pg_basebackup -D $BASE/standby -R -X stream -S qsint_standby -c fast -d 'host=/tmp port=$PPORT user=postgres dbname=postgres'" >/dev/null 2>&1 || die basebackup
{ echo "port = $SPORT"; echo "hot_standby_feedback = on"; echo "sync_replication_slots = on"; } >> "$BASE/standby/postgresql.conf"
su postgres -c "$PG/pg_ctl -D $BASE/standby -l $BASE/standby.log -w start" >/dev/null 2>&1 || die "standby start"
( cd "$WT/go" && go build -o "$BASE/qs-mirror" ./cmd/qs-mirror && go build -o "$BASE/qs-verify" ./cmd/qs-verify && go build -o "$BASE/qs-query" ./cmd/qs-query ) || die build
chown -R postgres:postgres "$BASE"
export QS_TABLES_OVERRIDE=public.orders,public.customers
if [ "$JOIN" = on ]; then export SIDECAR_ENV="QS_JOIN_DOMAINS=cust=public.customers.id,public.orders.customer_id"; fi
"$WT/ext/pg_quicksilver/validate/real/sidecar.sh" start || die "sidecar never ready"
psq $PPORT "VACUUM ANALYZE public.orders; VACUUM ANALYZE public.customers" >/dev/null
echo "primary :$PPORT standby :$SPORT orders $(psq $PPORT 'SELECT count(*) FROM public.orders') customers $(psq $PPORT 'SELECT count(*) FROM public.customers')"
echo "sizes: $(psq $PPORT "SELECT pg_size_pretty(pg_total_relation_size('orders')) || ' orders total, heap ' || pg_size_pretty(pg_relation_size('orders')) || ' ; db ' || pg_size_pretty(pg_database_size('app'))")"
