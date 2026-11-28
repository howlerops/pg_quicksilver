#!/usr/bin/env bash
# C scenario, cluster build (namespaced): primary P (6620), standby A (6621, sidecar health 6623),
# standby B (6622, sidecar health 6624). Each standby and its sidecar run in their own mount
# namespace, with their own mirror directory bind-mounted over $BASE/mirror (the same path on
# both, as two Pods see it). ROWS orders, NC customers (bench layout, join domains on).
set -uo pipefail
# Overridable: BASE (data dir), PG (bin dir of the server), PP/PA/PB/HA/HB (ports), WT (checkout
# with bench/engine_compare). The defaults are the original run's; see promote_live.md.
SD=$(cd "$(dirname "$0")" && pwd)
PG=${PG:-/var/lib/postgresql/w2e/pg/usr/lib/postgresql/17/bin}
BASE=${BASE:-/var/lib/postgresql/w2e}
export BASE
WT=${WT:-$(cd "$SD/../.." && pwd)}
C=$BASE/c
MIR=$BASE/mirror
PP=${PP:-6620}; PA=${PA:-6621}; PB=${PB:-6622}; HA=${HA:-6623}; HB=${HB:-6624}
ROWS=${ROWS:-300000}; NC=${NC:-50000}
die() { echo "FAIL: $*" >&2; exit 1; }
q() { su postgres -c "$PG/psql -X -h /tmp -p $1 -U postgres -d ${3:-app} -v ON_ERROR_STOP=1 -Atc \"$2\""; }
rm -rf "$C"; mkdir -p "$C" "$MIR" "$C/mirrorA" "$C/mirrorB"; chown -R postgres:postgres "$C" "$MIR"
su postgres -c "$PG/initdb -D $C/primary -U postgres -A trust -E UTF8 --locale=C.UTF-8 --locale-provider=icu --icu-locale=en-US" >/dev/null 2>&1 || die initdb
mkdir -p "$C/primary/pg_duckdb" && cp -a /var/lib/postgresql/qs-acc173/int/primary/pg_duckdb/extensions "$C/primary/pg_duckdb/" && chown -R postgres:postgres "$C/primary/pg_duckdb"
cat >> "$C/primary/postgresql.conf" <<CONF
port = $PP
listen_addresses = 'localhost'
unix_socket_directories = '/tmp'
wal_level = logical
shared_preload_libraries = 'pg_duckdb,pg_quicksilver'
shared_buffers = 512MB
max_wal_size = 1GB
min_wal_size = 128MB
timezone = 'UTC'
max_wal_senders = 10
max_replication_slots = 10
max_connections = 100
duckdb.postgres_role = 'qs_duck'
duckdb.enable_external_access = off
duckdb.allowed_directories = '$MIR,/tmp/qs_regress'
CONF
su postgres -c "$PG/pg_ctl -D $C/primary -l $C/primary.log -w start" >/dev/null 2>&1 || die "primary start"
q $PP "CREATE DATABASE app" postgres >/dev/null
q $PP "CREATE ROLE qs_duck" postgres >/dev/null
q $PP "CREATE EXTENSION pg_duckdb" >/dev/null || die "ext duckdb"
q $PP "CREATE EXTENSION pg_quicksilver" >/dev/null || die "ext qs"
q $PP "GRANT qs_duck TO quicksilver_mirror_reader" >/dev/null || die grant
q $PP "ALTER DATABASE app SET quicksilver.route = auto" >/dev/null
su postgres -c "$PG/psql -X -h /tmp -p $PP -U postgres -d app -q -v ON_ERROR_STOP=1" >/dev/null <<SQL || die "duckdb extensions"
SET duckdb.enable_external_access = on;
SET duckdb.allowed_directories = '';
SET client_min_messages = warning;
SELECT duckdb.install_extension('ducklake');
SELECT duckdb.install_extension('sqlite');
SQL
mkdir -p "$C/sql"; cp "$WT/bench/engine_compare/four_engines/scripts/load.sql" "$C/sql/"; chown -R postgres:postgres "$C/sql"
su postgres -c "$PG/psql -X -h /tmp -p $PP -U postgres -d app -q -v ON_ERROR_STOP=1 -v rows=$ROWS -v nc=$NC -f $C/sql/load.sql" >"$C/load.out" 2>&1 || die load
q $PP "SELECT pg_create_physical_replication_slot('qsint_a', true)" postgres >/dev/null || die slotA
q $PP "SELECT pg_create_physical_replication_slot('qsint_b', true)" postgres >/dev/null || die slotB
for n in A B; do
  dir=$C/standby$n; port=$([ $n = A ] && echo $PA || echo $PB); slot=qsint_$(echo $n | tr A-Z a-z)
  su postgres -c "$PG/pg_basebackup -D $dir -R -X stream -S $slot -c fast -d 'host=/tmp port=$PP user=postgres dbname=postgres'" >/dev/null 2>&1 || die "basebackup $n"
  { echo "port = $port"; echo "hot_standby_feedback = on"; echo "sync_replication_slots = on";
    echo "duckdb.allowed_directories = '$MIR,/tmp/qs_regress'"; } >> "$dir/postgresql.conf"
  chown -R postgres:postgres "$dir"
  ns_dir=$C/mirror$n
  "$SD/ns_exec.sh" "$ns_dir" "$MIR" su postgres -c "$PG/pg_ctl -D $dir -l $dir.log -w start" >/dev/null 2>&1 || die "standby $n start"
done
JD="QS_JOIN_DOMAINS=cust=public.customers.id,public.orders.customer_id"
mk_env() { # ENVFILE PRIMARY LOCAL HEALTH SLOT POD
  cat > "$1" <<ENV
QS_CLUSTER=qsint QS_MODE=shadow QS_INGEST=logical QS_DUCKLAKE=on QS_DUCKLAKE_RETENTION=60s QS_SERVE_WITH_HALTED_TABLES=on
QS_TABLES=public.orders,public.customers QS_SLOT=$5 QS_PUBLICATION=qs_int $JD QS_MIRROR_PATH=$MIR QS_FRESHNESS_SLO=30s
QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$2 QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=$3 QS_DATABASE=app QS_PGUSER=postgres
QS_POD_NAME=$6 QS_HEALTH_ADDR=127.0.0.1:$4
ENV
}
mk_env $C/side_A.env $PP $PA $HA qs_a qsint-a
mk_env $C/side_B.env $PP $PB $HB qs_b qsint-b
"$SD/ns_exec.sh" "$C/mirrorA" "$MIR" bash "$SD/start_side.sh" "$C/side_A.env" "$C/side_A.log" "$C/side_A.pid" || die "side A"
for _ in $(seq 1 600); do [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$HA/readyz)" = 200 ] && break; sleep 0.5; done
"$SD/ns_exec.sh" "$C/mirrorB" "$MIR" bash "$SD/start_side.sh" "$C/side_B.env" "$C/side_B.log" "$C/side_B.pid" || die "side B"
for _ in $(seq 1 600); do [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$HB/readyz)" = 200 ] && break; sleep 0.5; done
echo "primary :$PP  standby A :$PA (sidecar health $HA, pid $(cat $C/side_A.pid))  standby B :$PB (health $HB, pid $(cat $C/side_B.pid))"
echo "orders $(q $PP 'SELECT count(*) FROM public.orders') customers $(q $PP 'SELECT count(*) FROM public.customers')"
echo "A orders $(q $PA 'SELECT count(*) FROM public.orders') B orders $(q $PB 'SELECT count(*) FROM public.orders')"
echo "readyz A: $(curl -s http://127.0.0.1:$HA/readyz | cut -c1-200)"
echo "readyz B: $(curl -s http://127.0.0.1:$HB/readyz | cut -c1-200)"
