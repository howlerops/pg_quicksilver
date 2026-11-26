#!/usr/bin/env bash
# Part 3 / Part 4 cluster: primary (5820), hot standby (5821), sidecar (health 5822), mirror in $BB/mirror.
# Everything the server side runs is pinned to CPUS (default 0-2: the three cores the earlier runs had).
# DATA: the bench dataset of bench/engine_compare/four_engines/scripts/load.sql, ROWS orders and NC customers,
#       then staging.sql (the change workload's tables, wl.*), LOCALE=C so that ORDER BY on text routes.
# Usage: ROWS=10000000 NC=500000 bench_setup.sh      (the sidecar is started last and its start is timed)
set -uo pipefail
. "$(dirname "$0")/env.sh"; . "$(dirname "$0")/run.sh"
BB=$BASE/bench
CPUS=${CPUS:-0-2}
ROWS=${ROWS:-10000000}; NC=${NC:-500000}
BPORT=5820; BSTBY=5821; BHEALTH=5822
SQLDIR=$REPO/bench/engine_compare/four_engines/scripts
mkdir -p $BB/sql $OUT
rm -rf $BB/primary $BB/standby $BB/mirror $BB/mirror.pid; mkdir -p $BB/mirror
chown -R postgres:postgres $BB
cp $SQLDIR/load.sql $SQLDIR/staging.sql $BB/sql/
sed -i "s|/tmp/qsb/results/lookup_id.txt|$BB/lookup_id.txt|" $BB/sql/staging.sql
chown -R postgres:postgres $BB/sql
echo "$(date -u +%T) initdb"
su postgres -c "$PG/initdb -D $BB/primary -U postgres -A trust -E UTF8 --locale=C.UTF-8 --locale-provider=builtin --builtin-locale=C" >/dev/null 2>&1 || { echo initdb failed; exit 1; }
cat >> $BB/primary/postgresql.conf <<CONF
port = $BPORT
listen_addresses = 'localhost'
unix_socket_directories = '/tmp'
wal_level = logical
shared_preload_libraries = 'pg_duckdb,pg_quicksilver'
shared_buffers = 1GB
max_wal_size = 8GB
timezone = 'UTC'
max_wal_senders = 10
max_replication_slots = 10
max_connections = 200
duckdb.postgres_role = 'qs_duck'
duckdb.enable_external_access = off
duckdb.allowed_directories = '$BB/mirror,/tmp/qs_regress'
CONF
taskset -c $CPUS su postgres -c "$PG/pg_ctl -D $BB/primary -l $BB/primary.log -w start" >/dev/null 2>&1 || { tail $BB/primary.log; exit 1; }
echo $(cat $BB/primary/postmaster.pid | head -1) > /dev/null
PB=$(head -1 $BB/primary/postmaster.pid)
echo "primary pid $PB"
echo "$PB" > $BB/primary.pid
qb() { su postgres -c "LD_LIBRARY_PATH=$LD_LIBRARY_PATH $PG/psql -X -h /tmp -p $BPORT -U postgres -d ${2:-$DB} -v ON_ERROR_STOP=1 -Atc \"$1\"" 2>&1; }
qb "CREATE DATABASE $DB" postgres >/dev/null
qb "CREATE ROLE qs_duck" postgres >/dev/null
qb "CREATE EXTENSION pg_duckdb" >/dev/null || { echo "pg_duckdb"; exit 1; }
qb "CREATE EXTENSION pg_quicksilver" >/dev/null || { echo "pg_quicksilver"; exit 1; }
qb "GRANT qs_duck TO quicksilver_mirror_reader" >/dev/null
su postgres -c "LD_LIBRARY_PATH=$LD_LIBRARY_PATH $PG/psql -X -h /tmp -p $BPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1 -c \"SET duckdb.enable_external_access = on; SET duckdb.allowed_directories = ''; SET client_min_messages = warning; SELECT duckdb.install_extension('ducklake'); SELECT duckdb.install_extension('sqlite');\"" >/dev/null 2>&1 || echo "duckdb extension install reported an error (see log)"
qb "ALTER DATABASE $DB SET quicksilver.route = auto" >/dev/null
echo "$(date -u +%T) load ROWS=$ROWS NC=$NC"
T0=$(date +%s)
su postgres -c "LD_LIBRARY_PATH=$LD_LIBRARY_PATH $PG/psql -X -h /tmp -p $BPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1 -v rows=$ROWS -v nc=$NC -f $BB/sql/load.sql" > $OUT/bench_load.txt 2>&1 || { echo "load failed"; tail -5 $OUT/bench_load.txt; exit 1; }
echo "load seconds $(( $(date +%s) - T0 ))"
echo "$(date -u +%T) staging (change workload tables)"
su postgres -c "LD_LIBRARY_PATH=$LD_LIBRARY_PATH $PG/psql -X -h /tmp -p $BPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1 -f $BB/sql/staging.sql" > $OUT/bench_staging.txt 2>&1 || { echo "staging failed"; tail -5 $OUT/bench_staging.txt; exit 1; }
qb "VACUUM (ANALYZE) public.orders" >/dev/null; qb "VACUUM (ANALYZE) public.customers" >/dev/null; qb "CHECKPOINT" >/dev/null
echo "lookup id: $(cat $BB/lookup_id.txt)"
qb "SELECT pg_create_physical_replication_slot('bench_standby', true)" postgres >/dev/null
echo "$(date -u +%T) standby from base backup"
taskset -c $CPUS su postgres -c "$PG/pg_basebackup -D $BB/standby -R -X stream -S bench_standby -c fast -d 'host=/tmp port=$BPORT user=postgres dbname=postgres'" >/dev/null 2>&1 || { echo basebackup failed; exit 1; }
{ echo "port = $BSTBY"; echo "hot_standby_feedback = on"; echo "sync_replication_slots = on"; echo "duckdb.allowed_directories = '$BB/mirror,/tmp/qs_regress'"; } >> $BB/standby/postgresql.conf
chown -R postgres:postgres $BB/standby
taskset -c $CPUS su postgres -c "$PG/pg_ctl -D $BB/standby -l $BB/standby.log -w start" >/dev/null 2>&1 || { tail $BB/standby.log; exit 1; }
echo "standby started on $BSTBY"
echo "$(date -u +%T) sidecar start (timed)"
T1=$(date +%s.%N)
su postgres -c "env -i PATH=/usr/bin:/bin QS_CLUSTER=qsbench QS_MODE=shadow QS_INGEST=logical QS_DUCKLAKE=on QS_DUCKLAKE_RETENTION=60s QS_SERVE_WITH_HALTED_TABLES=on QS_TABLES=public.orders,public.customers QS_SLOT=qs_bench QS_PUBLICATION=qs_bench QS_MIRROR_PATH=$BB/mirror QS_FRESHNESS_SLO=30s QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$BPORT QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=$BSTBY QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=qsbench-1 QS_HEALTH_ADDR=127.0.0.1:$BHEALTH setsid taskset -c $CPUS $BASE/qs-mirror >> $BB/mirror.log 2>&1 & echo \$! > $BB/mirror.pid"
for _ in $(seq 36000); do [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$BHEALTH/readyz)" = 200 ] && break; sleep 0.1; done
T2=$(date +%s.%N)
echo "initial sync: sidecar start to ready $(python3 -c "print(round($T2-$T1,1))") s; pid $(cat $BB/mirror.pid)"
echo "readyz: $(curl -s http://127.0.0.1:$BHEALTH/readyz | cut -c1-200)"
echo "VmHWM at ready: $(awk '/VmHWM/{print $2" kB"}' /proc/$(cat $BB/mirror.pid)/status)  VmRSS: $(awk '/VmRSS/{print $2" kB"}' /proc/$(cat $BB/mirror.pid)/status)"
echo "$(date -u +%T) done"
