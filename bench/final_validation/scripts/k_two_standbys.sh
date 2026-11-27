#!/usr/bin/env bash
# Scenario k: two standbys with sidecars against the promoted primary (port 5801).
#   standby2 : port 5803, health 5804, sidecar slot qs_int2, mirror2   (from scenario j)
#   standby3 : port 5806, health 5807, sidecar slot qs_int3, mirror3   (built here)
# Concurrent writes, then both mirrors must MATCH; the primary must show two active logical slots.
. "$(dirname "$0")/env.sh"; . "$(dirname "$0")/run.sh"
P=$SPORT
SEED=$(( $(date +%s) % 100000 ))
echo "1. standby3 from the primary ($P), slot qs_s3"
q $P "SELECT pg_create_physical_replication_slot('qs_s3', true)" postgres >/dev/null
rm -rf $BASE/standby3
su postgres -c "$PG/pg_basebackup -D $BASE/standby3 -R -X stream -S qs_s3 -c fast -d 'host=/tmp port=$P user=postgres dbname=postgres'" && echo "basebackup ok"
{ echo "port = 5806"; echo "hot_standby_feedback = on"; echo "sync_replication_slots = on"; echo "duckdb.allowed_directories = '$BASE/mirror3,/tmp/qs_regress'"; } >> $BASE/standby3/postgresql.conf
chown -R postgres:postgres $BASE/standby3 2>/dev/null
su postgres -c "$PG/pg_ctl -D $BASE/standby3 -l $BASE/standby3.log -w start" >/dev/null && echo "standby3 started on 5806"
echo "   in recovery: $(q 5806 'SELECT pg_is_in_recovery()')"
echo "2. sidecar3 (slot qs_int3, mirror $BASE/mirror3)"
su postgres -c "QS_CLUSTER=qsint QS_MODE=shadow QS_INGEST=logical QS_DUCKLAKE=on QS_DUCKLAKE_RETENTION=60s QS_SERVE_WITH_HALTED_TABLES=on QS_TABLES=$ALLT QS_SLOT=qs_int3 QS_PUBLICATION=qs_int QS_MIRROR_PATH=$BASE/mirror3 QS_FRESHNESS_SLO=30s QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$P QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=5806 QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=qsint-4 QS_HEALTH_ADDR=127.0.0.1:5807 setsid $BASE/qs-mirror >> $BASE/mirror3.log 2>&1 & echo \$! > $BASE/mirror3.pid"
echo "   sidecar3 pid $(cat $BASE/mirror3.pid)"
for _ in $(seq 1200); do [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:5807/readyz)" = 200 ] && break; sleep 0.2; done
curl -s http://127.0.0.1:5807/readyz | cut -c1-200; echo
echo "3. concurrent writes: two writers against the primary, and reads on both standbys meanwhile"
python3 -I $FV/scripts/fv_mixed.py --port $P --rounds 15 --seed $SEED > $OUT/k_writer_a.txt 2>&1 &
WA=$!
python3 -I $FV/scripts/fv_mixed.py --port $P --rounds 15 --seed $((SEED+1)) > $OUT/k_writer_b.txt 2>&1 &
WB=$!
wait $WA; echo "writer a exit $?"; wait $WB; echo "writer b exit $?"
echo "4. active logical slots on the primary"
q $P "SELECT slot_name, slot_type, active, active_pid IS NOT NULL AS has_pid FROM pg_replication_slots WHERE slot_type='logical' ORDER BY slot_name" postgres
q $P "SELECT count(DISTINCT slot_name) FROM pg_replication_slots WHERE slot_type='logical' AND active" postgres | sed 's/^/distinct active logical slots: /'
echo "5. both mirrors against the primary"
P=$P S=5803 MD=$BASE/mirror2 bash $FV/scripts/check_all.sh k_standby2
P=$P S=5806 MD=$BASE/mirror3 bash $FV/scripts/check_all.sh k_standby3
