#!/usr/bin/env bash
# Scenario j: fail over. Stop the primary, promote the standby (its sidecar must stand down),
# build a new standby from the promoted server with pg_basebackup, and start a sidecar there.
# The new standby is on port 5803 (health 5804), its mirror in $BASE/mirror2, slot qs_s2 / qs_int2.
. "$(dirname "$0")/env.sh"; . "$(dirname "$0")/run.sh"
OLDPID=$(cat $BASE/mirror.pid)
echo "1. stop the old primary (fast) so that it cannot keep writing"
su postgres -c "$PG/pg_ctl -D $BASE/primary -m fast -w stop" | tail -1
echo "2. promote the standby on port $SPORT"
su postgres -c "$PG/pg_ctl -D $BASE/standby -w promote" | tail -1
for _ in $(seq 300); do kill -0 $OLDPID 2>/dev/null || break; sleep 0.2; done
kill -0 $OLDPID 2>/dev/null && echo "old sidecar pid $OLDPID STILL RUNNING" || echo "old sidecar pid $OLDPID exited"
echo "   sidecar log, after promotion:"
grep -iE "promot|recovery|stand|not a standby|exit|stop" $BASE/mirror.log | tail -4 | cut -c1-260
echo "3. primary is now: $(q $SPORT 'SELECT pg_is_in_recovery()') in recovery"
echo "4. new standby from the promoted server"
q $SPORT "SELECT pg_create_physical_replication_slot('qs_s2', true)" postgres >/dev/null
su postgres -c "$PG/pg_basebackup -D $BASE/standby2 -R -X stream -S qs_s2 -c fast -d 'host=/tmp port=$SPORT user=postgres dbname=postgres'" && echo "basebackup ok"
echo "port = 5803" >> $BASE/standby2/postgresql.conf
echo "hot_standby_feedback = on" >> $BASE/standby2/postgresql.conf
echo "sync_replication_slots = on" >> $BASE/standby2/postgresql.conf
# pg_basebackup copied the promoted server's duckdb.allowed_directories, which names the OLD
# node's mirror ($BASE/mirror); this node's sidecar writes $BASE/mirror2, so its own mirror
# must be listed too (k_two_standbys.sh does the same).
echo "duckdb.allowed_directories = '$BASE/mirror2,/tmp/qs_regress'" >> $BASE/standby2/postgresql.conf
su postgres -c "$PG/pg_ctl -D $BASE/standby2 -l $BASE/standby2.log -w start" >/dev/null && echo "standby2 started on 5803"
echo "   standby2 in recovery: $(q 5803 'SELECT pg_is_in_recovery()')"
echo "5. sidecar on standby2 (slot qs_int2, mirror $BASE/mirror2)"
su postgres -c "QS_CLUSTER=qsint QS_MODE=shadow QS_INGEST=logical QS_DUCKLAKE=on QS_DUCKLAKE_RETENTION=60s QS_SERVE_WITH_HALTED_TABLES=on QS_TABLES=$ALLT QS_SLOT=qs_int2 QS_PUBLICATION=qs_int QS_MIRROR_PATH=$BASE/mirror2 QS_FRESHNESS_SLO=30s QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$SPORT QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=5803 QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=qsint-3 QS_HEALTH_ADDR=127.0.0.1:5804 setsid $BASE/qs-mirror >> $BASE/mirror2.log 2>&1 & echo \$! > $BASE/mirror2.pid"
echo "   sidecar2 pid $(cat $BASE/mirror2.pid)"
for _ in $(seq 1200); do [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:5804/readyz)" = 200 ] && break; sleep 0.2; done
curl -s http://127.0.0.1:5804/readyz | cut -c1-240; echo
echo "6. checks against the new primary (5801), the new standby (5803), mirror2"
P=$SPORT S=5803 MD=$BASE/mirror2 bash $FV/scripts/check_all.sh j
