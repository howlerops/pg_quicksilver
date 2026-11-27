#!/usr/bin/env bash
# launch_sc.sh NAME SLOT LOCAL_PORT HEALTH_PORT MIRROR_DIR PRIMARY_PORT [VAR=VAL ...]
# Starts the production qs-mirror (as the postgres user, setsid) against the standby on LOCAL_PORT,
# records its PID in $BASE/NAME.pid and waits for /readyz. Extra VAR=VAL pairs go to its environment.
# The sidecar's stdout/stderr go to $BASE/NAME.log.
. "$(dirname "$0")/env.sh"; . "$(dirname "$0")/run.sh"
NAME=$1; SLOT=$2; LP=$3; HP=$4; MD=$5; PP=$6; shift 6
EXTRA=""
for kv in "$@"; do EXTRA="$EXTRA $kv"; done
su postgres -c "env -i PATH=/usr/bin:/bin QS_CLUSTER=qsint QS_MODE=shadow QS_INGEST=logical QS_DUCKLAKE=on QS_DUCKLAKE_RETENTION=60s QS_SERVE_WITH_HALTED_TABLES=on QS_TABLES=${SC_TABLES:-$ALLT} QS_SLOT=$SLOT QS_PUBLICATION=qs_int QS_MIRROR_PATH=$MD QS_FRESHNESS_SLO=30s QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$PP QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=$LP QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=$NAME QS_HEALTH_ADDR=127.0.0.1:$HP $EXTRA setsid $BASE/qs-mirror >> $BASE/$NAME.log 2>&1 & echo \$! > $BASE/$NAME.pid"
echo "$NAME pid $(cat $BASE/$NAME.pid)"
for _ in $(seq 1200); do [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$HP/readyz)" = 200 ] && { echo "$NAME ready"; exit 0; }; sleep 0.2; done
echo "$NAME NOT READY"; tail -3 $BASE/$NAME.log | cut -c1-250; exit 1
