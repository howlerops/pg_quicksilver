#!/usr/bin/env bash
# sidecar.sh start [on|off] | stop | kill9 | wipe
#   start   the production qs-mirror, QS_DUCKLAKE as given (default on); waits
#           for readiness. Every tick over 1 ms is logged, which is where the
#           apply-loop latency in ticks.py comes from.
#   stop    SIGTERM, by PID        kill9   SIGKILL, by PID
#   wipe    remove the mirror and its slot, so the next start bootstraps
set -uo pipefail
. "$(dirname "$0")/env.sh"
pid() { cat "$BASE/mirror.pid" 2>/dev/null; }
case "${1:-}" in
start)
  su postgres -c "QS_CLUSTER=lake QS_MODE=shadow QS_INGEST=logical QS_DUCKLAKE=${2:-on} \
    QS_DUCKLAKE_RETENTION=${RETENTION:-60s} QS_SLOW_TICK=1ms QS_PARTIAL_DELTAS=${PARTIAL:-1} \
    QS_TABLES=public.orders QS_SLOT=qs_lake QS_PUBLICATION=qs_lake \
    QS_MIRROR_PATH=$MIRROR QS_FRESHNESS_SLO=30s \
    QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$PPORT \
    QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=$SPORT \
    QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=lake-2 \
    QS_HEALTH_ADDR=$HEALTH setsid $BASE/qs-mirror >> $BASE/mirror.log 2>&1 & echo \$! > $BASE/mirror.pid"
  for _ in $(seq 1 600); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "http://$HEALTH/readyz" 2>/dev/null)" = 200 ] && exit 0
    kill -0 "$(pid)" 2>/dev/null || { tail -5 "$BASE/mirror.log"; exit 1; }
    sleep 0.5
  done
  tail -20 "$BASE/mirror.log"; exit 1 ;;
stop|kill9)
  sig=TERM; [ "$1" = kill9 ] && sig=KILL
  kill -$sig "$(pid)" 2>/dev/null
  for _ in $(seq 1 100); do kill -0 "$(pid)" 2>/dev/null || exit 0; sleep 0.1; done; exit 1 ;;
wipe)
  case "$MIRROR" in /var/lib/postgresql/qslake*/mirror) rm -rf "$MIRROR" ;; esac
  psq "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'qs_lake%'" >/dev/null ;;
*) echo "usage: $0 start [on|off] | stop | kill9 | wipe" >&2; exit 2 ;;
esac
