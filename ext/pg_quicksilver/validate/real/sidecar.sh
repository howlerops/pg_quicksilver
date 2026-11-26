#!/usr/bin/env bash
# sidecar.sh start|stop|kill9: the production qs-mirror against the STANDBY,
# configured by environment alone, as the plugin configures it in a Pod.
set -uo pipefail
. "$(dirname "$0")/env.sh"
pid() { cat "$BASE/mirror.pid" 2>/dev/null; }
case "${1:-}" in
start)
  T=${QS_TABLES_OVERRIDE:-public.orders,public.customers,public.qs_types$(for t in $EX_TABLES; do printf ',ex.%s' $t; done)}
  su postgres -c "QS_CLUSTER=qsint QS_MODE=shadow QS_INGEST=logical QS_DUCKLAKE=on \
    QS_DUCKLAKE_RETENTION=${RETENTION:-60s} QS_SERVE_WITH_HALTED_TABLES=on \
    QS_TABLES=$T QS_SLOT=qs_int QS_PUBLICATION=qs_int \
    QS_MIRROR_PATH=$MIRROR QS_FRESHNESS_SLO=30s ${SIDECAR_ENV:-} \
    QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$PPORT \
    QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=$SPORT \
    QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=qsint-2 \
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
*) echo "usage: $0 start|stop|kill9" >&2; exit 2 ;;
esac
