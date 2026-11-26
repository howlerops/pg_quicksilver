#!/usr/bin/env bash
# sidecar.sh start|stop: the production qs-mirror sidecar for setup.sh's cluster.
set -uo pipefail
. "$(dirname "$0")/env.sh"
case "${1:-}" in
start)
  T=public.orders,public.customers,public.qs_types; for t in $EX_TABLES; do T=$T,ex.$t; done
  su postgres -c "QS_CLUSTER=qsext QS_MODE=shadow QS_INGEST=logical \
    QS_TABLES=$T QS_SLOT=qs_ext QS_PUBLICATION=qs_ext \
    QS_MIRROR_PATH=$BASE/mirror QS_FRESHNESS_SLO=30s \
    QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$PPORT \
    QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=$SPORT \
    QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=qsext-2 \
    QS_HEALTH_ADDR=$HEALTH setsid $BASE/qs-mirror >> $BASE/mirror.log 2>&1 &"
  for _ in $(seq 1 300); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "http://$HEALTH/readyz" 2>/dev/null)" = 200 ] && exit 0
    sleep 1
  done
  tail -20 "$BASE/mirror.log"; exit 1 ;;
stop)
  # Anchored on this cluster's own binary: other qs-mirror processes are someone else's.
  pkill -f "^$BASE/qs-mirror\$"; for _ in $(seq 1 50); do pgrep -f "^$BASE/qs-mirror\$" >/dev/null || exit 0; sleep 0.2; done; exit 1 ;;
*) echo "usage: $0 start|stop" >&2; exit 2 ;;
esac
