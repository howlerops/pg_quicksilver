#!/usr/bin/env bash
# sidecar.sh start|stop — the production qs-mirror sidecar for setup.sh's cluster.
set -uo pipefail
BASE=${BASE:-/var/lib/postgresql/qsrouter}
PPORT=${PPORT:-5491}; SPORT=${SPORT:-5492}; HEALTH=127.0.0.1:${HPORT:-5493}
case "${1:-}" in
start)
  su postgres -c "QS_CLUSTER=router QS_MODE=shadow QS_INGEST=logical \
    QS_TABLES=public.orders,public.customers QS_SLOT=qs_router QS_PUBLICATION=qs_router \
    QS_MIRROR_PATH=$BASE/mirror QS_FRESHNESS_SLO=30s \
    QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$PPORT \
    QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=$SPORT \
    QS_DATABASE=app QS_PGUSER=postgres QS_POD_NAME=router-2 \
    QS_HEALTH_ADDR=$HEALTH setsid $BASE/qs-mirror >> $BASE/mirror.log 2>&1 &"
  for _ in $(seq 1 300); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "http://$HEALTH/readyz" 2>/dev/null)" = 200 ] && exit 0
    sleep 1
  done
  tail -20 "$BASE/mirror.log"; exit 1 ;;
stop)
  # Anchored on this cluster's own binary: other qs-mirror processes on the
  # machine are someone else's.
  pkill -f "^$BASE/qs-mirror\$"; for _ in $(seq 1 50); do pgrep -f "^$BASE/qs-mirror\$" >/dev/null || exit 0; sleep 0.2; done; exit 1 ;;
*) echo "usage: $0 start|stop" >&2; exit 2 ;;
esac
