#!/usr/bin/env bash
# router.sh start [qs-router flags...] | stop — builds and runs qs-router on
# 5490 in front of setup.sh's primary.
set -uo pipefail
BASE=${BASE:-/var/lib/postgresql/qsrouter}
GO=$(cd "$(dirname "$0")/../../.." && pwd)
case "${1:-}" in
start)
  shift
  [ -n "${NOKILL:-}" ] || { pkill -f "^$BASE/qs-router " 2>/dev/null; sleep 0.2; }
  ( cd "$GO" && go build -o "$BASE/qs-router" ./cmd/qs-router ) || exit 1
  setsid ${PIN:-} "$BASE/qs-router" -listen 127.0.0.1:${RPORT:-5490} -backend 127.0.0.1:${PPORT:-5491} "$@" > "$BASE/router.${RPORT:-5490}.log" 2>&1 &
  for _ in $(seq 1 50); do
    /usr/lib/postgresql/17/bin/pg_isready -q -h 127.0.0.1 -p ${RPORT:-5490} && exit 0; sleep 0.1
  done
  cat "$BASE/router.${RPORT:-5490}.log"; exit 1 ;;
stop) pkill -f "^$BASE/qs-router "; exit 0 ;;
*) echo "usage: $0 start [flags]|stop" >&2; exit 2 ;;
esac
