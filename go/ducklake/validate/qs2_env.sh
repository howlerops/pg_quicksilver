# shellcheck shell=bash
# Shared by the item scripts of the multi-table, rebuild and pooler demonstrations.
# Everything they start lives under $BASE and on ports 5660-5669. Sidecars are
# started here and their PIDs recorded in $OUT; nothing is killed by name.
PG=/usr/lib/postgresql/17/bin
BASE=${BASE:-/var/lib/postgresql/qslake2}
SOCK=$BASE/sock
PPORT=5660; SPORT=5661; HPORT=${HPORT:-5662}
DB=app
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT=$HERE/out
mkdir -p "$OUT"
SCR=${QS_SCRATCH:-/tmp/claude-0/-home-user-pg-quicksilver/58894071-fd3a-5a32-94ab-41a0530826ff/scratchpad}
BIN=$SCR/bin
die() { echo "FAIL: $*" >&2; exit 1; }
# psq PORT DB SQL: run SQL as the postgres OS user, tuples only.
psq() { su postgres -s /bin/bash -c "$PG/psql -X -A -t -q -v ON_ERROR_STOP=1 -h $SOCK -p $1 -U postgres -d $2 -c \"$3\""; }
# sc_start NAME PIDFILE_TAG [VAR=VAL ...]: start a sidecar in the background (as root,
# as the other validation scripts do) and record its PID in $OUT/<tag>.pid.
sc_start() {
  local name=$1 tag=$2; shift 2
  mkdir -p "$BASE/mirror/$name"
  nohup env QS_CLUSTER=qs2 QS_MODE=shadow QS_INGEST=logical QS_TABLES="${TABLES:-public.ev_uuid}" \
    QS_SLOT=qs2_$name QS_PUBLICATION=qs2_pub QS_MIRROR_PATH="$BASE/mirror/$name" QS_FRESHNESS_SLO=30s \
    QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$PPORT QS_LOCAL_SOCKET_DIR=$SOCK QS_LOCAL_PORT=$SPORT \
    QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=$name QS_HEALTH_ADDR=127.0.0.1:$HPORT \
    "$@" "${QS_MIRROR_BIN:-$BIN/qs-mirror}" >> "$OUT/$tag.log" 2>&1 < /dev/null &
  echo $! > "$OUT/$tag.pid"
  echo "started sidecar $tag pid=$(cat "$OUT/$tag.pid")"
}
# sc_ready: wait until /readyz answers 200 (up to 120 s).
sc_ready() {
  for _ in $(seq 1200); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$HPORT/readyz)" = 200 ] && return 0
    sleep 0.1
  done
  return 1
}
# sc_stop TAG [SIGNAL]: signal the PID recorded for TAG and wait for it to exit.
sc_stop() {
  local p; p=$(cat "$OUT/$1.pid" 2>/dev/null) || return 0
  kill "${2:--TERM}" "$p" 2>/dev/null || return 0
  for _ in $(seq 600); do kill -0 "$p" 2>/dev/null || break; sleep 0.1; done
  echo "stopped sidecar $1 pid=$p"
}
