#!/bin/bash
# Shared helpers for the qs-mirror validation scripts. Sourced, not run.
#
# The PostgreSQL servers run as the postgres OS user, which cannot read the
# session scratchpad, so every data directory lives under $BASE. The sidecars
# run as root from $BIN. Raw output of each demonstration goes to $OUT.
set -u
VAL=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT=$VAL/out
mkdir -p "$OUT"
SCRATCH=${QS_SCRATCH:-/tmp/claude-0/-home-user-pg-quicksilver/58894071-fd3a-5a32-94ab-41a0530826ff/scratchpad}
BIN=${QS_BIN:-$SCRATCH/bin}          # qs-mirror, qs-verify, qs-query
PG=/usr/lib/postgresql/17/bin
BASE=/var/lib/postgresql/qsy1
DB=app
BASE_SLOT=qs_v1                      # QS_SLOT, the base of every per-Pod slot
PUB=qs_v1                            # QS_PUBLICATION

# pgu CMD: run CMD as the postgres OS user.
pgu() { su postgres -s /bin/bash -c "$1"; }

# sql PORT DB: run SQL read from stdin on the server at PORT; prints tuples.
sql() {
  cat | su postgres -s /bin/bash -c "$PG/psql -X -A -t -q -v ON_ERROR_STOP=1 -h $BASE/sock -p $1 -U postgres -d $2 -f -"
}

# pg_up NAME PORT: start the server in $BASE/NAME, quietly.
pg_up() {
  pgu "$PG/pg_ctl -D $BASE/$1 -l $BASE/$1.log -w -o '-c port=$2 -c unix_socket_directories=$BASE/sock -c listen_addresses=localhost' start" >/dev/null
}

# pg_down NAME: fast stop of the server in $BASE/NAME.
pg_down() {
  pgu "$PG/pg_ctl -D $BASE/$1 -m fast -w stop" >/dev/null 2>&1 || true
}

# start_sc NAME POD HEALTH_PORT LOCAL_PORT PRIMARY_PORT [VAR=VAL ...]
# Starts a sidecar in the background as POD, records its PID in $OUT/NAME.pid,
# and appends its log to $OUT/NAME.log.
start_sc() {
  local name=$1 pod=$2 hp=$3 lp=$4 pp=$5; shift 5
  mkdir -p "$BASE/mirror/$name"
  nohup env QS_CLUSTER=qsv QS_MODE=shadow QS_INGEST=logical QS_TABLES="${TABLES:-public.t1}" \
    QS_SLOT=$BASE_SLOT QS_PUBLICATION=$PUB QS_MIRROR_PATH="$BASE/mirror/$name" QS_FRESHNESS_SLO=15s \
    QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$pp QS_LOCAL_SOCKET_DIR=$BASE/sock QS_LOCAL_PORT=$lp \
    QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=$pod QS_HEALTH_ADDR=127.0.0.1:$hp "$@" \
    "$BIN/qs-mirror" >> "$OUT/$name.log" 2>&1 < /dev/null &
  echo $! > "$OUT/$name.pid"
  echo "started sidecar $name pod=$pod pid=$(cat "$OUT/$name.pid")"
}

# stop_sc NAME [SIGNAL]: signals the PID recorded by start_sc, and waits.
stop_sc() {
  local p; p=$(cat "$OUT/$1.pid" 2>/dev/null) || return 0
  kill "${2:--TERM}" "$p" 2>/dev/null || return 0
  for _ in $(seq 300); do kill -0 "$p" 2>/dev/null || break; sleep 0.1; done
  echo "stopped sidecar $1 pid=$p"
}

# ready PORT [SECONDS]: wait for the health endpoint's /readyz to answer 200.
ready() {  # ready PORT [SECONDS]; a healthz for a primary, which never reports ready
  for _ in $(seq $(( ${2:-60} * 10 ))); do
    curl -sf "http://127.0.0.1:$1/readyz" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "NOT READY on $1"; return 1
}

# verify MIRROR_NAME TABLE PRIMARY_PORT: qs-verify of one table, prints MATCH/DIVERGED.
verify() {
  "$BIN/qs-verify" -dsn "postgres://postgres@localhost:$3/$DB?sslmode=disable" \
    -mirror "$BASE/mirror/$1" -table "$2" 2>&1
}

# lsn_of MIRROR_NAME TABLE: applied LSN of a table's mirror.
applied_lsn() { "$BIN/qs-query" -mirror "$BASE/mirror/$1" -table "$2" -lsn 2>/dev/null; }

# lsn_num LSN: a PostgreSQL LSN ("X/Y") as an integer, for comparison.
lsn_num() { python3 -c "a,b='$1'.split('/');print((int(a,16)<<32)|int(b,16))"; }

# wait_mirror NAME TABLE TARGET_LSN [SECONDS]: until the mirror's applied LSN is
# at or past TARGET_LSN.
wait_mirror() {
  local at
  for _ in $(seq $(( ${4:-120} * 10 ))); do
    at=$(applied_lsn "$1" "$2")
    [ -n "$at" ] && [ "$(lsn_num "$at")" -ge "$(lsn_num "$3")" ] && return 0
    sleep 0.1
  done
  echo "wait_mirror TIMEOUT $1 $2 applied=${at:-none} target=$3"; return 1
}
