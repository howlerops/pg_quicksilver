# common helpers for part 1
E=/tmp/claude-0/-home-user-pg-quicksilver/58894071-fd3a-5a32-94ab-41a0530826ff/scratchpad/e2e
PG=/usr/lib/postgresql/17/bin
BASE=/var/lib/postgresql/qse2e
PP=5450; SP=5451; DB=app
OUT=$E/out
psq() { su postgres -c "$PG/psql -X -h $BASE/sock -p ${2:-$PP} -U postgres -d ${3:-$DB} -Atc \"$1\""; }
# start_sc <name> <pod> <healthport> [extra env...]
start_sc() {
  local name=$1 pod=$2 hp=$3; shift 3
  env "$@" QS_CLUSTER=e2e QS_MODE=shadow QS_INGEST=logical QS_TABLES=public.items \
    QS_SLOT=qs_e2e QS_PUBLICATION=qs_e2e QS_MIRROR_PATH=$E/mirror_$name QS_FRESHNESS_SLO=15s \
    QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$PP QS_LOCAL_SOCKET_DIR=$BASE/sock QS_LOCAL_PORT=$SP \
    QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=$pod QS_HEALTH_ADDR=127.0.0.1:$hp \
    setsid nohup taskset -c 2-3 $E/bin/qs-mirror >> $OUT/sc_$name.log 2>&1 &
  echo $! > $OUT/sc_$name.pid
  echo "started $name pid $(cat $OUT/sc_$name.pid)"
}
stop_sc() { local p; p=$(cat $OUT/sc_$1.pid); kill ${2:--TERM} $p 2>/dev/null; for i in $(seq 100); do kill -0 $p 2>/dev/null || break; sleep 0.1; done; }
wait_ready() { for i in $(seq ${2:-600}); do curl -sf http://127.0.0.1:$1/readyz >/dev/null 2>&1 && return 0; sleep 0.1; done; echo "NOT READY on $1"; return 1; }
lsn_num() { python3 -c "a,b='$1'.split('/');print((int(a,16)<<32)|int(b,16))"; }
# wait_applied <name> [timeout s]: marker write, capture LSN, second marker; wait state.json applied_lsn >= mark
wait_applied() {
  local name=$1 mark at
  psq "UPDATE marker SET n=n+1" >/dev/null
  mark=$(lsn_num $(psq "SELECT pg_current_wal_lsn()::text"))
  psq "UPDATE items SET qty=qty WHERE seq=(SELECT min(seq) FROM items)" >/dev/null
  for i in $(seq $(( ${2:-120} * 10 ))); do
    at=$(python3 -c "import json;print(json.load(open('$E/mirror_$name/public.items/state.json')).get('applied_lsn') or '0/0')" 2>/dev/null)
    [ -n "$at" ] && [ "$(lsn_num $at)" -ge "$mark" ] && return 0
    sleep 0.1
  done
  echo "wait_applied TIMEOUT name=$name at=$at mark=$mark"; return 1
}
verify() { taskset -c 2-3 $E/bin/qs-verify -dsn "postgres://postgres@localhost:$PP/$DB" -mirror $E/mirror_$1 -table public.items 2>&1; }
slots() { psq "SELECT slot_name||' active='||active||' confirmed='||coalesce(confirmed_flush_lsn::text,'-')||' temp='||temporary FROM pg_replication_slots ORDER BY 1"; }
