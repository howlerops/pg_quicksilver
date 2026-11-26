#!/bin/bash
# Four-table memory reproduction: the item-1 mix (four tables, DuckLake on, four
# concurrent writers, TRUNCATE, ADD COLUMN, a check every minute; the driver is
# go/ducklake/validate/item1_mixed.py) with the sidecar's RSS sampled every second
# and Go heap profiles taken whenever RSS is above HEAP_DUMP_MB.
#
# Usage: memspike.sh TAG [DURATION_S]
#   QS_BIN       directory holding qs-mirror, qs-verify (default: the scratchpad bin)
#   CGROUP       if 1, the sidecar is moved into the v1 memory cgroup qsmem (set up by the caller)
#   HEAP_DUMP_MB RSS above which a heap profile is taken, at most one per 30 s (default 1200)
#
# Output: go/cmd/qs-mirror/validate/out/memspike_TAG/
#   sidecar.log   the sidecar's log      driver.txt   the driver's checks (JSON per check)
#   rss.csv       one row per second     dumps.txt    heap profiles taken and their RSS
#   heap.N.pb.gz / heap.N.debug1.txt     pprof heap profile and Go's own numbers
#
# Ports 5700-5702 (PostgreSQL, sidecar local port, health) and 6060 (pprof). Data under
# $BASE (default /var/lib/postgresql/qsmem), recreated on each run. Processes are stopped
# by the PIDs this script recorded, never by name.
set -u
VAL=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
GO=$(cd "$VAL/../../.." && pwd)
TAG=${1:?usage: memspike.sh TAG [DURATION_S]}
DUR=${2:-600}
SCRATCH=${QS_SCRATCH:-/tmp/claude-0/-home-user-pg-quicksilver/58894071-fd3a-5a32-94ab-41a0530826ff/scratchpad}
BIN=${QS_BIN:-$SCRATCH/bin}
PG=/usr/lib/postgresql/17/bin
BASE=${BASE:-/var/lib/postgresql/qsmem}
SOCK=$BASE/sock
PPORT=5700; SPORT=5701; HPORT=5702; DPORT=6060   # primary, local standby, health, pprof
OUT=$VAL/out/memspike_$TAG
HEAP_DUMP_MB=${HEAP_DUMP_MB:-1200}
TABLES=public.ev_uuid,public.ledger,public.wide_text,public.lookup
die() { echo "FAIL: $*" >&2; exit 1; }

rm -rf "$OUT" "$BASE"
mkdir -p "$OUT" "$BASE/sock"
chown -R postgres:postgres "$BASE"

pgsu() { su postgres -s /bin/bash -c "$1"; }

# Cluster: fresh, logical WAL, on PPORT.
pgsu "$PG/initdb -D $BASE/pg -U postgres -A trust >/dev/null" || die "initdb"
pgsu "$PG/pg_ctl -D $BASE/pg -l $BASE/pg.log -w -o '-p $PPORT -c unix_socket_directories=$SOCK -c listen_addresses=localhost -c wal_level=logical -c max_replication_slots=20 -c max_wal_senders=20' start >/dev/null" || die "pg start"
pg_stop() {
  pgsu "$PG/pg_ctl -D $BASE/standby -m fast -w stop >/dev/null 2>&1" || true
  pgsu "$PG/pg_ctl -D $BASE/pg -m fast -w stop >/dev/null 2>&1" || true
}
trap 'pg_stop' EXIT

pgsu "$PG/psql -X -q -h $SOCK -p $PPORT -U postgres -d postgres -c 'CREATE DATABASE app'" >/dev/null || die "createdb"
cat "$GO/ducklake/validate/item1_seed.sql" | pgsu "$PG/psql -X -q -v ON_ERROR_STOP=1 -h $SOCK -p $PPORT -U postgres -d app -f -" || die "seed"

# The sidecar checks its local server is a standby (the mirror is built on replicas),
# so a hot standby of the primary on SPORT is started; logical ingest still reads the primary.
pgsu "$PG/pg_basebackup -h localhost -p $PPORT -U postgres -D $BASE/standby -R -X stream --checkpoint=fast" >/dev/null 2>&1 || die "basebackup"
pgsu "$PG/pg_ctl -D $BASE/standby -l $BASE/standby.log -w -o '-p $SPORT -c unix_socket_directories=$SOCK -c listen_addresses=localhost -c wal_level=logical -c max_replication_slots=20 -c max_wal_senders=20' start >/dev/null" || die "standby start"

# Sidecar. Same environment as go/ducklake/validate/qs2_env.sh's sc_start, plus pprof.
mkdir -p "$BASE/mirror/item1"
nohup env QS_CLUSTER=memspike QS_MODE=shadow QS_INGEST=logical QS_TABLES=$TABLES \
  QS_SLOT=memspike_item1 QS_PUBLICATION=memspike_pub QS_MIRROR_PATH="$BASE/mirror/item1" QS_FRESHNESS_SLO=30s \
  QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$PPORT QS_LOCAL_SOCKET_DIR=$SOCK QS_LOCAL_PORT=$SPORT \
  QS_DATABASE=app QS_PGUSER=postgres QS_POD_NAME=item1 QS_HEALTH_ADDR=127.0.0.1:$HPORT \
  QS_DUCKLAKE=on QS_SLOW_TICK=1ms QS_DEBUG_ADDR=127.0.0.1:$DPORT \
  "$BIN/qs-mirror" >> "$OUT/sidecar.log" 2>&1 < /dev/null &
echo $! > "$OUT/sidecar.pid"
SC=$(cat "$OUT/sidecar.pid")
echo "sidecar pid=$SC"
for _ in $(seq 1200); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$HPORT/readyz)" = 200 ] && break
  sleep 0.1
done
[ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$HPORT/readyz)" = 200 ] || die "sidecar not ready"

if [ "${CGROUP:-0}" = 1 ]; then
  echo "$SC" > /sys/fs/cgroup/memory/qsmem/cgroup.procs || die "cgroup move"
  echo "sidecar moved into cgroup qsmem"
fi

# Sampler: VmRSS and VmHWM every second; a heap profile when RSS is above HEAP_DUMP_MB.
(
  echo "utc,rss_mb,hwm_mb" > "$OUT/rss.csv"
  : > "$OUT/dumps.txt"
  n=0; last=0
  while kill -0 "$SC" 2>/dev/null; do
    rss=$(awk '/^VmRSS/{print $2}' /proc/$SC/status 2>/dev/null)
    hwm=$(awk '/^VmHWM/{print $2}' /proc/$SC/status 2>/dev/null)
    [ -n "$rss" ] || break
    now=$(date -u +%s)
    echo "$(date -u +%H:%M:%S),$((rss/1024)),$((hwm/1024))" >> "$OUT/rss.csv"
    if [ $((rss/1024)) -ge "$HEAP_DUMP_MB" ] && [ $((now-last)) -ge 30 ]; then
      n=$((n+1)); last=$now
      curl -s "http://127.0.0.1:$DPORT/debug/pprof/heap" > "$OUT/heap.$n.pb.gz"
      curl -s "http://127.0.0.1:$DPORT/debug/pprof/heap?debug=1" > "$OUT/heap.$n.debug1.txt"
      echo "heap.$n at $(date -u +%H:%M:%S) rss_mb=$((rss/1024)) hwm_mb=$((hwm/1024))" >> "$OUT/dumps.txt"
    fi
    sleep 1
  done
) &
SAMPLER=$!

export BASE QS_PG_DSN="postgresql://postgres@localhost:$PPORT/app?sslmode=disable" QS_VERIFY_BIN=$BIN/qs-verify
ITEM1_DURATION=$DUR python3 "$GO/ducklake/validate/item1_mixed.py" "$BASE/mirror/item1" "$OUT/driver.txt" "$OUT/sidecar.pid"
rc=$?

kill -TERM "$SC" 2>/dev/null
for _ in $(seq 600); do kill -0 "$SC" 2>/dev/null || break; sleep 0.1; done
wait "$SAMPLER" 2>/dev/null
echo "driver exit $rc"
exit $rc
