#!/usr/bin/env bash
# What does restarting the sidecar cost, and when is it actually paid?
#
# A CNPG instance Pod is restarted for ordinary reasons: a rolling update, a
# node drain, an image bump. The sidecar goes with it. The chart tells an
# operator how much memory to give that sidecar (docs/29, ~90 bytes of RSS per
# live row) and says nothing about what a restart costs, which is the other
# number an operator needs — because a readiness probe that passes too early
# puts a Pod into service that is about to stall.
#
# The suspicion this is built to test: the key index is built LAZILY, on the
# first change rather than at startup. Resuming against a 20.5M-row mirror, the
# sidecar sat at 25 MB of RSS and reported ready — and the first 20,000-row
# UPDATE took it to 1,806 MB. If readiness passes before the index exists, the
# restart cost is not paid at startup where it is visible; it is paid on the
# first write, behind a probe that already said yes.
#
#   ROWS=2000000 bash bench/scripts/restart_cost.sh
#   ROWS=20000000 bash bench/scripts/restart_cost.sh
#
# Reports, in the order an operator cares about:
#   1. how long until /readyz says yes
#   2. what RSS is at that moment
#   3. how long the FIRST change after a restart takes to apply
#   4. what RSS peaks at while that happens
set -uo pipefail
cd "$(dirname "$0")/../.."

PG=${PG:-/usr/lib/postgresql/17/bin}
BASE=${BASE:-/var/lib/postgresql/qs17}
MIRROR=$BASE/rc-mirror
DB=${DB:-restartcost}
ROWS=${ROWS:-2000000}
HEALTH=127.0.0.1:9296
SLOT=rc_slot
PUB=rc_pub
FAIL=0

say()  { printf '\n=== %s ===\n' "$*"; }
ok()   { printf '  %s\n' "$*"; }
bad()  { printf '  FAIL: %s\n' "$*"; FAIL=1; }
skip() { printf '\nINCOMPLETE — skipped, which is NOT a pass: %s\n' "$*"; exit 2; }
psq()  { su postgres -c "$PG/psql -h /tmp -p 5443 -U postgres -d ${2:-$DB} -Atc \"$1\""; }
rss()  { awk '/VmRSS/{print $2}' "/proc/$1/status" 2>/dev/null; }

cleanup() { pkill -x qs-mirror 2>/dev/null; return 0; }
trap cleanup EXIT

start_mirror() {
  su postgres -c "QS_CLUSTER=rc QS_MODE=shadow QS_INGEST=logical \
    QS_TABLES=public.m QS_SLOT=$SLOT QS_PUBLICATION=$PUB \
    QS_MIRROR_PATH=$MIRROR QS_FRESHNESS_SLO=120s \
    QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=5443 \
    QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=5444 \
    QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=rc-1 \
    QS_HEALTH_ADDR=$HEALTH /tmp/qs-mirror" >> "$BASE/rc.log" 2>&1 &
}

ready_wait() {  # seconds until /readyz says yes, or empty on timeout
  local t0 t1
  t0=$(date +%s.%N)
  for _ in $(seq 6000); do
    curl -sf "http://$HEALTH/readyz" >/dev/null 2>&1 && {
      t1=$(date +%s.%N); echo "$(echo "$t1-$t0" | bc)"; return 0; }
    sleep 0.02
  done
  return 1
}

say "infrastructure"
psq "SELECT 1" postgres >/dev/null 2>&1 || skip "no primary on 5443 — run setup_cluster.sh"
command -v bc >/dev/null || skip "bc is required"
go -C go build -o /tmp/qs-mirror ./cmd/qs-mirror || skip "build failed"
chmod 755 /tmp/qs-mirror
pkill -x qs-mirror 2>/dev/null; sleep 1
rm -f "$BASE/rc.log"

psq "DROP DATABASE IF EXISTS $DB" postgres >/dev/null 2>&1
psq "SELECT pg_drop_replication_slot('$SLOT') FROM pg_replication_slots WHERE slot_name='$SLOT'" postgres >/dev/null 2>&1
psq "CREATE DATABASE $DB" postgres >/dev/null || skip "could not create $DB"
rm -rf "$MIRROR"; install -d -o postgres -g postgres "$MIRROR"

say "seeding $(printf "%'d" "$ROWS") rows"
psq "CREATE TABLE m(id bigint primary key, sku text, amount numeric(12,2), ts timestamptz)" >/dev/null
done_rows=0
while [ "$done_rows" -lt "$ROWS" ]; do
  hi=$((done_rows + 2000000)); [ "$hi" -gt "$ROWS" ] && hi=$ROWS
  psq "INSERT INTO m SELECT g,'SKU-'||(g%100000),(g%997)/7.0,now()
       FROM generate_series($((done_rows+1)),$hi) g" >/dev/null || { bad "seed failed"; break; }
  done_rows=$hi
  psq "CHECKPOINT" >/dev/null 2>&1
done
ok "seeded $(printf "%'d" "$done_rows") rows"

say "first start — this one pays for the snapshot"
t0=$(date +%s.%N)
start_mirror
boot=$(ready_wait) || skip "the mirror never became ready"
MPID=$(pgrep -x qs-mirror | head -1)
printf '  ready in %.1fs (bootstrap), RSS %s MB\n' "$boot" "$(( $(rss "$MPID") / 1024 ))"

# Drive one change so the index is definitely built and the mirror is settled.
psq "UPDATE m SET amount = amount + 1 WHERE id BETWEEN 1 AND 1000" >/dev/null
sleep 8
settled_rss=$(( $(rss "$MPID") / 1024 ))
ok "after one change, RSS ${settled_rss} MB"

say "restart"
kill "$MPID" 2>/dev/null
for _ in $(seq 100); do kill -0 "$MPID" 2>/dev/null || break; sleep 0.1; done
ok "stopped"

start_mirror
rdy=$(ready_wait) || skip "the mirror never became ready after restart"
MPID=$(pgrep -x qs-mirror | head -1)
rss_ready=$(( $(rss "$MPID") / 1024 ))
printf '  ready in %.2fs, RSS %s MB\n' "$rdy" "$rss_ready"

say "the first change AFTER the restart"
# This is the measurement the whole script exists for. If the index is built
# lazily, the cost of the restart is not in the number above — it is here,
# behind a probe that has already said the Pod is ready to serve.
lsn_before=$(psq "SELECT pg_current_wal_lsn()" postgres)
t0=$(date +%s.%N)
psq "UPDATE m SET amount = amount + 1 WHERE id BETWEEN 1 AND 20000" >/dev/null
peak=0
applied=""
for _ in $(seq 1200); do
  r=$(rss "$MPID"); [ -n "$r" ] && [ "$r" -gt "$peak" ] && peak=$r
  lag=$(curl -s "http://$HEALTH/readyz" 2>/dev/null | grep -o '"lag_bytes":[0-9]*' | cut -d: -f2)
  if [ "$lag" = "0" ]; then applied=$(echo "$(date +%s.%N)-$t0" | bc); break; fi
  sleep 0.1
done
[ -n "$applied" ] || bad "the first change never drained"
printf '  applied in %.1fs, RSS peaked at %s MB\n' "${applied:-0}" "$((peak/1024))"

say "what this means for a Pod"
printf '  readiness said yes at  %6.2fs and %5s MB\n' "$rdy" "$rss_ready"
printf '  the first write took   %6.1fs and %5s MB\n' "${applied:-0}" "$((peak/1024))"
grow=$(( peak/1024 - rss_ready ))
if [ "$(echo "$applied > 2.0" | bc)" = "1" ] && [ "$grow" -gt 200 ]; then
  cat <<TXT

  The readiness probe passes BEFORE the index exists, so a restarted Pod is put
  into service and then stalls on its first write while it builds one. At this
  size that is ${applied}s of stall and ${grow} MB of growth that the probe
  already said was fine. An operator sizing a memory limit from what RSS looks
  like just after a restart will size it for ${rss_ready} MB and get OOM-killed
  at ${grow} MB more.
TXT
else
  ok "readiness tracks the real cost at this size; re-run with a larger ROWS"
fi

printf '\n========================================\n'
[ "$FAIL" = "0" ] && printf 'PASS\n' || printf 'FAIL\n'
exit "$FAIL"
