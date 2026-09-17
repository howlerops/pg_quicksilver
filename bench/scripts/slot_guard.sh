#!/usr/bin/env bash
# Trip the slot guard on purpose, rather than by tripping over it.
#
# docs/28: a logical slot pins every WAL segment its consumer has not confirmed,
# so a mirror that cannot keep up fills the PRIMARY's disk. That was found by
# doing it — a 60-second workload took 6.6 GB of free space to 272 KB and
# stopped PostgreSQL — and the sidecar now bounds its own slot.
#
# Code that only runs during a failure is code that only runs when it is least
# affordable to be wrong. So this arranges the failure at a size that fits on a
# desk: a 16 MB limit instead of 4 GB, a workload that exceeds it in seconds,
# and four assertions about what must happen next.
#
#   1. the guard fires and says so
#   2. the slot is actually GONE — not merely reported as dropped
#   3. the PRIMARY is still serving, which is the entire point
#   4. the mirror rebuilds itself from a fresh snapshot
#
# Assertion 2 is the one worth having. The drop happens from the supervisor
# after the streaming connection dies, because PostgreSQL will not drop a slot
# that is in use, and a guard that logs "dropped" while the slot survives is
# worse than no guard at all — the primary keeps filling and the log says it is
# fine.
set -uo pipefail
cd "$(dirname "$0")/../.."

PG=/usr/lib/postgresql/17/bin
BASE=/var/lib/postgresql/qs17
PRIMARY=$BASE/primary
STANDBY=$BASE/standby
MIRROR=$BASE/guard-mirror
HEALTH=127.0.0.1:9194
DB=guard
ROWS=${ROWS:-400000}
LIMIT=${LIMIT:-16777216}          # 16 MB
FAIL=0

say()  { printf '\n=== %s ===\n' "$*"; }
ok()   { printf '  ok: %s\n' "$*"; }
bad()  { printf '  FAIL: %s\n' "$*"; FAIL=1; }
skip() { printf '\nINCOMPLETE — skipped, which is NOT a pass: %s\n' "$*"; exit 2; }
psq()  { su postgres -c "$PG/psql -h /tmp -p $1 -U postgres -d ${3:-$DB} -Atc \"$2\""; }

cleanup() {
  pkill -x qs-mirror 2>/dev/null
  psq 5443 "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active" postgres >/dev/null 2>&1
  psq 5443 "DROP DATABASE IF EXISTS $DB" postgres >/dev/null 2>&1
  return 0
}
trap cleanup EXIT

say "a primary, a standby and a small table"
psq 5443 "SELECT 1" postgres >/dev/null 2>&1 || \
  su postgres -c "$PG/pg_ctl -D $PRIMARY -l $BASE/primary.log -w start" >/dev/null 2>&1
psq 5443 "SELECT 1" postgres >/dev/null 2>&1 || skip "primary would not start"
psq 5443 "DROP DATABASE IF EXISTS $DB" postgres >/dev/null 2>&1
psq 5443 "CREATE DATABASE $DB" postgres >/dev/null || skip "could not create $DB"
psq 5443 "CREATE TABLE m(id bigint primary key, sku text, amount numeric(12,2), ts timestamptz)" >/dev/null
psq 5443 "INSERT INTO m SELECT g,'SKU-'||(g%1000),(g%997)/7.0,now() FROM generate_series(1,$ROWS) g" >/dev/null
[ "$(psq 5444 'SELECT pg_is_in_recovery()' postgres 2>/dev/null)" = "t" ] || \
  su postgres -c "$PG/pg_ctl -D $STANDBY -l $BASE/standby.log -w start" >/dev/null 2>&1
[ "$(psq 5444 'SELECT pg_is_in_recovery()' postgres 2>/dev/null)" = "t" ] \
  || skip "no standby in recovery — run e2e_mirror.sh or workload_matrix.sh first"
ok "$(printf "%'d" "$ROWS") rows, standby in recovery"

go -C go build -o /tmp/qs-mirror ./cmd/qs-mirror || skip "build failed"
chmod 755 /tmp/qs-mirror

say "a sidecar whose slot may hold only $(numfmt --to=iec "$LIMIT")"
rm -rf $MIRROR; install -d -o postgres -g postgres $MIRROR
psq 5443 "SELECT pg_drop_replication_slot('gd_slot') FROM pg_replication_slots WHERE slot_name='gd_slot'" >/dev/null 2>&1
psq 5443 "DROP PUBLICATION IF EXISTS gd_pub" >/dev/null 2>&1
su postgres -c "QS_CLUSTER=gd QS_MODE=shadow QS_INGEST=logical \
  QS_TABLES=public.m QS_SLOT=gd_slot QS_PUBLICATION=gd_pub \
  QS_MIRROR_PATH=$MIRROR QS_FRESHNESS_SLO=300s \
  QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=5443 \
  QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=5444 \
  QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=gd-2 \
  QS_MAX_SLOT_WAL_BYTES=$LIMIT QS_SLOT_WAL_INTERVAL=2s \
  QS_HEALTH_ADDR=$HEALTH /tmp/qs-mirror" > $BASE/guard.log 2>&1 &
for _ in $(seq 600); do curl -sf "http://$HEALTH/readyz" >/dev/null 2>&1 && break; sleep 0.1; done
curl -sf "http://$HEALTH/readyz" >/dev/null 2>&1 || { tail -8 $BASE/guard.log; skip "never became ready"; }
ok "bootstrapped and ready"

say "outrun it"
# Rewriting the whole table repeatedly produces WAL far faster than the mirror
# confirms it, which is the condition docs/28 is about, at a size that fits.
for i in $(seq 12); do
  psq 5443 "UPDATE m SET amount = amount + 1, ts = now()" >/dev/null 2>&1
  grep -q "dropping its own slot" $BASE/guard.log && break
  sleep 1
done
sleep 6

say "1. the guard fired, and said what it was doing"
if grep -q "dropping its own slot" $BASE/guard.log; then
  ok "$(grep -m1 'dropping its own slot' $BASE/guard.log | grep -o 'retained_bytes=[0-9]*')"
else
  bad "the guard never fired; the slot grew past $(numfmt --to=iec "$LIMIT") unchecked"
  tail -4 $BASE/guard.log | sed 's/^/    /'
fi

say "2. the slot is actually gone"
# The assertion that matters. The drop happens from the supervisor once the
# streaming connection is gone, and a guard that logs a drop it did not perform
# leaves the primary filling while the log says otherwise.
left=$(psq 5443 "SELECT count(*) FROM pg_replication_slots WHERE slot_name='gd_slot'" postgres)
if [ "$left" = "0" ]; then
  ok "gd_slot no longer exists, so no WAL is pinned on its behalf"
else
  bad "gd_slot still exists after the guard reported dropping it — the primary is still accumulating WAL"
fi

say "3. the primary is still serving"
if [ "$(psq 5443 "SELECT count(*) FROM m" 2>/dev/null)" = "$ROWS" ]; then
  ok "the primary answers, with all $(printf "%'d" "$ROWS") rows — which is the entire point"
else
  bad "the primary is not answering; the guard did not protect what it exists to protect"
fi

say "4. the mirror rebuilds itself"
# Losing a mirror costs a re-snapshot. That is the trade the guard makes, so it
# has to actually complete rather than leave a sidecar crash-looping.
for _ in $(seq 180); do
  [ "$(grep -c 'snapshot complete' $BASE/guard.log)" -ge 2 ] && break
  sleep 1
done
if [ "$(grep -c 'snapshot complete' $BASE/guard.log)" -ge 2 ]; then
  ok "a second snapshot completed: the mirror rebuilt on a fresh slot"
  go -C go build -o /tmp/qs-verify ./cmd/qs-verify && chmod 755 /tmp/qs-verify
  sleep 5
  if /tmp/qs-verify -dsn "postgres://postgres@localhost:5443/$DB" \
       -mirror "$MIRROR" -table public.m >/tmp/guard-verify.txt 2>&1; then
    ok "and it matches the source again"
  else
    bad "the rebuilt mirror does not match the source"
    head -5 /tmp/guard-verify.txt | sed 's/^/    /'
  fi
else
  bad "the mirror never rebuilt; the guard traded a mirror for nothing"
  tail -5 $BASE/guard.log | sed 's/^/    /'
fi

printf '\n========================================\n'
[ $FAIL -eq 0 ] && echo "PASS" || echo "FAIL"
exit $FAIL
