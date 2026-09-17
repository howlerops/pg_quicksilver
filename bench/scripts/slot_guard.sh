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
# desk: a 64 KB limit instead of 4 GB, a workload that exceeds it in seconds,
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

# 64 KB, not 16 MB, and the first version of this got that wrong.
#
# At 16 MB the guard never fired and the test reported a failure — but the
# failure was the TEST's. Twelve whole-table updates over about a minute, into a
# mirror draining ninety-odd thousand changes a second, is a mirror that KEEPS
# UP: the slot confirmed as fast as the workload produced, retention never grew,
# and the guard correctly did nothing.
#
# Which is the wrong experiment. Reproducing "the mirror falls behind" is what
# docs/28 already did, expensively, by taking the database down. What needs
# testing here is the MECHANISM — does the guard fire, does the slot actually
# go, does the primary survive, does the mirror come back — and that wants a
# threshold any lag at all will cross, not a realistic one.
LIMIT=${LIMIT:-65536}
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

say "produce more WAL than the limit allows"
# The limit is 64 KB, so a single ordinary write crosses it. What is being
# tested is the guard's behaviour once it decides to act, not the arithmetic
# that gets it there.
for i in $(seq 30); do
  psq 5443 "UPDATE m SET amount = amount + 1, ts = now() WHERE id % 97 = 0" >/dev/null 2>&1
  grep -q "dropping its own slot" $BASE/guard.log && break
  sleep 1
done
sleep 8
if grep -q "cannot measure its own slot" $BASE/guard.log; then
  bad "the guard could not measure its slot at all — it is not guarding anything"
  grep -m1 "cannot measure" $BASE/guard.log | cut -c1-200 | sed 's/^/    /'
fi

say "1. the guard fired, and said what it was doing"
if grep -q "dropping its own slot" $BASE/guard.log; then
  ok "$(grep -m1 'dropping its own slot' $BASE/guard.log | grep -o 'retained_bytes=[0-9]*')"
else
  bad "the guard never fired; the slot grew past $(numfmt --to=iec "$LIMIT") unchecked"
  tail -4 $BASE/guard.log | sed 's/^/    /'
fi

say "2. the slot was actually dropped, and the WAL came back"
# The assertion that matters, and the first version of it asked the wrong
# question: "does a slot named gd_slot exist?"  It does — the mirror REBUILDS
# after the guard fires, and the rebuild creates a slot with the same name.
# Checking existence by name cannot distinguish "never dropped" from "dropped
# and legitimately recreated", so it reported a failure while the product was
# behaving exactly as designed.
#
# What actually has to be true is that the supervisor performed the drop rather
# than only deciding to — the guard runs inside the apply loop, which cannot
# drop a slot its own stream is holding — and that the WAL the slot pinned is
# no longer pinned.
if grep -q "dropped this mirror's replication slot" $BASE/guard.log; then
  ok "the supervisor performed the drop, not just the guard deciding to"
elif grep -q "could not drop the runaway slot" $BASE/guard.log; then
  bad "the guard decided to drop the slot and the drop FAILED — the primary is still accumulating WAL"
  grep -m1 "could not drop" $BASE/guard.log | cut -c1-160 | sed 's/^/    /'
else
  bad "no drop was attempted after the guard fired"
fi
retained=$(psq 5443 "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),0)::bigint
                     FROM pg_replication_slots WHERE slot_name='gd_slot'" postgres)
if [ -z "$retained" ]; then
  ok "no gd_slot exists at all right now, so nothing is pinned on its behalf"
elif [ "$retained" -lt $((LIMIT * 64)) ]; then
  ok "the current gd_slot (rebuilt) pins $(numfmt --to=iec "$retained"), not the runaway amount"
else
  bad "a slot named gd_slot is pinning $(numfmt --to=iec "$retained") — the drop did not take effect"
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
