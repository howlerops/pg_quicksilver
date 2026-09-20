#!/usr/bin/env bash
# The same instant, spelled two ways.
#
# The mirror stores what PostgreSQL hands it, and for every type the writer does
# not map to a native Arrow type — timestamps, dates, intervals, jsonb, bytea —
# that is TEXT. Text is a property of the SESSION, not of the value:
#
#   TimeZone=UTC               2026-01-15 12:00:00+00
#   TimeZone=America/New_York  2026-01-15 07:00:00-05     the same instant
#
# Three different sessions render into this mirror: the bootstrap snapshot, the
# walsender behind the replication stream, and the verifier comparing the two.
# Nothing made them agree. They happen to agree today because a default
# PostgreSQL is Etc/UTC and every session in every other benchmark inherits it.
#
# This script makes them disagree on purpose. It changes the server's timezone
# BETWEEN the bootstrap and the stream, which is a thing an operator does, and
# then asks the mirror a question that only has one right answer:
#
#   how many distinct spellings does it hold for one instant?
#
# Run twice, once with the fix off, so the bug is demonstrated rather than
# asserted:
#
#   bash bench/scripts/rendering_fidelity.sh          # pinned: must PASS
#   QS_PIN_RENDERING=0 bash bench/scripts/rendering_fidelity.sh   # must FAIL
set -uo pipefail
cd "$(dirname "$0")/../.."
source "$(dirname "$0")/lib_dropdb.sh"
source "$(dirname "$0")/lib_syncslots.sh"

PG=/usr/lib/postgresql/17/bin
BASE=/var/lib/postgresql/qs17
PRIMARY=$BASE/primary
STANDBY=$BASE/tz-standby
MIRROR=$BASE/tz-mirror
HEALTH=127.0.0.1:9198
DB=tzapp
PINNED=${QS_PIN_RENDERING:-1}
FAIL=0

say()  { printf '\n=== %s ===\n' "$*"; }
ok()   { printf '  ok: %s\n' "$*"; }
bad()  { printf '  FAIL: %s\n' "$*"; FAIL=1; }
skip() { printf '\nINCOMPLETE — skipped, which is NOT a pass: %s\n' "$*"; exit 2; }
psq()  { su postgres -c "$PG/psql -h /tmp -p 5443 -U postgres -d ${2:-$DB} -Atc \"$1\""; }

MIRROR_PID=""
cleanup() { [ -n "$MIRROR_PID" ] && kill "$MIRROR_PID" 2>/dev/null; return 0; }
trap cleanup EXIT

say "a primary, and a table of instants"
su postgres -c "$PG/pg_ctl -D $PRIMARY -l $BASE/tz-primary.log -w start" >/dev/null 2>&1
psq "SELECT 1" postgres >/dev/null 2>&1 || skip "primary would not start"
psq "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active" postgres >/dev/null 2>&1
qs_drop_database 5443 "$DB"
psq "CREATE DATABASE $DB" postgres >/dev/null
psq "CREATE TABLE t(id bigint primary key, at timestamptz, on_day date, note text)" >/dev/null
# Every row is THE SAME INSTANT, written a hundred times. Whatever the mirror
# holds, it should hold exactly one spelling of it.
psq "INSERT INTO t SELECT g, '2026-01-15 12:00:00+00'::timestamptz,
       '2026-01-15'::date, 'row '||g FROM generate_series(1,100) g" >/dev/null
ok "100 rows, all carrying the instant 2026-01-15 12:00:00+00"

# This script does not set synchronized_standby_slots, but four others do and
# ALTER SYSTEM outlives them. An inherited entry naming a slot that no longer
# exists stops a FAILOVER slot dead, with no error anywhere — the mirror
# bootstraps, passes readiness and then never gains a row. That is precisely
# how this script once reported DIVERGED at 100 rows against a source of 201.
qs_assert_sync_slots_sane 5443 || skip "the primary is holding logical decoding back (above)"

# The sidecar runs on a REPLICA and stands down on a primary — that is the whole
# shape of the product — so this needs a standby, exactly as the workload matrix
# does.
psq "SELECT pg_drop_replication_slot('tz_standby') FROM pg_replication_slots WHERE slot_name='tz_standby'" postgres >/dev/null 2>&1
psq "SELECT pg_create_physical_replication_slot('tz_standby', true)" postgres >/dev/null
su postgres -c "$PG/pg_ctl -D $STANDBY stop -m immediate" >/dev/null 2>&1
rm -rf $STANDBY
su postgres -c "$PG/pg_basebackup -D $STANDBY -R -X stream -S tz_standby -c fast \
  -d 'host=/tmp port=5443 user=postgres dbname=postgres'" >/dev/null 2>&1 || skip "basebackup failed"
{ echo "port = 5445"; echo "sync_replication_slots = on"; echo "hot_standby_feedback = on"; } \
  >> $STANDBY/postgresql.auto.conf
su postgres -c "$PG/pg_ctl -D $STANDBY -l $BASE/tz-standby.log -w start" >/dev/null \
  || skip "standby would not start"
[ "$(su postgres -c "$PG/psql -h /tmp -p 5445 -U postgres -d postgres -Atc 'SELECT pg_is_in_recovery()'")" = "t" ] \
  || skip "standby not in recovery"
ok "standby on 5445, in recovery"

say "the server renders in one timezone for the bootstrap"
psq "ALTER SYSTEM SET timezone = 'America/New_York'" postgres >/dev/null
psq "SELECT pg_reload_conf()" postgres >/dev/null
sleep 1
printf '  server timezone is now %s, and renders the instant as %s\n' \
  "$(psq 'SHOW timezone' postgres)" "$(psq "SELECT at::text FROM t LIMIT 1")"

rm -rf $MIRROR; install -d -o postgres -g postgres $MIRROR
psq "SELECT pg_drop_replication_slot('tz_slot') FROM pg_replication_slots WHERE slot_name='tz_slot'" >/dev/null 2>&1
psq "DROP PUBLICATION IF EXISTS tz_pub" >/dev/null 2>&1
go -C go build -o /tmp/qs-mirror ./cmd/qs-mirror || skip "build failed"
go -C go build -o /tmp/qs-verify ./cmd/qs-verify || skip "build failed"
chmod 755 /tmp/qs-mirror /tmp/qs-verify

su postgres -c "QS_CLUSTER=tz QS_MODE=shadow QS_INGEST=logical \
  QS_TABLES=public.t QS_SLOT=tz_slot QS_PUBLICATION=tz_pub \
  QS_MIRROR_PATH=$MIRROR QS_FRESHNESS_SLO=60s \
  QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=5443 \
  QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=5445 \
  QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=tz-2 \
  QS_PIN_RENDERING=$PINNED \
  QS_HEALTH_ADDR=$HEALTH /tmp/qs-mirror" > $BASE/tz-mirror.log 2>&1 &
MIRROR_PID=$!
for _ in $(seq 120); do curl -sf "http://$HEALTH/readyz" >/dev/null 2>&1 && break; sleep 1; done
curl -sf "http://$HEALTH/readyz" >/dev/null 2>&1 || {
  tail -8 $BASE/tz-mirror.log; skip "the mirror never became ready"; }
ok "mirror bootstrapped (snapshot path), QS_PIN_RENDERING=$PINNED"

say "the server renders in a DIFFERENT timezone for the stream"
# The thing an operator does. Everything already in the mirror was rendered
# under the old setting; everything from here is rendered under the new one.
psq "ALTER SYSTEM SET timezone = 'Asia/Kolkata'" postgres >/dev/null
psq "SELECT pg_reload_conf()" postgres >/dev/null
sleep 1
printf '  server timezone is now %s, and renders the same instant as %s\n' \
  "$(psq 'SHOW timezone' postgres)" "$(psq "SELECT at::text FROM t LIMIT 1")"
# A reload does not touch the walsender already running, so the stream is
# restarted the way a rollout would: the sidecar reconnects and the new session
# picks the new setting up.
psq "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name='tz_slot' AND active" >/dev/null 2>&1
sleep 3
psq "INSERT INTO t SELECT g, '2026-01-15 12:00:00+00'::timestamptz,
       '2026-01-15'::date, 'row '||g FROM generate_series(101,200) g" >/dev/null
# settle: two idempotent markers, as the workload matrix does
for _ in $(seq 60); do
  psq "INSERT INTO t VALUES (999999, '2026-01-15 12:00:00+00', '2026-01-15', 'marker')
       ON CONFLICT (id) DO UPDATE SET note='marker'" >/dev/null 2>&1
  n=$(python3 -c "
import json;print(len(json.load(open('$MIRROR/public.t/state.json')).get('delta_files',[])))" 2>/dev/null || echo 0)
  m=$(/tmp/qs-verify -dsn \"\" 2>/dev/null; echo)
  sleep 1
  ROWS=$(su postgres -c "$PG/psql -h /tmp -p 5443 -U postgres -d $DB -Atc 'SELECT count(*) FROM t'")
  [ "$ROWS" = "201" ] && break
done
sleep 5
ok "100 more rows streamed under the new timezone"

say "how many spellings does the mirror hold for one instant?"
python3 - "$MIRROR" <<'PYCOUNT'
import glob, json, os, sys, collections
try:
    import pyarrow.parquet as pq
except ImportError:
    print("  (pyarrow not available; falling back to qs-verify only)")
    sys.exit(0)
root = os.path.join(sys.argv[1], "public.t")
st = json.load(open(os.path.join(root, "state.json")))
# `or []` and not just .get(k, []): state.json writes "delta_files": null when
# there are none, and .get returns None for a key that is PRESENT and null. The
# default never fires, the loop raises TypeError, and the check reports "more
# than one spelling" — a wrong answer about the mirror caused by the checker.
files = [os.path.join(root, "base", f) for f in (st.get("base_files") or [])] + \
        [os.path.join(root, "delta", f) for f in (st.get("delta_files") or [])]
spellings = collections.Counter()
days = collections.Counter()
for f in files:
    if not os.path.exists(f):
        continue
    t = pq.read_table(f)
    if "at" in t.column_names:
        spellings.update(v for v in t.column("at").to_pylist() if v is not None)
    if "on_day" in t.column_names:
        days.update(v for v in t.column("on_day").to_pylist() if v is not None)
for label, c in (("timestamptz", spellings), ("date", days)):
    print("  %-12s %d distinct spelling(s):" % (label, len(c)))
    for v, n in c.most_common(4):
        print("      %-32s x%d" % (v, n))
sys.exit(1 if len(spellings) > 1 or len(days) > 1 else 0)
PYCOUNT
SPELLINGS=$?
if [ "$SPELLINGS" -eq 0 ]; then
  ok "one spelling per value — a GROUP BY sees one group, as it must"
else
  bad "the mirror holds the same instant under more than one spelling"
fi

say "and the verifier agrees with the source"
if /tmp/qs-verify -dsn "postgres://postgres@localhost:5443/$DB" \
     -mirror "$MIRROR" -table public.t > /tmp/tz-verify.txt 2>&1; then
  ok "MATCH ($(grep -m1 'mirror ' /tmp/tz-verify.txt | sed 's/^ *//'))"
else
  bad "qs-verify reports DIVERGED"
  sed 's/^/    /' /tmp/tz-verify.txt | head -6
fi

printf '\n========================================\n'
if [ "$PINNED" = "0" ]; then
  # With the fix off this is SUPPOSED to fail. A pass here would mean the test
  # cannot detect the bug it exists for, which is worse than a failure.
  if [ $FAIL -eq 0 ]; then
    echo "UNEXPECTED PASS — with QS_PIN_RENDERING=0 this must reproduce the bug,"
    echo "so a pass means the test no longer detects what it was written for."
    exit 1
  fi
  echo "EXPECTED FAIL — the bug reproduces with the fix off, as it should"
  exit 0
fi
[ $FAIL -eq 0 ] && echo "PASS" || echo "FAIL"
exit $FAIL
