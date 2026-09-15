#!/usr/bin/env bash
# End-to-end run of the PRODUCTION sidecar (cmd/qs-mirror) against a real
# PostgreSQL 17 primary + streaming standby, in the shape the CNPG-I plugin
# deploys it: the sidecar sits on a replica, streams from the primary, gates its
# own readiness, and stands down if its node is promoted.
#
# This is not the harness. qs-phase1 and qs-phase2 drive the pipeline from a test
# program; here the sidecar binary runs on its own, configured only by the
# environment variables internal/plugin/lifecycle.go sets, and everything is
# observed from outside through /readyz, /metrics and qs-verify.
#
# A section that cannot run exits 2 as INCOMPLETE. It must never report success.
set -uo pipefail

PG=/usr/lib/postgresql/17/bin
BASE=/var/lib/postgresql/qs17
PRIMARY=$BASE/primary
STANDBY=$BASE/standby
MIRROR=$BASE/e2e-mirror
GO=/home/user/pg_quicksilver/go
HEALTH=127.0.0.1:9199
DB=app
FAIL=0

say()  { printf '\n== %s ==\n' "$*"; }
bad()  { printf 'FAIL: %s\n' "$*"; FAIL=1; }
skip() { printf '\nINCOMPLETE — section skipped, which is NOT a pass: %s\n' "$*"; exit 2; }
psq()  { su postgres -c "$PG/psql -h /tmp -p $1 -U postgres -d ${3:-$DB} -Atc \"$2\""; }

cleanup() { [ -n "${MIRROR_PID:-}" ] && kill "$MIRROR_PID" 2>/dev/null; }
trap cleanup EXIT

say "0. primary on 5443, listening on localhost"
su postgres -c "$PG/pg_ctl -D $STANDBY stop -m immediate" >/dev/null 2>&1
su postgres -c "$PG/pg_ctl -D $PRIMARY stop -m immediate"  >/dev/null 2>&1
grep -q "^listen_addresses = 'localhost'" $PRIMARY/postgresql.conf \
  || echo "listen_addresses = 'localhost'" >> $PRIMARY/postgresql.conf
# ALTER SYSTEM from an earlier run must not leak into this one.
rm -f $PRIMARY/postgresql.auto.conf; touch $PRIMARY/postgresql.auto.conf
chown postgres:postgres $PRIMARY/postgresql.auto.conf
su postgres -c "$PG/pg_ctl -D $PRIMARY -l $BASE/primary.log -w start" >/dev/null || skip "primary would not start"
psq 5443 "SELECT 1" postgres >/dev/null || skip "primary not reachable"

psq 5443 "DROP DATABASE IF EXISTS $DB" postgres >/dev/null
psq 5443 "CREATE DATABASE $DB" postgres >/dev/null
psq 5443 "CREATE TABLE events(id bigint primary key, sku text, amount numeric(12,2), ts timestamptz)" >/dev/null
psq 5443 "INSERT INTO events SELECT g,'SKU-'||g,(g%997)/7.0,now() FROM generate_series(1,20000) g" >/dev/null
echo "  seeded $(psq 5443 'SELECT count(*) FROM events') rows BEFORE the mirror exists"

say "1. standby with sync_replication_slots"
psq 5443 "SELECT pg_create_physical_replication_slot('e2e_standby', true)" postgres >/dev/null
psq 5443 "ALTER SYSTEM SET synchronized_standby_slots = 'e2e_standby'" postgres >/dev/null
psq 5443 "SELECT pg_reload_conf()" postgres >/dev/null
rm -rf $STANDBY
su postgres -c "$PG/pg_basebackup -D $STANDBY -R -X stream -S e2e_standby \
  -d 'host=/tmp port=5443 user=postgres dbname=postgres'" || skip "pg_basebackup failed"
{ echo "port = 5444"; echo "sync_replication_slots = on"; echo "hot_standby_feedback = on"; } \
  >> $STANDBY/postgresql.auto.conf
su postgres -c "$PG/pg_ctl -D $STANDBY -l $BASE/standby.log -w start" >/dev/null || skip "standby would not start"
sleep 2
[ "$(psq 5444 'SELECT pg_is_in_recovery()' postgres)" = "t" ] || skip "standby is not in recovery"
echo "  standby up and in recovery"

say "2. the sidecar, configured only by the plugin's environment"
rm -rf $MIRROR; install -d -o postgres -g postgres $MIRROR
cd $GO && go build -o /tmp/qs-mirror ./cmd/qs-mirror || skip "build failed"
go build -o /tmp/qs-verify ./cmd/qs-verify || skip "build failed"
chmod 755 /tmp/qs-mirror /tmp/qs-verify

su postgres -c "QS_CLUSTER=e2e QS_MODE=shadow QS_INGEST=logical \
  QS_TABLES=public.events QS_SLOT=qs_e2e QS_PUBLICATION=qs_e2e \
  QS_MIRROR_PATH=$MIRROR QS_FRESHNESS_SLO=15s \
  QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=5443 \
  QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=5444 \
  QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=e2e-2 \
  QS_HEALTH_ADDR=$HEALTH /tmp/qs-mirror" > $BASE/mirror.log 2>&1 &
MIRROR_PID=$!

for _ in $(seq 60); do
  curl -sf "http://$HEALTH/healthz" >/dev/null 2>&1 && break
  sleep 1
done
curl -sf "http://$HEALTH/healthz" >/dev/null || { tail -20 $BASE/mirror.log; skip "sidecar never became live"; }
echo "  sidecar live; /healthz answering"

say "3. it bootstraps by snapshot and becomes ready"
ready=""
for _ in $(seq 90); do
  if curl -sf "http://$HEALTH/readyz" >/dev/null 2>&1; then ready=yes; break; fi
  sleep 1
done
[ -n "$ready" ] || { tail -30 $BASE/mirror.log; bad "sidecar never reported ready"; }
grep -q "snapshot complete" $BASE/mirror.log \
  && echo "  $(grep -m1 'snapshot complete' $BASE/mirror.log | sed 's/.*msg=//')" \
  || bad "no snapshot was taken, yet 20000 rows predate the slot"

say "4. live changes reach the mirror"
psq 5443 "INSERT INTO events SELECT g,'NEW-'||g,(g%31)/3.0,now() FROM generate_series(100001,102000) g" >/dev/null
psq 5443 "UPDATE events SET amount = amount + 1 WHERE id % 1000 = 0" >/dev/null
psq 5443 "DELETE FROM events WHERE id BETWEEN 1 AND 500" >/dev/null
sleep 8
/tmp/qs-verify -dsn "postgres://postgres@localhost:5443/$DB" -mirror $MIRROR -table public.events \
  || bad "mirror diverged from the source after live DML"

say "5. DDL crosses as a barrier, not as corruption"
psq 5443 "ALTER TABLE events ADD COLUMN channel text DEFAULT 'web'" >/dev/null
psq 5443 "INSERT INTO events VALUES (200001,'POST-DDL',1.00,now(),'mobile')" >/dev/null
sleep 8
if grep -q "schema evolved at a DDL barrier" $BASE/mirror.log; then
  echo "  barrier observed: $(grep -m1 'schema evolved' $BASE/mirror.log | sed 's/.*change=//')"
else
  bad "no DDL barrier was observed; post-DDL rows would carry the pre-DDL column list"
fi
/tmp/qs-verify -dsn "postgres://postgres@localhost:5443/$DB" -mirror $MIRROR -table public.events \
  || bad "mirror diverged after ADD COLUMN"

say "5b. a backfill the stream cannot carry HALTS the mirror, durably"
# A volatile default rewrites the heap: every pre-existing row gets its own
# value and none of it appears in the change stream. Serving NULL there would be
# silent corruption, so the mirror must stop — and must still be stopped after a
# restart, since a restart re-reads the catalog and would otherwise accept the
# new column as if it had always been there.
psq 5443 "ALTER TABLE events ADD COLUMN nonce double precision DEFAULT random()" >/dev/null
psq 5443 "INSERT INTO events VALUES (200002,'AFTER-REWRITE',1.00,now(),'web',0.5)" >/dev/null
sleep 10
if grep -q "backfilled by a table rewrite" $BASE/mirror.log; then
  echo "  halted: $(grep -m1 'backfilled by a table rewrite' $BASE/mirror.log | sed 's/.*err=//' | cut -c1-110)"
else
  bad "a table-rewrite backfill was accepted; the mirror would serve NULL where the source has values"
fi
if grep -q '"halted"' $MIRROR/public.events/state.json 2>/dev/null; then
  echo "  halt is persisted in state.json, so a restart cannot quietly resume"
else
  bad "the halt was not persisted; restarting would re-read the catalog and serve the corruption"
fi
curl -sf "http://$HEALTH/readyz" >/dev/null \
  && bad "a halted mirror still reports ready; it would stay in the endpoint" \
  || echo "  /readyz is failing, so this node leaves the read endpoint"

say "5c. recovery: clearing the halt rebuilds from a fresh snapshot"
kill $MIRROR_PID 2>/dev/null; wait $MIRROR_PID 2>/dev/null
rm -rf $MIRROR; install -d -o postgres -g postgres $MIRROR
psq 5443 "SELECT pg_drop_replication_slot('qs_e2e') FROM pg_replication_slots WHERE slot_name='qs_e2e'" >/dev/null
su postgres -c "QS_CLUSTER=e2e QS_MODE=shadow QS_INGEST=logical \
  QS_TABLES=public.events QS_SLOT=qs_e2e QS_PUBLICATION=qs_e2e \
  QS_MIRROR_PATH=$MIRROR QS_FRESHNESS_SLO=15s \
  QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=5443 \
  QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=5444 \
  QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=e2e-2 \
  QS_HEALTH_ADDR=$HEALTH /tmp/qs-mirror" >> $BASE/mirror.log 2>&1 &
MIRROR_PID=$!
for _ in $(seq 90); do curl -sf "http://$HEALTH/readyz" >/dev/null 2>&1 && break; sleep 1; done
curl -sf "http://$HEALTH/readyz" >/dev/null || { tail -20 $BASE/mirror.log; bad "rebuilt mirror never became ready"; }
/tmp/qs-verify -dsn "postgres://postgres@localhost:5443/$DB" -mirror $MIRROR -table public.events \
  || bad "the rebuilt mirror does not match the source"

say "6. readiness reflects freshness, and idle is not stale"
sleep 20   # longer than the 15s SLO, with no writes at all
curl -sf "http://$HEALTH/readyz" >/dev/null \
  && echo "  still ready after 20s idle with a 15s SLO — idle is correctly not stale" \
  || bad "an idle source was reported stale; absence of data is not evidence of staleness"
curl -s "http://$HEALTH/metrics" | grep -E "^quicksilver_" | head -6 | sed 's/^/  /'

say "7. promote the standby: the sidecar must stand down"
su postgres -c "$PG/pg_ctl -D $STANDBY promote -w" >/dev/null || skip "promotion failed"
sleep 15
if grep -q "standing down" $BASE/mirror.log; then
  echo "  $(grep -m1 'standing down' $BASE/mirror.log | sed 's/.*msg=//' | cut -c1-90)"
else
  bad "the sidecar kept mirroring after its own node was promoted"
fi
if grep -q "synchronized_standby_slots entry that does not exist" $BASE/mirror.log; then
  echo "  cleared the inherited synchronized_standby_slots entry (docs/15)"
else
  bad "the inherited synchronized_standby_slots entry was not cleared; logical decoding on this new primary would hang silently"
fi
left=$(psq 5444 "SHOW synchronized_standby_slots" postgres)
[ -z "$left" ] && echo "  synchronized_standby_slots is now empty on the new primary" \
              || bad "synchronized_standby_slots still set to '$left'"

printf '\n========================================\n'
if [ $FAIL -eq 0 ]; then echo "PASS"; else echo "FAIL"; fi
exit $FAIL
