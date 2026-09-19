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
# The repository, wherever it is. This was hardcoded to one machine's absolute
# path, which worked everywhere it was ever run by hand and nowhere else: on a
# CI runner the checkout is under /home/runner/work, so `cd $GO && go build`
# failed and the whole e2e section reported INCOMPLETE. That is the class of bug
# CI exists to find, and it found this one on its first real run.
GO=$(cd "$(dirname "$0")/../../go" && pwd)
source "$(dirname "$0")/lib_dropdb.sh"
source "$(dirname "$0")/lib_lsn.sh"
HEALTH=127.0.0.1:9199
DB=app
FAIL=0

say()  { printf '\n== %s ==\n' "$*"; }
bad()  { printf 'FAIL: %s\n' "$*"; FAIL=1; }
skip() { printf '\nINCOMPLETE — section skipped, which is NOT a pass: %s\n' "$*"; exit 2; }
psq()  { su postgres -c "$PG/psql -h /tmp -p $1 -U postgres -d ${3:-$DB} -Atc \"$2\""; }

cleanup() { pkill -x qs-mirror 2>/dev/null; return 0; }
trap cleanup EXIT

# start_sidecar <logfile> <tables> — the sidecar as the plugin launches it.
# Appending to the log rather than truncating matters once the sidecar is
# restarted: the sections that assert on earlier lines still have them.
start_sidecar() {
  su postgres -c "QS_CLUSTER=e2e QS_MODE=shadow QS_INGEST=logical \
    QS_TABLES=$2 QS_SLOT=qs_e2e QS_PUBLICATION=qs_e2e \
    QS_MIRROR_PATH=$MIRROR QS_FRESHNESS_SLO=15s \
    QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=5443 \
    QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=5444 \
    QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=e2e-2 \
    QS_HEALTH_ADDR=$HEALTH /tmp/qs-mirror" >> "$1" 2>&1 &
  MIRROR_PID=$!
}

# wait_ready — readiness, polled finely enough to time.
wait_ready() {
  for _ in $(seq ${1:-90}); do
    curl -sf "http://$HEALTH/readyz" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

# wait_applied <timeout-seconds> — block until the mirror has applied everything
# committed so far, rather than sleeping and hoping.
#
# Every `qs-verify` in this script used to be preceded by `sleep 8`, which is a
# fixed timeout against an asynchronous applier. It lost on a CI runner in
# section 6c and the failure read as a correctness bug:
#
#     DIVERGED
#       rows only in source: 0, only in mirror: 0
#       differing columns (count of rows):
#         amount               400
#
# Exactly the 400 rows that section's UPDATE had just touched, nothing missing
# and nothing extra — a mirror that was behind, not wrong. And 6c is the section
# most likely to lose, because it deletes the persisted index first: the rebuild
# it forces is the very work that makes eight seconds too short.
#
# The marker is written TWICE with the LSN captured between, so there is
# decodable WAL strictly past the captured position and the applied LSN must
# cross it rather than stopping exactly on it. `SET amount = amount` writes a new
# tuple version — real WAL — while changing no value, so the marker cannot alter
# what qs-verify then compares.
#
# Every table is checked, not just the one the marker touched: Apply advances a
# table's applied LSN for every transaction it sees, including ones that change
# no row of that table, so one marker settles them all.
wait_applied() {
  # max(id) rather than a fixed id: section 4 deletes 1-500 and later sections
  # insert higher ranges, so any constant is a row that exists for some callers
  # and not others — and an UPDATE matching no row writes no WAL, which is a
  # marker that can never be crossed.
  local touch="UPDATE events SET amount = amount WHERE id = (SELECT max(id) FROM events)"
  psq 5443 "$touch" >/dev/null 2>&1
  local mark
  mark=$(psq 5443 "SELECT pg_current_wal_lsn()::text" postgres)
  psq 5443 "$touch" >/dev/null 2>&1
  [ -n "$mark" ] || { sleep 8; return 0; }   # no marker, no better than before

  local deadline=$(( ${1:-60} * 10 )) at behind
  for _ in $(seq "$deadline"); do
    behind=0
    for st in "$MIRROR"/*/state.json; do
      [ -e "$st" ] || continue
      at=$(python3 -c "
import json;print(json.load(open('$st')).get('applied_lsn',''))" 2>/dev/null)
      if [ -z "$at" ] || ! lsn_ge "$at" "$mark"; then behind=1; break; fi
    done
    [ "$behind" -eq 0 ] && return 0
    sleep 0.1
  done
  return 1
}

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

qs_drop_database 5443 "$DB"
psq 5443 "CREATE DATABASE $DB" postgres >/dev/null
psq 5443 "CREATE TABLE events(id bigint primary key, sku text, amount numeric(12,2), ts timestamptz)" >/dev/null
psq 5443 "INSERT INTO events SELECT g,'SKU-'||g,(g%997)/7.0,now() FROM generate_series(1,20000) g" >/dev/null
echo "  seeded $(psq 5443 'SELECT count(*) FROM events') rows BEFORE the mirror exists"

say "1. standby with sync_replication_slots"
psq 5443 "SELECT pg_create_physical_replication_slot('e2e_standby', true)" postgres >/dev/null
psq 5443 "ALTER SYSTEM SET synchronized_standby_slots = 'e2e_standby'" postgres >/dev/null
psq 5443 "SELECT pg_reload_conf()" postgres >/dev/null
rm -rf $STANDBY
# `-c fast`, which every other script in this directory already passes and this
# one did not. Without it pg_basebackup waits for a SPREAD checkpoint — throttled
# across checkpoint_timeout * checkpoint_completion_target, so up to ~270s at the
# defaults — and how long that takes depends entirely on how dirty the primary
# happens to be when the run starts.
#
# On CI it is always fast, because CI builds the primary fresh in the same job.
# Run locally after a benchmark has been hammering the same cluster, this one
# line turned a four-minute section into a four-minute WAIT, which reads as a
# hung test and was twice mistaken for one.
su postgres -c "$PG/pg_basebackup -D $STANDBY -R -X stream -S e2e_standby -c fast \
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

start_sidecar $BASE/mirror.log public.events

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
wait_applied 60 || bad "the mirror never caught up with the live DML"
/tmp/qs-verify -dsn "postgres://postgres@localhost:5443/$DB" -mirror $MIRROR -table public.events \
  || bad "mirror diverged from the source after live DML"

say "5. DDL crosses as a barrier, not as corruption"
psq 5443 "ALTER TABLE events ADD COLUMN channel text DEFAULT 'web'" >/dev/null
psq 5443 "INSERT INTO events VALUES (200001,'POST-DDL',1.00,now(),'mobile')" >/dev/null
wait_applied 60 || bad "the mirror never caught up with the post-DDL insert"
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

say "6b. the sidecar is killed and restarts: it RESUMES, it does not re-snapshot"
# The failure this guards against is expensive rather than wrong: a sidecar that
# re-snapshots on every restart turns a pod reschedule into a full table read,
# and on a large table that is minutes of unreadiness per restart.
psq 5443 "INSERT INTO events(id,sku,amount,ts,channel) SELECT g,'PRE-KILL-'||g,1.0,now(),'web' FROM generate_series(300001,300500) g" >/dev/null
sleep 6
before_snapshots=$(grep -c "snapshot complete" $BASE/mirror.log)
applied_before=$(python3 -c "
import json;print(json.load(open('$MIRROR/public.events/state.json'))['applied_lsn'])" 2>/dev/null)

pkill -x qs-mirror; sleep 2
# Changes committed while NOTHING is running have to be picked up on restart.
# The slot holds them; the mirror has to ask.
psq 5443 "INSERT INTO events(id,sku,amount,ts,channel) SELECT g,'WHILE-DOWN-'||g,2.0,now(),'web' FROM generate_series(300501,301000) g" >/dev/null

start_sidecar $BASE/mirror.log public.events
wait_ready 90 || { tail -20 $BASE/mirror.log; bad "the sidecar did not come back ready"; }
after_snapshots=$(grep -c "snapshot complete" $BASE/mirror.log)
if [ "$after_snapshots" -gt "$before_snapshots" ]; then
  bad "the restart re-snapshotted; applied_lsn $applied_before should have been resumed from"
else
  echo "  resumed from applied_lsn $applied_before without a new snapshot"
fi
wait_applied 60 || bad "the mirror never caught up after the restart"
/tmp/qs-verify -dsn "postgres://postgres@localhost:5443/$DB" -mirror $MIRROR -table public.events \
  || bad "rows committed while the sidecar was down are missing from the mirror"

say "6c. the key index is rebuilt from the base file, not from a cache"
# The snapshot stopped writing index/*.idx.json: the base file already holds the
# key at every position, and a column store can read one column back. This
# deletes every persisted index, restarts, and then UPDATES rows — each update
# has to find and retire the row the rebuilt index points at. A wrong position
# leaves two live rows for one key while every count still matches, which is
# exactly the shape of bug that does not announce itself.
pkill -x qs-mirror; sleep 2
idx_before=$(ls $MIRROR/public.events/index/*.idx.json 2>/dev/null | wc -l)
rm -f $MIRROR/public.events/index/*.idx.json
start_sidecar $BASE/mirror.log public.events
wait_ready 90 || { tail -20 $BASE/mirror.log; bad "no readiness after the index was removed"; }
psq 5443 "UPDATE events SET amount = amount + 100 WHERE id BETWEEN 300001 AND 300400" >/dev/null
wait_applied 90 || bad "the mirror never caught up after the index was removed"
if /tmp/qs-verify -dsn "postgres://postgres@localhost:5443/$DB" -mirror $MIRROR -table public.events; then
  # Say what was actually true. The first run of this printed "0 index file(s)
  # deleted" and called it a pass, which reads as though a cache had been
  # cleared when there was none to clear — the snapshot no longer writes one.
  # Zero is the expected state and the assertion is that the mirror is correct
  # WITHOUT it; a non-zero count means a compaction had written one and that
  # was cleared too.
  if [ "$idx_before" -eq 0 ]; then
    echo "  no persisted index existed; updates still retired the right rows"
  else
    echo "  $idx_before index file(s) left by an older build deleted; updates still retired the right rows"
  fi
else
  bad "with no persisted index the mirror diverged: the rebuild does not agree with the file"
fi
# Nothing writes one any more: it measured 2.7x the size of the Parquet file it
# indexed and was slower to load than rebuilding from it. A fresh one appearing
# here means a rewrite path started writing them again.
[ -z "$(ls $MIRROR/public.events/index/*.idx.json 2>/dev/null)" ] \
  || bad "a compaction wrote a persisted index again"

say "6d. the PUBLISHED VIEW answers what the source answers"
# Everything above verifies the mirror through qs-verify, which reads it the way
# the writer wrote it. That is not how a customer reads it. This runs the SQL
# qs-query publishes through an engine that knows nothing about the layout, and
# compares against PostgreSQL — the deletion vectors, the partial deltas and the
# manifest all have to be right for the two to agree.
cd $GO && go build -o /tmp/qs-query ./cmd/qs-query && chmod 755 /tmp/qs-query
if python3 -c "import duckdb" 2>/dev/null; then
  python3 - "$MIRROR" "$DB" <<'PYVIEW'
import subprocess, sys
import duckdb, psycopg
mirror, db = sys.argv[1], sys.argv[2]
out = subprocess.run(["/tmp/qs-query", "-mirror", mirror, "-table", "public.events"],
                     capture_output=True, text=True)
if out.returncode != 0:
    print("  FAIL: qs-query:", out.stderr.strip()); sys.exit(1)
header, view = out.stdout.split("\n", 1)
print(" ", header.strip())
con = duckdb.connect(); con.execute(f"CREATE VIEW qs AS {view}")
pg = psycopg.connect(f"postgres://postgres@localhost:5443/{db}")
bad = 0
for label, q in [
        ("count(*)",      "SELECT count(*) FROM {t}"),
        ("sum(amount)",   "SELECT round(sum(amount),2) FROM {t}"),
        ("group by channel",
         "SELECT channel, count(*) FROM {t} GROUP BY 1 ORDER BY 1 NULLS LAST"),
        ("a deleted range is gone",
         "SELECT count(*) FROM {t} WHERE id BETWEEN 1 AND 500")]:
    with pg.cursor() as c:
        c.execute(q.format(t="public.events")); want = c.fetchall()
    got = con.execute(q.format(t="qs")).fetchall()
    norm = lambda rs: sorted(tuple(round(float(v),2) if isinstance(v,(int,float)) or
                                   type(v).__name__=="Decimal" else str(v) for v in r) for r in rs)
    ok = norm(want) == norm(got)
    print(f"    {label:<26} {'agrees' if ok else 'DIFFERS'}   source={want[:2]} view={got[:2]}")
    if not ok: bad += 1
sys.exit(1 if bad else 0)
PYVIEW
  [ $? -eq 0 ] || bad "the published view does not agree with the source"
else
  echo "  (duckdb not installed; the view was generated but not evaluated)"
fi

say "6e. two tables in one sidecar"
# QS_TABLES is a list and has only ever been exercised with one entry. A second
# table must get its own mirror directory, its own snapshot and its own state,
# and the publication has to carry both.
psq 5443 "CREATE TABLE IF NOT EXISTS orders(id bigint primary key, total numeric(12,2), placed timestamptz)" >/dev/null
psq 5443 "INSERT INTO orders SELECT g,(g%97)/7.0,now() FROM generate_series(1,5000) g ON CONFLICT DO NOTHING" >/dev/null
pkill -x qs-mirror; sleep 2
# A new table joining an existing publication needs the slot's publication
# refreshed; the sidecar owns that, so dropping the publication makes it rebuild.
psq 5443 "DROP PUBLICATION IF EXISTS qs_e2e" >/dev/null 2>&1
start_sidecar $BASE/mirror.log "public.events,public.orders"
wait_ready 120 || { tail -20 $BASE/mirror.log; bad "no readiness with two tables"; }
psq 5443 "INSERT INTO orders SELECT g,(g%97)/7.0,now() FROM generate_series(5001,6000) g" >/dev/null
psq 5443 "UPDATE orders SET total = total + 1 WHERE id % 500 = 0" >/dev/null
wait_applied 90 || bad "the mirror never caught up with two tables in one sidecar"
for tbl in public.events public.orders; do
  if /tmp/qs-verify -dsn "postgres://postgres@localhost:5443/$DB" -mirror $MIRROR -table $tbl; then
    echo "  $tbl matches"
  else
    bad "$tbl diverged with two tables in one sidecar"
  fi
done

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
