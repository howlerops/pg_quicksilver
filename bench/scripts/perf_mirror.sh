#!/usr/bin/env bash
# Measure the PRODUCTION path end to end: PostgreSQL 17 primary, a real
# streaming standby, the qs-mirror sidecar running as its own process with only
# the environment the CNPG-I plugin sets, and then queries answered from the
# mirror it built.
#
# Six numbers:
#   snapshot rate        how long before a mirror is useful at all
#   drain rate           the apply ceiling, backlog rows per second
#   commit-to-visible    p50/p90/p95/p99 — the freshness SLO's real constraint
#   storage ratio        heap+indexes vs parquet, which is the economics
#   sidecar cost         CPU seconds and RSS for the whole run
#   query speedup        the same analytical queries, PostgreSQL vs the mirror
#
# A section that cannot run exits 2 as INCOMPLETE. It never reports success.
set -uo pipefail
cd "$(dirname "$0")/../.."
source "$(dirname "$0")/lib_dropdb.sh"

PG=/usr/lib/postgresql/17/bin
BASE=/var/lib/postgresql/qs17
PRIMARY=$BASE/primary
STANDBY=$BASE/standby
MIRROR=$BASE/perf-mirror
HEALTH=127.0.0.1:9199
DB=app
SEED=${SEED:-2000000}
BURST=${BURST:-200000}
# The knob this script is the A/B for. QS_COMPACT_DEAD_FRACTION makes the mirror
# rewrite its base once the deletion vector covers this fraction of it, which
# buys back the read penalty measured in dv_cost_by_shape.py and costs an extra
# O(table) rewrite on the WRITE path. It ships off (0) precisely because that
# second half had never been measured, and the drain rate and commit-to-visible
# percentiles below are the measurement. So it has to be settable from outside,
# or the harness cannot vary the one thing it is being asked about.
DEAD_FRACTION=${QS_COMPACT_DEAD_FRACTION:-0}
FAIL=0

say()  { printf '\n== %s ==\n' "$*"; }
bad()  { printf 'FAIL: %s\n' "$*"; FAIL=1; }
skip() { printf '\nINCOMPLETE — section skipped, which is NOT a pass: %s\n' "$*"; exit 2; }
psq()  { su postgres -c "$PG/psql -h /tmp -p $1 -U postgres -d ${3:-$DB} -Atc \"$2\""; }

# qs_clear_sync_slots: ALTER SYSTEM outlives this run, and a leftover entry
# stalls the NEXT script's failover slot silently. See lib_syncslots.sh.
cleanup() { [ -n "${MIRROR_PID:-}" ] && kill "$MIRROR_PID" 2>/dev/null; qs_clear_sync_slots 5443; return 0; }
trap cleanup EXIT

printf 'perf_mirror: seed=%s burst=%s compact_dead_fraction=%s\n' \
  "$SEED" "$BURST" "$DEAD_FRACTION"

say "0. a clean PostgreSQL 17 primary with $SEED rows"
su postgres -c "$PG/pg_ctl -D $STANDBY stop -m immediate" >/dev/null 2>&1
su postgres -c "$PG/pg_ctl -D $PRIMARY stop -m immediate"  >/dev/null 2>&1
rm -f $PRIMARY/postgresql.auto.conf; touch $PRIMARY/postgresql.auto.conf
chown postgres:postgres $PRIMARY/postgresql.auto.conf
grep -q "^listen_addresses = 'localhost'" $PRIMARY/postgresql.conf \
  || echo "listen_addresses = 'localhost'" >> $PRIMARY/postgresql.conf
su postgres -c "$PG/pg_ctl -D $PRIMARY -l $BASE/primary.log -w start" >/dev/null || skip "primary would not start"

qs_drop_database 5443 "$DB"
psq 5443 "CREATE DATABASE $DB" postgres >/dev/null
# A realistically WIDE table. Column count is the single biggest driver of the
# columnar advantage: a column store reads only the columns a query touches,
# so a 4-column table gives it almost nothing to skip. docs/11 measured 30x on
# a 30-column table; the same queries on 4 columns measured 7x. Anything in
# between is a function of the schema, not of the engine, and a benchmark on a
# narrow table quietly understates the design it is testing.
psq 5443 "CREATE TABLE events(
  id bigint primary key, sku text, amount numeric(12,2), ts timestamptz,
  region text, channel text, customer_id bigint, session_id text,
  status text, qty int, discount numeric(6,2), note text)" >/dev/null
echo "  seeding $SEED rows..."
t0=$(date +%s.%N)
psq 5443 "INSERT INTO events SELECT g,'SKU-'||(g%100000),(g%997)/7.0,
  now()-(g%86400)*interval '1 second',
  (ARRAY['us-east','us-west','eu-west','ap-south'])[1+g%4],
  (ARRAY['web','mobile','api','partner'])[1+g%4],
  g%250000, md5(g::text), (ARRAY['ok','pending','refunded'])[1+g%3],
  1+g%9, (g%50)/10.0, repeat('x', 20+g%40)
  FROM generate_series(1,$SEED) g" >/dev/null
psq 5443 "ANALYZE events" >/dev/null
t1=$(date +%s.%N)
printf '  seeded in %.1fs; heap+indexes %s\n' "$(echo "$t1-$t0"|bc)" \
  "$(psq 5443 "SELECT pg_size_pretty(pg_total_relation_size('events'))")"

say "1. standby"
psq 5443 "SELECT pg_drop_replication_slot('perf_standby') FROM pg_replication_slots WHERE slot_name='perf_standby'" postgres >/dev/null
psq 5443 "SELECT pg_create_physical_replication_slot('perf_standby', true)" postgres >/dev/null
psq 5443 "ALTER SYSTEM SET synchronized_standby_slots = 'perf_standby'" postgres >/dev/null
psq 5443 "SELECT pg_reload_conf()" postgres >/dev/null
rm -rf $STANDBY
su postgres -c "$PG/pg_basebackup -D $STANDBY -R -X stream -S perf_standby -c fast \
  -d 'host=/tmp port=5443 user=postgres dbname=postgres'" || skip "pg_basebackup failed"
{ echo "port = 5444"; echo "sync_replication_slots = on"; echo "hot_standby_feedback = on"; } \
  >> $STANDBY/postgresql.auto.conf
su postgres -c "$PG/pg_ctl -D $STANDBY -l $BASE/standby.log -w start" >/dev/null || skip "standby would not start"
[ "$(psq 5444 'SELECT pg_is_in_recovery()' postgres)" = "t" ] || skip "standby not in recovery"
echo "  up and in recovery"

say "2. snapshot rate — how long until the mirror is useful"
rm -rf $MIRROR; install -d -o postgres -g postgres $MIRROR
go -C go build -o /tmp/qs-mirror ./cmd/qs-mirror || skip "build failed"
go -C go build -o /tmp/qs-perf   ./cmd/qs-perf   || skip "build failed"
chmod 755 /tmp/qs-mirror /tmp/qs-perf

snap_t0=$(date +%s.%N)
su postgres -c "QS_CLUSTER=perf QS_MODE=shadow QS_INGEST=logical \
  QS_TABLES=public.events QS_SLOT=qs_perf QS_PUBLICATION=qs_perf \
  QS_MIRROR_PATH=$MIRROR QS_FRESHNESS_SLO=30s \
  QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=5443 \
  QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=5444 \
  QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=perf-2 \
  QS_COMPACT_DEAD_FRACTION=$DEAD_FRACTION \
  QS_HEALTH_ADDR=$HEALTH /tmp/qs-mirror" > $BASE/perf-mirror.log 2>&1 &
MIRROR_PID=$!
# the sidecar is launched through su, so the binary's own pid is a child
sleep 2
REAL_PID=$(pgrep -x qs-mirror | head -1)   # -x: the binary, not the su wrapper

# A node that is still snapshotting has nothing to serve. If it reports ready
# here, Kubernetes puts it in the read endpoint with an empty mirror.
if curl -sf "http://$HEALTH/readyz" >/dev/null 2>&1; then
  bad "the mirror reported READY while its snapshot was still running"
else
  echo "  correctly NOT ready while bootstrapping: $(curl -s http://$HEALTH/readyz | head -1)"
fi

for _ in $(seq 900); do
  curl -sf "http://$HEALTH/readyz" >/dev/null 2>&1 && break
  sleep 1
done
snap_t1=$(date +%s.%N)
if ! curl -sf "http://$HEALTH/readyz" >/dev/null 2>&1; then
  tail -20 $BASE/perf-mirror.log; skip "mirror never became ready"
fi
snap=$(echo "$snap_t1-$snap_t0"|bc)
if grep -q "snapshot complete" $BASE/perf-mirror.log; then
  grep -m1 "snapshot complete" $BASE/perf-mirror.log | sed 's/.*msg=/  /'
else
  bad "became ready without ever taking a snapshot"
fi
printf '  bootstrap to ready: %.1fs for %s rows -> %.0f rows/s\n' \
  "$snap" "$SEED" "$(echo "$SEED/$snap"|bc -l)"

say "3. ingest performance (drain rate, latency, storage, cost)"
/tmp/qs-perf -dsn "postgres://postgres@localhost:5443/$DB" -mirror $MIRROR \
  -table public.events -health $HEALTH -pid "$REAL_PID" -burst $BURST \
  || bad "qs-perf reported a failure"

say "4. query performance — PostgreSQL vs the mirror it built"
psq 5443 "ANALYZE events" >/dev/null
python3 bench/scripts/query_compare.py --dsn "postgres://postgres@localhost:5443/$DB" \
  --mirror "$MIRROR/public.events" || bad "query comparison failed"

printf '\n========================================\n'
[ $FAIL -eq 0 ] && echo "PASS" || echo "FAIL"
exit $FAIL
