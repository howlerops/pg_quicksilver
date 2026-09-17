#!/usr/bin/env bash
# The mirror at a size where the shortcuts stop working.
#
# Every number this project has published comes from 200,000 to 400,000 seeded
# rows and a few million changes. That is enough to find correctness bugs and it
# is NOT enough to find the ones that only appear when a structure outgrows a
# cache: an index that fits in L3 at 400k and does not at 40M, a compaction that
# rewrites in one pass at one scale and thrashes at another, a base file with 55
# row groups against one with 3,000.
#
# So this runs one shape as large as the disk allows, and reports what it costs
# at every stage rather than one headline. It is slow on purpose and it is not
# part of the default suite.
#
#   bash bench/scripts/large_scale.sh                  # sized from free disk
#   ROWS=20000000 bash bench/scripts/large_scale.sh    # or say
#
# WHAT IT ACTUALLY CHECKS, in the order it matters:
#   1. it stays correct — qs-verify, full checksum, at the end
#   2. it stays bounded — RSS, file count, slow ticks, disk
#   3. it stays fast — bootstrap, drain, and the read path against PostgreSQL
#
# A run that is fast and wrong is a failure. A run that is correct and slow is a
# result.
set -uo pipefail
cd "$(dirname "$0")/../.."

PG=/usr/lib/postgresql/17/bin
BASE=/var/lib/postgresql/qs17
PRIMARY=$BASE/primary
STANDBY=$BASE/standby
MIRROR=$BASE/large-mirror
HEALTH=127.0.0.1:9196
DEBUG=127.0.0.1:9195
DB=large
SHAPE=${SHAPE:-narrow}
SECONDS_PER=${SECONDS_PER:-60}
FAIL=0

say()  { printf '\n=== %s ===\n' "$*"; }
ok()   { printf '  %s\n' "$*"; }
bad()  { printf '  FAIL: %s\n' "$*"; FAIL=1; }
skip() { printf '\nINCOMPLETE — skipped, which is NOT a pass: %s\n' "$*"; exit 2; }
psq()  { su postgres -c "$PG/psql -h /tmp -p $1 -U postgres -d ${3:-$DB} -Atc \"$2\""; }

cleanup() { pkill -x qs-mirror 2>/dev/null; return 0; }
trap cleanup EXIT

# ---- how big can this actually get? -----------------------------------------
#
# The source is stored TWICE before the mirror exists — the primary has it and
# the standby has a physical copy — and the mirror is roughly a third of one
# copy. Add WAL, which the replication slot pins until the mirror confirms it.
# Guessing wrong here does not produce a bad measurement, it produces a full
# disk halfway through an hour-long run, so it is computed rather than assumed.
AVAIL_KB=$(df --output=avail / | tail -1)
AVAIL_MB=$((AVAIL_KB / 1024))
RESERVE_MB=${RESERVE_MB:-2000}              # logs, headroom, the checkpoint gap
USABLE_MB=$((AVAIL_MB - RESERVE_MB))

# 190 B/row, not 89, and the difference is the whole lesson.
#
# The first version of this took 89 from pg_total_relation_size/rows, which is
# what the heap and its primary-key index actually weigh. It then sized the run
# at 41 million rows and ran the disk from 9.7 GB to 4.1 GB by row 28 million,
# because the table is not the only thing written:
#
#   heap + pk index        ~89 B/row
#   WAL for the inserts    ~85 B/row, retained until a checkpoint releases it
#   the standby's copy     the whole thing again
#   the mirror             ~25 B/row
#
# So the real figure is roughly (89 + 85) * 2 + 25. An estimate that leaves out
# WAL is not conservative, it is wrong by a factor of two, and the failure it
# produces is a full disk in the middle of an hour-long run rather than a bad
# number at the end of one.
BYTES_PER_ROW=${BYTES_PER_ROW:-190}
MAX_ROWS=$(( USABLE_MB * 1024 * 1024 / (BYTES_PER_ROW * 2) ))
ROWS=${ROWS:-$MAX_ROWS}

say "budget"
ok "$(df -h / | tail -1 | awk '{print $4}') free, reserving ${RESERVE_MB} MB"
ok "shape=$SHAPE at ${BYTES_PER_ROW} B/row, stored twice plus a mirror"
ok "rows: $(printf "%'d" "$ROWS")"
if [ "$ROWS" -lt 1000000 ]; then
  skip "only ${ROWS} rows fit; this test is pointless below a million"
fi

say "infrastructure"
pkill -x qs-mirror 2>/dev/null
rm -f /tmp/qs-workload-matrix.lock
psq 5443 "SELECT 1" postgres >/dev/null 2>&1 || \
  su postgres -c "$PG/pg_ctl -D $PRIMARY -l $BASE/primary.log -w start" >/dev/null 2>&1
psq 5443 "SELECT 1" postgres >/dev/null 2>&1 || skip "primary would not start"
# Reclaim whatever earlier benchmarks left behind before sizing anything.
for d in app tzapp; do psq 5443 "DROP DATABASE IF EXISTS $d" postgres >/dev/null 2>&1; done
psq 5443 "DROP DATABASE IF EXISTS $DB" postgres >/dev/null 2>&1
psq 5443 "CREATE DATABASE $DB" postgres >/dev/null || skip "could not create $DB"

psq 5443 "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active" postgres >/dev/null 2>&1
# The physical slot is created AFTER seeding, deliberately. Creating it first
# pins every WAL segment the seed produces -- 4.6 GB of pg_wal that no
# checkpoint can recycle, for a slot with no consumer yet. That is what filled
# the disk on the first attempt at this script.

say "seeding $(printf "%'d" "$ROWS") rows"
seed_t0=$(date +%s.%N)
# generate_series in one statement would build the whole set in memory. Chunked,
# each statement is bounded and the WAL it produces is checkpointed as it goes.
psq 5443 "CREATE TABLE m(id bigint primary key, sku text, amount numeric(12,2), ts timestamptz)" >/dev/null
CHUNK=${CHUNK:-2000000}
done_rows=0
while [ "$done_rows" -lt "$ROWS" ]; do
  hi=$((done_rows + CHUNK)); [ "$hi" -gt "$ROWS" ] && hi=$ROWS
  psq 5443 "INSERT INTO m SELECT g,'SKU-'||(g%100000),(g%997)/7.0,now()
            FROM generate_series($((done_rows+1)),$hi) g" >/dev/null || { bad "seed failed"; break; }
  done_rows=$hi
  # A checkpoint per chunk lets WAL be recycled as we go; without it the seed's
  # own WAL is a second copy of the table on disk.
  psq 5443 "CHECKPOINT" >/dev/null 2>&1
  free_mb=$(( $(df --output=avail / | tail -1) / 1024 ))
  printf '\r  %s rows, %s MB free   ' "$(printf "%'d" "$done_rows")" "$free_mb"
  # Stop before the disk does. A run that dies at 90% full leaves a cluster to
  # clean up and no measurement; one that stops early leaves a smaller, valid
  # measurement.
  if [ "$free_mb" -lt "$RESERVE_MB" ]; then
    printf '\n'
    ok "stopping the seed at $(printf "%'d" "$done_rows") rows: only ${free_mb} MB left"
    ROWS=$done_rows
    break
  fi
done
printf '\n'
seed=$(echo "$(date +%s.%N)-$seed_t0"|bc)
SRC_BYTES=$(psq 5443 "SELECT pg_total_relation_size('m')")
printf '  seeded in %.0fs — %s on disk (%.0f B/row)\n' "$seed" \
  "$(numfmt --to=iec "$SRC_BYTES")" "$(echo "$SRC_BYTES/$ROWS"|bc -l)"

say "standby"
# Now the slot, with the seed WAL already checkpointed away.
psq 5443 "CHECKPOINT" >/dev/null 2>&1
psq 5443 "SELECT pg_drop_replication_slot('lg_standby') FROM pg_replication_slots WHERE slot_name='lg_standby'" postgres >/dev/null 2>&1
psq 5443 "SELECT pg_create_physical_replication_slot('lg_standby', true)" postgres >/dev/null
psq 5443 "ALTER SYSTEM SET synchronized_standby_slots = 'lg_standby'" postgres >/dev/null
psq 5443 "SELECT pg_reload_conf()" postgres >/dev/null
su postgres -c "$PG/pg_ctl -D $STANDBY stop -m immediate" >/dev/null 2>&1
rm -rf $STANDBY
su postgres -c "$PG/pg_basebackup -D $STANDBY -R -X stream -S lg_standby -c fast \
  -d 'host=/tmp port=5443 user=postgres dbname=postgres'" >/dev/null 2>&1 || skip "basebackup failed"
{ echo "port = 5444"; echo "sync_replication_slots = on"; echo "hot_standby_feedback = on"; } \
  >> $STANDBY/postgresql.auto.conf
su postgres -c "$PG/pg_ctl -D $STANDBY -l $BASE/standby.log -w start" >/dev/null || skip "standby would not start"
[ "$(psq 5444 'SELECT pg_is_in_recovery()' postgres)" = "t" ] || skip "standby not in recovery"
ok "standby on 5444, in recovery"
ok "$(df -h / | tail -1 | awk '{print $4}') free after two copies"

go -C go build -o /tmp/qs-mirror ./cmd/qs-mirror || skip "build failed"
go -C go build -o /tmp/qs-verify ./cmd/qs-verify || skip "build failed"
go -C go build -o /tmp/qs-query  ./cmd/qs-query  || skip "build failed"
chmod 755 /tmp/qs-mirror /tmp/qs-verify /tmp/qs-query

say "bootstrap"
rm -rf $MIRROR; install -d -o postgres -g postgres $MIRROR
psq 5443 "SELECT pg_drop_replication_slot('lg_slot') FROM pg_replication_slots WHERE slot_name='lg_slot'" >/dev/null 2>&1
psq 5443 "DROP PUBLICATION IF EXISTS lg_pub" >/dev/null 2>&1
boot_t0=$(date +%s.%N)
su postgres -c "QS_CLUSTER=lg QS_MODE=shadow QS_INGEST=logical \
  QS_TABLES=public.m QS_SLOT=lg_slot QS_PUBLICATION=lg_pub \
  QS_MIRROR_PATH=$MIRROR QS_FRESHNESS_SLO=120s \
  QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=5443 \
  QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=5444 \
  QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=lg-2 \
  QS_DEBUG_ADDR=$DEBUG QS_HEALTH_ADDR=$HEALTH /tmp/qs-mirror" > $BASE/large.log 2>&1 &
sleep 1
MPID=$(pgrep -x qs-mirror | head -1)
for _ in $(seq 60000); do curl -sf "http://$HEALTH/readyz" >/dev/null 2>&1 && break; sleep 0.02; done
curl -sf "http://$HEALTH/readyz" >/dev/null 2>&1 || { tail -8 $BASE/large.log; skip "never became ready"; }
boot=$(echo "$(date +%s.%N)-$boot_t0"|bc)
printf '  %.1fs -> %.0f rows/s -> %.1f MB/s\n' "$boot" \
  "$(echo "$ROWS/$boot"|bc -l)" "$(echo "$SRC_BYTES/$boot/1000000"|bc -l)"
rss_boot=$(awk '/VmRSS/{print $2}' /proc/$MPID/status 2>/dev/null)
printf '  RSS after bootstrap: %s\n' "$(numfmt --to=iec $((${rss_boot:-0} * 1024)))"

say "a workload on top, for ${SECONDS_PER}s"
# Updates, not inserts: an update has to FIND the row, so this exercises the
# index at full size, which is the part that only misbehaves when it stops
# fitting in cache.
work_t0=$(date +%s.%N)
END=$(( $(date +%s) + SECONDS_PER ))
changes=0
paused=0
# How far the slot may get ahead before the workload waits for it. 512 MB is
# large enough that the mirror is genuinely under load and small enough that
# the disk is never in question.
PACE_BYTES=${PACE_BYTES:-536870912}
while [ "$(date +%s)" -lt "$END" ]; do
  lo=$((RANDOM * RANDOM % (ROWS > 100000 ? ROWS - 100000 : 1) + 1))
  psq 5443 "UPDATE m SET amount = amount + 1, ts = now()
            WHERE id BETWEEN $lo AND $((lo + 20000))" >/dev/null 2>&1
  changes=$((changes + 20001))

  # THE WORKLOAD IS THE DANGEROUS PHASE, not the seed, and the first version of
  # this script only guarded the seed.
  #
  # Updates write WAL, and the mirror's logical slot pins every segment it has
  # not confirmed. Producing changes faster than the mirror drains them turns
  # the difference into WAL that no checkpoint can reclaim. Measured here:
  # 196,632 changes/s in against a mirror draining ~95,000/s took 6.6 GB of
  # free space to 272 KB in under a minute and stopped PostgreSQL.
  #
  # That is a real property of the product, not of this script — see docs/28 —
  # and the sidecar now bounds its own slot.
  #
  # PACE, don't just guard. An unpaced workload measures how fast the disk
  # fills, which is docs/28 and is already known. What has never been measured
  # is the thing this run exists for: a mirror at twenty-odd million rows,
  # verified. So the workload waits when the slot gets ahead, which is what a
  # real source that is not actively trying to break its replica looks like.
  for _ in $(seq 60); do
    retained=$(psq 5443 "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),0)::bigint
                         FROM pg_replication_slots WHERE slot_name='lg_slot'" postgres 2>/dev/null)
    [ -z "$retained" ] && break
    [ "$retained" -lt "$PACE_BYTES" ] && break
    paused=$((paused + 1))
    sleep 1
  done

  free_mb=$(( $(df --output=avail / | tail -1) / 1024 ))
  if [ "$free_mb" -lt "$RESERVE_MB" ]; then
    printf '\n'
    ok "stopping the workload early: ${free_mb} MB left, which is the slot"
    ok "pinning WAL faster than the mirror confirms it — see docs/28"
    break
  fi
done
work=$(echo "$(date +%s.%N)-$work_t0"|bc)
printf '  %s row-changes in %.0fs (%.0f/s into PostgreSQL)\n' \
  "$(printf "%'d" "$changes")" "$work" "$(echo "$changes/$work"|bc -l)"
printf '  paced: waited %ss for the mirror to catch up (slot kept under %s)\n' \
  "$paused" "$(numfmt --to=iec "$PACE_BYTES")"

say "drain"
drain_t0=$(date +%s.%N)
# Two idempotent markers, then wait for the mirror to reach the first — the
# settle protocol the workload matrix uses, for the reason documented there.
psq 5443 "INSERT INTO m VALUES (999999999,'MARK',0,now()) ON CONFLICT (id) DO UPDATE SET ts=now()" >/dev/null
MARK=$(psq 5443 "SELECT pg_current_wal_lsn()::text")
psq 5443 "INSERT INTO m VALUES (999999999,'MARK',0,now()) ON CONFLICT (id) DO UPDATE SET ts=now()" >/dev/null
for _ in $(seq 3600); do
  applied=$(python3 -c "
import json;print(json.load(open('$MIRROR/public.m/state.json')).get('applied_lsn',''))" 2>/dev/null)
  [ -n "$applied" ] && python3 -c "
import sys
def p(l):
    a,b=l.split('/');return (int(a,16)<<32)|int(b,16)
sys.exit(0 if p('$applied') >= p('$MARK') else 1)" && break
  sleep 1
done
drain=$(echo "$(date +%s.%N)-$drain_t0"|bc)
printf '  caught up in %.0fs (%.0f row-changes/s through the mirror)\n' \
  "$drain" "$(echo "$changes/$drain"|bc -l)"

say "what it cost"
MIRROR_BYTES=$(du -sb $MIRROR | cut -f1)
LIVE=$(psq 5443 "SELECT count(*) FROM m")
printf '  source %s / mirror %s -> %.1fx smaller (%s live rows)\n' \
  "$(numfmt --to=iec "$SRC_BYTES")" "$(numfmt --to=iec "$MIRROR_BYTES")" \
  "$(echo "$SRC_BYTES/$MIRROR_BYTES"|bc -l)" "$(printf "%'d" "$LIVE")"
printf '  files: %s base + %s delta\n' \
  "$(ls $MIRROR/public.m/base/*.parquet 2>/dev/null | wc -l)" \
  "$(ls $MIRROR/public.m/delta/*.parquet 2>/dev/null | wc -l)"
rss=$(awk '/VmRSS/{print $2}' /proc/$MPID/status 2>/dev/null)
printf '  RSS: %s (%.0f bytes per live row)\n' \
  "$(numfmt --to=iec $((${rss:-0} * 1024)))" "$(echo "${rss:-0}*1024/$LIVE"|bc -l)"
SLOW=$(grep -c "slow tick" $BASE/large.log 2>/dev/null || echo 0)
printf '  slow ticks: %s\n' "$SLOW"
[ "$SLOW" -gt 0 ] && grep "slow tick" $BASE/large.log | sed 's/.*total_ms=/total_ms=/' \
  | sort -t= -k2 -rn | head -3 | sed 's/^/    /'
printf '  %s free\n' "$(df -h / | tail -1 | awk '{print $4}')"

say "correctness — the only part that is not optional"
if /tmp/qs-verify -dsn "postgres://postgres@localhost:5443/$DB" \
     -mirror "$MIRROR" -table public.m > /tmp/large-verify.txt 2>&1; then
  ok "MATCH ($(grep -m1 'mirror ' /tmp/large-verify.txt | sed 's/^ *//'))"
else
  bad "DIVERGED at $(printf "%'d" "$LIVE") rows"
  sed 's/^/    /' /tmp/large-verify.txt | head -8
fi

say "the read path at this size"
if python3 -c "import duckdb, psycopg" 2>/dev/null; then
  python3 -u bench/scripts/serve_compare.py --shape narrow --mirror "$MIRROR" \
    --dsn "postgres://postgres@localhost:5443/$DB" || bad "serving comparison failed"
else
  ok "(duckdb/psycopg not installed; read path not compared)"
fi

printf '\n========================================\n'
[ $FAIL -eq 0 ] && echo "PASS" || echo "FAIL"
exit $FAIL
