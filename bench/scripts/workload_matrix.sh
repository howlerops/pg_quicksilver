#!/usr/bin/env bash
# Run the production sidecar across the table shapes that break things.
#
# One PostgreSQL 17 primary + standby, reused across shapes; a fresh table,
# fresh mirror and fresh sidecar per shape. Every shape is VERIFIED against its
# source with qs-verify before its throughput number is believed — a fast mirror
# that does not match is not a result, and the jsonb shape is in this matrix
# precisely because it used to fail that check silently.
#
#   bash bench/scripts/workload_matrix.sh            # all shapes
#   SHAPES="jsonb churn" bash bench/scripts/workload_matrix.sh
set -uo pipefail
cd "$(dirname "$0")/../.."
source "$(dirname "$0")/lib_dropdb.sh"

PG=/usr/lib/postgresql/17/bin
BASE=/var/lib/postgresql/qs17
PRIMARY=$BASE/primary
STANDBY=$BASE/standby
HEALTH=127.0.0.1:9199
DB=app
ROWS=${ROWS:-400000}
SECONDS_PER=${SECONDS_PER:-20}
SHAPES=${SHAPES:-"narrow wide jsonb inline churn deletes purge"}
FAIL=0

# Exactly one matrix at a time. Two concurrent runs share a database, a table
# named m, a replication slot and this output file — and the second one silently
# recreates the table the first is verifying. That produced a "DIVERGED" with
# 68,234 missing rows that was entirely an artifact of the harness racing
# itself, and it is the fourth time in this project a measurement bug has
# impersonated a product bug.
#
# The lock is a PID file, not flock. `exec 9>` makes the descriptor inheritable
# by every child, and this script starts PostgreSQL daemons — which held the
# lock long after the run that took it had finished, so the next run was refused
# by a database. A lock a long-lived daemon can inherit is not a lock.
LOCK=/tmp/qs-workload-matrix.lock
if [ -f "$LOCK" ] && kill -0 "$(cat "$LOCK" 2>/dev/null)" 2>/dev/null; then
  echo "another workload_matrix.sh is already running (pid $(cat "$LOCK")); refusing to start"
  exit 2
fi
echo $$ > "$LOCK"

say()  { printf '\n=== %s ===\n' "$*"; }
bad()  { printf '  FAIL: %s\n' "$*"; FAIL=1; }
skip() { printf '\nINCOMPLETE — skipped, which is NOT a pass: %s\n' "$*"; exit 2; }
psq()  { su postgres -c "$PG/psql -h /tmp -p $1 -U postgres -d ${3:-$DB} -Atc \"$2\""; }

cleanup() { pkill -x qs-mirror 2>/dev/null; rm -f "$LOCK"; }
trap cleanup EXIT

say "infrastructure"
pkill -x qs-mirror 2>/dev/null
# Drop every inactive slot before starting. A slot left behind by an earlier run
# pins WAL forever: six abandoned slots from previous benchmarks had pg_wal at
# 12 GB and filled the disk mid-run. An inactive slot is not free.
su postgres -c "$PG/psql -h /tmp -p 5443 -U postgres -d postgres -Atc \
  'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active'" >/dev/null 2>&1
su postgres -c "$PG/pg_ctl -D $STANDBY stop -m immediate" >/dev/null 2>&1
su postgres -c "$PG/pg_ctl -D $PRIMARY stop -m immediate"  >/dev/null 2>&1
rm -f $PRIMARY/postgresql.auto.conf; touch $PRIMARY/postgresql.auto.conf
chown postgres:postgres $PRIMARY/postgresql.auto.conf
grep -q "^listen_addresses = 'localhost'" $PRIMARY/postgresql.conf \
  || echo "listen_addresses = 'localhost'" >> $PRIMARY/postgresql.conf
su postgres -c "$PG/pg_ctl -D $PRIMARY -l $BASE/primary.log -w start" >/dev/null || skip "primary would not start"
qs_drop_database 5443 "$DB"
psq 5443 "CREATE DATABASE $DB" postgres >/dev/null

psq 5443 "SELECT pg_drop_replication_slot('mx_standby') FROM pg_replication_slots WHERE slot_name='mx_standby'" postgres >/dev/null
psq 5443 "SELECT pg_create_physical_replication_slot('mx_standby', true)" postgres >/dev/null
psq 5443 "ALTER SYSTEM SET synchronized_standby_slots = 'mx_standby'" postgres >/dev/null
psq 5443 "SELECT pg_reload_conf()" postgres >/dev/null
rm -rf $STANDBY
su postgres -c "$PG/pg_basebackup -D $STANDBY -R -X stream -S mx_standby -c fast \
  -d 'host=/tmp port=5443 user=postgres dbname=postgres'" >/dev/null 2>&1 || skip "basebackup failed"
{ echo "port = 5444"; echo "sync_replication_slots = on"; echo "hot_standby_feedback = on"; } \
  >> $STANDBY/postgresql.auto.conf
su postgres -c "$PG/pg_ctl -D $STANDBY -l $BASE/standby.log -w start" >/dev/null || skip "standby would not start"
[ "$(psq 5444 'SELECT pg_is_in_recovery()' postgres)" = "t" ] || skip "standby not in recovery"

go -C go build -o /tmp/qs-mirror ./cmd/qs-mirror || skip "build failed"
go -C go build -o /tmp/qs-matrix ./cmd/qs-matrix || skip "build failed"
go -C go build -o /tmp/qs-verify ./cmd/qs-verify || skip "build failed"
chmod 755 /tmp/qs-mirror /tmp/qs-matrix /tmp/qs-verify
echo "  primary 5443, standby 5444, ${ROWS} rows/shape, ${SECONDS_PER}s workload, row_group=${ROW_GROUP:-65536}"
echo "  compact_dead_fraction=${QS_COMPACT_DEAD_FRACTION:-0}"
echo "  column-partial deltas: ${QS_PARTIAL_DELTAS:-1} (0 writes every update as a whole row)"
echo "  elide unchanged large values: ${QS_ELIDE_UNCHANGED:-1}"
echo "  max changes per tick: ${QS_MAX_TICK_CHANGES:-250000} (0 = unbounded, the old behaviour)"
echo "  max delta files per merge: ${QS_MAX_MERGE_FILES:-8} (0 = fold every mergeable file at once)"
echo "  zstd level: ${QS_ZSTD_LEVEL:-1}  (QS_SNAPPY_DELTAS=${QS_SNAPPY_DELTAS:-0} restores Snappy deltas)"

for SHAPE in $SHAPES; do
  say "shape: $SHAPE"
  MIRROR=$BASE/mx-$SHAPE
  rm -rf $MIRROR; install -d -o postgres -g postgres $MIRROR
  psq 5443 "SELECT pg_drop_replication_slot('mx_slot') FROM pg_replication_slots WHERE slot_name='mx_slot'" >/dev/null 2>&1
  psq 5443 "DROP PUBLICATION IF EXISTS mx_pub" >/dev/null 2>&1

  /tmp/qs-matrix -dsn "postgres://postgres@localhost:5443/$DB" -shape "$SHAPE" \
    -setup -rows "$ROWS" || { bad "setup failed"; continue; }

  boot_t0=$(date +%s.%N)
  su postgres -c "QS_CLUSTER=mx QS_MODE=shadow QS_INGEST=logical \
    QS_TABLES=public.m QS_SLOT=mx_slot QS_PUBLICATION=mx_pub \
    QS_MIRROR_PATH=$MIRROR QS_FRESHNESS_SLO=60s \
    QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=5443 \
    QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=5444 \
    QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=mx-2 \
    QS_ROW_GROUP=${ROW_GROUP:-65536} ${QS_DEBUG_ADDR:+QS_DEBUG_ADDR=$QS_DEBUG_ADDR} \
    QS_PARTIAL_DELTAS=${QS_PARTIAL_DELTAS:-1} \
    QS_ELIDE_UNCHANGED=${QS_ELIDE_UNCHANGED:-1} \
    QS_ZSTD_LEVEL=${QS_ZSTD_LEVEL:-1} \
    QS_SNAPPY_DELTAS=${QS_SNAPPY_DELTAS:-0} \
    QS_SLOW_TICK=${QS_SLOW_TICK:-200ms} \
    QS_MAX_TICK_CHANGES=${QS_MAX_TICK_CHANGES:-250000} \
    QS_MAX_MERGE_FILES=${QS_MAX_MERGE_FILES:-8} \
    QS_COMPACT_DEAD_FRACTION=${QS_COMPACT_DEAD_FRACTION:-0} \
    QS_HEALTH_ADDR=$HEALTH /tmp/qs-mirror" > $BASE/mx-$SHAPE.log 2>&1 &
  sleep 1
  MPID=$(pgrep -x qs-mirror | head -1)

  # 20 ms, not 1 second. The bootstrap figure this reports is the time until
  # readyz answers, so the poll interval is the measurement's resolution: at
  # `sleep 1` the narrow shape's ~1.5s bootstrap was quantised to 1.0s or 2.0s
  # depending on where the tick fell, and two runs of the SAME build differed by
  # "2x". Every bootstrap rows/s in docs/18 and docs/19 carries that error bar.
  for _ in $(seq 30000); do curl -sf "http://$HEALTH/readyz" >/dev/null 2>&1 && break; sleep 0.02; done
  if ! curl -sf "http://$HEALTH/readyz" >/dev/null 2>&1; then
    tail -6 $BASE/mx-$SHAPE.log; bad "never became ready"; pkill -x qs-mirror; continue
  fi
  boot=$(echo "$(date +%s.%N)-$boot_t0"|bc)
  printf '  bootstrap     %.1fs -> %.0f rows/s\n' "$boot" "$(echo "$ROWS/$boot"|bc -l)"

  # A CPU profile spanning the workload and the drain. Two hypotheses about
  # where the sidecar's time goes have already been wrong; a profile settles it
  # in one command.
  if [ -n "${QS_DEBUG_ADDR:-}" ]; then
    curl -s "http://$QS_DEBUG_ADDR/debug/pprof/profile?seconds=${CPU_PROFILE_SECONDS:-45}" \
        -o /tmp/cpu-$SHAPE.pb.gz &
    CPUPROF=$!
  fi

  /tmp/qs-matrix -dsn "postgres://postgres@localhost:5443/$DB" -shape "$SHAPE" \
    -measure -mirror "$MIRROR" -pid "$MPID" -seconds "$SECONDS_PER" || bad "measure failed"

  # The CPU profile must finish before the sidecar is killed, or curl comes back
  # with nothing and the profile that was supposed to settle an argument is
  # simply absent.
  if [ -n "${CPUPROF:-}" ]; then
    wait $CPUPROF 2>/dev/null
    [ -s /tmp/cpu-$SHAPE.pb.gz ] && echo "  cpu profile   /tmp/cpu-$SHAPE.pb.gz"
    CPUPROF=""
  fi

  # Heap profile before the process goes away, when asked for. RSS is a
  # high-water mark the Go runtime does not return promptly, so it says almost
  # nothing about what is actually live.
  if [ -n "${QS_DEBUG_ADDR:-}" ]; then
    curl -s "http://$QS_DEBUG_ADDR/debug/pprof/heap" -o /tmp/heap-$SHAPE.pb.gz && \
      echo "  heap profile  /tmp/heap-$SHAPE.pb.gz"
  fi

  /tmp/qs-matrix -dsn "postgres://postgres@localhost:5443/$DB" -shape "$SHAPE" \
    -settle -mirror "$MIRROR" || bad "mirror never settled"

  # Attribute the latency tail rather than reporting it as a bare number. Each
  # slow-tick line names every phase of the apply loop that took a millisecond
  # or more, so the worst tick comes with its cause attached. The wide shape's
  # tail was inferable from the merge log; the narrow shape's was not, and that
  # is exactly why the loop is now instrumented rather than read between.
  # `grep -c` PRINTS the count and then exits 1 when that count is zero, so
  # `$(grep -c … || echo 0)` returns "0\n0" on a clean run — and `[ "0
  # 0" -gt 0 ]` is not a false negative, it is a shell error printed into the
  # middle of the results table. Latent since this line was written; the purge
  # shape is simply the first one quiet enough to have no slow ticks at all.
  SLOW=$(grep -c "slow tick" $BASE/mx-$SHAPE.log 2>/dev/null)
  SLOW=${SLOW:-0}
  if [ "$SLOW" -gt 0 ]; then
    echo "  slow ticks    $SLOW over ${QS_SLOW_TICK:-200ms}; worst:"
    grep "slow tick" $BASE/mx-$SHAPE.log \
      | sed 's/.*total_ms=/total_ms=/' \
      | sort -t= -k2 -rn | head -3 | sed 's/^/                  /'
  else
    echo "  slow ticks    none over ${QS_SLOW_TICK:-200ms}"
  fi

  # Correctness, every time. The jsonb shape is here because it used to pass
  # every throughput check while silently blanking a column.
  if /tmp/qs-verify -dsn "postgres://postgres@localhost:5443/$DB" \
       -mirror "$MIRROR" -table public.m > /tmp/verify-$SHAPE.txt 2>&1; then
    echo "  correctness   MATCH ($(grep -m1 'mirror ' /tmp/verify-$SHAPE.txt | sed 's/^ *//'))"
  else
    bad "mirror DIVERGED from source"
    sed 's/^/    /' /tmp/verify-$SHAPE.txt | head -5
  fi

  # The other half of the question. Everything above is the write path; this
  # asks whether reading the mirror is actually faster than reading PostgreSQL,
  # and whether it gives the same answers. Opt-in, because it needs duckdb and
  # psycopg and roughly doubles the run.
  if [ "${QS_SERVE_COMPARE:-0}" = "1" ]; then
    go -C go build -o /tmp/qs-query ./cmd/qs-query && chmod 755 /tmp/qs-query
    python3 -u bench/scripts/serve_compare.py --shape "$SHAPE" --mirror "$MIRROR" \
      --dsn "postgres://postgres@localhost:5443/$DB" || bad "serving comparison failed"
  fi

  pkill -x qs-mirror; sleep 1

  # And the same comparison with nothing cached. This runs AFTER the sidecar is
  # stopped, deliberately: it restarts the primary before every timing, which
  # would otherwise tear down the replication stream underneath a running
  # mirror. The mirror is already built and already verified by this point, so
  # there is nothing left for the sidecar to do.
  if [ "${QS_COLD:-0}" = "1" ]; then
    printf '\n  --- cold cache ---\n'
    python3 -u bench/scripts/serve_compare.py --shape "$SHAPE" --mirror "$MIRROR" \
      --cold --pgdata "$PRIMARY" \
      --dsn "postgres://postgres@localhost:5443/$DB" || bad "cold comparison failed"
    # The restarts dropped the standby's stream. It reconnects on its own, but
    # the next shape needs it up, so wait rather than assume.
    for _ in $(seq 60); do
      [ -n "$(su postgres -c "$PG/psql -h /tmp -p 5443 -U postgres -d postgres -Atc \
        \"SELECT 1 FROM pg_stat_replication LIMIT 1\"" 2>/dev/null)" ] && break
      sleep 1
    done
  fi
done

printf '\n========================================\n'
[ $FAIL -eq 0 ] && echo "PASS" || echo "FAIL"
exit $FAIL
