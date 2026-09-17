#!/usr/bin/env bash
# Where does the bootstrap actually spend its time?
#
# The workload matrix reports bootstrap as rows/s, and across shapes that number
# spans more than an order of magnitude:
#
#   narrow  4 columns          ~193,000 rows/s
#   wide    12 columns          ~98,000 rows/s
#   inline  1.2 KB in the heap  ~49,000 rows/s
#   jsonb   6 KB TOASTed        ~16,000 rows/s
#
# The obvious reading is "bigger rows, fewer rows per second", and it is not
# obviously wrong. But rows/s is the wrong unit for deciding that: narrow moves
# 17 MB in the second it spends, and jsonb moves roughly 1.2 GB in the twelve it
# spends. In BYTES the ranking inverts, and an inverted ranking means the two
# shapes are limited by different things.
#
# This script settles it with a CPU profile taken DURING the bootstrap rather
# than after it -- the matrix's own profile starts once the sidecar is ready,
# which is precisely when the bootstrap has finished.
#
#   bash bench/scripts/bootstrap_profile.sh              # narrow and jsonb
#   SHAPES="wide" ROWS=400000 bash bench/scripts/bootstrap_profile.sh
set -uo pipefail
cd "$(dirname "$0")/../.."

PG=/usr/lib/postgresql/17/bin
BASE=/var/lib/postgresql/qs17
PRIMARY=$BASE/primary
STANDBY=$BASE/standby
HEALTH=127.0.0.1:9197
DEBUG=127.0.0.1:9196
DB=app
ROWS=${ROWS:-400000}
SHAPES=${SHAPES:-"narrow jsonb"}
OUT=${OUT:-/tmp/bootstrap-profiles}
FAIL=0

say()  { printf '\n=== %s ===\n' "$*"; }
bad()  { printf '  FAIL: %s\n' "$*"; FAIL=1; }
skip() { printf '\nINCOMPLETE — skipped, which is NOT a pass: %s\n' "$*"; exit 2; }
psq()  { su postgres -c "$PG/psql -h /tmp -p $1 -U postgres -d ${3:-$DB} -Atc \"$2\""; }

cleanup() { pkill -x qs-mirror 2>/dev/null; return 0; }
trap cleanup EXIT

mkdir -p "$OUT"

say "infrastructure"
pkill -x qs-mirror 2>/dev/null
psq 5443 "SELECT 1" postgres >/dev/null 2>&1 || {
  su postgres -c "$PG/pg_ctl -D $PRIMARY -l $BASE/primary.log -w start" >/dev/null 2>&1; }
psq 5443 "SELECT 1" postgres >/dev/null 2>&1 || skip "primary would not start"
psq 5444 "SELECT pg_is_in_recovery()" postgres 2>/dev/null | grep -q t || {
  su postgres -c "$PG/pg_ctl -D $STANDBY -l $BASE/standby.log -w start" >/dev/null 2>&1; }
[ "$(psq 5444 'SELECT pg_is_in_recovery()' postgres 2>/dev/null)" = "t" ] \
  || skip "standby not in recovery — run workload_matrix.sh first to build one"

go -C go build -o /tmp/qs-mirror ./cmd/qs-mirror || skip "build failed"
go -C go build -o /tmp/qs-matrix ./cmd/qs-matrix || skip "build failed"
chmod 755 /tmp/qs-mirror /tmp/qs-matrix
echo "  ${ROWS} rows/shape, profiles under $OUT"

for SHAPE in $SHAPES; do
  say "shape: $SHAPE"
  MIRROR=$BASE/bp-$SHAPE
  rm -rf $MIRROR; install -d -o postgres -g postgres $MIRROR
  psq 5443 "SELECT pg_drop_replication_slot('bp_slot') FROM pg_replication_slots WHERE slot_name='bp_slot'" >/dev/null 2>&1
  psq 5443 "DROP PUBLICATION IF EXISTS bp_pub" >/dev/null 2>&1

  /tmp/qs-matrix -dsn "postgres://postgres@localhost:5443/$DB" -shape "$SHAPE" \
    -setup -rows "$ROWS" || { bad "setup failed"; continue; }

  # How big is the thing being read? rows/s alone cannot tell bandwidth-bound
  # from allocation-bound, and that is the entire question here.
  BYTES=$(psq 5443 "SELECT pg_total_relation_size('public.m')" )

  boot_t0=$(date +%s.%N)
  su postgres -c "QS_CLUSTER=bp QS_MODE=shadow QS_INGEST=logical \
    QS_TABLES=public.m QS_SLOT=bp_slot QS_PUBLICATION=bp_pub \
    QS_MIRROR_PATH=$MIRROR QS_FRESHNESS_SLO=60s \
    QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=5443 \
    QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=5444 \
    QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=bp-2 \
    QS_DEBUG_ADDR=$DEBUG \
    QS_HEALTH_ADDR=$HEALTH /tmp/qs-mirror" > $BASE/bp-$SHAPE.log 2>&1 &

  # Start profiling AS SOON AS pprof answers, which is before the snapshot
  # begins. The matrix's profile starts at readyz, and readyz is exactly the
  # moment the bootstrap ends -- so it has never seen this code run.
  for _ in $(seq 100); do
    curl -sf "http://$DEBUG/debug/pprof/" >/dev/null 2>&1 && break; sleep 0.1
  done
  curl -s "http://$DEBUG/debug/pprof/profile?seconds=${PROFILE_SECONDS:-30}" \
    -o "$OUT/boot-$SHAPE.pb.gz" &
  PROF=$!

  for _ in $(seq 900); do curl -sf "http://$HEALTH/readyz" >/dev/null 2>&1 && break; sleep 1; done
  if ! curl -sf "http://$HEALTH/readyz" >/dev/null 2>&1; then
    tail -6 $BASE/bp-$SHAPE.log; bad "never became ready"; kill $PROF 2>/dev/null
    pkill -x qs-mirror; continue
  fi
  boot=$(echo "$(date +%s.%N)-$boot_t0"|bc)

  # The profile has to finish before the process goes away or curl returns
  # nothing and the profile that was meant to settle the argument is absent.
  wait $PROF 2>/dev/null

  printf '  source        %s (%.0f B/row)\n' \
    "$(numfmt --to=iec $BYTES)" "$(echo "$BYTES/$ROWS"|bc -l)"
  printf '  bootstrap     %.1fs -> %.0f rows/s -> %.1f MB/s\n' \
    "$boot" "$(echo "$ROWS/$boot"|bc -l)" "$(echo "$BYTES/$boot/1000000"|bc -l)"

  if [ -s "$OUT/boot-$SHAPE.pb.gz" ]; then
    echo "  profile       $OUT/boot-$SHAPE.pb.gz"
    go tool pprof -top -nodecount=12 /tmp/qs-mirror "$OUT/boot-$SHAPE.pb.gz" 2>/dev/null \
      | sed -n '1,20p' | sed 's/^/    /'
    echo "    --- allocation, cumulative ---"
    go tool pprof -top -cum -nodecount=14 /tmp/qs-mirror "$OUT/boot-$SHAPE.pb.gz" 2>/dev/null \
      | grep -E "mirror|pgx|runtime\.(mallocgc|growslice|mapassign|makemap)" \
      | head -10 | sed 's/^/    /'
  else
    bad "no profile was captured"
  fi

  pkill -x qs-mirror; sleep 1
done

printf '\n========================================\n'
[ $FAIL -eq 0 ] && echo "PASS" || echo "FAIL"
exit $FAIL
