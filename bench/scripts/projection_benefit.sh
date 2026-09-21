#!/usr/bin/env bash
# Does the projection actually pay, and for which queries?
#
# The claim this project rests on is that a columnar mirror answers analytical
# queries faster than the heap they came from. bench/scripts/serving_pg17.sh
# section 9 reports six ratios at one scale on a FIVE-column table, and that is
# not enough to answer the question honestly, for two reasons:
#
#   1. A column store's advantage is BYTES NOT READ. On a five-column table
#      there are barely any bytes to skip, so the measurement is taken exactly
#      where the mechanism is weakest and says little about a real fact table.
#   2. One scale cannot distinguish "faster" from "faster and getting better",
#      and those imply different things about whether to deploy this.
#
# So this varies the thing the mechanism actually depends on — HOW MANY COLUMNS
# A QUERY TOUCHES — on a deliberately wide table, at more than one scale.
#
# THREE PATHS, because the difference between two of them is the finding:
#
#   heap      PostgreSQL over the source table. The baseline.
#   view      PostgreSQL over the published view. THE DEPLOYED PATH.
#   pushdown  the same logical query with the aggregate pushed INTO DuckDB.
#
# `view` and `pushdown` read the identical Parquet files. They differ in where
# the work happens, and that is not a detail: qs-query's view projects EVERY
# column out of duckdb.query(...) so that the result is a table to PostgreSQL,
# which means `SELECT sum(amount) FROM events_pq` makes DuckDB return all
# twenty columns for every row and PostgreSQL adds them up. The deployed path
# has no projection pushdown at all. `pushdown` is the same data with the
# projection where a column store wants it, so the gap between the two columns
# is the cost of the bridge rather than a property of the mirror.
#
# Reporting only `view` would understate the mirror. Reporting only `pushdown`
# would describe something nobody can currently deploy. Both, side by side, is
# the only version of this table that is not an argument for something.
#
#   bash bench/scripts/projection_benefit.sh
#   SCALES="2000000" REPS=5 bash bench/scripts/projection_benefit.sh
#
# Writes bench/results/projection_benefit.json, which is what the chart in
# docs/36 is drawn from — no number in that document is typed by hand.
set -uo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
PG=/usr/lib/postgresql/17/bin
BASE=/var/lib/postgresql/qs-proj
PRIMARY=$BASE/primary
STANDBY=$BASE/standby
MIRROR=$BASE/mirror
PPORT=5465
SPORT=5466
DB=app
GO=$REPO/go
HEALTH=127.0.0.1:9202
# Two scales by default. The question "is it getting better with size" needs at
# least two points, and a third costs more than it adds on a shared machine.
SCALES=${SCALES:-"500000 2000000"}
REPS=${REPS:-3}
OUT_JSON=${OUT_JSON:-$REPO/bench/results/projection_benefit.json}
TSV=$BASE/rows.tsv
FAIL=0

say()  { printf '\n== %s ==\n' "$*"; }
bad()  { printf 'FAIL: %s\n' "$*"; FAIL=1; }
skip() { printf '\nINCOMPLETE — section skipped, which is NOT a pass: %s\n' "$*"; exit 2; }
psq()  { su postgres -c "$PG/psql -h /tmp -p $1 -U ${3:-postgres} -d ${4:-$DB} -Atc \"$2\"" 2>&1; }

# SQL THROUGH A FILE, never through -c, for anything containing dollar quoting.
#
# psq() interpolates its argument into a double-quoted string that `su -c` then
# hands to a SECOND shell. That shell expands $qs$ — an unset variable — to
# nothing, leaving a bare `$`, and every pushdown query came back as
#
#     ERROR: syntax error at or near "$"
#     LINE 1: SELECT (r['v'])::text FROM duckdb.query($
#
# in about 0.3ms, which is exactly fast enough to look like a spectacular
# result. The file never goes near a shell.
sqlfile() { # <path> <sql>
  printf '%s\n' "$2" > "$1"; chown postgres:postgres "$1"
}
run_f()   { su postgres -c "$PG/psql -h /tmp -p $1 -U $2 -d $DB -Atq -f $3" 2>&1; }
time_f()  { su postgres -c "$PG/psql -h /tmp -p $1 -U $2 -d $DB -q -c '\\timing on' -f $3" 2>&1; }

cleanup() {
  pkill -x qs-mirror 2>/dev/null
  su postgres -c "$PG/pg_ctl -D $STANDBY -m immediate stop" >/dev/null 2>&1
  su postgres -c "$PG/pg_ctl -D $PRIMARY -m immediate stop" >/dev/null 2>&1
  return 0
}
trap cleanup EXIT

# ---------------------------------------------------------------- preconditions
say "0. preconditions"
printf 'host: %s vCPU, %s\n' "$(nproc)" "$(awk '/MemTotal/{print $2, $3}' /proc/meminfo)"
LIBDIR=$($PG/pg_config --pkglibdir 2>/dev/null) || skip "no PostgreSQL 17 pg_config"
[ -f "$LIBDIR/pg_duckdb.so" ] || skip "no pg_duckdb.so in $LIBDIR — see bench/patches/README.md"
# The GUC, not the file: a stock build passes a file check and then fails much
# later as "permission denied" on the mirror, which reads like a different bug.
grep -qa "duckdb.allowed_directories" "$LIBDIR/pg_duckdb.so" \
  || skip "pg_duckdb.so has no duckdb.allowed_directories: stock build, not the patched one"

# Disk, checked here because of how it presents otherwise. The widest scale
# writes a heap, a basebackup of it, and a mirror; when it runs out, PostgreSQL
# reports it as a failure to complete crash recovery several minutes later.
need_gb=$(awk -v s="$(echo "$SCALES" | tr ' ' '\n' | sort -n | tail -1)" \
  'BEGIN{ printf "%d", 3 + s*420*2.4/1073741824 }')
avail_gb=$(df -BG --output=avail / 2>/dev/null | tail -1 | tr -dc '0-9')
if [ -n "$avail_gb" ] && [ "$avail_gb" -lt "$need_gb" ]; then
  skip "${avail_gb}G free on /, and the widest scale needs about ${need_gb}G"
fi
echo "  ${avail_gb}G free, about ${need_gb}G needed"
echo "  scales: $SCALES   reps: $REPS (min, not mean)"

( cd "$GO" && go build -o "$BASE.qs-mirror" ./cmd/qs-mirror ) 2>/dev/null \
  || { mkdir -p "$BASE"; ( cd "$GO" && go build -o "$BASE.qs-mirror" ./cmd/qs-mirror ) \
       || skip "qs-mirror did not build"; }
( cd "$GO" && go build -o "$BASE.qs-query" ./cmd/qs-query ) || skip "qs-query did not build"

: > "$TSV" 2>/dev/null || { mkdir -p "$BASE"; : > "$TSV"; }

# ---------------------------------------------------------------- the fixture
#
# TWENTY columns, and the width is the point. Five numeric columns to aggregate,
# four low-cardinality text columns to group and filter on, and eleven wide text
# columns that most queries never touch — which is what a fact table looks like
# and what gives a column store something to skip. On the five-column table in
# serving_pg17.sh there is almost nothing to skip, so that measurement is taken
# where this mechanism is weakest.
build() {
  local rows=$1
  cleanup
  rm -rf "$BASE"; mkdir -p "$BASE" "$(dirname "$TSV")"; chown postgres:postgres "$BASE"
  : > "$TSV"

  su postgres -c "$PG/initdb -D $PRIMARY -A trust" >/dev/null 2>&1 || skip "initdb failed"
  { echo "port = $PPORT"; echo "listen_addresses = 'localhost'";
    echo "unix_socket_directories = '/tmp'"; echo "wal_level = logical";
    echo "shared_preload_libraries = 'pg_duckdb'";
    echo "duckdb.postgres_role = 'duckdb_users'";
    echo "duckdb.allowed_directories = '$MIRROR'";
    # Both sides get the same memory. A comparison where one engine was given
    # more RAM than the other is not a comparison.
    echo "shared_buffers = 1GB"; echo "work_mem = 64MB";
  } >> "$PRIMARY/postgresql.conf"
  su postgres -c "$PG/pg_ctl -D $PRIMARY -l $BASE/primary.log -w start" >/dev/null 2>&1 \
    || { tail -20 "$BASE/primary.log"; skip "primary would not start"; }

  psq $PPORT "CREATE DATABASE $DB" postgres postgres >/dev/null
  psq $PPORT "CREATE EXTENSION pg_duckdb" >/dev/null 2>&1 || skip "CREATE EXTENSION pg_duckdb failed"

  psq $PPORT "CREATE TABLE events(
      id          bigint PRIMARY KEY,
      ts          timestamptz    NOT NULL,
      sku         text           NOT NULL,
      region      text           NOT NULL,
      channel     text           NOT NULL,
      status      text           NOT NULL,
      qty         integer        NOT NULL,
      amount      numeric(12,2)  NOT NULL,
      discount    numeric(12,2)  NOT NULL,
      tax         numeric(12,2)  NOT NULL,
      customer_id bigint         NOT NULL,
      session_id  text           NOT NULL,
      user_agent  text           NOT NULL,
      referrer    text           NOT NULL,
      note        text,
      attrs_a     text           NOT NULL,
      attrs_b     text           NOT NULL,
      attrs_c     text           NOT NULL,
      attrs_d     text           NOT NULL,
      attrs_e     text           NOT NULL)" >/dev/null

  psq $PPORT "INSERT INTO events SELECT
      g,
      now() - (g||' seconds')::interval,
      'SKU-'||(g%997),
      (ARRAY['emea','amer','apac','latam'])[1+g%4],
      (ARRAY['web','ios','android','partner','pos'])[1+g%5],
      (ARRAY['paid','pending','refunded'])[1+g%3],
      1+g%9,
      (g%10000)/100.0,
      (g%700)/100.0,
      (g%900)/100.0,
      1+g%50000,
      md5(g::text),
      'Mozilla/5.0 (compatible; bench/1.0; build '||md5((g*7)::text)||')',
      'https://example.test/'||md5((g*13)::text)||'/landing?utm='||(g%64),
      CASE WHEN g <= 3 THEN NULL ELSE 'n-'||lpad(g::text,9,'0') END,
      md5((g*3)::text), md5((g*5)::text), md5((g*11)::text),
      md5((g*17)::text), md5((g*19)::text)
    FROM generate_series(1,$rows) g" >/dev/null || skip "seed failed at $rows rows"

  # Deletes and updates BEFORE the mirror is built, so it carries a deletion
  # vector and column-partial deltas and has to reconstruct rather than scan one
  # clean file. A serving path measured only on a freshly compacted mirror is
  # measuring something nobody runs.
  psq $PPORT "DELETE FROM events WHERE id % 50 = 0" >/dev/null
  psq $PPORT "UPDATE events SET amount = amount + 1 WHERE id % 37 = 0" >/dev/null
  psq $PPORT "VACUUM ANALYZE events" >/dev/null
  LIVE=$(psq $PPORT 'SELECT count(*) FROM events')
  HEAP_BYTES=$(psq $PPORT "SELECT pg_total_relation_size('events')")

  # standby
  psq $PPORT "SELECT pg_create_physical_replication_slot('proj_standby', true)" postgres postgres >/dev/null
  su postgres -c "$PG/pg_basebackup -D $STANDBY -R -X stream -S proj_standby -c fast \
    -d 'host=/tmp port=$PPORT user=postgres dbname=postgres'" >/dev/null 2>&1 \
    || skip "pg_basebackup failed"
  { echo "port = $SPORT"; echo "unix_socket_directories = '/tmp'";
    echo "hot_standby_feedback = on"; } >> "$STANDBY/postgresql.conf"
  su postgres -c "$PG/pg_ctl -D $STANDBY -l $BASE/standby.log -w start" >/dev/null 2>&1 \
    || { tail -20 "$BASE/standby.log"; skip "standby would not start"; }

  # the mirror, built by the production sidecar on the standby
  cp "$BASE.qs-mirror" "$BASE/qs-mirror"; cp "$BASE.qs-query" "$BASE/qs-query"
  chown -R postgres:postgres "$BASE"
  su postgres -c "QS_CLUSTER=proj QS_MODE=shadow QS_INGEST=logical \
    QS_TABLES=public.events QS_SLOT=qs_proj QS_PUBLICATION=qs_proj \
    QS_MIRROR_PATH=$MIRROR QS_FRESHNESS_SLO=30s \
    QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$PPORT \
    QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=$SPORT \
    QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=proj-2 \
    QS_HEALTH_ADDR=$HEALTH $BASE/qs-mirror" >> "$BASE/mirror.log" 2>&1 &

  local code=
  for _ in $(seq 1 600); do
    code=$(curl -s -o /dev/null -w '%{http_code}' "http://$HEALTH/readyz" 2>/dev/null)
    [ "$code" = "200" ] && break
    sleep 1
  done
  [ "${code:-}" = "200" ] || { tail -30 "$BASE/mirror.log"; skip "the sidecar never became ready"; }
  MIRROR_BYTES=$(du -sb "$MIRROR" 2>/dev/null | cut -f1)

  # the view, and the bare SELECT inside it
  VIEW_SQL=$(su postgres -c "$BASE/qs-query -mirror $MIRROR -table public.events -view events_pq \
    -engine postgres -dsn 'host=/tmp port=$PPORT user=postgres dbname=$DB'" 2>/dev/null) \
    || skip "qs-query produced no view"
  printf '%s\n' "$VIEW_SQL" | su postgres -c "$PG/psql -h /tmp -p $PPORT -U postgres -d $DB -q" \
    || skip "the published view would not create"

  # The mirror's own SELECT, lifted back out of the view between the $qs$
  # markers. Extracted rather than regenerated so the pushdown column is
  # provably reading the SAME files through the SAME scan as the view column —
  # if it were built separately the two could drift and the comparison would
  # silently stop being like-for-like.
  MIRROR_SELECT=$(printf '%s\n' "$VIEW_SQL" | awk '/\$qs\$/{n++; next} n==1')
  [ -n "$MIRROR_SELECT" ] || skip "could not lift the mirror SELECT out of the view"

  psq $PPORT "CREATE ROLE duckdb_users" postgres >/dev/null
  psq $PPORT "CREATE ROLE analyst LOGIN IN ROLE duckdb_users" postgres >/dev/null
  psq $PPORT "GRANT USAGE ON SCHEMA public TO analyst" >/dev/null
  psq $PPORT "GRANT SELECT ON events_pq TO analyst" >/dev/null
  psq $PPORT "SELECT pg_switch_wal()" postgres postgres >/dev/null
  for _ in $(seq 1 120); do
    [ "$(psq $SPORT "SELECT to_regclass('public.events_pq') IS NOT NULL")" = "t" ] && break
    sleep 0.5
  done
  [ "$(psq $SPORT "SELECT to_regclass('public.events_pq') IS NOT NULL")" = "t" ] \
    || skip "the view never reached the standby"

  printf '  %s live rows, heap %s, mirror %s\n' "$LIVE" \
    "$(numfmt --to=iec "$HEAP_BYTES")" "$(numfmt --to=iec "${MIRROR_BYTES:-0}")"
}

# timed <port> <user> <sqlfile>  -> best of $REPS, in ms
#
# The MINIMUM, not the mean. What is being measured is how fast the engine CAN
# answer; a mean over a few runs on a shared machine measures the noise as much
# as the query, and the noise is one-sided.
timed() {
  local port=$1 user=$2 f=$3 best=999999 t
  for _ in $(seq 1 "$REPS"); do
    t=$(time_f "$port" "$user" "$f" | sed -n 's/^Time: \([0-9.]*\) ms.*/\1/p' | tail -1)
    [ -n "$t" ] || { echo "ERR"; return; }
    awk -v a="$t" -v b="$best" 'BEGIN{exit !(a<b)}' && best=$t
  done
  echo "$best"
}

# row <label> <columns-touched> <heap-sql> <view-sql> <duckdb-select>
#
# The last argument is a COMPLETE DuckDB SELECT returning one column `v`, not an
# expression to be pasted into one. The group-by row needs its own subquery
# shape, and building it by string-substitution into a fixed template produced a
# query that scanned the mirror twice.
#
# Every timing is also CHECKED. A faster wrong answer is not a result, and the
# mirror has three separate ways to be quietly wrong about it (retired rows,
# partial deltas, a stale manifest).
row() {
  local label=$1 cols=$2 heap_sql=$3 view_sql=$4 duck_select=$5
  local push_sql="SELECT (r['v'])::text FROM duckdb.query(\$qs\$
$duck_select
\$qs\$) r"

  local fh=$BASE/q_heap.sql fv=$BASE/q_view.sql fp=$BASE/q_push.sql
  sqlfile "$fh" "$heap_sql"
  sqlfile "$fv" "$view_sql"
  sqlfile "$fp" "$push_sql"

  local h v p hv vv pv agree=yes
  h=$(timed $SPORT postgres "$fh")
  v=$(timed $SPORT analyst  "$fv")
  p=$(timed $SPORT analyst  "$fp")

  hv=$(run_f $SPORT postgres "$fh")
  vv=$(run_f $SPORT analyst  "$fv")
  pv=$(run_f $SPORT analyst  "$fp")
  # Numeric equality, not string equality: the two engines format the same value
  # differently often enough (trailing zeros, exponent form) that comparing text
  # would report disagreements that are not disagreements.
  same() { awk -v a="$1" -v b="$2" 'BEGIN{
      if (a==b) {print "y"; exit}
      if (a+0==0 && a!="0" || b+0==0 && b!="0") {print "n"; exit}
      d=a-b; if (d<0) d=-d; m=(a<0?-a:a); if (m<1) m=1;
      print (d/m < 1e-9) ? "y" : "n" }'; }
  [ "$(same "$hv" "$vv")" = "y" ] || { agree="NO view=$vv heap=$hv"; bad "$label: view disagrees ($vv vs $hv)"; }
  [ "$(same "$hv" "$pv")" = "y" ] || { agree="NO push=$pv heap=$hv"; bad "$label: pushdown disagrees ($pv vs $hv)"; }

  local rv rp
  rv=$(awk -v h="$h" -v x="$v" 'BEGIN{ printf (x>0)? "%.2f" : "0", h/x }')
  rp=$(awk -v h="$h" -v x="$p" 'BEGIN{ printf (x>0)? "%.2f" : "0", h/x }')
  printf '  %-26s %3s %10s %10s %10s   %6sx %6sx  %s\n' \
    "$label" "$cols" "$h" "$v" "$p" "$rv" "$rp" "$agree"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$SCALE" "$LIVE" "$label" "$cols" "$h" "$v" "$p" "$rv" "$rp" "$agree" >> "$TSV"
}

# ---------------------------------------------------------------- the sweep
ALL_TSV=$BASE.all.tsv
: > "$ALL_TSV"
for SCALE in $SCALES; do
  say "scale: $SCALE rows"
  build "$SCALE"

  printf '\n  %-26s %3s %10s %10s %10s   %7s %7s  %s\n' \
    query cols "heap ms" "view ms" "push ms" "view" "push" agree
  printf '  %s\n' "$(printf '%.0s-' $(seq 1 92))"

  # count(*) touches no columns at all, and Parquet answers it from row-group
  # metadata without reading any. Kept, and labelled 0, precisely so the graph
  # shows where the curve starts rather than starting it at a flattering point.
  row "count(*)" 0 \
    "SELECT count(*)::text FROM events" \
    "SELECT count(*)::text FROM events_pq" \
    "SELECT count(*) AS v FROM ( $MIRROR_SELECT )"

  row "sum 1 column" 1 \
    "SELECT sum(amount)::text FROM events" \
    "SELECT sum(amount)::text FROM events_pq" \
    "SELECT sum(amount) AS v FROM ( $MIRROR_SELECT )"

  row "sum 2 columns" 2 \
    "SELECT (sum(amount)+sum(qty))::text FROM events" \
    "SELECT (sum(amount)+sum(qty))::text FROM events_pq" \
    "SELECT sum(amount)+sum(qty) AS v FROM ( $MIRROR_SELECT )"

  row "filter + aggregate" 2 \
    "SELECT sum(amount)::text FROM events WHERE status = 'paid'" \
    "SELECT sum(amount)::text FROM events_pq WHERE status = 'paid'" \
    "SELECT sum(amount) FILTER (WHERE status = 'paid') AS v FROM ( $MIRROR_SELECT )"

  row "group by, 2 columns" 2 \
    "SELECT sum(amount)::text FROM (SELECT region, sum(amount) AS amount FROM events GROUP BY region) t" \
    "SELECT sum(amount)::text FROM (SELECT region, sum(amount) AS amount FROM events_pq GROUP BY region) t" \
    "SELECT sum(a) AS v FROM (SELECT region, sum(amount) AS a FROM ( $MIRROR_SELECT ) GROUP BY region)"

  row "sum 5 columns" 5 \
    "SELECT (sum(amount)+sum(discount)+sum(tax)+sum(qty)+sum(customer_id))::text FROM events" \
    "SELECT (sum(amount)+sum(discount)+sum(tax)+sum(qty)+sum(customer_id))::text FROM events_pq" \
    "SELECT sum(amount)+sum(discount)+sum(tax)+sum(qty)+sum(customer_id) AS v FROM ( $MIRROR_SELECT )"

  row "touch 10 columns" 10 \
    "SELECT (sum(amount)+sum(discount)+sum(tax)+sum(qty)+sum(customer_id)
             +sum(length(sku))+sum(length(region))+sum(length(channel))
             +sum(length(status))+sum(length(session_id)))::text FROM events" \
    "SELECT (sum(amount)+sum(discount)+sum(tax)+sum(qty)+sum(customer_id)
             +sum(length(sku))+sum(length(region))+sum(length(channel))
             +sum(length(status))+sum(length(session_id)))::text FROM events_pq" \
    "SELECT sum(amount)+sum(discount)+sum(tax)+sum(qty)+sum(customer_id)
     +sum(length(sku))+sum(length(region))+sum(length(channel))
     +sum(length(status))+sum(length(session_id)) AS v FROM ( $MIRROR_SELECT )"

  # The unflattering end, and it has to be here. A query that reads every column
  # gives a column store nothing to skip, so if the mirror still won here it
  # would be winning on encoding rather than on projection — which is a
  # different claim and worth being able to tell apart.
  row "touch all 19 columns" 19 \
    "SELECT (sum(amount)+sum(discount)+sum(tax)+sum(qty)+sum(customer_id)
             +sum(length(sku))+sum(length(region))+sum(length(channel))
             +sum(length(status))+sum(length(session_id))+sum(length(user_agent))
             +sum(length(referrer))+sum(coalesce(length(note),0))
             +sum(length(attrs_a))+sum(length(attrs_b))+sum(length(attrs_c))
             +sum(length(attrs_d))+sum(length(attrs_e))
             +sum(extract(epoch from ts)::bigint))::text FROM events" \
    "SELECT (sum(amount)+sum(discount)+sum(tax)+sum(qty)+sum(customer_id)
             +sum(length(sku))+sum(length(region))+sum(length(channel))
             +sum(length(status))+sum(length(session_id))+sum(length(user_agent))
             +sum(length(referrer))+sum(coalesce(length(note),0))
             +sum(length(attrs_a))+sum(length(attrs_b))+sum(length(attrs_c))
             +sum(length(attrs_d))+sum(length(attrs_e))
             +sum(extract(epoch from ts)::bigint))::text FROM events_pq" \
    "SELECT sum(amount)+sum(discount)+sum(tax)+sum(qty)+sum(customer_id)
     +sum(length(sku))+sum(length(region))+sum(length(channel))
     +sum(length(status))+sum(length(session_id))+sum(length(user_agent))
     +sum(length(referrer))+sum(coalesce(length(note),0))
     +sum(length(attrs_a))+sum(length(attrs_b))+sum(length(attrs_c))
     +sum(length(attrs_d))+sum(length(attrs_e))
     +sum(epoch(ts)::bigint) AS v FROM ( $MIRROR_SELECT )"

  # The one the mirror loses, and the reason mode: takeover is gated behind an
  # acknowledgement. Leaving it out would make this a sales document.
  row "point lookup by key" 1 \
    "SELECT sum(qty)::text FROM events WHERE id = 123457" \
    "SELECT sum(qty)::text FROM events_pq WHERE id = 123457" \
    "SELECT sum(qty) FILTER (WHERE id = 123457) AS v FROM ( $MIRROR_SELECT )"

  cat "$TSV" >> "$ALL_TSV"
  printf '%s\t%s\t%s\t%s\n' "__meta__" "$SCALE" "$LIVE" "${HEAP_BYTES}:${MIRROR_BYTES}" >> "$ALL_TSV"
done

# ---------------------------------------------------------------- the record
say "recording"
mkdir -p "$(dirname "$OUT_JSON")"
python3 - "$ALL_TSV" "$OUT_JSON" "$REPS" <<'PY'
import json, subprocess, sys, datetime
tsv, out, reps = sys.argv[1], sys.argv[2], int(sys.argv[3])
rows, meta = [], {}
for line in open(tsv):
    f = line.rstrip("\n").split("\t")
    if f[0] == "__meta__":
        heap, mirror = f[3].split(":")
        meta[int(f[1])] = {"live_rows": int(f[2]),
                           "heap_bytes": int(heap), "mirror_bytes": int(mirror or 0)}
        continue
    if len(f) < 10:
        continue
    rows.append({"scale": int(f[0]), "live_rows": int(f[1]), "query": f[2],
                 "columns_touched": int(f[3]),
                 "heap_ms": float(f[4]), "view_ms": float(f[5]), "pushdown_ms": float(f[6]),
                 "view_speedup": float(f[7]), "pushdown_speedup": float(f[8]),
                 "agrees": f[9] == "yes"})
doc = {
    "generated": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
    "reps": reps, "statistic": "min",
    "host": {"vcpu": int(subprocess.run(["nproc"], capture_output=True, text=True).stdout or 0),
             "mem_kb": int(open("/proc/meminfo").readline().split()[1])},
    "scales": meta, "rows": rows,
}
json.dump(doc, open(out, "w"), indent=2, sort_keys=True)
print(f"  wrote {out}: {len(rows)} measurements over {len(meta)} scale(s)")
PY

say "result"
if [ $FAIL -eq 0 ]; then
  echo "PASS — every timed query returned the same answer from all three paths."
  echo "The numbers, and what they mean, are in docs/36."
else
  echo "FAIL — a path disagreed with the heap, so its timings mean nothing."
fi
exit $FAIL
