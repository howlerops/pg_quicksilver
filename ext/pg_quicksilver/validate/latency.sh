#!/usr/bin/env bash
# What the extension costs a statement it does not route: a primary-key lookup
# on public.orders (a MIRRORED table, so the extension looks at every one of
# them), one pgbench client pinned to CPU 2, per-transaction latencies from
# pgbench's own log.
#
#   not loaded   shared_preload_libraries = 'pg_duckdb'
#   off          pg_quicksilver loaded, quicksilver.route = off
#   auto         pg_quicksilver loaded, quicksilver.route = auto
#
# Each of prepared and simple protocol, SECS seconds, ROUNDS interleaved rounds
# (this host's own median moves between minutes; see docs/38). The server is
# restarted to change shared_preload_libraries.
set -uo pipefail
. "$(dirname "$0")/env.sh"
SECS=${SECS:-20}; ROUNDS=${ROUNDS:-3}
OUT=${OUT:-$HERE/results/latency.tsv}
W=$BASE/latency; rm -rf "$W"; mkdir -p "$W" "$(dirname "$OUT")"; chown postgres:postgres "$W"
cat > "$W/key.sql" <<'SQL'
\set id random(1, 1000000)
SELECT * FROM public.orders WHERE id = :id;
SQL
chown postgres:postgres "$W/key.sql"
preload() { # value
  sed -i '/^shared_preload_libraries/d' "$BASE/primary/postgresql.conf"
  echo "shared_preload_libraries = '$1'" >> "$BASE/primary/postgresql.conf"
  su postgres -c "$PG/pg_ctl -D $BASE/primary -l $BASE/primary.log -w restart" >/dev/null || die restart
  for _ in $(seq 1 30); do psq $PPORT "SELECT 1" >/dev/null 2>&1 && break; sleep 1; done
}
run() { # label protocol options
  local tag=$1 proto=$2 opts=$3 round=$4
  su postgres -c "cd $W && rm -f lat.* && PGOPTIONS='$opts' taskset -c 2 $PG/pgbench -n -h /tmp -p $PPORT -U postgres -c 1 -T $SECS -M $proto -f key.sql -l --log-prefix=lat $DB" > "$W/pgbench.out" 2>&1 || { cat "$W/pgbench.out"; die pgbench; }
  # pgbench log: client txno latency_us ...
  cat "$W"/lat.* | awk '{print $3}' | sort -n > "$W/sorted"
  local n; n=$(wc -l < "$W/sorted")
  local med p95; med=$(sed -n "$(( (n + 1) / 2 ))p" "$W/sorted"); p95=$(sed -n "$(( n * 95 / 100 ))p" "$W/sorted")
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$tag" "$proto" "$round" "$n" "$med" "$p95" | tee -a "$OUT"
}
printf 'config\tprotocol\tround\ttransactions\tmedian_us\tp95_us\n' > "$OUT"
for round in $(seq 1 "$ROUNDS"); do
  preload 'pg_duckdb'
  for proto in prepared simple; do run "not loaded" $proto "" $round; done
  preload 'pg_duckdb,pg_quicksilver'
  for proto in prepared simple; do
    run "off" $proto "-c quicksilver.route=off" $round
    run "auto" $proto "-c quicksilver.route=auto" $round
  done
done
preload 'pg_duckdb,pg_quicksilver'
# the applied LSNs lived in shared memory: report them again from the durable copy
psq $PPORT "SELECT quicksilver.report_applied_lsn(heap, applied_lsn) FROM quicksilver.mirrors WHERE applied_lsn IS NOT NULL" >/dev/null
rm -rf "$W"
