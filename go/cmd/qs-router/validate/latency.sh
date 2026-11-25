#!/usr/bin/env bash
# What the router adds to a primary-key lookup: per-statement latency over
# N executions, one client, direct and through the router, with the decision
# cache warm and with it off (-cache-ttl 0: every statement is planned first).
#
# pgbench is pinned to CPU 2 (taskset -c 2). PostgreSQL is not pinned. The
# router is pinned to CPU $ROUTER_CPU if that is set, and left alone otherwise;
# the output says which.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
BASE=${BASE:-/var/lib/postgresql/qsrouter}
PG=/usr/lib/postgresql/17/bin
N=${N:-5000}
W=$BASE/bench; rm -rf "$W"; mkdir -p "$W"
cat > "$W/pk.sql" <<'EOF'
\set id random(1, 1000000)
SELECT * FROM orders WHERE id = :id;
EOF
[ -n "${ROUTER_CPU:-}" ] && export PIN="taskset -c $ROUTER_CPU"
where="not pinned"; [ -n "${ROUTER_CPU:-}" ] && where="on CPU $ROUTER_CPU"
echo "host: $(nproc) vCPU; pgbench on CPU 2; router $where; $N executions per row; microseconds"

run() { # label port protocol
  ( cd "$W" && rm -f pgbench_log.* &&
    taskset -c 2 $PG/pgbench -n -h 127.0.0.1 -p "$2" -U postgres -f pk.sql -c 1 -t 200 -M "$3" app >/dev/null 2>&1 &&
    taskset -c 2 $PG/pgbench -n -h 127.0.0.1 -p "$2" -U postgres -f pk.sql -c 1 -t "$N" -M "$3" --log app >/dev/null 2>&1 ) || { echo "pgbench failed: $1"; return 1; }
  sort -n -k3 "$W"/pgbench_log.* | awk -v l="$1" '{a[NR]=$3} END {printf "%-44s n=%d  median %4d  p95 %4d  p99 %4d\n", l, NR, a[int(NR*0.5)], a[int(NR*0.95)], a[int(NR*0.99)]}'
}

# Two routers at once and the three cells interleaved, repeated: this host's
# direct median alone moved between 15 and 80 microseconds from one minute to
# the next, so cells measured minutes apart are not comparable.
"$HERE/router.sh" start >/dev/null || exit 1
RPORT=5494 NOKILL=1 "$HERE/router.sh" start -cache-ttl 0 >/dev/null || exit 1
for proto in simple prepared; do
  for rep in 1 2 3; do
    run "direct, $proto" "${PPORT:-5491}" $proto
    run "router, warm decision cache, $proto" 5490 $proto
    run "router, no cache (plans every time), $proto" 5494 $proto
  done
done
"$HERE/router.sh" stop
