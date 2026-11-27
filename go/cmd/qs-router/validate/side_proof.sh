#!/usr/bin/env bash
# Proves that the side the router REPORTS is the side that ANSWERED, and that
# the freshness gate works, by making the two sides disagree:
#
#   stop the sidecar, then insert one order on the primary. The heap now has
#   N+1 orders and the mirror N, and customer 1 has one more order on the heap.
#
# Every count below can only have come from one side.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
BASE=${BASE:-/var/lib/postgresql/qsrouter}
R="host=127.0.0.1 port=${RPORT:-5490} dbname=app user=postgres"
D="host=127.0.0.1 port=${PPORT:-5491} dbname=app user=postgres"
FAIL=0
rq() { psql "$R" -XAt "$@" 2>&1; }
dq() { psql "$D" -XAtc "$1" 2>&1; }
expect() { # label got want
  if [ "$2" = "$3" ]; then echo "ok    $1: $2"; else echo "FAIL  $1: got '$2', want '$3'"; FAIL=1; fi
}
route() { rq -c "$1" -c "SHOW qs.last_route" | tail -1 | sed 's/ \[cached\]//'; }
answer() { rq -c "$1" | head -1; }

"$HERE/sidecar.sh" stop || { echo "sidecar would not stop"; exit 1; }
"$HERE/publish.sh" || exit 1   # the views must name the files as they are now
dq "DELETE FROM orders WHERE id = 0" >/dev/null
N=$(dq "SELECT count(*) FROM qs_mirror.orders")
dq "INSERT INTO orders VALUES (0, 1, 'new', 1.00, '2024-01-01', 'only on the heap')" >/dev/null
H=$(dq "SELECT count(*) FROM public.orders")
echo "heap has $H orders, mirror has $N (sidecar stopped)"
[ "$H" = "$((N + 1))" ] || { echo "FAIL: the sides do not differ by one"; FAIL=1; }
STATE=$BASE/mirror/public.orders/state.json
[ -f "$STATE" ] || STATE=$(ls "$BASE"/mirror/*orders*/state.json 2>/dev/null | head -1)
echo "mirror applied_lsn: $(grep -o '"applied_lsn": *"[^"]*"' "$STATE")  server: $(dq 'SELECT pg_current_wal_lsn()')"

echo; echo "== 1. gate open (bound 16 MiB): the mirror answers what the router says it answers"
"$HERE/router.sh" start -mirror-lsn "file:$STATE" -max-lag-bytes 16777216 || exit 1
expect "count(*) answer"            "$(answer 'SELECT count(*) FROM orders')" "$N"
expect "count(*) route"             "$(route 'SELECT count(*) FROM orders' | cut -d: -f1)" "mirror"
expect "pk lookup of the heap-only row" "$(answer 'SELECT note FROM orders WHERE id = 0')" "only on the heap"
expect "pk lookup route"            "$(route 'SELECT note FROM orders WHERE id = 0' | cut -d: -f1)" "heap"
expect "qs.route=heap count(*)"     "$(rq -c "SET qs.route = 'heap'" -c 'SELECT count(*) FROM orders' | tail -1)" "$H"
expect "qs.route=mirror pk lookup of the heap-only row" "$(rq -c "SET qs.route = 'mirror'" -c 'SELECT count(*) FROM orders WHERE id = 0' | tail -1)" "0"
expect "schema-qualified count(*)"  "$(answer 'SELECT count(*) FROM public.orders')" "$H"
expect "count(*) in a transaction"  "$(rq -c 'BEGIN' -c 'SELECT count(*) FROM orders' -c 'COMMIT' | sed -n 2p)" "$H"
# One named server-side prepared statement, two parameters, three rounds.
C1H=$(dq "SELECT count(*) FROM public.orders WHERE customer_id = 1")
C5=$(dq "SELECT count(*) FROM public.orders WHERE customer_id = 5")
P=$(python3 "$HERE/prepared_sides.py" "$R" 2>&1)
WANT="1 $((C1H - 1)) mirror
5 $C5 heap
1 $((C1H - 1)) mirror
5 $C5 heap
1 $((C1H - 1)) mirror
5 $C5 heap
server-side statements: 1"
expect "prepared statement (customer, answer, route) x3" "$(echo "$P" | tr '\n' '|')" "$(echo "$WANT" | tr '\n' '|')"
# The same through psql's \bind (extended protocol, unnamed statement).
expect "psql extended protocol, customer 1" "$(printf '%s\n' 'SELECT count(*) FROM orders WHERE customer_id = $1 \bind 1 \g' | psql "$R" -XAt 2>&1)" "$((C1H - 1))"

echo; echo "== 2. gate closed (bound 64 bytes): the same statements are answered by the heap"
"$HERE/router.sh" start -mirror-lsn "file:$STATE" -max-lag-bytes 64 || exit 1
expect "count(*) answer" "$(answer 'SELECT count(*) FROM orders')" "$H"
echo "      route: $(route 'SELECT count(*) FROM orders')"
expect "count(*) route" "$(route 'SELECT count(*) FROM orders' | cut -d' ' -f1-3)" "heap: mirror is"
expect "qs.route=mirror does not override the gate" "$(rq -c "SET qs.route = 'mirror'" -c 'SELECT count(*) FROM orders' | tail -1)" "$H"

echo; echo "== 3. position unreadable: not knowing is not fresh"
"$HERE/router.sh" start -mirror-lsn "file:$BASE/no-such-state.json" || exit 1
expect "count(*) answer" "$(answer 'SELECT count(*) FROM orders')" "$H"
echo "      route: $(route 'SELECT count(*) FROM orders')"
"$HERE/router.sh" start -mirror-lsn "http://127.0.0.1:${HPORT:-5493}/readyz" || exit 1
expect "count(*) answer, sidecar endpoint down" "$(answer 'SELECT count(*) FROM orders')" "$H"
echo "      route: $(route 'SELECT count(*) FROM orders')"

echo; echo "== 4. put it back"
dq "DELETE FROM orders WHERE id = 0" >/dev/null
"$HERE/sidecar.sh" start || { echo "FAIL: sidecar did not come back"; FAIL=1; }
for _ in $(seq 1 60); do
  [ "$(dq 'SELECT count(*) FROM qs_mirror.orders')" = "$(dq 'SELECT count(*) FROM public.orders')" ] && break; sleep 0.5
done
expect "sides agree again" "$(dq 'SELECT count(*) FROM qs_mirror.orders')" "$(dq 'SELECT count(*) FROM public.orders')"
"$HERE/publish.sh" || FAIL=1   # the restart compacted; the views named the old files
exit $FAIL
