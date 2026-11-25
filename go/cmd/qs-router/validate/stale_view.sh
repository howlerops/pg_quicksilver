#!/usr/bin/env bash
# What a compaction does to a published view, and what the router does about
# it. The view names the mirror's files; this points it at a file that is not
# there (which is what the view looks like after its files were replaced),
# with the router already holding a cached "mirror" decision.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
BASE=${BASE:-/var/lib/postgresql/qsrouter}
PG=/usr/lib/postgresql/17/bin
R="host=127.0.0.1 port=${RPORT:-5490} dbname=app user=postgres"
FAIL=0
expect() { if [ "$2" = "$3" ]; then echo "ok    $1: $2"; else echo "FAIL  $1: got '$2', want '$3'"; FAIL=1; fi; }

"$HERE/publish.sh" || exit 1
"$HERE/router.sh" start -cache-ttl 10m || exit 1
python3 - "$R" <<'EOF' &
import sys, time, psycopg
with psycopg.connect(sys.argv[1], autocommit=True) as c:
    q = "SELECT count(*) FROM orders WHERE customer_id > %s"
    show = lambda: c.execute("SHOW qs.last_route", prepare=False).fetchone()[0]
    print("before:", c.execute(q, (0,), prepare=True).fetchone()[0], "|", show(), flush=True)
    time.sleep(3)   # the view is broken in here
    print("named prepared statement, view broken:", c.execute(q, (0,), prepare=True).fetchone()[0], "|", show(), flush=True)
    print("again:", c.execute(q, (0,), prepare=True).fetchone()[0], "|", show(), flush=True)
EOF
expect "simple query before" "$(psql "$R" -XAt -c 'SELECT count(*) FROM orders' -c 'SHOW qs.last_route' | tr '\n' '|' | cut -d: -f1)" "1000000|mirror"
sleep 1
sed 's/[0-9]*\.parquet/999999.parquet/g' "$BASE/view.orders.sql" > "$BASE/view.broken.sql"; chown postgres "$BASE/view.broken.sql"
su postgres -c "$PG/psql -h /tmp -p ${PPORT:-5491} -d app -q -v ON_ERROR_STOP=1 -f $BASE/view.broken.sql" || exit 1
echo "direct:  $(psql "host=127.0.0.1 port=${PPORT:-5491} dbname=app user=postgres" -XAtc 'SELECT count(*) FROM qs_mirror.orders' 2>&1 | head -1 | cut -c1-110)"
out=$(psql "$R" -XAt -c 'SELECT count(*) FROM orders' -c 'SHOW qs.last_route' 2>&1)
expect "simple query, cached mirror decision, view broken" "$(echo "$out" | head -1)" "1000000"
echo "      $(echo "$out" | tail -1 | cut -c1-150)"
wait
"$HERE/publish.sh" || exit 1
exit $FAIL
