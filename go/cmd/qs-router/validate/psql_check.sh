#!/usr/bin/env bash
# psql through qs-router: simple protocol, \bind (extended), SQL-level PREPARE,
# a transaction, COPY out and in, an sslmode=prefer connect, and a cancel
# (SIGINT, which psql turns into a CancelRequest on a second connection).
set -uo pipefail
R="host=127.0.0.1 port=${RPORT:-5490} dbname=app user=postgres"
FAIL=0
expect() { if [ "$2" = "$3" ]; then echo "ok    $1: $2"; else echo "FAIL  $1: got '$2', want '$3'"; FAIL=1; fi; }
q() { psql "$R" -XAt "$@" 2>&1; }

expect "simple query" "$(q -c 'SELECT count(*) FROM customers')" "10000"
expect "extended protocol (\\bind)" "$(printf '%s\n' 'SELECT name FROM customers WHERE id = $1 \bind 42 \g' | q)" "cust-42"
expect "SQL PREPARE / EXECUTE" "$(q -c 'PREPARE p(int) AS SELECT name FROM customers WHERE id = $1' -c 'EXECUTE p(7)' -c 'SHOW qs.last_route' | tail -2 | tr '\n' '|')" "cust-7|heap: not a plain read (EXECUTE)|"
expect "transaction" "$(q -c 'BEGIN' -c "INSERT INTO audit_log(what) VALUES ('psql-tx')" -c "SELECT count(*) FROM audit_log WHERE what = 'psql-tx'" -c 'ROLLBACK' -c "SELECT count(*) FROM audit_log WHERE what = 'psql-tx'" | tr '\n' ' ')" "BEGIN INSERT 0 1 1 ROLLBACK 0 "
expect "COPY out" "$(q -c '\copy (SELECT id FROM orders WHERE id <= 2000) TO STDOUT' | wc -l | tr -d ' ')" "2000"
expect "COPY in" "$(seq 1 3000 | sed 's/^/psql-copy-/' | q -c '\copy audit_log(what) FROM STDIN' -c "DELETE FROM audit_log WHERE what LIKE 'psql-copy-%'" | tr '\n' ' ')" "COPY 3000 DELETE 3000 "
expect "multi-statement Query" "$(q -c 'SELECT 1; SELECT 2' | tr '\n' ' ')" "1 2 "
expect "error passes through" "$(q -c 'SELECT * FROM no_such_table' | head -1)" 'ERROR:  relation "no_such_table" does not exist'
expect "sslmode=prefer (SSLRequest refused, falls back)" "$(psql "$R sslmode=prefer" -XAtc 'SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()' 2>&1)" "f"
out=$(psql "$R sslmode=require" -XAtc 'SELECT 1' 2>&1)
case "$out" in *"does not support SSL"*) echo "ok    sslmode=require is refused: $out" ;; *) echo "FAIL  sslmode=require: $out"; FAIL=1 ;; esac

start=$(date +%s.%N)
psql "$R" -XAtc 'SELECT pg_sleep(30)' > /var/tmp/qs_router_psql_cancel.$$ 2>&1 &
pid=$!
sleep 0.5
kill -INT $pid
wait $pid
took=$(echo "$(date +%s.%N) - $start" | bc)
expect "cancel (SIGINT)" "$(tail -1 /var/tmp/qs_router_psql_cancel.$$)" "ERROR:  canceling statement due to user request"
rm -f /var/tmp/qs_router_psql_cancel.$$
echo "      cancelled after ${took}s of a 30s sleep"
exit $FAIL
