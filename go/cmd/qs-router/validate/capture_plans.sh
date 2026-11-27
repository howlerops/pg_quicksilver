#!/usr/bin/env bash
# Writes the EXPLAIN (VERBOSE, FORMAT JSON) output the decision tests run over,
# from setup.sh's primary, into ../testdata. These are the plans PostgreSQL
# 17.12 produced for the 1M-row table in seed.sql; they are captured, not typed.
set -euo pipefail
OUT=$(cd "$(dirname "$0")/.." && pwd)/testdata
mkdir -p "$OUT"
cap() {
  psql "host=127.0.0.1 port=${PPORT:-5491} dbname=app user=postgres" -XAtc "EXPLAIN (VERBOSE, FORMAT JSON) $2" > "$OUT/$1.json"
}
cap pk_lookup        "SELECT * FROM orders WHERE id = 4242"
cap in_list          "SELECT * FROM orders WHERE id IN (1, 2, 3, 500000)"
cap small_range      "SELECT * FROM orders WHERE created_at >= '2024-01-05' AND created_at < '2024-01-05 00:10'"
cap count_star       "SELECT count(*) FROM orders"
cap group_by         "SELECT status, sum(amount) FROM orders GROUP BY status"
cap range_aggregate  "SELECT sum(amount) FROM orders WHERE created_at >= '2024-01-03' AND created_at < '2024-01-09'"
cap join             "SELECT c.region, sum(o.amount) FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY c.region"
cap limit_no_order   "SELECT * FROM orders LIMIT 10"
cap limit_over_sort  "SELECT * FROM orders ORDER BY amount DESC LIMIT 10"
cap select_for_update "SELECT * FROM orders WHERE id = 7 FOR UPDATE"
cap cte_write        "WITH w AS (INSERT INTO audit_log(what) VALUES ('x') RETURNING id) SELECT count(*) FROM w, orders"
cap function_call    "SELECT order_total(5)"
cap function_scan    "SELECT count(*) FROM orders o JOIN generate_series(1, 10) g ON g = o.id"
cap unmirrored       "SELECT count(*) FROM orders o JOIN audit_log a ON a.id = o.id"
cap customer_selective   "SELECT count(*) FROM orders WHERE customer_id = 5"
cap customer_unselective "SELECT count(*) FROM orders WHERE customer_id = 1"
cap pk_join          "SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.id = 42"
cap no_table         "SELECT now()"
cap tid_scan         "SELECT * FROM orders WHERE ctid = '(0,1)'"
# The same statement with the mirror schema in front: what pg_duckdb answers.
psql "host=127.0.0.1 port=${PPORT:-5491} dbname=app user=postgres" -XAt \
  -c "SET search_path = qs_mirror, public" -c "EXPLAIN (FORMAT JSON) SELECT count(*) FROM orders" | tail -n +2 > "$OUT/mirror_count_star.json"
ls "$OUT"
