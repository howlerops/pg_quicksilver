-- EXPLAIN, EXPLAIN ANALYZE and auto_explain on routed statements, against the
-- lake the sidecar built. An explained statement runs on the side it would
-- have run on, once; pg_duckdb's own node is never shown as a child (its
-- EXPLAIN output is made by running the statement again inside DuckDB).
\set VERBOSITY terse
SET quicksilver.route = auto;
SET max_parallel_workers_per_gather = 0;
\set q 'SELECT customer_id, count(*), sum(amount) FROM public.orders WHERE customer_id <= 3 GROUP BY customer_id'
\set last 'SELECT side, reason FROM quicksilver.last_decision();'
:q ORDER BY 1;
:last

-- EXPLAIN: which side, and (with costs or VERBOSE) why and DuckDB's plan
EXPLAIN (COSTS OFF) :q;
\o /tmp/qs_regress/explain.out
EXPLAIN :q;
\o
\! grep -c -E "Quicksilver: mirror|Quicksilver Reason: [0-9]+ rows from mirrored tables|Mirror Plan:" /tmp/qs_regress/explain.out
\! grep -c -i -E "HASH_GROUP_BY|SCAN|PROJECTION" /tmp/qs_regress/explain.out | sed 's/[1-9][0-9]*/DuckDB operators shown/'

-- EXPLAIN ANALYZE: the statement runs on the mirror, and the node says what came back
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) :q;
:last
\t on
\pset format unaligned
\o /tmp/qs_regress/explain.out
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) :q;
\o
\t off
\pset format aligned
\! grep -c -E '"Quicksilver": "mirror"|"Mirror Rows": 3|"Mirror Time": [0-9.]+|"Actual Total Time"' /tmp/qs_regress/explain.out | sed 's/[1-9][0-9]*/found/'
SELECT pg_read_file('/tmp/qs_regress/explain.out')::jsonb -> 0 -> 'Plan' ->> 'Node Type' AS valid_json_top_node;
-- a statement the heap answers (a lookup): the JSON parses too; the decision is text only there
\pset format unaligned
\t on
\o /tmp/qs_regress/explain_heap.out
EXPLAIN (FORMAT JSON) SELECT * FROM public.orders WHERE id = 4242;
\o
\t off
\pset format aligned
SELECT pg_read_file('/tmp/qs_regress/explain_heap.out')::jsonb -> 0 -> 'Plan' ->> 'Node Type' AS heap_json_top_node;
\o /tmp/qs_regress/explain.out
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, WAL, SETTINGS, FORMAT YAML) :q;
EXPLAIN (ANALYZE, FORMAT XML) :q;
\o
\! grep -c -E 'Mirror Rows: 3|<Mirror-Rows>3</Mirror-Rows>' /tmp/qs_regress/explain.out
-- the same statement, not explained, is still the mirror's
:q ORDER BY 1;
:last

-- a statement that stays on the heap says so, analysed or not
EXPLAIN (COSTS OFF) SELECT * FROM public.orders WHERE id = 4242;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT * FROM public.orders WHERE id = 4242;

-- EXPLAIN EXECUTE, plain and analysed
PREPARE ep (int) AS SELECT customer_id, count(*), sum(amount) FROM public.orders WHERE customer_id <= $1 GROUP BY customer_id ORDER BY customer_id;
EXPLAIN (COSTS OFF) EXECUTE ep(3);
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) EXECUTE ep(3);
EXECUTE ep(3);
DEALLOCATE ep;

-- a result larger than work_mem is streamed, and the node says that too
SET work_mem = '64kB';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT id, amount FROM public.orders WHERE customer_id = 1;
RESET work_mem;

-- in a transaction block: analysed, then a savepoint, then routed again
BEGIN;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) :q;
SAVEPOINT s;
RELEASE s;
:q ORDER BY 1;
:last
COMMIT;
-- the one place where explaining changes a later statement's side: after an
-- EXPLAIN without ANALYZE in a transaction block, pg_duckdb counts an executor
-- that never finished and refuses ("inside functions") until the transaction
-- ends. The statements are answered by the heap; nothing fails.
BEGIN;
EXPLAIN (COSTS OFF) :q;
:q ORDER BY 1;
SELECT side, reason, detail FROM quicksilver.last_decision();
COMMIT;
:q ORDER BY 1;
:last

-- auto_explain: every statement explained after it ran, with ANALYZE and BUFFERS, nested ones too
LOAD 'auto_explain';
SET auto_explain.log_min_duration = 0;
SET auto_explain.log_analyze = on;
SET auto_explain.log_buffers = on;
SET auto_explain.log_verbose = on;
SET auto_explain.log_nested_statements = on;
SET auto_explain.log_format = json;
:q ORDER BY 1;
:last
BEGIN;
:q ORDER BY 1;
SAVEPOINT s;
RELEASE s;
DECLARE c CURSOR FOR SELECT id, amount FROM public.orders WHERE customer_id = 2 ORDER BY id;
FETCH 2 FROM c;
COMMIT;
SET work_mem = '64kB';
SELECT count(*) FROM (SELECT id, amount FROM public.orders WHERE customer_id = 1) s;
RESET work_mem;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) :q;
SET auto_explain.log_format = text;
:q ORDER BY 1;
:last
SET auto_explain.log_min_duration = -1;
