-- Transactions, savepoints, cursors. The mirror's rows say 'mirror'.
\set last 'SELECT side, reason, detail FROM quicksilver.last_decision();'
SET client_min_messages = warning;
CREATE SCHEMA qt;
CREATE SCHEMA qtm;
CREATE TABLE qt.h (id int PRIMARY KEY, a int, b text);
INSERT INTO qt.h SELECT g, g * 10, 'heap' FROM generate_series(1, 5) g;
CREATE VIEW qtm.h AS SELECT r['id']::int AS id, r['a']::int AS a, r['b']::text AS b
  FROM duckdb.query($$ SELECT i AS id, i * 10 AS a, 'mirror' AS b FROM range(1, 6) t(i) $$) r;
SELECT quicksilver.register_mirror('qt.h', 'qtm.h', 1000000000000);
SELECT quicksilver.report_applied_lsn('qt.h', pg_current_wal_lsn());
SET quicksilver.route = mirror;

-- outside a transaction block: routed
SELECT b, count(*) FROM qt.h GROUP BY b;

-- inside one: routed too, where it is safe (READ COMMITTED, no savepoint open,
-- nothing written). pg_duckdb aborts a transaction that takes a SAVEPOINT while
-- its DuckDB transaction is open, and every driver's nested transaction is a
-- SAVEPOINT: so the mirror's answer is taken whole when the statement starts
-- and DuckDB's transaction is ended with it. The SAVEPOINT below works; inside
-- it the heap answers, because pg_duckdb refuses to start there.
BEGIN;
SELECT b, count(*) FROM qt.h GROUP BY b;
:last
SAVEPOINT s;
SELECT b, count(*) FROM qt.h GROUP BY b;
RELEASE s;
COMMIT;

-- a multi-statement Query message is an implicit transaction block
SELECT b, count(*) FROM qt.h GROUP BY b \; SELECT side, reason FROM quicksilver.last_decision();

-- opted out: the heap, in any block
SET quicksilver.route_in_transaction = off;
BEGIN;
SELECT b, count(*) FROM qt.h GROUP BY b;
:last
COMMIT;
SELECT b, count(*) FROM qt.h GROUP BY b \; SELECT side, reason FROM quicksilver.last_decision();
-- the default again: routed, in a READ COMMITTED block with no savepoint open
SET quicksilver.route_in_transaction = on;
BEGIN;
SELECT b, count(*) FROM qt.h GROUP BY b;
:last
INSERT INTO qt.h VALUES (6, 60, 'heap');
-- a transaction that has written is the heap's from then on: the mirror cannot see its own write
SELECT b, count(*) FROM qt.h GROUP BY b;
SELECT id, b FROM qt.h WHERE id = 6 FOR UPDATE;
ROLLBACK;
-- inside a savepoint pg_duckdb refuses, so the statement is not offered to it
BEGIN;
SAVEPOINT s;
SELECT b, count(*) FROM qt.h GROUP BY b;
:last
ROLLBACK TO s;
SELECT b, count(*) FROM qt.h GROUP BY b;
:last
COMMIT;
-- a SAVEPOINT after a routed statement: used to abort the transaction
BEGIN;
SELECT b, count(*) FROM qt.h GROUP BY b;
SAVEPOINT s;
ROLLBACK;
-- a snapshot isolation level would be reading two snapshots: heap
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT b, count(*) FROM qt.h GROUP BY b;
:last
COMMIT;
BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT b, count(*) FROM qt.h GROUP BY b;
COMMIT;
BEGIN READ ONLY;
SELECT b, count(*) FROM qt.h GROUP BY b;
COMMIT;

-- after an EXPLAIN in the same transaction pg_duckdb refuses to execute
-- ("not supported inside functions"). The statement does not fail: the mirror
-- is asked first, says no before any row, and the heap answers.
BEGIN;
EXPLAIN (COSTS OFF) SELECT 1;
SELECT b, count(*) FROM qt.h GROUP BY b;
SELECT side, reason FROM quicksilver.last_decision();
COMMIT;

-- cursors
BEGIN;
DECLARE c CURSOR FOR SELECT id, b FROM qt.h ORDER BY id;
FETCH 2 FROM c;
FETCH 2 FROM c;
:last
FETCH ALL FROM c;
FETCH 1 FROM c;
CLOSE c;
-- a scrollable cursor needs to go backwards: heap. So does one declared with neither
-- option whose heap plan can scan backward (the sort above): PostgreSQL makes it
-- scrollable, and a routed plan cannot go backward, so the heap keeps it
DECLARE s SCROLL CURSOR FOR SELECT id, b FROM qt.h ORDER BY id;
FETCH 2 FROM s;
FETCH BACKWARD 1 FROM s;
:last
DECLARE h CURSOR WITH HOLD FOR SELECT id, b FROM qt.h ORDER BY id;
FETCH 1 FROM h;
COMMIT;
FETCH 1 FROM h;
CLOSE h;
-- a holdable cursor whose plan cannot scan backward is routed, and its rows outlive
-- the transaction like any other holdable cursor's
BEGIN;
DECLARE g CURSOR WITH HOLD FOR SELECT count(*) FROM qt.h;
FETCH 1 FROM g;
:last
COMMIT;
FETCH 1 FROM g;
CLOSE g;
-- a cursor that is closed half-read, and one whose transaction aborts
BEGIN;
DECLARE c CURSOR FOR SELECT id, b FROM qt.h ORDER BY id;
FETCH 1 FROM c;
CLOSE c;
DECLARE c CURSOR FOR SELECT id, b FROM qt.h ORDER BY id;
FETCH 1 FROM c;
SELECT 1 / 0;
ROLLBACK;
SELECT b, count(*) FROM qt.h GROUP BY b;

-- PL/pgSQL around it, an exception block (a subtransaction) included
RESET quicksilver.route_in_transaction;
SET client_min_messages = notice;
DO $$
DECLARE r text;
BEGIN
  BEGIN
    SELECT b INTO r FROM qt.h WHERE id = 1;
    RAISE EXCEPTION 'read % then failed', r;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'caught: %', SQLERRM;
  END;
  FOR r IN SELECT b FROM qt.h WHERE id <= 2 LOOP
    RAISE NOTICE 'loop read: %', r;
  END LOOP;
END $$;
SET client_min_messages = warning;
SELECT b, count(*) FROM qt.h GROUP BY b;


-- ---------------------------------------------------------------- what may follow a routed read in the same block
-- the default, from here on (the RESET above): safe, so every block below that has
-- not opted out is routed where its state allows it
SHOW quicksilver.route_in_transaction;
\set VERBOSITY terse
CREATE TABLE qt.w (i int);
\set r 'SELECT b, count(*) FROM qt.h GROUP BY b;'
\set side 'SELECT side, reason, detail FROM quicksilver.last_decision();'
-- a savepoint; a routed read after it is released; a write in a savepoint that is rolled
-- back (the transaction has an xid from then on: the heap); a write; a read after the write
BEGIN;
:r
:side
SAVEPOINT b;
RELEASE b;
:r
:side
SAVEPOINT a;
INSERT INTO qt.w VALUES (1);
ROLLBACK TO a;
RELEASE a;
:r
:side
INSERT INTO qt.w VALUES (2);
:r
:side
COMMIT;
SELECT * FROM qt.w;
-- a PL/pgSQL exception block (an internal subtransaction): this used to end the session
BEGIN;
:r
DO $$ BEGIN BEGIN PERFORM 1/0; EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'caught'; END; END $$;
:r
:side
COMMIT;
-- EXPLAIN and EXPLAIN ANALYZE after it, and a routed read after EXPLAIN ANALYZE
BEGIN;
:r
EXPLAIN (COSTS OFF) SELECT b, count(*) FROM qt.h GROUP BY b;
ROLLBACK;
BEGIN;
:r
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT b, count(*) FROM qt.h GROUP BY b;
SAVEPOINT a;
RELEASE a;
:r
:side
COMMIT;
-- a cursor: its rows are the mirror's as of DECLARE, and anything may come between the fetches
BEGIN;
DECLARE c CURSOR FOR SELECT id, b FROM qt.h ORDER BY id;
:side
SAVEPOINT a;
FETCH 2 FROM c;
ROLLBACK TO a;
INSERT INTO qt.w VALUES (3);
FETCH 2 FROM c;
COMMIT;
BEGIN;
DECLARE hc CURSOR WITH HOLD FOR SELECT id, b FROM qt.h ORDER BY id;
:side
SAVEPOINT a;
RELEASE a;
COMMIT;
FETCH 2 FROM hc;
CLOSE hc;
DECLARE hc CURSOR WITH HOLD FOR SELECT id, b FROM qt.h ORDER BY id;
FETCH 2 FROM hc;
CLOSE hc;
-- an error after a routed read, then the same transaction shape again
BEGIN;
:r
SELECT 1/0;
ROLLBACK;
BEGIN;
:r
:side
COMMIT;
-- READ ONLY: routed; REPEATABLE READ and SERIALIZABLE, read only or not: the heap
BEGIN READ ONLY;
:r
:side
SAVEPOINT a;
ROLLBACK;
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
:r
:side
COMMIT;
BEGIN ISOLATION LEVEL SERIALIZABLE;
:r
:side
COMMIT;
-- DuckDB used directly in the block (a superuser can): its DuckDB transaction is
-- not the router's to end, so nothing is routed after it; and the error the
-- SAVEPOINT then gets is pg_duckdb's own, with the router or without
BEGIN;
SELECT * FROM duckdb.query($$ SELECT 42 AS x $$);
:r
:side
SAVEPOINT a;
ROLLBACK;
-- the next transaction is routed again
BEGIN;
:r
:side
COMMIT;
\set VERBOSITY default

DROP SCHEMA qt, qtm CASCADE;
SELECT quicksilver.prune();
