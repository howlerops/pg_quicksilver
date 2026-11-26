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

-- inside one: the heap, by default. pg_duckdb aborts a transaction that takes
-- a SAVEPOINT after it has run a query, and every driver's nested transaction
-- is a SAVEPOINT.
BEGIN;
SELECT b, count(*) FROM qt.h GROUP BY b;
:last
SAVEPOINT s;
SELECT b, count(*) FROM qt.h GROUP BY b;
RELEASE s;
COMMIT;

-- a multi-statement Query message is an implicit transaction block
SELECT b, count(*) FROM qt.h GROUP BY b \; SELECT side, reason FROM quicksilver.last_decision();

-- opted in: routed, in a READ COMMITTED block with no savepoint open
SET quicksilver.route_in_transaction = on;
BEGIN;
SELECT b, count(*) FROM qt.h GROUP BY b;
:last
INSERT INTO qt.h VALUES (6, 60, 'heap');
-- the mirror cannot see this transaction's own write: the price of opting in
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
-- and this is the hazard the default avoids: a SAVEPOINT after a routed statement
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
-- a scrollable or holdable cursor needs to go backwards or outlive the transaction: heap
DECLARE s SCROLL CURSOR FOR SELECT id, b FROM qt.h ORDER BY id;
FETCH 2 FROM s;
FETCH BACKWARD 1 FROM s;
:last
DECLARE h CURSOR WITH HOLD FOR SELECT id, b FROM qt.h ORDER BY id;
FETCH 1 FROM h;
COMMIT;
FETCH 1 FROM h;
CLOSE h;
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

DROP SCHEMA qt, qtm CASCADE;
SELECT quicksilver.prune();
