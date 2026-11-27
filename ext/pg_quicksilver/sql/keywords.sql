-- Column and table names that DuckDB reads as keywords (docs/40, "Keyword names").
-- pg_duckdb writes a mirror subquery's names as PostgreSQL's quote_identifier
-- leaves them, and DuckDB fails on, or misreads, AT, PIVOT, QUALIFY, ASOF, ...
-- A statement that reads such a name is answered by the heap, with the name in
-- its reason; a statement that reads only other names is routed. Every answer
-- is the heap's.
\set last 'SELECT side, reason, left(detail, 150) FROM quicksilver.last_decision();'
SET search_path = public;
SET client_min_messages = warning;
DROP SCHEMA IF EXISTS kw, kwm CASCADE;
CREATE SCHEMA kw;
CREATE SCHEMA kwm;
CREATE TABLE kw.h (id int PRIMARY KEY, "at" int, "pivot" int, "qualify" text, asof int, anti int,
  semi int, positional int, lambda int, describe int, summarize int, show int, columns int,
  exclude int, replace int, "Pivot" int, "MixedCase" int, "we""ird" text, "a""b at" int);
-- 19 values: id, at, pivot, qualify | asof .. replace (11) | Pivot, MixedCase | we"ird, a"b at
INSERT INTO kw.h SELECT g, g, g * 2, 'q' || g,
  g, g, g, g, g, g, g, g, g, g, g,
  g, g, 'w' || g, g
  FROM generate_series(1, 4) g;
-- a table with no keyword name, whose names are mixed case and carry a quote: routed
CREATE TABLE kw.m (id int PRIMARY KEY, "MixedCase" int, "we""ird" text, "Asof2" int);
INSERT INTO kw.m SELECT g, g * 10, 'w' || g, g FROM generate_series(1, 4) g;
CREATE VIEW kwm.m AS
  SELECT r['id']::int AS id, r['MixedCase']::int AS "MixedCase", r['we"ird']::text AS "we""ird",
         r['Asof2']::int AS "Asof2"
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 100, 'm1', 1), (2, 200, 'm2', 2)) t(id, "MixedCase", "we""ird", "Asof2") $$) r;
SELECT quicksilver.register_mirror('kw.m', 'kwm.m', 1000000000000);
SELECT quicksilver.report_applied_lsn('kw.m', pg_current_wal_insert_lsn() + 1000000000);
-- the mirror: two rows the heap does not have, so every routed answer says which side it came from
CREATE VIEW kwm.h AS
  SELECT r['id']::int AS id, r['at']::int AS "at", r['pivot']::int AS "pivot", r['qualify']::text AS "qualify",
         r['asof']::int AS asof, r['anti']::int AS anti, r['semi']::int AS semi,
         r['positional']::int AS positional, r['lambda']::int AS lambda, r['describe']::int AS describe,
         r['summarize']::int AS summarize, r['show']::int AS show, r['columns']::int AS columns,
         r['exclude']::int AS exclude, r['replace']::int AS replace, r['Pivot']::int AS "Pivot",
         r['MixedCase']::int AS "MixedCase", r['we"ird']::text AS "we""ird", r['a"b at']::int AS "a""b at"
  FROM duckdb.query($$ SELECT * FROM (VALUES
      (1, 10, 20, 'm1', 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 100, 'm', 10),
      (2, 20, 40, 'm2', 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 200, 'm', 20))
    t(id, "at", "pivot", "qualify", asof, anti, semi, positional, lambda, describe, summarize, show, columns,
      exclude, replace, "Pivot", "MixedCase", "we""ird", "a""b at") $$) r;
SELECT quicksilver.register_mirror('kw.h', 'kwm.h', 1000000000000);
SELECT quicksilver.report_applied_lsn('kw.h', pg_current_wal_insert_lsn() + 1000000000);
SET quicksilver.route = mirror;

-- names DuckDB takes as they are: routed, and the mirror answers
SELECT "MixedCase", "we""ird" FROM kw.m ORDER BY id;
:last
SELECT count(*) FROM kw.m;
:last
-- one keyword column in the table: the whole table is the heap's
SELECT count(*) FROM kw.h;
:last

-- each keyword name on its own: the heap answers
SELECT "at" FROM kw.h ORDER BY id;
:last
SELECT "pivot", "qualify" FROM kw.h ORDER BY id;
:last
SELECT asof, anti, semi FROM kw.h ORDER BY id;
:last
SELECT positional, lambda, describe, summarize, show FROM kw.h ORDER BY id;
:last
SELECT columns, exclude, replace FROM kw.h ORDER BY id;
:last
-- case does not make a keyword safe: DuckDB matches its keywords without regard to case
SELECT "Pivot" FROM kw.h ORDER BY id;
:last
-- an embedded quote and a space do not matter; the keyword inside the name does not either
SELECT "a""b at" FROM kw.h ORDER BY id;
:last
-- SELECT * and whole-row references read every name
SELECT * FROM kw.h ORDER BY id;
:last
SELECT h FROM kw.h h ORDER BY id;
:last

-- a table named like a keyword
CREATE TABLE kw."at" (id int PRIMARY KEY, v int);
INSERT INTO kw."at" SELECT g, g * 3 FROM generate_series(1, 3) g;
CREATE VIEW kwm."at" AS SELECT r['id']::int AS id, r['v']::int AS v
  FROM duckdb.query($$ SELECT * FROM (VALUES (1, 30), (2, 60)) t(id, v) $$) r;
SELECT quicksilver.register_mirror('kw.at', 'kwm.at', 1000000000000);
SELECT quicksilver.report_applied_lsn('kw.at', pg_current_wal_insert_lsn() + 1000000000);
SELECT v FROM kw."at" ORDER BY id;
:last

-- the heap of the same statements, for comparison
SET quicksilver.route = heap;
SELECT "MixedCase", "we""ird" FROM kw.m ORDER BY id;
SELECT "at" FROM kw.h ORDER BY id;
SELECT v FROM kw."at" ORDER BY id;
SET quicksilver.route = off;

DELETE FROM quicksilver.mirrors WHERE heap IN ('kw.h'::regclass, 'kw.m'::regclass, 'kw.at'::regclass);
DROP SCHEMA kw, kwm CASCADE;
SELECT quicksilver.prune();
