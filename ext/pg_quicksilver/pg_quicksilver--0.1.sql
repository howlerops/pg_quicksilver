-- pg_quicksilver 0.1
\echo Use "CREATE EXTENSION pg_quicksilver" to load this file. \quit

-- ---------------------------------------------------------------- the mirror role
-- The mirror side runs as this role (docs/40, "Ordinary roles"); the C code
-- switches to it around each call into pg_duckdb (QS_MIRROR_ROLE). It is
-- NOLOGIN, owns the mirror relations (the sidecar makes them so: go/cmd/
-- qs-mirror, quicksilver_mirror), holds no privilege on a heap table, and is
-- the one role an operator makes a member of duckdb.postgres_role:
--
--   GRANT <duckdb.postgres_role> TO quicksilver_mirror_reader;
--
-- No application role may be a member of that role: a member runs DuckDB
-- statements itself, and DuckDB can read every mirrored table's files.
-- Created only if absent, so that another database of the cluster that has
-- the extension does not fail; a role is cluster-wide and is not dropped with
-- the extension.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'quicksilver_mirror_reader') THEN
        CREATE ROLE quicksilver_mirror_reader NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
END
$$;

-- ---------------------------------------------------------------- registry
-- One row per mirrored heap table. The sidecar (or whoever publishes the
-- mirror) owns these rows. applied_lsn here is the durable copy, written on
-- the primary and read when a backend (re)loads the registry;
-- quicksilver.report_applied_lsn() is the live one: shared memory, no WAL,
-- works on a standby. The newer of the two is believed, except that a report
-- of 0/0 means "do not read this table's mirror on this node" and wins. The
-- sidecar reports and leaves the column NULL: every node has its own mirror
-- and the column is the same on all of them.
CREATE TABLE quicksilver.mirrors (
    heap           regclass PRIMARY KEY,
    mirror         regclass NOT NULL,
    applied_lsn    pg_lsn,
    max_lag_bytes  bigint CHECK (max_lag_bytes >= 0),   -- NULL: quicksilver.max_lag
    enabled        boolean NOT NULL DEFAULT true,
    -- Retired. For a type whose allowlist row has the option 'finite', a column
    -- is routable only if it is listed here. No row of the shipped allowlist
    -- has it any more: the mirror keeps +-infinity (docs/40). The sidecar does
    -- not maintain this column.
    finite_columns name[] NOT NULL DEFAULT '{}',
    note           text
);

-- ---------------------------------------------------------------- lakes
-- The DuckLake catalogs the mirror relations read. pg_duckdb's DuckDB lives in
-- the backend, so a lake has to be attached in every session before a mirror
-- relation over it can be planned. The extension does that itself, before it
-- first asks the mirror, and only where the catalog file exists: on a node
-- with no mirror (the primary) nothing is attached and nothing fails. The path
-- is the same on every instance. Written by the sidecar, on the primary.
CREATE TABLE quicksilver.lakes (
    alias    name PRIMARY KEY,
    catalog  text NOT NULL CHECK (catalog LIKE '/%')
);

-- One row per sidecar (pod): the tables it is configured for, and when it last
-- said so (go/cmd/qs-mirror/claims.go). The sidecars write it and the janitor on
-- the primary reads it, both under the registry's advisory lock; a row whose
-- tables nobody claims any more is what lets the janitor remove a registry row.
-- No trigger: a claim changes no routing, so it must not invalidate the registry.
CREATE TABLE quicksilver.sidecar_claims (
    node    text PRIMARY KEY,
    tables  text[] NOT NULL,
    seen    timestamptz NOT NULL
);
REVOKE ALL ON quicksilver.sidecar_claims FROM PUBLIC;


-- ---------------------------------------------------------------- allowlist
-- What the mirror is known to answer exactly as PostgreSQL does. This is data:
-- a statement is routed only if every construct in its typed query tree has a
-- row here. Identity is the catalog object the ident resolves to with only
-- pg_catalog on the path, so it survives dump/restore and cannot be shadowed.
--
--   kind       ident (as regtype / regoperator / regprocedure print it)
--   type       integer
--   operator   =(integer,integer)
--   function   "numeric"(integer)          casts with a function are functions
--   aggregate  sum(integer)
--   window     row_number()
--   cast       integer->oid                binary (RelabelType) and, with option
--                                          'io', I/O conversions
--   node       BoolExpr                    an expression node type
--   feature    order_by                    a statement-level feature
--
-- collation_rule: what the entry requires of the collation its input is compared
-- under. none = the entry does not cover collatable input; any = insensitive;
-- deterministic = equality semantics; c = ordering, and the mirror only knows
-- byte order.
-- volatility: must be stated for anything that is not immutable; an entry that
-- says 'immutable' for a stable function is refused at load.
-- options:
--   typmod    type: its scale lives in the column, not the value. A column must
--             be declared with a precision; a literal must carry a decimal
--             point; CASE/COALESCE must not merge different scales.
--   finite    type: a column is routable only if quicksilver.mirrors lists it
--             in finite_columns. Not used by any shipped row (docs/40).
--   cmp       operator: a comparison. A bare literal or a cmp_only cast is
--             exact as its direct operand.
--   cmp_only  function: exact only as the direct operand of a cmp operator.
--   io        cast: also as an I/O conversion.
--   errors    PostgreSQL can raise an error from it (integer overflow) that the
--             mirror's optimizer may never evaluate. Routed only under
--             quicksilver.tolerate_lost_errors.
CREATE TABLE quicksilver.allowlist (
    kind         text NOT NULL CHECK (kind IN ('type','operator','function','aggregate','window','cast','node','feature','timezone')),
    ident        text NOT NULL,
    collation_rule text NOT NULL DEFAULT 'none' CHECK (collation_rule IN ('none','any','deterministic','c')),
    volatility   text NOT NULL DEFAULT 'immutable' CHECK (volatility IN ('immutable','stable','volatile')),
    options      text[] NOT NULL DEFAULT '{}',
    verified_by  text NOT NULL CHECK (verified_by <> ''),
    note         text,
    PRIMARY KEY (kind, ident)
);

REVOKE ALL ON quicksilver.mirrors, quicksilver.allowlist, quicksilver.lakes FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('quicksilver.lakes', '');
SELECT pg_catalog.pg_extension_config_dump('quicksilver.mirrors', '');
SELECT pg_catalog.pg_extension_config_dump('quicksilver.allowlist', '');

CREATE FUNCTION quicksilver._changed() RETURNS trigger
    AS 'MODULE_PATHNAME', 'qs_changed' LANGUAGE C;
-- A change tells every backend (and every standby) to reread the table and to
-- discard the plans that were decided under it. An UPDATE of applied_lsn alone
-- does not: that column is only where a restarted server starts from, and the
-- sidecar may write it as often as it likes.
CREATE TRIGGER mirrors_changed AFTER INSERT OR DELETE OR TRUNCATE ON quicksilver.mirrors
    FOR EACH STATEMENT EXECUTE FUNCTION quicksilver._changed();
CREATE TRIGGER mirrors_updated AFTER UPDATE OF heap, mirror, max_lag_bytes, enabled, finite_columns ON quicksilver.mirrors
    FOR EACH STATEMENT EXECUTE FUNCTION quicksilver._changed();
CREATE TRIGGER lakes_changed AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON quicksilver.lakes
    FOR EACH STATEMENT EXECUTE FUNCTION quicksilver._changed();
CREATE TRIGGER allowlist_changed AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON quicksilver.allowlist
    FOR EACH STATEMENT EXECUTE FUNCTION quicksilver._changed();

-- ---------------------------------------------------------------- functions
CREATE FUNCTION quicksilver.register_mirror(heap regclass, mirror regclass, max_lag_bytes bigint DEFAULT NULL)
    RETURNS void LANGUAGE sql
    SET search_path = pg_catalog
    AS $$
    INSERT INTO quicksilver.mirrors AS m (heap, mirror, max_lag_bytes) VALUES ($1, $2, $3)
    ON CONFLICT (heap) DO UPDATE SET mirror = EXCLUDED.mirror, max_lag_bytes = EXCLUDED.max_lag_bytes, enabled = true;
$$;

CREATE FUNCTION quicksilver.report_applied_lsn(heap regclass, lsn pg_lsn) RETURNS void
    AS 'MODULE_PATHNAME', 'qs_report_applied_lsn' LANGUAGE C STRICT;

-- The join domain columns this node reads complete (docs/40, "Join keys"): shared memory,
-- like report_applied_lsn, and the same privilege. complete = false is always safe.
CREATE FUNCTION quicksilver.report_join_complete(heap regclass, column_name name, complete boolean) RETURNS void
    AS 'MODULE_PATHNAME', 'qs_report_join_complete' LANGUAGE C STRICT;

-- Attaches the lakes in this session, for somebody who wants to read a mirror
-- relation by name. A routed statement needs no such call. Returns how many
-- are attached, -1 if one could not be (no catalog on this node, for one).
CREATE FUNCTION quicksilver.attach_lakes() RETURNS integer
    AS 'MODULE_PATHNAME', 'qs_attach_lakes_sql' LANGUAGE C;

CREATE FUNCTION quicksilver.mirror_status(
    OUT heap regclass, OUT mirror regclass, OUT usable boolean, OUT reason text,
    OUT unusable_columns text, OUT applied_lsn pg_lsn, OUT current_lsn pg_lsn,
    OUT fresh boolean, OUT stale_because text)
    RETURNS SETOF record AS 'MODULE_PATHNAME', 'qs_mirror_status' LANGUAGE C;

-- The constructs that keep a statement on the heap, one per row; no rows means
-- the allowlist covers it. The statement is analysed, never planned or run.
CREATE FUNCTION quicksilver.unsafe_constructs(statement text, OUT kind text, OUT ident text, OUT detail text)
    RETURNS SETOF record AS 'MODULE_PATHNAME', 'qs_check' LANGUAGE C STRICT;

-- The decision for the last statement of this session that read a mirrored table.
CREATE FUNCTION quicksilver.last_decision(
    OUT side text, OUT reason text, OUT detail text, OUT constructs jsonb,
    OUT queryid bigint, OUT heap_cost float8, OUT mirrored_rows float8)
    RETURNS SETOF record AS 'MODULE_PATHNAME', 'qs_last_decision' LANGUAGE C;

-- So that a caller can tell "stayed on the heap" from "was never considered".
CREATE FUNCTION quicksilver.forget_last() RETURNS void
    AS 'MODULE_PATHNAME', 'qs_forget_last' LANGUAGE C;

CREATE FUNCTION quicksilver.decisions(
    OUT queryid bigint, OUT mirror bigint, OUT heap bigint, OUT fallbacks bigint,
    OUT last_side text, OUT last_reason text, OUT last_detail text)
    RETURNS SETOF record AS 'MODULE_PATHNAME', 'qs_decisions' LANGUAGE C;
CREATE VIEW quicksilver.decision_stats AS SELECT * FROM quicksilver.decisions();

CREATE FUNCTION quicksilver.reasons(OUT side text, OUT reason text, OUT statements bigint)
    RETURNS SETOF record AS 'MODULE_PATHNAME', 'qs_reasons' LANGUAGE C;
CREATE VIEW quicksilver.reason_stats AS SELECT * FROM quicksilver.reasons();

CREATE FUNCTION quicksilver.reset() RETURNS void
    AS 'MODULE_PATHNAME', 'qs_reset' LANGUAGE C;

-- Allowlist rows that were not loaded, and why.
CREATE FUNCTION quicksilver.allowlist_problems() RETURNS SETOF text
    AS 'MODULE_PATHNAME', 'qs_allowlist_problems' LANGUAGE C;

-- Rows whose heap or mirror relation has been dropped are ignored by the
-- extension; this removes them.
CREATE FUNCTION quicksilver.prune() RETURNS bigint LANGUAGE sql
    SET search_path = pg_catalog
    AS $$
    WITH gone AS (
        DELETE FROM quicksilver.mirrors m
        WHERE NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = m.heap)
           OR NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = m.mirror)
        RETURNING 1)
    SELECT count(*) FROM gone;
$$;

GRANT USAGE ON SCHEMA quicksilver TO PUBLIC;
REVOKE ALL ON FUNCTION quicksilver.prune() FROM PUBLIC;
REVOKE ALL ON FUNCTION quicksilver.register_mirror(regclass, regclass, bigint),
    quicksilver.report_applied_lsn(regclass, pg_lsn),
    quicksilver.report_join_complete(regclass, name, boolean), quicksilver.reset(),
    quicksilver.attach_lakes() FROM PUBLIC;

-- The metadata that describes other sessions and the cluster: the mirrors,
-- their names and LSNs, and the counters every session shares. A view runs its
-- function with the view owner's rights, so the views are closed the same way.
-- quicksilver.last_decision() stays public: it reads this session's own last
-- statement and nothing else.
REVOKE ALL ON FUNCTION quicksilver.mirror_status(), quicksilver.unsafe_constructs(text),
    quicksilver.decisions(), quicksilver.reasons(), quicksilver.allowlist_problems() FROM PUBLIC;
REVOKE ALL ON quicksilver.decision_stats, quicksilver.reason_stats FROM PUBLIC;
GRANT EXECUTE ON FUNCTION quicksilver.mirror_status(), quicksilver.unsafe_constructs(text),
    quicksilver.decisions(), quicksilver.reasons(), quicksilver.allowlist_problems() TO pg_monitor;
GRANT SELECT ON quicksilver.decision_stats, quicksilver.reason_stats TO pg_monitor;

-- ---------------------------------------------------------------- seed
-- Small on purpose. Every row was verified by ext/pg_quicksilver/validate/
-- equality.py: statements built from these constructs returned byte-identical
-- rows and an identical RowDescription from the heap and from the mirror
-- (docs/40). A larger list is to be generated by running bench/exactness
-- through quicksilver.unsafe_constructs() and quicksilver.route = mirror.
INSERT INTO quicksilver.allowlist (kind, ident, collation_rule, options, verified_by)
SELECT k, i, c, o, 'docs/40 equality harness, pg_duckdb 1.2.0'
FROM (VALUES
  -- types
  ('type', 'smallint', 'none', '{}'::text[]),
  ('type', 'integer', 'none', '{}'),
  ('type', 'bigint', 'none', '{}'),
  ('type', 'numeric', 'none', '{typmod}'),
  ('type', 'boolean', 'none', '{}'),
  ('type', 'text', 'none', '{}'),
  ('type', 'uuid', 'none', '{}'),
  -- no 'finite': the mirror keeps +-infinity as DuckDB's own infinity and it
  -- reads back identical through DuckLake and pg_duckdb (docs/40)
  ('type', 'date', 'none', '{}'),
  ('type', 'timestamp without time zone', 'none', '{}'),
  ('type', 'timestamp with time zone', 'none', '{}'),
  -- expression nodes
  ('node', 'BoolExpr', 'none', '{}'),
  ('node', 'NullTest', 'none', '{}'),
  ('node', 'BooleanTest', 'none', '{}'),
  ('node', 'CaseExpr', 'none', '{}'),
  ('node', 'CoalesceExpr', 'none', '{}'),
  -- statement features
  ('feature', 'join_inner', 'none', '{}'),
  ('feature', 'join_left', 'none', '{}'),
  ('feature', 'group_by', 'none', '{}'),
  ('feature', 'having', 'none', '{}'),
  ('feature', 'order_by', 'none', '{}'),
  ('feature', 'limit', 'none', '{}'),
  ('feature', 'subquery_in_from', 'none', '{}'),
  ('feature', 'in_list', 'none', '{}'),
  -- implicit integer widening; integer -> numeric only under a comparison (as an
  -- operand of + - * the mirror gives the integer three decimals: n * 2 came back
  -- as -9841.62000 where PostgreSQL says -9841.62)
  ('function', 'int4(smallint)', 'none', '{}'),
  ('function', 'int8(smallint)', 'none', '{}'),
  ('function', 'int8(integer)', 'none', '{}'),
  ('function', '"numeric"(smallint)', 'none', '{cmp_only}'),
  ('function', '"numeric"(integer)', 'none', '{cmp_only}'),
  -- aggregates
  ('aggregate', 'count()', 'none', '{}'),
  ('aggregate', 'count("any")', 'any', '{}'),
  ('aggregate', 'sum(integer)', 'none', '{}'),
  ('aggregate', 'sum(bigint)', 'none', '{}'),
  ('aggregate', 'sum(numeric)', 'none', '{}'),
  ('aggregate', 'min(text)', 'c', '{}'),
  ('aggregate', 'max(text)', 'c', '{}')
) v(k, i, c, o);

-- comparison within and across the integer widths
INSERT INTO quicksilver.allowlist (kind, ident, options, verified_by)
SELECT 'operator', format('%s(%s,%s)', op, a, b), '{cmp}', 'docs/40 equality harness, pg_duckdb 1.2.0'
FROM unnest(ARRAY['=','<>','<','<=','>','>=']) op,
     unnest(ARRAY['smallint','integer','bigint']) a,
     unnest(ARRAY['smallint','integer','bigint']) b;

-- + - * on integer and bigint (smallint arithmetic is not verified). The values
-- are exact; the overflow error is not always raised (docs/40), hence 'errors'.
INSERT INTO quicksilver.allowlist (kind, ident, options, verified_by)
SELECT 'operator', format('%s(%s,%s)', op, a, b), '{errors}', 'docs/40 equality harness, pg_duckdb 1.2.0'
FROM unnest(ARRAY['+','-','*']) op,
     unnest(ARRAY['integer','bigint']) a,
     unnest(ARRAY['integer','bigint']) b;

-- numeric, date, timestamp, timestamptz: comparison; numeric: + - *
INSERT INTO quicksilver.allowlist (kind, ident, options, verified_by)
SELECT 'operator', format('%s(%s,%s)', op, t, t), '{cmp}'::text[], 'docs/40 equality harness, pg_duckdb 1.2.0'
FROM unnest(ARRAY['=','<>','<','<=','>','>=']) op,
     unnest(ARRAY['numeric','date','timestamp without time zone','timestamp with time zone']) t
UNION ALL
SELECT 'operator', format('%s(numeric,numeric)', op), '{}', 'docs/40 equality harness, pg_duckdb 1.2.0'
FROM unnest(ARRAY['+','-','*']) op;

-- equality only, and only under a deterministic collation, on text; uuid; boolean
INSERT INTO quicksilver.allowlist (kind, ident, collation_rule, options, verified_by)
SELECT 'operator', format('%s(%s,%s)', op, t, t), CASE WHEN t = 'text' THEN 'deterministic' ELSE 'none' END,
       '{cmp}', 'docs/40 equality harness, pg_duckdb 1.2.0'
FROM unnest(ARRAY['=','<>']) op, unnest(ARRAY['text','uuid','boolean']) t;

-- ordering of text: byte order, so only under the C collation
INSERT INTO quicksilver.allowlist (kind, ident, collation_rule, options, verified_by)
SELECT 'operator', format('%s(text,text)', op), 'c', '{cmp}', 'docs/40 equality harness, pg_duckdb 1.2.0'
FROM unnest(ARRAY['<','<=','>','>=']) op;

-- min/max on the ordered types
INSERT INTO quicksilver.allowlist (kind, ident, verified_by)
SELECT 'aggregate', format('%s(%s)', f, t), 'docs/40 equality harness, pg_duckdb 1.2.0'
FROM unnest(ARRAY['min','max']) f,
     unnest(ARRAY['smallint','integer','bigint','numeric','date','timestamp without time zone','timestamp with time zone']) t;

-- BEGIN widened by validate/widen.py
-- Generated: do not edit by hand. Each row was proposed from quicksilver.unsafe_constructs(),
-- routed through the DuckLake mirror on a hot standby, and kept only because every routed
-- statement that exercises it returned the heap's bytes and RowDescription, in the ICU en-US
-- and in the C locale database. Which statements: validate/results/widen-*.json.
INSERT INTO quicksilver.allowlist (kind, ident, collation_rule, options, verified_by) VALUES
  ('aggregate', 'array_agg(anynonarray)', 'none', '{}', 'validate/widen.py: 3 statements routed and identical, both locales'),
  ('feature', 'agg_filter', 'none', '{}', 'validate/widen.py: 3 statements routed and identical, both locales'),
  ('feature', 'agg_order_by', 'none', '{}', 'validate/widen.py: 3 statements routed and identical, both locales'),
  ('feature', 'offset', 'none', '{}', 'validate/widen.py: 7 statements routed and identical, both locales'),
  ('feature', 'sublink_any', 'none', '{}', 'validate/widen.py: 7 statements routed and identical, both locales'),
  ('feature', 'sublink_exists', 'none', '{}', 'validate/widen.py: 3 statements routed and identical, both locales'),
  ('feature', 'sublink_expr', 'none', '{}', 'validate/widen.py: 3 statements routed and identical, both locales'),
  ('feature', 'union', 'none', '{}', 'validate/widen.py: 4 statements routed and identical, both locales'),
  ('feature', 'union_all', 'none', '{}', 'validate/widen.py: 6 statements routed and identical, both locales'),
  ('feature', 'window', 'none', '{}', 'validate/widen.py: 9 statements routed and identical, both locales'),
  ('operator', '<(boolean,boolean)', 'none', '{cmp}', 'validate/widen.py: 3 statements routed and identical, both locales'),
  ('operator', '~~(text,text)', 'deterministic', '{}', 'validate/widen.py: 11 statements routed and identical, both locales'),
  ('type', 'double precision', 'none', '{}', 'validate/widen.py: 7 statements routed and identical, both locales'),
  ('type', 'integer[]', 'none', '{}', 'validate/widen.py: 3 statements routed and identical, both locales'),
  ('type', 'real', 'none', '{}', 'validate/widen.py: 6 statements routed and identical, both locales'),
  ('window', 'rank()', 'none', '{}', 'validate/widen.py: 5 statements routed and identical, both locales'),
  ('window', 'row_number()', 'none', '{}', 'validate/widen.py: 4 statements routed and identical, both locales');
-- END widened by validate/widen.py

-- date_trunc(text, timestamptz) reads the session TimeZone (STABLE). Admitted
-- only with a unit proven in the function walk (qs_unit_proven), and only in a
-- zone the 'timezone' rows below admit (qs_zone_ok). validate/tz_check.py.
INSERT INTO quicksilver.allowlist (kind, ident, collation_rule, volatility, options, verified_by) VALUES
  ('function', 'date_trunc(text,timestamp with time zone)', 'any', 'stable', '{zone}',
   'validate/tz_check.py over every pg_timezone_names zone; validate/equality.py');

-- BEGIN time zones (validate/tz_check.py): generated; do not edit by hand. Each zone was compared with
-- date_trunc in PostgreSQL and DuckDB over the instants described there; see
-- validate/results/tz_check.tsv for the zones denied, and why.
INSERT INTO quicksilver.allowlist (kind, ident, collation_rule, options, verified_by) VALUES
  ('timezone', 'Africa/Maputo', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Africa/Monrovia', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Africa/Ndjamena', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Africa/Sao_Tome', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Adak', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Anchorage', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Atka', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Bahia_Banderas', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Barbados', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Belize', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Bogota', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Boise', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Cambridge_Bay', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Cancun', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Cayenne', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Chicago', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Chihuahua', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Ciudad_Juarez', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Costa_Rica', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Dawson', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Dawson_Creek', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Denver', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Detroit', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Fort_Nelson', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Glace_Bay', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Goose_Bay', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Guatemala', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Hermosillo', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Indiana/Indianapolis', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Indiana/Knox', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Indiana/Marengo', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Indiana/Vevay', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Indiana/Winamac', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Inuvik', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Iqaluit', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Juneau', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Kentucky/Louisville', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Kentucky/Monticello', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/La_Paz', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Los_Angeles', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Matamoros', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Mazatlan', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Menominee', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Merida', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Metlakatla', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Mexico_City', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Moncton', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Monterrey', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Montreal', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/New_York', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Nipigon', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Nome', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/North_Dakota/Beulah', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/North_Dakota/Center', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/North_Dakota/New_Salem', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Nuuk', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Ojinaga', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Panama', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Pangnirtung', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Phoenix', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Port-au-Prince', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Puerto_Rico', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Rainy_River', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Rankin_Inlet', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Resolute', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Santo_Domingo', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Scoresbysund', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Shiprock', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Sitka', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/St_Johns', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Thule', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Thunder_Bay', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Toronto', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Vancouver', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Whitehorse', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Winnipeg', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'America/Yakutat', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Antarctica/Macquarie', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Antarctica/Mawson', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Antarctica/Palmer', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Antarctica/Rothera', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Antarctica/Troll', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Almaty', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Anadyr', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Aqtau', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Aqtobe', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Ashgabat', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Atyrau', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Bangkok', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Barnaul', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Bishkek', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Chita', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Damascus', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Dili', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Dubai', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Dushanbe', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Famagusta', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Ho_Chi_Minh', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Hong_Kong', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Irkutsk', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Jerusalem', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Kamchatka', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Krasnoyarsk', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Macau', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Magadan', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Nicosia', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Novosibirsk', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Omsk', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Oral', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Qatar', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Qostanay', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Qyzylorda', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Riyadh', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Sakhalin', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Samarkand', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Srednekolymsk', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Tashkent', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Tehran', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Tel_Aviv', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Thimphu', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Tokyo', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Tomsk', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Ust-Nera', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Vladivostok', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Yakutsk', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Asia/Yekaterinburg', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Atlantic/Bermuda', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Atlantic/Faroe', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Atlantic/South_Georgia', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Australia/Adelaide', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Australia/Brisbane', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Australia/Broken_Hill', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Australia/Canberra', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Australia/Currie', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Australia/Darwin', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Australia/Eucla', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Australia/Hobart', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Australia/Lindeman', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Australia/Melbourne', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Australia/Perth', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Australia/Sydney', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Australia/Yancowinna', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'CST6CDT', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'EST5EDT', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT-0', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT-1', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT-10', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT-11', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT-12', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT-13', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT-14', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT-2', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT-3', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT-4', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT-5', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT-6', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT-7', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT-8', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT-9', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT+0', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT+1', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT+10', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT+11', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT+12', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT+2', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT+3', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT+4', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT+5', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT+6', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT+7', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT+8', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT+9', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/GMT0', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/Greenwich', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/UCT', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/Universal', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/UTC', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Etc/Zulu', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Andorra', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Astrakhan', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Athens', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Belfast', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Belgrade', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Berlin', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Bratislava', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Bucharest', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Budapest', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Busingen', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Chisinau', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Dublin', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Gibraltar', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Isle_of_Man', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Kaliningrad', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Kirov', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Kyiv', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/London', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Madrid', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Minsk', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Moscow', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Nicosia', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Paris', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Podgorica', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Prague', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Riga', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Samara', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Saratov', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Sofia', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Tiraspol', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Ulyanovsk', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Vaduz', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Vienna', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Volgograd', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Warsaw', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Europe/Zurich', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Factory', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'GMT', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Indian/Maldives', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'MST7MDT', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Apia', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Auckland', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Bougainville', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Efate', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Fiji', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Gambier', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Guadalcanal', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Honolulu', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Johnston', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Kanton', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Kiritimati', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Kwajalein', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Marquesas', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Nauru', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Niue', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Noumea', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Port_Moresby', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Rarotonga', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'Pacific/Tahiti', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'PST8PDT', 'none', '{}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)'),
  ('timezone', 'UTC', 'none', '{hour}', 'validate/tz_check.py: the proven units agree in this zone (results/tz_check.tsv)');
-- END time zones

-- The join domain objects are last in this script on purpose: CREATE EXTENSION
-- assigns object identifiers in order, so an object created after a new one gets
-- a different identifier, and a query identifier (which hashes some of them)
-- would change for installed statements that name one. Nothing above uses them.
-- ---------------------------------------------------------------- join domains
-- The uuid columns that are compared for equality across tables, and the
-- companion column the sidecar keeps for each (docs/16, "Join domains"). Written
-- by the sidecar on the primary, in step with the mirror relations: a row exists
-- for a column only while every file of its table carries the companion and the
-- mirror relation exposes it. A join is rewritten only where both sides are in
-- the same domain (docs/40, "Join keys"). Optional: without this table there are
-- no rewrites.
CREATE TABLE quicksilver.join_domains (
    heap         regclass NOT NULL,
    column_name  name NOT NULL,
    domain       name NOT NULL,
    companion    name NOT NULL,
    PRIMARY KEY (heap, column_name)
);
REVOKE ALL ON quicksilver.join_domains FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('quicksilver.join_domains', '');
CREATE TRIGGER join_domains_changed AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON quicksilver.join_domains
    FOR EACH STATEMENT EXECUTE FUNCTION quicksilver._changed();
