-- ===========================================================================
-- S0 — workload profile for a Quicksilver assessment
--
-- Answers: what fraction of this cluster's read TIME is analytical (columnar
-- wins) vs OLTP-shaped (columnar loses), and how many replicas Quicksilver
-- would consolidate to.
--
-- Run against a replica (or the primary) with pg_stat_statements enabled:
--     psql -d yourdb -f s0_workload_profile.sql
--
-- Reads only pg_stat_statements and pg_stat_activity. Emits NO query text,
-- so the output is safe to share. Requires PG 13+ (total_exec_time) and
-- pg_stat_statements; nothing is written.
--
-- Classification uses EXECUTION SHAPE, not SQL text: blocks touched per call.
-- A call touching thousands of 8 KB blocks is a scan whatever it looks like;
-- one touching a handful is a lookup. That is dialect- and ORM-independent,
-- which regex over query text is not.
-- ===========================================================================

\pset footer off
-- Tunables. NOTE: psql's \set takes the rest of the line, so no trailing
-- comments on these three lines.
--   ANALYTICAL_BLOCKS: blocks/call at or above which a call counts as a scan
--                      (5000 x 8 KB ~ 40 MB)
--   REPLICAS:          your current read-replica count
--   SPEEDUP:           measured S*C = 22.2x in-product x 0.77 concurrency
\set ANALYTICAL_BLOCKS 5000
\set REPLICAS 8
\set SPEEDUP 17.1

\echo ''
\echo '=== 0. preconditions ==================================================='
SELECT
  current_setting('server_version')                                AS version,
  (SELECT count(*) FROM pg_extension WHERE extname='pg_stat_statements') = 1
                                                                   AS pg_stat_statements,
  (SELECT stats_reset::date FROM pg_stat_statements_info)           AS stats_since;

\echo ''
\echo '=== 1. read workload split (the f that matters is BY TIME) ============='
WITH s AS (
  SELECT calls, total_exec_time, rows,
         (shared_blks_hit + shared_blks_read)::numeric / NULLIF(calls,0) AS blocks_per_call
  FROM pg_stat_statements
  WHERE calls > 0
    -- read-only statements only: writes never move to the mirror
    AND query ~* '^[[:space:]]*(SELECT|WITH)\y'
    AND query !~* '\y(INSERT|UPDATE|DELETE|MERGE)\y'
), c AS (
  SELECT CASE WHEN blocks_per_call >= :ANALYTICAL_BLOCKS THEN 'analytical'
              WHEN blocks_per_call >= 100                THEN 'middling'
              ELSE 'oltp' END AS class,
         calls, total_exec_time, blocks_per_call
  FROM s
)
SELECT class,
       count(*)                                              AS statements,
       sum(calls)                                            AS calls,
       round((100.0*sum(calls)/NULLIF(sum(sum(calls)) OVER (),0))::numeric, 1)              AS pct_calls,
       round((sum(total_exec_time)/1000)::numeric, 1)        AS seconds,
       round((100.0*sum(total_exec_time)/NULLIF(sum(sum(total_exec_time)) OVER (),0))::numeric, 1) AS pct_time,
       round(avg(blocks_per_call)::numeric)                           AS avg_blocks_per_call
FROM c GROUP BY class ORDER BY 6 DESC NULLS LAST;

\echo ''
\echo '--- f, and what it implies -------------------------------------------'
WITH s AS (
  SELECT calls, total_exec_time,
         (shared_blks_hit + shared_blks_read)::numeric / NULLIF(calls,0) AS bpc
  FROM pg_stat_statements
  WHERE calls > 0 AND query ~* '^[[:space:]]*(SELECT|WITH)\y'
    AND query !~* '\y(INSERT|UPDATE|DELETE|MERGE)\y'
), agg AS (
  SELECT
    sum(total_exec_time) FILTER (WHERE bpc >= :ANALYTICAL_BLOCKS) AS ana_time,
    sum(total_exec_time)                                          AS all_time,
    sum(calls)           FILTER (WHERE bpc >= :ANALYTICAL_BLOCKS) AS ana_calls,
    sum(calls)                                                    AS all_calls
  FROM s
), f AS (
  SELECT COALESCE(ana_time/NULLIF(all_time,0), 0)   AS f_time,
         COALESCE(ana_calls::numeric/NULLIF(all_calls,0), 0) AS f_calls
  FROM agg
)
SELECT
  round((100*f_time)::numeric, 1)  AS "f_by_time_%",
  round((100*f_calls)::numeric, 1) AS "f_by_calls_%",
  round((100 / (:REPLICAS * (1 - 1/:SPEEDUP::numeric)))::numeric, 1) AS "break_even_f_%",
  :REPLICAS             AS replicas_today,
  GREATEST(2, ceil((f_time * :REPLICAS / :SPEEDUP + (1-f_time) * :REPLICAS)::numeric))::int
                        AS replicas_after_quicksilver,
  CASE WHEN f_time >= (1.0/(:REPLICAS * (1 - 1/:SPEEDUP::numeric)))::numeric
       THEN 'above break-even — consolidation pays'
       ELSE 'below break-even — not worth it at this replica count' END AS verdict
FROM f;

\echo ''
\echo '=== 2. concentration — is read time dominated by a few statements? ====='
\echo '    (high concentration favours Quicksilver: a handful of heavy queries'
\echo '     is exactly what a columnar mirror absorbs)'
WITH s AS (
  SELECT total_exec_time,
         row_number() OVER (ORDER BY total_exec_time DESC) AS rn,
         count(*)    OVER ()                               AS n,
         sum(total_exec_time) OVER ()                      AS tot
  FROM pg_stat_statements
  WHERE calls > 0 AND query ~* '^[[:space:]]*(SELECT|WITH)\y'
)
SELECT bucket,
       round((100.0*sum(total_exec_time)/MAX(tot))::numeric, 1) AS pct_of_read_time
FROM (
  SELECT total_exec_time, tot,
         CASE WHEN rn <= GREATEST(1, (n*0.001)::int) THEN 'top 0.1% of statements'
              WHEN rn <= GREATEST(1, (n*0.01)::int)  THEN 'top 1%'
              WHEN rn <= GREATEST(1, (n*0.10)::int)  THEN 'top 10%'
              ELSE 'the other 90%' END AS bucket
  FROM s
) b GROUP BY bucket ORDER BY 2 DESC;

\echo ''
\echo '=== 3. WHY do you have replicas? (consolidation is worthless if the =='
\echo '       answer is HA, geography, or connection count) =================='
SELECT
  (SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend')
                                                     AS current_connections,
  current_setting('max_connections')::int            AS max_connections,
  round(100.0*(SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend')
        / current_setting('max_connections')::numeric, 1) AS pct_conn_used,
  CASE WHEN (SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend')
            > 0.7*current_setting('max_connections')::int
       THEN 'CONNECTION-BOUND — Quicksilver saves little; see docs/10 section 4a'
       ELSE 'not obviously connection-bound' END      AS caveat;

\echo ''
\echo '=== 4. sensitivity — does the verdict depend on the threshold? ========='
WITH thresholds AS (SELECT unnest(ARRAY[1000,2500,5000,10000,25000]) AS t),
s AS (
  SELECT total_exec_time,
         (shared_blks_hit + shared_blks_read)::numeric / NULLIF(calls,0) AS bpc
  FROM pg_stat_statements
  WHERE calls > 0 AND query ~* '^[[:space:]]*(SELECT|WITH)\y'
    AND query !~* '\y(INSERT|UPDATE|DELETE|MERGE)\y'
)
SELECT t AS blocks_per_call_threshold,
       round((100.0*COALESCE(sum(total_exec_time) FILTER (WHERE bpc >= t),0)
             / NULLIF(sum(total_exec_time),0))::numeric, 1) AS "f_by_time_%"
FROM thresholds, s GROUP BY t ORDER BY t;

\echo ''
\echo 'Send section 1 and 2 output to evaluate fit. No query text is emitted.'
\echo ''
