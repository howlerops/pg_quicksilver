-- Analytical queries: scan-and-aggregate shapes. The workload that makes
-- teams add read replicas. Identical SQL runs on both engines.
-- Data spans 2025-09-14 .. 2026-08-27; windows are literal so both engines
-- see exactly the same predicate.

-- name: A1_daily_revenue_30d
-- Classic dashboard query: daily revenue over the trailing 30 days.
SELECT date_trunc('day', ts) AS d, count(*) AS n, sum(amount) AS revenue
FROM events
WHERE ts >= TIMESTAMP '2026-07-28' AND ts < TIMESTAMP '2026-08-27'
GROUP BY 1 ORDER BY 1;

-- name: A2_top_campaigns
-- Full-table group-by on a medium-cardinality dimension.
SELECT campaign, count(*) AS n, sum(amount) AS revenue, avg(price) AS avg_price
FROM events
GROUP BY campaign ORDER BY revenue DESC LIMIT 20;

-- name: A3_country_device_matrix
-- Multi-dimension breakdown with a filter: 3 grouping cols, 3 measures.
SELECT country, device, channel,
       count(*) AS n, sum(amount) AS revenue, avg(quantity) AS avg_qty
FROM events
WHERE ts >= TIMESTAMP '2026-05-01' AND status = 'ok' AND NOT is_test
GROUP BY 1,2,3 ORDER BY revenue DESC LIMIT 50;

-- name: A4_full_table_aggregate
-- Whole-table scan, 4 columns of 30 projected.
SELECT count(*) AS n, sum(amount) AS revenue, avg(price) AS avg_price, max(quantity) AS max_qty
FROM events;

-- name: A5_distinct_users_by_type
-- Distinct-count aggregation, the expensive shape.
SELECT event_type, count(DISTINCT user_id) AS users, count(*) AS events
FROM events
WHERE ts >= TIMESTAMP '2026-02-01'
GROUP BY 1 ORDER BY users DESC;

-- name: A6_cohort_funnel
-- Heavier: filter + group + HAVING over a wide window.
SELECT category, subcategory, count(*) AS n, sum(amount - discount + tax) AS net
FROM events
WHERE ts >= TIMESTAMP '2025-12-01'
  AND event_type IN ('purchase','add_to_cart')
  AND currency IN ('USD','EUR')
GROUP BY 1,2 HAVING count(*) > 1000
ORDER BY net DESC LIMIT 40;
