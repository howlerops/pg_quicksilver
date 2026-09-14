-- OLTP-shaped queries: the traffic that ALSO lands on -ro today and that
-- docs/06 predicts a columnar mirror will regress. Reported honestly
-- alongside the analytical wins — goal G3 is a hard constraint.
-- Literals are fixed so both engines do identical work.

-- name: O1_point_lookup_pk
-- Single-row fetch by primary key. The canonical B-tree win.
SELECT event_id, user_id, event_type, ts, amount, status
FROM events WHERE event_id = 17384625;

-- name: O2_user_recent_events
-- "Show me this user's last 20 events" — index scan + limit.
SELECT event_id, ts, event_type, amount, status
FROM events WHERE user_id = 1384625
ORDER BY ts DESC LIMIT 20;

-- name: O3_session_lookup
-- Lookup by a high-cardinality uuid.
SELECT event_id, ts, event_type, amount
FROM events WHERE session_id = '00000000-0000-4000-8000-0000001a2b3c'::uuid;

-- name: O4_tenant_recent_window
-- Small bounded range scan for one tenant.
SELECT event_id, user_id, ts, event_type, amount
FROM events
WHERE tenant_id = 23
  AND ts >= TIMESTAMP '2026-08-01' AND ts < TIMESTAMP '2026-08-02'
ORDER BY ts LIMIT 100;

-- name: O5_user_aggregate_small
-- Small per-user aggregate — the "analytical shape, tiny data" case that
-- sits between the two classes.
SELECT count(*) AS n, sum(amount) AS spend
FROM events WHERE user_id = 1384625;
