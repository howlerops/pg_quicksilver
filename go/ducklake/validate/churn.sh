#!/usr/bin/env bash
# churn.sh N [PAUSE_SECONDS]: N transactions against the source, each 2,000
# updates + 400 deletes + 400 inserts on random rows (the prototype's apply),
# PAUSE apart (default 0.3 s, so that each lands in its own apply tick).
set -uo pipefail
. "$(dirname "$0")/env.sh"
N=${1:-50}; PAUSE=${2:-0.3}
{
  for _ in $(seq 1 "$N"); do
    cat <<'SQL'
BEGIN;
UPDATE orders SET amount = (random()*100000)::numeric(12,2),
       status = (ARRAY['new','paid','shipped','delivered','cancelled','refunded'])[1+(random()*5)::int]
 WHERE seq IN (SELECT (random()*(SELECT last_value FROM orders_seq_next))::bigint FROM generate_series(1,2000));
DELETE FROM orders WHERE seq IN (SELECT (random()*(SELECT last_value FROM orders_seq_next))::bigint FROM generate_series(1,400));
INSERT INTO orders
SELECT gen_random_uuid(), nextval('orders_seq_next'), (random()*100000)::numeric(12,2),
       timestamptz '2023-01-01 00:00:00+00' + (random()*3*365*86400 || ' seconds')::interval,
       'new', repeat(md5(g::text || clock_timestamp()::text), 6)
FROM generate_series(1,400) g;
COMMIT;
SQL
    echo "SELECT pg_sleep($PAUSE);"
  done
} | su postgres -c "$PG/psql -h /tmp -p $PPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1" >/dev/null || die churn
"$HERE/settle.sh"
echo "churn: $N transactions; source rows $(psq 'SELECT count(*) FROM orders')"
