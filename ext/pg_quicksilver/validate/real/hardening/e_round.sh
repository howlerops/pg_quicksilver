#!/usr/bin/env bash
# e_round.sh: one round of experiment e, run by a psql \! from the app session (as the postgres OS
# user, so psql is called directly). Inserts 1,000 rows on the primary and commits, takes the
# primary's insert position as LSN, then waits until the standby has replayed it and the orders
# mirror reports an applied LSN at or past it (quicksilver.mirror_status(), a superuser query).
set -uo pipefail
PG=${PG:-/var/lib/postgresql/qsexp/pg17/usr/lib/postgresql/17/bin}
lsn=$("$PG/psql" -X -q -t -A -h /tmp -p 5860 -U postgres -d app -f - 2>/dev/null <<'SQL' | tail -1
INSERT INTO public.orders (id, customer_id, status, amount, created_at)
SELECT (SELECT max(id) FROM public.orders) + g, 1 + g % 10000, 'new', 1.00, now() FROM generate_series(1, 1000) g;
SELECT pg_current_wal_lsn();
SQL
)
ok=f
for _ in $(seq 1 1200); do
  ok=$("$PG/psql" -X -q -t -A -h /tmp -p 5861 -U postgres -d app -c "SELECT (SELECT applied_lsn FROM quicksilver.mirror_status() WHERE heap = 'public.orders'::regclass) >='$lsn'::pg_lsn AND pg_last_wal_replay_lsn() >= '$lsn'::pg_lsn" 2>/dev/null)
  [ "$ok" = t ] && break
  sleep 0.05
done
echo "e_round: committed at $lsn; mirror and standby replay at or past it: $ok"
