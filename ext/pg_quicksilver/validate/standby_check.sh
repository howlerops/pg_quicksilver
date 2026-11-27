#!/usr/bin/env bash
# The extension on the hot standby, which is where the product keeps the mirror.
# The registry and the allowlist arrive by replication; the applied LSN is
# compared with the replay position; a change made on the primary reaches a
# standby session's kept plan.
set -uo pipefail
. "$(dirname "$0")/env.sh"
fail=0
check() { if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1: got '$2', want '$3'"; fail=1; fi; }
sb() { su postgres -c "$PG/psql -X -h /tmp -p $SPORT -U postgres -d $DB -v ON_ERROR_STOP=1 -Atq" 2>&1; }
pr() { psq $PPORT "$1" >/dev/null; }
wait_replay() { local l; l=$(psq $PPORT "SELECT pg_current_wal_lsn()"); for _ in $(seq 1 100); do [ "$(psq $SPORT "SELECT pg_last_wal_replay_lsn() >= '$l'")" = t ] && return; sleep 0.1; done; }

check "it is a standby" "$(psq $SPORT 'SELECT pg_is_in_recovery()')" t
check "the registry replicated" "$(psq $SPORT "SELECT count(*) > 0 FROM quicksilver.mirror_status() WHERE usable")" t

out=$(sb <<'SQL'
SET quicksilver.route = auto;
SELECT count(*), sum(amount) FROM public.orders;
SELECT side FROM quicksilver.last_decision();
SELECT id FROM public.orders WHERE id = 7;
SELECT side || ': ' || reason FROM quicksilver.last_decision();
SQL
)
check "an aggregate is routed, a lookup is not" "$(echo "$out" | tr '\n' ' ')" "1000000|499995000.00 mirror 7 heap: cheap on the heap "

# the sidecar reports what it applied to the standby it sits next to: shared memory, no WAL
psq $SPORT "SELECT quicksilver.report_applied_lsn('public.orders', '0/10')" >/dev/null
check "report_applied_lsn works on a standby" "$(psq $SPORT "SELECT applied_lsn > '0/10' FROM quicksilver.mirror_status() WHERE heap = 'public.orders'::regclass")" t
check "(the durable copy from the primary still counts)" "$(psq $SPORT "SELECT fresh FROM quicksilver.mirror_status() WHERE heap = 'public.orders'::regclass")" t

# a kept plan on the standby, an allowlist change on the primary
exec 3> >(su postgres -c "$PG/psql -X -h /tmp -p $SPORT -U postgres -d $DB -Atq" > "$BASE/standby_check.out" 2>&1)
cat >&3 <<'SQL'
SET quicksilver.route = auto;
PREPARE s AS SELECT status, count(*) FROM public.orders WHERE amount * 2.0 > 2.00 GROUP BY status;
EXECUTE s \g /dev/null
SELECT 'before: ' || side FROM quicksilver.last_decision();
SQL
sleep 1
pr "DELETE FROM quicksilver.allowlist WHERE kind = 'operator' AND ident = '*(numeric,numeric)'"
wait_replay
cat >&3 <<'SQL'
EXECUTE s \g /dev/null
SELECT 'removed on the primary: ' || side || ', ' || detail FROM quicksilver.last_decision();
SQL
sleep 1
pr "INSERT INTO quicksilver.allowlist (kind, ident, verified_by) VALUES ('operator', '*(numeric,numeric)', 'docs/40 equality harness, pg_duckdb 1.2.0')"
wait_replay
cat >&3 <<'SQL'
EXECUTE s \g /dev/null
SELECT 'restored: ' || side FROM quicksilver.last_decision();
SQL
exec 3>&-
sleep 1
check "a kept plan on the standby follows the primary's allowlist" "$(tr '\n' ';' < "$BASE/standby_check.out")" \
  "before: mirror;removed on the primary: heap, operator pg_catalog.*(numeric,numeric);restored: mirror;"
rm -f "$BASE/standby_check.out"
exit $fail
