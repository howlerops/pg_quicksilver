#!/usr/bin/env bash
# A PostgreSQL view over the lake table, through pg_duckdb: created ONCE (by
# qs-query -ducklake -engine postgres), then read across publishes — on the
# primary and on the hot standby, in sessions that existed before the publish
# and in new ones — and compared with the heap each time.
set -uo pipefail
. "$(dirname "$0")/env.sh"
ROUNDS=${1:-3}
su postgres -c "$BASE/qs-query -mirror $MIRROR -table public.orders -ducklake -engine postgres \
   -view qs_lake.orders -attach-on-login -dsn 'host=/tmp port=$PPORT user=postgres dbname=$DB'" 2>/dev/null > "$BASE/lake_view.sql" || die qs-query
# The schema has to exist before the view; the trigger block creates it.
su postgres -c "$PG/psql -h /tmp -p $PPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1 -f $BASE/lake_view.sql" >/dev/null 2>"$BASE/lake_view.err" || { cat "$BASE/lake_view.err"; die "create the view"; }
echo "view created once: $(grep -c . "$BASE/lake_view.sql") lines of SQL, md5 $(md5sum < "$BASE/lake_view.sql" | cut -c1-12)"
Q='SELECT count(*), sum(amount), count(*) FILTER (WHERE status = chr(110)||chr(101)||chr(119)) FROM'
# One session per node that stays open for the whole run: it attached at login,
# before any of the publishes it is about to read.
for port in $PPORT $SPORT; do
  mkfifo "$BASE/fifo.$port"
  su postgres -c "$PG/psql -h /tmp -p $port -U postgres -d $DB -At -f $BASE/fifo.$port > $BASE/session.$port.out 2>&1" &
  exec {fd}>"$BASE/fifo.$port"; eval "FD_$port=$fd"
done
ok=1
for round in $(seq 1 "$ROUNDS"); do
  "$HERE/churn.sh" 10 0.2 >/dev/null
  # the standby has to have replayed what the heap is compared at
  want=$(psq "SELECT pg_current_wal_lsn()")
  for _ in $(seq 1 100); do [ "$(psq "SELECT pg_last_wal_replay_lsn() >= '$want'" "$DB" "$SPORT")" = t ] && break; sleep 0.1; done
  heap=$(psq "$Q public.orders")
  for port in $PPORT $SPORT; do
    fd=$(eval "echo \$FD_$port"); before=$(wc -l < "$BASE/session.$port.out")
    echo "$Q qs_lake.orders;" >&"$fd"
    new=$(psq "$Q qs_lake.orders" "$DB" "$port")
    for _ in $(seq 1 100); do [ "$(wc -l < "$BASE/session.$port.out")" -gt "$before" ] && break; sleep 0.1; done
    old=$(tail -1 "$BASE/session.$port.out")
    verdict=EQUAL; { [ "$new" = "$heap" ] && [ "$old" = "$heap" ]; } || { verdict=DIFFERENT; ok=0; }
    echo "round $round port $port: heap $heap | view, new session $new | view, session open since before $old | $verdict"
  done
done
for port in $PPORT $SPORT; do fd=$(eval "echo \$FD_$port"); echo '\q' >&"$fd"; exec {fd}>&-; rm -f "$BASE/fifo.$port"; done
wait
echo "view definition regenerated: no (same file, md5 $(md5sum < "$BASE/lake_view.sql" | cut -c1-12))"
[ $ok = 1 ] || die "the view and the heap disagreed"
