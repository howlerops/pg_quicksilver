#!/bin/bash
# Item 5: two standbys, each with its own sidecar (pods s1 and s2), concurrent
# writes on the primary for one minute, and kill -9 of the s2 sidecar part way
# through and its restart. Both mirrors must verify MATCH, and both replication
# slots must exist and be active on the primary at the end.
. "$(dirname "$0")/lib.sh"
exec > >(tee "$OUT/05_two_standbys.txt") 2>&1
set -x
export PS4='+ '
TABLES=public.items

rm -f "$OUT/s1.log" "$OUT/s1.pid" "$OUT/s2.log" "$OUT/s2.pid"
for x in s1 s2 p5; do pg_down $x 2>/dev/null; done
rm -rf $BASE/p5 $BASE/s1 $BASE/s2 $BASE/mirror
install -d -o postgres -g postgres $BASE $BASE/sock $BASE/mirror
pgu "$PG/initdb -D $BASE/p5 -U postgres -A trust --locale=C.UTF-8 -E UTF8" >/dev/null
cat >> $BASE/p5/postgresql.conf <<C
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
C
pg_up p5 5600
sql 5600 postgres <<'Q'
CREATE DATABASE app;
Q
sql 5600 app <<'Q'
CREATE TABLE items (id bigint PRIMARY KEY, v text, n int);
INSERT INTO items SELECT g, md5(g::text), 0 FROM generate_series(1, 10000) g;
Q
for s in s1 s2; do
  sql 5600 postgres <<Q
SELECT pg_create_physical_replication_slot('${s}phys');
Q
  pgu "$PG/pg_basebackup -c fast -h $BASE/sock -p 5600 -U postgres -D $BASE/$s -R -X stream -S ${s}phys" >/dev/null
done
printf "\nport = 5601\nlisten_addresses = 'localhost'\nunix_socket_directories = '$BASE/sock'\nprimary_conninfo = 'host=localhost port=5600 user=postgres dbname=app application_name=s1'\nprimary_slot_name = 's1phys'\nhot_standby_feedback = on\nsync_replication_slots = on\n" >> $BASE/s1/postgresql.auto.conf
printf "\nport = 5602\nlisten_addresses = 'localhost'\nunix_socket_directories = '$BASE/sock'\nprimary_conninfo = 'host=localhost port=5600 user=postgres dbname=app application_name=s2'\nprimary_slot_name = 's2phys'\nhot_standby_feedback = on\nsync_replication_slots = on\n" >> $BASE/s2/postgresql.auto.conf
pgu "$PG/pg_ctl -D $BASE/s1 -l $BASE/s1.log -w start" >/dev/null
pgu "$PG/pg_ctl -D $BASE/s2 -l $BASE/s2.log -w start" >/dev/null

start_sc s1 s1 5611 5601 5600 >/dev/null
start_sc s2 s2 5612 5602 5600 >/dev/null
ready 5611 120 && echo "s1 ready"
ready 5612 120 && echo "s2 ready"
sql 5600 postgres <<'Q'
SELECT slot_name, active FROM pg_replication_slots ORDER BY 1;
Q

# One minute of concurrent writes: batches of inserts and random updates.
writer() {
  local end=$((SECONDS + 60)) n=0
  while [ $SECONDS -lt $end ]; do
    n=$((n + 1))
    sql 5600 app >/dev/null 2>&1 <<Q
INSERT INTO items SELECT g, md5(g::text), $n FROM generate_series($((10000 + n * 20)), $((10000 + n * 20 + 19))) g ON CONFLICT DO NOTHING;
UPDATE items SET n = n + 1 WHERE id = 1 + (random() * 9999)::int;
UPDATE items SET v = md5(random()::text) WHERE id = 1 + (random() * 9999)::int;
Q
  done
  echo "writer done: $n batches"
}
echo "=== writes start"
writer &
W=$!
sleep 20

echo "=== kill -9 the s2 sidecar (pid $(cat "$OUT/s2.pid")) while writes continue"
kill -9 "$(cat "$OUT/s2.pid")"
sleep 1
echo "--- slots on the primary during the outage"
sql 5600 postgres <<'Q'
SELECT slot_name, active FROM pg_replication_slots ORDER BY 1;
Q
start_sc s2 s2 5612 5602 5600 >/dev/null
echo "=== s2 sidecar restarted (pid $(cat "$OUT/s2.pid"))"
ready 5612 120 && echo "s2 ready after restart"

wait "$W"
echo "=== writes stopped"
TARGET=$(sql 5600 app <<'Q'
SELECT pg_current_wal_lsn()::text;
Q
)
wait_mirror s1 public.items "$TARGET" 300 && echo "s1 caught up"
wait_mirror s2 public.items "$TARGET" 300 && echo "s2 caught up"
sleep 2

echo "=== verify both mirrors"
verify s1 public.items 5600
verify s2 public.items 5600
echo "=== slots on the primary at the end"
sql 5600 postgres <<'Q'
SELECT slot_name, active, active_pid IS NOT NULL AS has_pid FROM pg_replication_slots ORDER BY 1;
Q
echo "=== kill -9 and restart in the s2 log"
grep -n "mirror starting\|mirror stopped\|resum\|new replication slot" "$OUT/s2.log" | cut -c1-220

stop_sc s1; stop_sc s2
pg_down s1; pg_down s2; pg_down p5
