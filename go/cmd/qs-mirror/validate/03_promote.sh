#!/bin/bash
# Item 3: promotion of a standby that runs a sidecar, then a new standby of the
# promoted node under the same pod name.
#
# Primary P (5600). Standby S1 (5601) with a physical slot s1phys and a sidecar
# (pod s1, health 5611). The logical slot qs_v1_s1 is created with FAILOVER, so
# S1 receives it through sync_replication_slots. Then: stop P (a failure), promote
# S1, watch its sidecar stand down and release its own slot. Then a new standby
# N (5602) of S1 by pg_basebackup, a sidecar there under the same pod name s1,
# rows written on the new primary, and qs-verify MATCH.
. "$(dirname "$0")/lib.sh"
exec > >(tee "$OUT/03_promote.txt") 2>&1
set -x
export PS4='+ '
TABLES=public.orders

rm -f "$OUT/s1.log" "$OUT/s1.pid" "$OUT/n1.log" "$OUT/n1.pid"
for x in s1 n1 p3; do pg_down $x 2>/dev/null; done
rm -rf $BASE/p3 $BASE/s1 $BASE/n1 $BASE/mirror
install -d -o postgres -g postgres $BASE $BASE/sock $BASE/mirror
pgu "$PG/initdb -D $BASE/p3 -U postgres -A trust --locale=C.UTF-8 -E UTF8" >/dev/null
cat >> $BASE/p3/postgresql.conf <<C
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
C
pg_up p3 5600
sql 5600 postgres <<'Q'
CREATE DATABASE app;
Q
sql 5600 app <<'Q'
CREATE TABLE orders (id int PRIMARY KEY, amount int, note text);
INSERT INTO orders SELECT g, g * 3, 'before ' || g FROM generate_series(1, 10000) g;
Q
sql 5600 postgres <<'Q'
SELECT pg_create_physical_replication_slot('s1phys');
Q
pgu "$PG/pg_basebackup -c fast -h $BASE/sock -p 5600 -U postgres -D $BASE/s1 -R -X stream -S s1phys" >/dev/null
printf "\nport = 5601\nlisten_addresses = 'localhost'\nunix_socket_directories = '$BASE/sock'\nprimary_conninfo = 'host=localhost port=5600 user=postgres dbname=app application_name=s1'\nprimary_slot_name = 's1phys'\nhot_standby_feedback = on\nsync_replication_slots = on\n" >> $BASE/s1/postgresql.auto.conf
pgu "$PG/pg_ctl -D $BASE/s1 -l $BASE/s1.log -w start" >/dev/null

start_sc s1 s1 5611 5601 5600 >/dev/null
ready 5611 120 && echo "s1 ready"
TARGET=$(sql 5600 app <<'Q'
SELECT pg_current_wal_lsn()::text;
Q
)
wait_mirror s1 public.orders "$TARGET" 120 && echo "s1 caught up"
# The failover slot reaches the standby through slot synchronisation, and the
# standby only accepts it once it has replayed past the slot's position. The
# slot was created after the standby started, so write until it is synced.
echo "--- wait until the standby has synced qs_v1_s1 (failover slot)"
for _ in $(seq 240); do
  n=$(sql 5601 postgres <<'Q'
SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'qs_v1_s1' AND synced;
Q
)
  [ "$n" = 1 ] && { echo "synced"; break; }
  sql 5600 app <<'Q' >/dev/null
INSERT INTO orders SELECT g, g, 'pre ' || g FROM generate_series(20001, 20010) g;
DELETE FROM orders WHERE id BETWEEN 20001 AND 20010;
Q
  sleep 0.5
done
TARGET=$(sql 5600 app <<'Q'
SELECT pg_current_wal_lsn()::text;
Q
)
wait_mirror s1 public.orders "$TARGET" 120 && echo "s1 caught up again"
echo "--- before the failover: verify on the primary"
verify s1 public.orders 5600
echo "--- before the failover: slots on the primary"
sql 5600 postgres <<'Q'
SELECT slot_name, active, failover FROM pg_replication_slots ORDER BY 1;
Q
echo "--- before the failover: the same slot as the standby sees it (synced)"
sql 5601 postgres <<'Q'
SELECT slot_name, active, synced, temporary FROM pg_replication_slots ORDER BY 1;
Q

echo "=== FAILOVER: primary stopped, S1 promoted"
pgu "$PG/pg_ctl -D $BASE/p3 -m immediate -w stop" >/dev/null
pgu "$PG/pg_ctl -D $BASE/s1 -w promote" >/dev/null
for _ in $(seq 300); do
  grep -q "standing down" "$OUT/s1.log" 2>/dev/null && break
  sleep 0.2
done
sleep 3
echo "--- sidecar log on the promoted node"
grep -iE "primary|standing|slot|promot" "$OUT/s1.log" | cut -c1-260
echo "--- slots on the new primary after its sidecar stood down"
sql 5601 postgres <<'Q'
SELECT slot_name, active, synced FROM pg_replication_slots ORDER BY 1;
Q
echo "--- pg_is_in_recovery on the new primary"
sql 5601 postgres <<'Q'
SELECT pg_is_in_recovery();
Q

echo "=== NEW STANDBY of the promoted node, same pod name (s1), pg_basebackup"
sql 5601 postgres <<'Q'
SELECT pg_create_physical_replication_slot('n1phys');
Q
pgu "$PG/pg_basebackup -c fast -h $BASE/sock -p 5601 -U postgres -D $BASE/n1 -R -X stream -S n1phys" >/dev/null
printf "\nport = 5602\nlisten_addresses = 'localhost'\nunix_socket_directories = '$BASE/sock'\nprimary_conninfo = 'host=localhost port=5601 user=postgres dbname=app application_name=s1'\nprimary_slot_name = 'n1phys'\nhot_standby_feedback = on\nsync_replication_slots = on\n" >> $BASE/n1/postgresql.auto.conf
pgu "$PG/pg_ctl -D $BASE/n1 -l $BASE/n1.log -w start" >/dev/null
start_sc n1 s1 5612 5602 5601 >/dev/null
ready 5612 120 && echo "new standby sidecar ready"
TARGET=$(sql 5601 app <<'Q'
SELECT pg_current_wal_lsn()::text;
Q
)
wait_mirror n1 public.orders "$TARGET" 180 && echo "new standby caught up"

echo "=== writes on the new primary"
sql 5601 app <<'Q'
INSERT INTO orders SELECT g, g * 3, 'after ' || g FROM generate_series(10001, 12000) g;
UPDATE orders SET amount = amount + 1 WHERE id <= 500;
Q
TARGET=$(sql 5601 app <<'Q'
SELECT pg_current_wal_lsn()::text;
Q
)
wait_mirror n1 public.orders "$TARGET" 120 && echo "new standby caught up with the new writes"
echo "--- slots on the new primary"
sql 5601 postgres <<'Q'
SELECT slot_name, active FROM pg_replication_slots ORDER BY 1;
Q
echo "--- verify the new standby's mirror against the new primary"
verify n1 public.orders 5601

stop_sc n1; stop_sc s1
pg_down n1; pg_down s1
