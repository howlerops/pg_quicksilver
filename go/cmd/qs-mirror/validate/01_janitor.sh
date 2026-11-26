#!/bin/bash
# Item 1: the slot janitor, against real servers.
#
# Topology: primary P (5600) with a sidecar (pod p1) that stands down and runs
# the janitor; standby S1 (5601) with a sidecar (pod s1) that consumes its slot
# qs_v1_s1; standby S2 (5602) with NO sidecar, only streaming, as application
# s2. Slots on P before the janitor runs:
#   qs_v1_s1   active (S1's sidecar)                      -> must be kept
#   qs_v1_s2   inactive, but pod s2 is streaming          -> must be kept
#   qs_v1_gone inactive for longer than QS_SLOT_JANITOR_AFTER, no pod -> dropped
# QS_SLOT_JANITOR_AFTER=5s so the inactivity rule can be shown in a minute.
. "$(dirname "$0")/lib.sh"
exec > >(tee "$OUT/01_janitor.txt") 2>&1
set -x
export PS4='+ '

rm -f "$OUT/p1.log" "$OUT/s1.log" "$OUT/p1.pid" "$OUT/s1.pid"
for d in $BASE/p1 $BASE/s1 $BASE/s2 $BASE/mirror; do rm -rf "$d"; done
install -d -o postgres -g postgres $BASE $BASE/sock $BASE/mirror

# Primary.
pgu "$PG/initdb -D $BASE/p1 -U postgres -A trust --locale=C.UTF-8 -E UTF8" >/dev/null
cat >> $BASE/p1/postgresql.conf <<C
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
C
pg_up p1 5600
sql 5600 postgres <<'Q'
CREATE DATABASE app;
Q
sql 5600 app <<'Q'
CREATE TABLE t1 (id int PRIMARY KEY, v text);
Q

# Standbys: streaming from P. S1 gets a physical slot; S2 none.
sql 5600 postgres <<'Q'
SELECT pg_create_physical_replication_slot('s1phys');
Q
pgu "$PG/pg_basebackup -c fast -h $BASE/sock -p 5600 -U postgres -D $BASE/s1 -R -X stream -S s1phys" >/dev/null
pgu "$PG/pg_basebackup -c fast -h $BASE/sock -p 5600 -U postgres -D $BASE/s2 -R -X stream" >/dev/null
for s in s1 s2; do
  port=5601; [ $s = s2 ] && port=5602
  app=$s
  printf "\nport = $port\nlisten_addresses = 'localhost'\nunix_socket_directories = '$BASE/sock'\nprimary_conninfo = 'host=localhost port=5600 user=postgres dbname=app application_name=$app'\nhot_standby_feedback = on\n" >> $BASE/$s/postgresql.auto.conf
  [ $s = s1 ] && printf "primary_slot_name = 's1phys'\nsync_replication_slots = on\n" >> $BASE/$s/postgresql.auto.conf
  pgu "$PG/pg_ctl -D $BASE/$s -l $BASE/$s.log -w start" >/dev/null
done

# The slots the janitor is to judge. qs_v1_gone is created by hand and never
# consumed; qs_v1_s2 is inactive while pod s2 streams from P.
sql 5600 postgres <<'Q'
SELECT pg_create_logical_replication_slot('qs_v1_gone', 'pgoutput');
SELECT pg_create_logical_replication_slot('qs_v1_s2', 'pgoutput');
Q
echo "--- slots on P as created (nothing consumes them yet)"
sql 5600 postgres <<'Q'
SELECT slot_name, active FROM pg_replication_slots ORDER BY 1;
Q

# Sidecars: P's (janitor, fast cadence for the demo), then S1's consumer.
TABLES=public.t1 start_sc p1 p1 5610 5600 5600 QS_SLOT_JANITOR_INTERVAL=2s QS_SLOT_JANITOR_AFTER=5s QS_PRIMARY_POLL_INTERVAL=1s
TABLES=public.t1 start_sc s1 s1 5611 5601 5600
ready_alive() { for _ in $(seq 100); do curl -sf "http://127.0.0.1:$1/healthz" >/dev/null && return 0; sleep 0.1; done; return 1; }
ready_alive 5610 && echo "p1 alive (it stands down, so it is never ready)"
ready 5611

sql 5600 app <<'Q'
INSERT INTO t1 SELECT g, 'row ' || g FROM generate_series(1, 1000) g;
Q
TARGET=$(sql 5600 app <<'Q'
SELECT pg_current_wal_lsn()::text;
Q
)
wait_mirror s1 public.t1 "$TARGET" 120 && echo "mirror s1 caught up to $TARGET"

# Wait for the drop (the janitor runs every 2 s; qs_v1_gone must be older than 5 s).
for _ in $(seq 150); do
  n=$(sql 5600 postgres <<'Q'
SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'qs_v1_gone';
Q
)
  [ "$n" = 0 ] && break
  sleep 0.2
done
sleep 4   # let the janitor run again after the drop, to show the rest is kept

echo "--- slots on P after the janitor"
sql 5600 postgres <<'Q'
SELECT slot_name, active FROM pg_replication_slots ORDER BY 1;
Q
echo "--- janitor lines from the primary's sidecar"
grep -i "janitor" "$OUT/p1.log" || true
echo "--- S1's sidecar still consuming (applied rows on the mirror)"
sql 5600 app <<'Q'
UPDATE t1 SET v = v || '!' WHERE id <= 10;
Q
sleep 2
echo "mirror t1 applied LSN: $(applied_lsn s1 public.t1)"
verify s1 public.t1 5600

stop_sc s1; stop_sc p1
pg_down s2; pg_down s1; pg_down p1
