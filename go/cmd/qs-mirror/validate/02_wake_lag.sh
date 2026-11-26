#!/bin/bash
# Item 2: commit-to-visible lag on a standby, before and after the event-driven
# apply loop. Usage: 02_wake_lag.sh <label> <sidecar binary>
#   label "before" with the commit 39702d4 build, "after" with this tree.
#
# Topology: primary (5600) with a 500k-row table public.bench; standby (5601)
# with one sidecar (pod s1, health 5611) that bootstraps the table by snapshot
# and then consumes the stream. lag.py commits 30 x 50 rows and 30 x 5,000 rows,
# one at a time, and times how long until qs-query -lsn reports the commit's LSN.
. "$(dirname "$0")/lib.sh"
LABEL=${1:?label}
export QS_MIRROR_BIN=${2:?sidecar binary}
exec > >(tee "$OUT/02_wake_lag_$LABEL.txt") 2>&1
set -x
export PS4='+ '
TABLES=public.bench

pg_down s1 2>/dev/null; pg_down p2 2>/dev/null
rm -f "$OUT/s1.log" "$OUT/s1.pid"
rm -rf $BASE/p2 $BASE/s1 $BASE/mirror
install -d -o postgres -g postgres $BASE $BASE/sock $BASE/mirror
pgu "$PG/initdb -D $BASE/p2 -U postgres -A trust --locale=C.UTF-8 -E UTF8" >/dev/null
cat >> $BASE/p2/postgresql.conf <<C
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
shared_buffers = 512MB
max_wal_size = 4GB
C
pg_up p2 5600
sql 5600 postgres <<'Q'
CREATE DATABASE app;
Q
sql 5600 app <<'Q'
CREATE TABLE bench (id bigint PRIMARY KEY, grp int, payload text);
INSERT INTO bench SELECT g, g % 100, md5(g::text) FROM generate_series(1, 500000) g;
CHECKPOINT;
Q
sql 5600 postgres <<'Q'
SELECT pg_create_physical_replication_slot('s1phys');
Q
pgu "$PG/pg_basebackup -c fast -h $BASE/sock -p 5600 -U postgres -D $BASE/s1 -R -X stream -S s1phys" >/dev/null
printf "\nport = 5601\nlisten_addresses = 'localhost'\nunix_socket_directories = '$BASE/sock'\nprimary_conninfo = 'host=localhost port=5600 user=postgres dbname=app application_name=s1'\nprimary_slot_name = 's1phys'\nhot_standby_feedback = on\nsync_replication_slots = on\n" >> $BASE/s1/postgresql.auto.conf
pgu "$PG/pg_ctl -D $BASE/s1 -l $BASE/s1.log -w start" >/dev/null

start_sc s1 s1 5611 5601 5600
for _ in $(seq 600); do
  [ -n "$(applied_lsn s1 public.bench 2>/dev/null)" ] && break
  sleep 0.2
done
echo "mirror of public.bench first LSN: $(applied_lsn s1 public.bench)"
ready 5611 120 && echo "s1 ready"

python3 "$(dirname "$0")/lag.py" "$LABEL" "$BIN/qs-query" "$BASE/mirror/s1" "$BASE/sock" 5600

stop_sc s1
pg_down s1; pg_down p2
