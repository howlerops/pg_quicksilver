#!/bin/bash
# Item 4: a schema change that cannot be mirrored halts only its own table.
#
# Topology: primary (5600), standby (5601) with one sidecar (pod s1, health
# 5611). Tables t1, t2, t3 are written continuously while the change lands.
#   phase A: ALTER TABLE t2 ALTER COLUMN x TYPE bigint       (ddl.Compare: retyped)
#   phase B: ALTER TABLE t2 ADD COLUMN r double precision
#            DEFAULT random()                                  (evolve: a table rewrite)
# Each phase: t1 and t3 must still MATCH, /readyz and /metrics must name t2 and
# the reason; then a REBUILD file in t2's mirror directory and a restart of the
# sidecar must bring all three back to MATCH.
. "$(dirname "$0")/lib.sh"
exec > >(tee "$OUT/04_halt.txt") 2>&1
set -x
export PS4='+ '
TABLES=public.t1,public.t2,public.t3
HEALTH=127.0.0.1:5611
STOP=$OUT/04_writers.stop

rm -f "$STOP" "$OUT/s1.log" "$OUT/s1.pid"
pg_down s1 2>/dev/null; pg_down p4 2>/dev/null
rm -rf $BASE/p4 $BASE/s1 $BASE/mirror
install -d -o postgres -g postgres $BASE $BASE/sock $BASE/mirror
pgu "$PG/initdb -D $BASE/p4 -U postgres -A trust --locale=C.UTF-8 -E UTF8" >/dev/null
cat >> $BASE/p4/postgresql.conf <<C
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
C
pg_up p4 5600
sql 5600 postgres <<'Q'
CREATE DATABASE app;
Q
for t in t1 t2 t3; do
sql 5600 app <<Q
CREATE TABLE $t (id bigint PRIMARY KEY, x int, v text);
INSERT INTO $t SELECT g, g % 1000, md5(g::text) FROM generate_series(1, 20000) g;
Q
done
sql 5600 postgres <<'Q'
SELECT pg_create_physical_replication_slot('s1phys');
Q
pgu "$PG/pg_basebackup -c fast -h $BASE/sock -p 5600 -U postgres -D $BASE/s1 -R -X stream -S s1phys" >/dev/null
printf "\nport = 5601\nlisten_addresses = 'localhost'\nunix_socket_directories = '$BASE/sock'\nprimary_conninfo = 'host=localhost port=5600 user=postgres dbname=app application_name=s1'\nprimary_slot_name = 's1phys'\nhot_standby_feedback = on\nsync_replication_slots = on\n" >> $BASE/s1/postgresql.auto.conf
pgu "$PG/pg_ctl -D $BASE/s1 -l $BASE/s1.log -w start" >/dev/null

# writers START: one loop per table on the primary, until $STOP exists.
writers_start() {
  rm -f "$STOP"
  for t in t1 t2 t3; do
    ( n=0
      while [ ! -f "$STOP" ]; do
        n=$((n+1))
        sql 5600 app >/dev/null 2>&1 <<Q
UPDATE $t SET v = md5(random()::text) WHERE id % 97 = $((n % 97));
INSERT INTO $t SELECT g, g % 1000, md5(g::text) FROM generate_series($((100000 + n*20)), $((100000 + n*20 + 19))) g ON CONFLICT DO NOTHING;
Q
      done ) &
  done
}
writers_stop() { touch "$STOP"; wait; rm -f "$STOP"; }

# wait_halt: until /readyz names t2 (it is not ready once a table is halted).
wait_halt() {
  for _ in $(seq 600); do
    curl -s "http://$HEALTH/readyz" | grep -q 'public.t2' && return 0
    sleep 0.2
  done
  echo "t2 was not reported as halted"; return 1
}

# show_halt: the readiness body and the halt metrics.
show_halt() {
  echo "--- /readyz (HTTP status, body)"
  curl -s -w '\nHTTP %{http_code}\n' "http://$HEALTH/readyz"
  echo "--- /metrics lines about halted tables"
  curl -s "http://$HEALTH/metrics" | grep -iE 'halt' | grep -v '^#'
}

# rebuild_t2: REBUILD in t2's mirror directory, restart the sidecar, wait for all three.
rebuild_t2() {
  touch "$BASE/mirror/s1/public.t2/REBUILD"
  ls -la "$BASE/mirror/s1/public.t2/REBUILD"
  stop_sc s1
  start_sc s1 s1 5611 5601 5600 >/dev/null
  ready 5611 120 && echo "s1 ready after restart"
  local target
  target=$(sql 5600 app <<'Q'
SELECT pg_current_wal_lsn()::text;
Q
)
  for t in t1 t2 t3; do wait_mirror s1 public.$t "$target" 180 && echo "$t caught up to $target"; done
}

# Let the stream start, from a snapshot of all three.
start_sc s1 s1 5611 5601 5600 >/dev/null
ready 5611 120 && echo "s1 ready"
TARGET=$(sql 5600 app <<'Q'
SELECT pg_current_wal_lsn()::text;
Q
)
for t in t1 t2 t3; do wait_mirror s1 public.$t "$TARGET" 120 && echo "$t caught up"; done

echo "=== PHASE A: ALTER COLUMN TYPE on t2, while t1, t2 and t3 are written"
writers_start; sleep 4
sql 5600 app <<'Q'
ALTER TABLE t2 ALTER COLUMN x TYPE bigint;
Q
wait_halt && echo "t2 halted"
sleep 3
show_halt
writers_stop
sleep 2
echo "--- phase A: verify (t2 is expected to DIVERGE until it is rebuilt)"
for t in t1 t2 t3; do echo "--- $t"; verify s1 public.$t 5600; done
echo "--- phase A: REBUILD and restart"
rebuild_t2
for t in t1 t2 t3; do echo "--- after rebuild: $t"; verify s1 public.$t 5600; done

echo "=== PHASE B: a table rewrite (volatile default) on t2, while all three are written"
writers_start; sleep 4
sql 5600 app <<'Q'
ALTER TABLE t2 ADD COLUMN r double precision DEFAULT random();
Q
wait_halt && echo "t2 halted again"
sleep 3
show_halt
writers_stop
sleep 2
for t in t1 t3; do echo "--- phase B: $t"; verify s1 public.$t 5600; done
echo "--- phase B: REBUILD and restart"
rebuild_t2
for t in t1 t2 t3; do echo "--- final: $t"; verify s1 public.$t 5600; done

stop_sc s1
pg_down s1; pg_down p4
