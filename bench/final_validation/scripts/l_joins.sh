#!/usr/bin/env bash
# Scenario l (experimental): join domains on the uuid key and a uuid foreign key.
#   fv.events.id (uuid key) and fv.lines.event_id (uuid, referring to it); fv.lines is new (300,000 rows).
# Sidecar3 (standby 5806) is restarted with QS_JOIN_DOMAINS='ev=fv.events.id,fv.lines.event_id'.
# Then: wait for quicksilver.join_domains to have rows, EXPLAIN the join, compare with the heap.
# Then the same sidecar with QS_JOIN_DOMAINS unset: nothing rewritten.
. "$(dirname "$0")/env.sh"; . "$(dirname "$0")/run.sh"
SP=5806; SMD=$BASE/mirror4
L_TABLES=$ALLT,fv.lines
echo "1. fv.lines on the primary ($PPORT is gone; promoted primary is $SPORT)"
if false; then q $SPORT "DROP TABLE IF EXISTS fv.lines; CREATE TABLE fv.lines (id bigint PRIMARY KEY, event_id uuid NOT NULL, qty int NOT NULL); INSERT INTO fv.lines SELECT i, md5('e' || ((i % 200000) + 1)::text)::uuid, 1 + i % 7 FROM generate_series(1, 300000) i; ANALYZE fv.lines" >/dev/null && echo "fv.lines created"; fi
echo "2. sidecar3 restarted with join domains"
kill "$(cat $BASE/mirror4.pid)" 2>/dev/null; for _ in $(seq 300); do kill -0 "$(cat $BASE/mirror4.pid)" 2>/dev/null || break; sleep 0.2; done
SC_TABLES=$L_TABLES bash $FV/scripts/launch_sc.sh mirror4 qs_int4 5806 5807 $SMD $SPORT QS_JOIN_DOMAINS='ev=fv.events.id,fv.lines.event_id'
# wait until the standby has caught up (readyz lag under one second), then for the domain's rows
for _ in $(seq 1200); do curl -s http://127.0.0.1:5807/readyz | grep -q '"lag_seconds":0\.[0-9]' && break; sleep 0.5; done
for _ in $(seq 360); do n=$(q $SPORT "SELECT count(*) FROM quicksilver.join_domains" | tail -1); case "$n" in ''|*[!0-9]*) ;; *) [ "$n" -gt 0 ] && break;; esac; sleep 0.5; done
echo "join_domains rows (primary $SPORT, standby $SP): $(q $SPORT 'SELECT count(*) FROM quicksilver.join_domains' | tail -1) / $(q $SP 'SELECT count(*) FROM quicksilver.join_domains' | tail -1)"
q $SP "SELECT * FROM quicksilver.join_domains LIMIT 4" 2>&1 | cut -c1-200
JQ="SELECT count(*), sum(l.qty) FROM fv.lines l JOIN fv.events e ON e.id = l.event_id WHERE e.amount > 0"
echo "--- EXPLAIN with join domains (standby $SP)"
su postgres -c "LD_LIBRARY_PATH=$LD_LIBRARY_PATH $PG/psql -X -h /tmp -p $SP -U postgres -d $DB -Atc \"EXPLAIN (VERBOSE, COSTS OFF) $JQ\"" 2>&1 | grep -iE "quicksilver|domain|rewrit|companion|Join|Hash" | head -12 | cut -c1-220
# the standby must replay the domain rows and the registration; then the join is judged for freshness
for _ in $(seq 360); do [ "$(q $SP "SELECT count(*) FROM quicksilver.join_domains" | tail -1)" = "$(q $SPORT "SELECT count(*) FROM quicksilver.join_domains" | tail -1)" ] && break; sleep 0.5; done
for _ in $(seq 120); do python3 -I $FV/scripts/fv_probe.py --port $SP --sql "$JQ" > $OUT/l_join_on.txt 2>&1; grep -q "routed_to_mirror=True" $OUT/l_join_on.txt && break; sleep 2; done
echo "probe exit $?"; cut -c1-220 $OUT/l_join_on.txt | head -6
echo "--- sidecar3 with QS_JOIN_DOMAINS unset"
kill "$(cat $BASE/mirror4.pid)" 2>/dev/null; for _ in $(seq 300); do kill -0 "$(cat $BASE/mirror4.pid)" 2>/dev/null || break; sleep 0.2; done
SC_TABLES=$L_TABLES bash $FV/scripts/launch_sc.sh mirror4 qs_int4 5806 5807 $SMD $SPORT
sleep 3
echo "join_domains rows after restart without the variable: $(q $SP 'SELECT count(*) FROM quicksilver.join_domains' 2>&1 | head -1)"
su postgres -c "LD_LIBRARY_PATH=$LD_LIBRARY_PATH $PG/psql -X -h /tmp -p $SP -U postgres -d $DB -Atc \"EXPLAIN (VERBOSE, COSTS OFF) $JQ\"" 2>&1 | grep -iE "quicksilver|domain|rewrit|companion" | head -6 | cut -c1-220
for _ in $(seq 120); do curl -s http://127.0.0.1:5807/readyz | grep -q '"lag_seconds":0\.[0-9]' && break; sleep 0.5; done
python3 -I $FV/scripts/fv_probe.py --port $SP --sql "$JQ" > $OUT/l_join_off.txt 2>&1; echo "probe exit $?"; cut -c1-220 $OUT/l_join_off.txt | head -6
