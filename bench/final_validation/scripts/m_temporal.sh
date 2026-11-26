#!/usr/bin/env bash
# Scenario m: escape hatch QS_TEMPORAL_TYPES=0 on a fresh mirror directory (mirror5, slot qs_int5),
# the sidecar on standby3 (5806) against the primary (5801). The other sidecar on this cluster
# (mirror4, from scenario l) is stopped first: two sidecars with one registry fight over it.
. "$(dirname "$0")/env.sh"; . "$(dirname "$0")/run.sh"
P=$SPORT
kill $(cat $BASE/mirror4.pid) 2>/dev/null; for _ in $(seq 300); do kill -0 $(cat $BASE/mirror4.pid) 2>/dev/null || break; sleep 0.2; done
echo "mirror4 sidecar stopped"
rm -rf $BASE/mirror5 && mkdir -p $BASE/mirror5 && chown postgres $BASE/mirror5
echo "duckdb.allowed_directories = '$BASE/mirror3,$BASE/mirror4,$BASE/mirror5,/tmp/qs_regress'" >> $BASE/standby3/postgresql.conf
su postgres -c "$PG/pg_ctl -D $BASE/standby3 -w reload" >/dev/null && echo "standby3 reloaded"
SC_TABLES=$ALLT bash $REPO/bench/final_validation/scripts/launch_sc.sh mirror5 qs_int5 5806 5807 $BASE/mirror5 $P QS_TEMPORAL_TYPES=0
grep -iE "temporal" $BASE/mirror5.log | head -3 | cut -c1-240
for _ in $(seq 600); do curl -s http://127.0.0.1:5807/readyz | grep -q '"lag_seconds":0\.[0-9]' && break; sleep 0.5; done
sleep 5
P=$P S=5806 MD=$BASE/mirror5 bash $FV/scripts/check_all.sh m_temporal_off
