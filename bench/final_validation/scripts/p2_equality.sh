#!/usr/bin/env bash
# Part 2: the equality harness on the standby (5806), as the superuser and as the ordinary role fv_app.
# First the sidecar on this standby goes back to the default configuration (QS_TEMPORAL_TYPES unset) on a fresh
# mirror directory (mirror6, slot qs_int6): scenario m left a text-timestamp mirror behind.
. "$(dirname "$0")/env.sh"; . "$(dirname "$0")/run.sh"
P=$SPORT; SP=5806
kill $(cat $BASE/mirror5.pid) 2>/dev/null; for _ in $(seq 300); do kill -0 $(cat $BASE/mirror5.pid) 2>/dev/null || break; sleep 0.2; done
echo "mirror5 sidecar stopped"
rm -rf $BASE/mirror6 && mkdir -p $BASE/mirror6 && chown postgres $BASE/mirror6
echo "duckdb.allowed_directories = '$BASE/mirror3,$BASE/mirror4,$BASE/mirror5,$BASE/mirror6,/tmp/qs_regress'" >> $BASE/standby3/postgresql.conf
su postgres -c "$PG/pg_ctl -D $BASE/standby3 -w reload" >/dev/null
bash $FV/scripts/launch_sc.sh mirror6 qs_int6 5806 5807 $BASE/mirror6 $P
for _ in $(seq 1200); do curl -s http://127.0.0.1:5807/readyz | grep -q '"lag_seconds":0\.[0-9]' && break; sleep 0.5; done
sleep 5
q $P "GRANT SELECT ON ALL TABLES IN SCHEMA ex, public TO fv_app" >/dev/null && echo "fv_app may read ex and public"
echo "--- superuser"
timeout 3000 python3 -I $REPO/ext/pg_quicksilver/validate/equality.py --port $SP --user postgres --out $OUT/p2_equality_superuser.json > $OUT/p2_equality_superuser.txt 2>&1; echo "exit $?"
tail -n 25 $OUT/p2_equality_superuser.txt | cut -c1-200
echo "--- ordinary role fv_app"
timeout 3000 python3 -I $REPO/ext/pg_quicksilver/validate/equality.py --port $SP --user fv_app --out $OUT/p2_equality_fv_app.json > $OUT/p2_equality_fv_app.txt 2>&1; echo "exit $?"
tail -n 25 $OUT/p2_equality_fv_app.txt | cut -c1-200
