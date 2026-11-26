#!/usr/bin/env bash
# Scenario h: ordinary roles on the standby.
#   fv_app   : LOGIN, not superuser, not a member of qs_duck, SELECT on fv.accounts and public.orders
#   fv_nosel : LOGIN, no SELECT on fv.accounts
# Checks: a qualifying aggregate is routed and equal to the heap for fv_app; fv_app is refused duckdb.query(...);
# fv_nosel gets the heap's permission error.
. "$(dirname "$0")/env.sh"; . "$(dirname "$0")/run.sh"
q $PPORT "DROP ROLE IF EXISTS fv_app" >/dev/null 2>&1
q $PPORT "DROP ROLE IF EXISTS fv_nosel" >/dev/null 2>&1
q $PPORT "CREATE ROLE fv_app LOGIN" && q $PPORT "CREATE ROLE fv_nosel LOGIN"
q $PPORT "GRANT CONNECT ON DATABASE app TO fv_app, fv_nosel"
q $PPORT "GRANT USAGE ON SCHEMA fv, public TO fv_app, fv_nosel"
q $PPORT "GRANT SELECT ON fv.accounts, fv.events, public.orders TO fv_app"
echo "--- role attributes"
q $PPORT "SELECT rolname, rolsuper, rolinherit, pg_has_role(rolname, 'qs_duck', 'MEMBER') AS member_of_qs_duck FROM pg_roles WHERE rolname IN ('fv_app','fv_nosel')"
sleep 2
echo "--- routed aggregate as fv_app (standby)"
python3 -I $FV/scripts/fv_probe.py --port $SPORT --user fv_app --expect-mirror \
  --sql "SELECT status, count(*), sum(amount) FROM public.orders GROUP BY status" \
  --sql "SELECT count(*), sum(amount) FROM fv.events WHERE amount > 1000" \
  --sql "SELECT label, count(*), sum(balance) FROM fv.accounts GROUP BY label" > $OUT/h_probe_fv_app.txt 2>&1
echo "probe exit $?"; cut -c1-220 $OUT/h_probe_fv_app.txt
echo "--- duckdb.query as fv_app (standby): expected refusal"
su postgres -c "LD_LIBRARY_PATH=$LD_LIBRARY_PATH $PG/psql -X -h /tmp -p $SPORT -U fv_app -d $DB -v ON_ERROR_STOP=1 -Atc \"SELECT count(*) FROM duckdb.query('SELECT 1 AS x')\"" > $OUT/h_duckdb_refused.txt 2>&1; echo "exit $?"; cut -c1-300 $OUT/h_duckdb_refused.txt
echo "--- aggregate as fv_nosel (no SELECT on fv.accounts): expected heap permission error"
su postgres -c "LD_LIBRARY_PATH=$LD_LIBRARY_PATH $PG/psql -X -h /tmp -p $SPORT -U fv_nosel -d $DB -v ON_ERROR_STOP=1 -Atc \"SELECT label, count(*) FROM fv.accounts GROUP BY label\"" > $OUT/h_nosel.txt 2>&1; echo "exit $?"; cut -c1-300 $OUT/h_nosel.txt
echo "--- and the same role on the heap-only statement it may read: fv.lookup (no grant)"
su postgres -c "LD_LIBRARY_PATH=$LD_LIBRARY_PATH $PG/psql -X -h /tmp -p $SPORT -U fv_app -d $DB -v ON_ERROR_STOP=1 -Atc \"SELECT count(*) FROM fv.lookup\"" 2>&1 | cut -c1-200
