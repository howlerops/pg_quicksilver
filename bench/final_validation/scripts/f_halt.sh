#!/usr/bin/env bash
# Scenario f: an unsupported schema change on one table (fv.lookup: id integer -> bigint).
# Only that table should halt; /readyz names it; the other eleven keep matching and routing.
# Then create REBUILD in its mirror directory, restart the sidecar, and check all twelve.
. "$(dirname "$0")/env.sh"; . "$(dirname "$0")/run.sh"
q $PPORT "ALTER TABLE fv.lookup ALTER COLUMN id TYPE bigint" && echo "altered fv.lookup.id to bigint"
python3 -I $FV/scripts/fv_mixed.py --port $PPORT --rounds 2 --seed 7 > $OUT/f_writes.txt 2>&1; echo "writes exit $?"
for _ in $(seq 150); do grep -q "table halted" $BASE/mirror.log && grep "fv.lookup" $BASE/mirror.log | grep -q "table halted" && break; sleep 0.2; done
echo "--- readyz (HTTP $(curl -s -o /dev/null -w '%{http_code}' http://$HEALTH/readyz)):"
curl -s http://$HEALTH/readyz | python3 -c 'import json,sys; d=json.load(sys.stdin); print("ready=",d.get("ready")); print("halted_tables=",json.dumps(d.get("halted_tables"))[:600]); print("reason=",d.get("reason"))'
echo "--- metrics:"; curl -s http://$HEALTH/metrics | grep -E "^quicksilver_halted_tables" | cut -c1-200
echo "--- log:"; grep "table halted" $BASE/mirror.log | tail -2 | cut -c1-400
echo "--- while halted:"
bash $FV/scripts/check_all.sh f_halted "$(echo $ALLT | sed "s/,fv.lookup//")"
echo "--- rebuild:"
mkdir -p $BASE/mirror/fv.lookup && touch $BASE/mirror/fv.lookup/REBUILD && chown postgres $BASE/mirror/fv.lookup/REBUILD && ls -la $BASE/mirror/fv.lookup/REBUILD
sidecar_stop; sidecar_start && echo "restarted sidecar pid $(cat $BASE/mirror.pid)"; wait_ready && echo "ready"
bash $FV/scripts/check_all.sh f_rebuilt
