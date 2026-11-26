#!/usr/bin/env bash
# Scenario c: kill -9 the sidecar during writes, restart it, check MATCH and routing.
. "$(dirname "$0")/env.sh"; . "$(dirname "$0")/run.sh"
echo "before: sidecar pid $(cat $BASE/mirror.pid)"
python3 -I $FV/scripts/fv_mixed.py --port $PPORT --rounds 40 --seed ${SEED:-$(( $(date +%s) % 100000 ))} > $OUT/c_writes.txt 2>&1 &
WP=$!
sleep 3
PID=$(cat $BASE/mirror.pid)
echo "kill -9 sidecar pid $PID at $(date -u +%T) while writer $WP runs; writer rounds so far: $(grep -c committed $OUT/c_writes.txt)"
sidecar_kill9
sleep 2
kill -0 $PID 2>/dev/null && echo "pid $PID still alive" || echo "pid $PID gone"
echo "restart at $(date -u +%T)"
sidecar_start && echo "restarted sidecar pid $(cat $BASE/mirror.pid)"
wait_ready && echo "ready"
wait $WP; echo "writer exit $? rounds committed: $(grep -c committed $OUT/c_writes.txt)"
bash $FV/scripts/check_all.sh c
