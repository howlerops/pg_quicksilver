#!/bin/bash
# scenario b: kill -9 mid-workload, restart
. "$(dirname "$0")/lib.sh"
{
taskset -c 2-3 python3 $E/scripts/wl.py 200 500 0.04 2 > $OUT/b_wl.json &
W=$!
sleep 4
echo "state before kill: $(tr -d '\n ' < $E/mirror_a/public.items/state.json | cut -c1-300)"
stop_sc a -KILL; echo "killed -9 at $(date -u +%T)"
slots
wait $W; cat $OUT/b_wl.json
echo "state after kill: $(tr -d '\n ' < $E/mirror_a/public.items/state.json | cut -c1-300)"
echo "--- restart $(date -u +%T)"
start_sc a pod-a 9301
wait_ready 9301 && echo READY
wait_applied a && echo applied
verify a | head -8
slots
} 2>&1 | tee $OUT/b.txt
