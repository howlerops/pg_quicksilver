#!/bin/bash
# scenario c: two sidecars, same QS_SLOT, different QS_POD_NAME
. "$(dirname "$0")/lib.sh"
{
start_sc b pod-b 9302
wait_ready 9302 && echo "b READY"
echo "--- slots with both running"; slots
taskset -c 2-3 python3 $E/scripts/wl.py 60 500 0.02 3
wait_applied a && wait_applied b && echo "both applied"
echo "--- verify a (both running)"; verify a | head -8
echo "--- verify b (both running)"; verify b | head -8
echo "--- stop a (SIGTERM)"; stop_sc a; slots
taskset -c 2-3 python3 $E/scripts/wl.py 60 500 0.02 4
wait_applied b && echo "b applied"
echo "--- verify b (a stopped, workload ran meanwhile)"; verify b | head -8
echo "--- restart a"; start_sc a pod-a 9301; wait_ready 9301 && echo "a READY"; wait_applied a && echo "a applied"
echo "--- verify a (after restart)"; verify a | head -8
echo "--- verify b"; verify b | head -8
slots
stop_sc b
echo "--- b stopped"; slots
} 2>&1 | tee $OUT/c.txt
