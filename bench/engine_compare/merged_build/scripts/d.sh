#!/bin/bash
# scenario d: TRUNCATE while a background compaction is running
. "$(dirname "$0")/lib.sh"
{
psq "SELECT pg_drop_replication_slot('qs_e2e_pod_b')" >/dev/null; rm -rf $E/mirror_b
L=$OUT/sc_a.log
n0=$(grep -c 'compaction started' $L); c0=$(grep -c 'msg=compacted' $L)
echo "churn burst at $(date -u +%T.%N)"
psq "UPDATE items SET amount=coalesce(amount,0)+1, note='churn' WHERE seq % 2 = 0"
# shellcheck disable=SC2034  # not read here: a loop counter, or read by a script that sources this
for i in $(seq 600); do [ "$(grep -c 'compaction started' $L)" -gt "$n0" ] && break; sleep 0.02; done
echo "compaction started seen at $(date -u +%T.%N); compacted-so-far=$(( $(grep -c 'msg=compacted' $L) - c0 ))"
psq "TRUNCATE items"; echo "TRUNCATE committed at $(date -u +%T.%N); compacted-so-far=$(( $(grep -c 'msg=compacted' $L) - c0 ))"
psq "INSERT INTO items SELECT gen_random_uuid(), g, 1+g%200, 'post-trunc', round((random()*1e6)::numeric,2), now(), jsonb_build_object('g',g), CASE WHEN g%3=0 THEN NULL ELSE 'pt '||g END, g, g%2=0, current_date FROM generate_series(1,20000) g"
wait_applied a && echo applied
sleep 3
verify a | head -8
echo "--- restart"; stop_sc a; start_sc a pod-a 9301; wait_ready 9301 && echo READY
taskset -c 2-3 python3 $E/scripts/wl.py 10 200 0 5
wait_applied a && echo applied
verify a | head -8
} 2>&1 | tee $OUT/d.txt
grep -n "" $L | tail -12 | cut -c1-300 | tee $OUT/d_log_tail.txt
