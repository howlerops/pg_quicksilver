#!/bin/bash
# scenario g: a backlog several times the buffer caps (defaults: tick 250k, pending 1M, stream 250k changes)
. "$(dirname "$0")/lib.sh"
# shellcheck disable=SC2034  # not read here: a loop counter, or read by a script that sources this
L=$OUT/sc_a.log
rss_sampler() { local pid=$1 f=$2; ( while kill -0 $pid 2>/dev/null; do echo "$(date +%s.%N) $(awk '/VmRSS|VmHWM/{printf "%s %s ", $1, $2}' /proc/$pid/status 2>/dev/null)"; sleep 0.2; done > $f ) & }
{
echo "--- recover from f: remove the NaN row, drop slot, remove mirror, fresh snapshot"
stop_sc a
psq "DELETE FROM items WHERE status IN ('nan-row','after-nan')"
psq "SELECT pg_drop_replication_slot('qs_e2e_pod_a')" >/dev/null; rm -rf $E/mirror_a
start_sc a pod-a 9301; wait_ready 9301 && echo READY; wait_applied a
verify a | head -4
sleep 1
echo "idle RSS before burst: $(awk '/VmRSS|VmHWM/{printf "%s %s kB  ", $1, $2}' /proc/"$(cat $OUT/sc_a.pid)"/status)"
stop_sc a
echo "--- sidecar stopped; writing the backlog at $(date -u +%T)"
l0=$(psq "SELECT pg_current_wal_lsn()")
for r in $(seq 16); do for k in 0 1 2 3; do
  psq "UPDATE items SET qty=coalesce(qty,0)+1, amount=coalesce(amount,0)+0.01, note='burst $r' WHERE seq % 4 = $k" >/dev/null
done; done
psq "DELETE FROM items WHERE seq % 10 = 3"
echo "backlog: $(psq "SELECT pg_size_pretty(pg_current_wal_lsn() - '$l0'::pg_lsn)") of WAL, 64 txns x ~49.5k row updates + 1 delete txn; rows now $(psq 'select count(*) from items')"
echo "--- start sidecar at $(date -u +%T)"
t0=$(date +%s.%N)
start_sc a pod-a 9301
rss_sampler "$(cat $OUT/sc_a.pid)" $OUT/g_rss.log
wait_applied a 600 && echo "drained after $(echo "$(date +%s.%N) - $t0" | bc) s"
sleep 5
verify a | head -6
python3 - <<P
rows=[l.split() for l in open("$OUT/g_rss.log") if "VmRSS" in l]
rss=[int(r[r.index("VmRSS:")+1]) for r in rows]; hwm=[int(r[r.index("VmHWM:")+1]) for r in rows]
print(f"RSS samples={len(rss)} peak_VmRSS_sampled={max(rss)/1024:.0f} MB  VmHWM={max(hwm)/1024:.0f} MB  final_RSS={rss[-1]/1024:.0f} MB")
step=max(1,len(rss)//12); print("RSS MB over time:", [round(x/1024) for x in rss[::step]])
P
du -sh $E/mirror_a | cut -f1
} 2>&1 | tee $OUT/g.txt
