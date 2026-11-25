#!/bin/bash
# scenario g, repeated: g2.sh <tag> <rounds> [ENV=val ...]   backlog = rounds x 4 txns x ~1/4 of the table
. "$(dirname "$0")/lib.sh"
tag=$1; rounds=$2; shift 2
{
stop_sc a
echo "--- [$tag] sidecar stopped; writing the backlog at $(date -u +%T); env: $*"
l0=$(psq "SELECT pg_current_wal_lsn()"); n=0
for r in $(seq $rounds); do for k in 0 1 2 3; do
  c=$(psq "UPDATE items SET qty=coalesce(qty,0)+1, amount=coalesce(amount,0)+0.01, note='burst $tag $r' WHERE seq % 4 = $k"); n=$(( n + ${c#UPDATE } ))
done; done
echo "backlog: $n row changes in $((rounds*4)) txns, $(psq "SELECT pg_size_pretty(pg_current_wal_lsn() - '$l0'::pg_lsn)") of WAL; rows now $(psq 'select count(*) from items')"
t0=$(date +%s.%N)
start_sc a pod-a 9301 "$@"
pid=$(cat $OUT/sc_a.pid)
( while kill -0 $pid 2>/dev/null; do echo "$(date +%s.%N) $(awk '/VmRSS|VmHWM/{printf "%s %s ", $1, $2}' /proc/$pid/status 2>/dev/null)"; sleep 0.2; done > $OUT/g_rss_$tag.log 2>/dev/null ) </dev/null >/dev/null 2>&1 &
wait_applied a 900 && echo "drained after $(echo "$(date +%s.%N) - $t0" | bc) s"
sleep 4
verify a | head -6
python3 - <<P
rows=[l.split() for l in open("$OUT/g_rss_$tag.log") if "VmRSS" in l]
rss=[int(r[r.index("VmRSS:")+1]) for r in rows]; hwm=[int(r[r.index("VmHWM:")+1]) for r in rows]
print(f"[$tag] RSS samples={len(rss)} VmHWM={max(hwm)/1024:.0f} MB  final_RSS={rss[-1]/1024:.0f} MB")
step=max(1,len(rss)//14); print("RSS MB over time:", [round(x/1024) for x in rss[::step]])
P
} 2>&1 | tee $OUT/g_$tag.txt
