#!/bin/bash
# part 2: old build (bench/bin, c5421c5) and merged build side by side on the same data, same pinning (cpus 2-3)
set -u
Q=/tmp/qsb; S=$Q/scripts_merged; R=$Q/results_merged
say() { echo "=== $(date -u +%T) $*"; }
bin() { [ "$1" = old ] && echo $Q/bin || echo $Q/bin_merged; }
start() { QS_M=$Q/mirror_$1 QS_BIN=$(bin $1) bash $S/qs_start.sh $2 >/dev/null 2>&1 </dev/null; }
stop() { local p; p=$(cat $Q/qs.pid); kill $p 2>/dev/null; for i in $(seq 300); do kill -0 $p 2>/dev/null || break; sleep 0.1; done; sleep 1; }
qbench() { QS_M=$Q/mirror_$1 QS_BIN=$(bin $1) RESULT_TAG=_$3 VIEW_TAG=$2_$3 taskset -c 2-3 python3 -W ignore $S/bench.py --engine qs --state $2 --mode warm 2>&1 | grep -v DeprecationWarning; }
pgbench_() { taskset -c 2-3 python3 -W ignore $S/bench.py --engine pg --state $1 --mode warm 2>&1 | grep -v DeprecationWarning; }
hwm() { awk '{for(i=1;i<=NF;i++) if($i=="VmHWM:" && $(i+1)>m) m=$(i+1)} END{printf "%.0f MB", m/1024}' $R/qs_rss_$1.log; }
lag() { QS_M=$Q/mirror_$1 QS_TAG=$2 taskset -c 2-3 python3 -W ignore $S/live_lag.py qs $2 2>&1 | tail -1; }

for b in new old; do
  say "initial sync, $b build"
  rm -rf $Q/mirror_$b; mkdir -p $Q/mirror_$b
  start $b initial_$b; bash $S/qs_wait_ready.sh initial_$b
  echo "peak RSS so far ($b): $(hwm initial_$b)"; grep -v "slow tick" $R/qs_mirror_initial_$b.log | cut -c1-200
  [ $b = new ] && { say "pg A"; pgbench_ A; }
  say "qs A warm, $b"; qbench $b A $b
  echo "peak RSS at stop ($b): $(hwm initial_$b)"; du -sh $Q/mirror_$b | cut -f1
  stop
done
say "workload"; taskset -c 2-3 python3 -W ignore $S/workload.py | cut -c1-600
say "pg B"; pgbench_ B
say "drain, new build"; QS_M=$Q/mirror_new QS_BIN=$Q/bin_merged python3 -W ignore $S/qs_catchup.py catchup_new 2>&1 | tail -1
say "qs B warm, new (immediately after drain)"; qbench new B new
say "waiting for the idle rewrite"
# shellcheck disable=SC2034  # not read here: a loop counter, or read by a script that sources this
for i in $(seq 1800); do grep -q 'msg=compacted table=public.orders' $R/qs_mirror_catchup_new.log && break; sleep 0.5; done
grep 'compaction started\|msg=compacted\|merged deltas' $R/qs_mirror_catchup_new.log | cut -c1-220
say "qs B warm, new, after idle compaction"; qbench new B new_idle
echo "peak RSS drain+idle rewrite (new): $(hwm catchup_new)"
say "qs-verify orders, new"; ( time taskset -c 2-3 $Q/bin_merged/qs-verify -dsn postgres://postgres@localhost:5440/app -mirror $Q/mirror_new -table public.orders ) 2>&1 | grep -v '^$\|user\|sys'
taskset -c 2-3 $Q/bin_merged/qs-verify -dsn postgres://postgres@localhost:5440/app -mirror $Q/mirror_new -table public.customers 2>&1 | tail -1
say "live lag, new (sidecar already running, post-compaction)"; lag new new_run1; sleep 3; lag new new_run2
grep 'compaction started\|msg=compacted' $R/qs_mirror_catchup_new.log | tail -n +3 | cut -c1-220
stop
say "drain, old build"; QS_M=$Q/mirror_old QS_BIN=$Q/bin python3 -W ignore $S/qs_catchup.py catchup_old 2>&1 | tail -1
say "qs B warm, old"; qbench old B old
echo "peak RSS drain (old): $(hwm catchup_old)"
say "live lag, old"; sleep 3; lag old old_run1; sleep 3; lag old old_run2
grep -v "slow tick" $R/qs_mirror_catchup_old.log | cut -c1-220 | tail -5
stop
say "P2_DONE"
