#!/usr/bin/env bash
# Part 4 phases, each run with the sidecar inside the 1 GiB memory cgroup (the plugin's default sidecarMemory):
#   p4_phase.sh sync      drop the slot and the mirror, start the sidecar: the initial snapshot sync
#   p4_phase.sh workload  stop the sidecar, rebuild the change-workload tables (wl.*), run the workload
#   p4_phase.sh drain     start the sidecar: the backlog drain, timed to the workload's end LSN
#   p4_phase.sh status    cgroup usage, peak, failcnt, the sidecar's state and any OOM in the log
# The cgroup is /sys/fs/cgroup/memory/qsfinal, limit 1073741824 bytes (DefaultSidecarMemory = 1Gi).
. "$(dirname "$0")/env.sh"; . "$(dirname "$0")/run.sh"
CG=/sys/fs/cgroup/memory/qsfinal
BB=$BASE/bench
export BENCH_PRIMARY=5820 BENCH_STANDBY=5821 BENCH_HEALTH=5822 BENCH_MIRROR=$BB/mirror BENCH_DIR=$BB BENCH_LOOKUP=$BB/lookup_id.txt
SQLDIR=$REPO/bench/engine_compare/four_engines/scripts
bq() { su postgres -c "LD_LIBRARY_PATH=$LD_LIBRARY_PATH $PG/psql -X -h /tmp -p 5820 -U postgres -d $DB -v ON_ERROR_STOP=1 -Atc \"$1\"" 2>&1; }
stop_sidecar() {
  local P; P=$(cat $BB/mirror.pid 2>/dev/null)
  [ -n "$P" ] && kill -0 $P 2>/dev/null && { kill $P; for _ in $(seq 600); do kill -0 $P 2>/dev/null || break; sleep 0.1; done; }
  echo "sidecar stopped (pid ${P:-none})"
}
incg() { echo $BASHPID > $CG/cgroup.procs; }     # this shell and its children are in the cgroup
case "${1:-}" in
sync)
  stop_sidecar
  bq "SELECT pg_drop_replication_slot('qs_bench')" >/dev/null && echo "slot qs_bench dropped"
  rm -rf $BB/mirror && mkdir -p $BB/mirror && chown postgres $BB/mirror
  echo 0 > $CG/memory.failcnt 2>/dev/null || true
  echo "cgroup before: usage $(cat $CG/memory.usage_in_bytes) max $(cat $CG/memory.max_usage_in_bytes)"
  incg; bash $FV/scripts/bench_sidecar.sh start
  echo "cgroup after ready: usage $(cat $CG/memory.usage_in_bytes) max $(cat $CG/memory.max_usage_in_bytes) failcnt $(cat $CG/memory.failcnt)"
  echo "readyz: $(curl -s http://127.0.0.1:5822/readyz | cut -c1-160)"
  ;;
workload)
  stop_sidecar
  bq "DROP SCHEMA IF EXISTS wl CASCADE" >/dev/null
  sed "s|/tmp/qsb/results/lookup_id.txt|$BB/lookup_id.txt|" $SQLDIR/staging.sql > $BB/sql/staging.sql && chown postgres $BB/sql/staging.sql
  T=$(date +%s); su postgres -c "LD_LIBRARY_PATH=$LD_LIBRARY_PATH $PG/psql -X -h /tmp -p 5820 -U postgres -d $DB -q -v ON_ERROR_STOP=1 -f $BB/sql/staging.sql" >/dev/null 2>&1 || echo "staging failed"
  echo "staging seconds $(( $(date +%s) - T ))"
  T=$(date +%s); taskset -c 3 python3 -I $FV/scripts/fv_workload.py | tail -n 1 | cut -c1-200; echo "workload seconds $(( $(date +%s) - T ))"
  python3 -c "import json;print(json.load(open('$BB/results/workload.json'))['lsn_end'])" > $OUT/p4_workload_end_lsn.txt
  echo "end LSN $(cat $OUT/p4_workload_end_lsn.txt)"
  ;;
drain)
  LSN=$(cat $OUT/p4_workload_end_lsn.txt)
  incg; T0=$(date +%s.%N)
  NOWAIT=1 bash $FV/scripts/bench_sidecar.sh start >/dev/null
  bash $FV/scripts/bench_sidecar.sh drain $LSN
  python3 -c "print('drain wall seconds from sidecar start: %.1f' % ($(date +%s.%N)-$T0))"
  echo "cgroup after drain: usage $(cat $CG/memory.usage_in_bytes) max $(cat $CG/memory.max_usage_in_bytes) failcnt $(cat $CG/memory.failcnt)"
  echo "sidecar VmHWM $(awk '/VmHWM/{print $2" kB"}' /proc/"$(cat $BB/mirror.pid)"/status 2>/dev/null)"
  ;;
status)
  P=$(cat $BB/mirror.pid 2>/dev/null)
  echo "sidecar pid ${P:-none} alive: $(kill -0 $P 2>/dev/null && echo yes || echo no)"
  echo "cgroup usage $(cat $CG/memory.usage_in_bytes) max $(cat $CG/memory.max_usage_in_bytes) failcnt $(cat $CG/memory.failcnt) oom_control $(grep -E 'oom_kill ' $CG/memory.oom_control 2>/dev/null)"
  echo "memory.stat rss $(awk '$1=="rss"{print $2}' $CG/memory.stat) cache $(awk '$1=="cache"{print $2}' $CG/memory.stat)"
  echo "sidecar VmHWM $(awk '/VmHWM/{print $2" kB"}' /proc/$P/status 2>/dev/null)"
  echo "log lines with killed/oom/panic: $(grep -ciE 'killed|out of memory|oom|panic' $BB/mirror.log)"
  grep -iE "compact|table halted|ERROR" $BB/mirror.log | tail -n 4 | cut -c1-200
  ;;
*) echo "usage: $0 sync|workload|drain|status" >&2; exit 2 ;;
esac
