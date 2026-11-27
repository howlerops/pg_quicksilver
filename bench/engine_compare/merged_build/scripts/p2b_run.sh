#!/bin/bash
# part 2, continued after p2_run.sh hung on `qs_catchup.py | tail -1` (the RSS sampler it spawns kept the pipe open;
# the drain itself had finished and qs_catchup_catchup_new.json was written). The idle rewrite had therefore already
# landed before "B immediately after the workload" could be timed for the merged build, so a SECOND workload round
# (re-staged from the current table, same shape) supplies that number.
set -u
Q=/tmp/qsb; S=$Q/scripts_merged; R=$Q/results_merged; PG=/usr/lib/postgresql/17/bin
say() { echo "=== $(date -u +%T) $*"; }
bin() { [ "$1" = old ] && echo $Q/bin || echo $Q/bin_merged; }
start() { QS_M=$Q/mirror_$1 QS_BIN=$(bin $1) bash $S/qs_start.sh $2 >/dev/null 2>&1 </dev/null; }
stop() { local p; p=$(cat $Q/qs.pid); kill $p 2>/dev/null; for i in $(seq 300); do kill -0 $p 2>/dev/null || break; sleep 0.1; done; sleep 1; }
qbench() { QS_M=$Q/mirror_$1 QS_BIN=$(bin $1) RESULT_TAG=_$3 VIEW_TAG=$2_$3 taskset -c 2-3 python3 -W ignore $S/bench.py --engine qs --state $2 --mode warm > $R/_o.txt 2>&1; grep -v Deprecation $R/_o.txt; }
pgb() { taskset -c 2-3 python3 -W ignore $S/bench.py --engine pg --state $1 --mode warm > $R/_o.txt 2>&1; grep -v Deprecation $R/_o.txt; }
hwm() { awk '{for(i=1;i<=NF;i++) if($i=="VmHWM:" && $(i+1)>m) m=$(i+1)} END{printf "%.0f MB", m/1024}' $R/qs_rss_$1.log; }
lag() { QS_M=$Q/mirror_$1 QS_TAG=$2 taskset -c 2-3 python3 -W ignore $S/live_lag.py qs $2 > $R/_o.txt 2>&1; tail -1 $R/_o.txt; }
catchup() { QS_M=$Q/mirror_$1 QS_BIN=$(bin $1) python3 -W ignore $S/qs_catchup.py $2 > $R/_c.txt 2>&1 </dev/null; tail -1 $R/_c.txt; }
verify() { ( time taskset -c 2-3 $Q/bin_merged/qs-verify -dsn postgres://postgres@localhost:5440/app -mirror $Q/mirror_new -table public.$1 ) > $R/_o.txt 2>&1; grep -v '^$\|user\|sys' $R/_o.txt | cut -c1-200 | head -8; }
psq() { su postgres -c "$PG/psql -X -h $Q/sock -p 5440 -d app -Atc \"$1\""; }

say "new sidecar, quiet since 19:30:55: readyz"; curl -s -w ' HTTP %{http_code}\n' http://127.0.0.1:9199/readyz
cat $R/qs_catchup_catchup_new.json | tr -d '\n'; echo; echo "peak RSS drain+idle rewrite (new): $(hwm catchup_new)"
say "qs B warm, new, AFTER the idle rewrite (PG still at state B)"; qbench new B new_idle
say "qs-verify, new, state B"; verify orders; verify customers
stop
say "drain, old build"; catchup old catchup_old; echo "peak RSS drain (old): $(hwm catchup_old)"
say "qs B warm, old"; qbench old B old
say "live lag, old"; sleep 3; lag old old_run1; sleep 3; lag old old_run2
grep -v "slow tick\|delivering nothing" $R/qs_mirror_catchup_old.log | cut -c1-220 | tail -4
stop
say "live lag, new"; start new lag_new; bash $S/qs_wait_ready.sh lag_new; sleep 8; lag new new_run1; sleep 3; lag new new_run2
grep -v "slow tick\|delivering nothing" $R/qs_mirror_lag_new.log | cut -c1-220 | tail -4
stop
say "second workload round: re-stage picks from the current table"
cp $R/workload.json $R/workload_round1.json; cp $R/lookup_id.txt $R/lookup_id_round1.txt
psq "DROP SCHEMA wl CASCADE" >/dev/null 2>&1
su postgres -c "taskset -c 2-3 $PG/psql -X -h $Q/sock -p 5440 -d app -f $S/staging.sql" > $R/pg_staging2.log 2>&1
taskset -c 2-3 python3 -W ignore $S/workload.py > $R/_o.txt 2>&1; cut -c1-500 $R/_o.txt
say "pg B2"; pgb B2
say "drain round 2, new build (QS_COMPACT_IDLE_SECONDS=120 so the timed queries do not overlap the rewrite)"
export QS_COMPACT_IDLE_SECONDS=120; catchup new catchup2_new
say "qs B2 warm, new, immediately after the drain (before any rewrite)"; qbench new B2 new
grep -c 'compaction started' $R/qs_mirror_catchup2_new.log
say "waiting for the idle rewrite"
# shellcheck disable=SC2034  # not read here: a loop counter, or read by a script that sources this
for i in $(seq 1800); do grep -q 'msg=compacted table=public.orders' $R/qs_mirror_catchup2_new.log && break; sleep 0.5; done
grep 'compaction started\|msg=compacted\|merged deltas' $R/qs_mirror_catchup2_new.log | cut -c1-220
say "qs B2 warm, new, after the idle rewrite"; qbench new B2 new_idle
echo "peak RSS drain2+rewrite (new): $(hwm catchup2_new)"
say "qs-verify, new, state B2"; verify orders
stop
say "P2B_DONE"
