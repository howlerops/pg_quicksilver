#!/bin/bash
# part 2 prep: bind mount, adapted copies of the harness, start PG pinned to 2-3, reload 10M rows
set -u
E=/tmp/claude-0/-home-user-pg-quicksilver/58894071-fd3a-5a32-94ab-41a0530826ff/scratchpad/e2e
B=/tmp/claude-0/-home-user-pg-quicksilver/58894071-fd3a-5a32-94ab-41a0530826ff/scratchpad/bench
PG=/usr/lib/postgresql/17/bin; Q=/tmp/qsb
mkdir -p $Q; mountpoint -q $Q || mount --bind $B $Q
mkdir -p $Q/results_merged $Q/bin_merged $Q/scripts_merged; chmod 777 $Q/results_merged
cp $E/bin/qs-* $Q/bin_merged/
S=$Q/scripts_merged
cp $Q/scripts/{bench.py,qs_start.sh,qs_wait_ready.sh,qs_catchup.py,live_lag.py,workload.py,load.sql,staging.sql} $S/
# results -> results_merged ; mirror and bin chosen per build through QS_M / QS_BIN ; pinning 0-2 -> 2-3
sed -i 's#/results#/results_merged#g; s#/scripts/#/scripts_merged/#g' $S/*
sed -i 's#\$Q/mirror#$QS_M#g; s#\$Q/bin/#$QS_BIN/#g; s#taskset -c 0-2#taskset -c 2-3#g' $S/qs_start.sh
sed -i 's#{Q}/mirror#{M}#g; s#{Q}/bin/#{BIN}/#g' $S/bench.py $S/qs_catchup.py $S/live_lag.py
sed -i '0,/^Q *= *"\/tmp\/qsb".*/s##Q = "/tmp/qsb"\nimport os as _os; M = _os.environ.get("QS_M"); BIN = _os.environ.get("QS_BIN")#' $S/bench.py
sed -i 's#^Q="/tmp/qsb"; tag=sys.argv\[1\]#Q="/tmp/qsb"; tag=sys.argv[1]; M=os.environ["QS_M"]#' $S/qs_catchup.py
sed -i 's#^Q="/tmp/qsb"; eng=sys.argv\[1\]; tagbase=sys.argv\[2\]#import os; M=os.environ["QS_M"]\nQ="/tmp/qsb"; eng=sys.argv[1]; tagbase=sys.argv[2]#' $S/live_lag.py
sed -i 's#live_lag_{eng}.json#live_lag_{eng}_{os.environ["QS_TAG"]}.json#' $S/live_lag.py
sed -i 's#SET threads = 3#SET threads = 2#' $S/bench.py
grep -n 'M\b\|BIN\|threads' $S/bench.py $S/qs_catchup.py $S/live_lag.py $S/qs_start.sh | grep -n 'QS_M\|{M}\|BIN\|threads' | cut -c1-170
su postgres -c "taskset -c 2-3 $PG/pg_ctl -D $Q/pg/primary -l $Q/pg/primary.log -w start" | tail -1
su postgres -c "taskset -c 2-3 $PG/pg_ctl -D $Q/pg/probe -l $Q/pg/probe.log -w start" | tail -1
psq() { su postgres -c "$PG/psql -X -h $Q/sock -p 5440 -d app -Atc \"$1\""; }
psq "SELECT slot_name, active, wal_status FROM pg_replication_slots"
psq "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active" >/dev/null
psq "DROP PUBLICATION IF EXISTS qs_bench" ; psq "DROP SCHEMA IF EXISTS wl CASCADE; DROP TABLE IF EXISTS public.orders, public.customers"
psq "CHECKPOINT"
t0=$(date +%s.%N)
su postgres -c "taskset -c 2-3 $PG/psql -X -h $Q/sock -p 5440 -d app -v rows=10000000 -v nc=500000 -f $S/load.sql" > $Q/results_merged/pg_load.log 2>&1
echo "load_wall_s $(echo "$(date +%s.%N) - $t0" | bc)" | tee -a $Q/results_merged/pg_load.log
su postgres -c "taskset -c 2-3 $PG/psql -X -h $Q/sock -p 5440 -d app -f $S/staging.sql" > $Q/results_merged/pg_staging.log 2>&1
tail -4 $Q/results_merged/pg_staging.log; cat $Q/results_merged/lookup_id.txt
psq "select count(*) from orders"; psq "select pg_is_in_recovery()"; echo PREP_DONE
