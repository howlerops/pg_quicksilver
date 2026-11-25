#!/bin/bash
# cold.sh <engine> <state>: one cold run per query; engine restarted and page cache dropped before each
Q=/tmp/qsb; PG=/usr/lib/postgresql/17/bin; e=$1; st=$2
for q in count_by_status revenue_by_month_join pk_lookup count_star sum_amount month_filter_agg; do
  case $e in
    pg) su postgres -c "taskset -c 0-2 $PG/pg_ctl -D $Q/pg/primary -l $Q/pg/primary.log -w restart" >/dev/null; python3 -W ignore $Q/scripts/probe.py pg; sleep 2;;
    mason) $Q/scripts/mason_ctl.sh stop; $Q/scripts/mason_ctl.sh start; python3 -W ignore $Q/scripts/probe.py mason; sleep 2;;
  esac
  sync; echo 3 > /proc/sys/vm/drop_caches
  taskset -c 0-2 python3 -W ignore $Q/scripts/bench.py --engine $e --state $st --mode cold --only $q 2>&1 | grep -v '"at"'
done
