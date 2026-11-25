#!/bin/bash
# usage: qs_start.sh <tag>   starts the sidecar pinned to cpus 0-2, samples RSS
Q=/tmp/qsb; tag=$1
QS_CLUSTER=bench QS_MODE=shadow QS_INGEST=logical QS_TABLES=public.orders,public.customers \
 QS_SLOT=qs_bench QS_PUBLICATION=qs_bench QS_MIRROR_PATH=$Q/mirror QS_FRESHNESS_SLO=15s \
 QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=5440 QS_LOCAL_SOCKET_DIR=$Q/sock QS_LOCAL_PORT=5441 \
 QS_DATABASE=app QS_PGUSER=postgres QS_POD_NAME=bench-1 QS_HEALTH_ADDR=127.0.0.1:9199 \
 setsid nohup taskset -c 0-2 $Q/bin/qs-mirror >> $Q/results/qs_mirror_$tag.log 2>&1 &
pid=$!
echo $pid > $Q/qs.pid
date +%s.%N > $Q/results/qs_start_$tag.ts
( while kill -0 $pid 2>/dev/null; do echo "$(date +%s.%N) $(grep -E 'VmRSS|VmHWM' /proc/$pid/status 2>/dev/null | awk '{printf "%s %s ", $1, $2}')"; sleep 0.5; done > $Q/results/qs_rss_$tag.log ) &
echo started $pid
