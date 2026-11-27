#!/usr/bin/env bash
# bench_sidecar.sh start|stop|drain LSN: the benchmark cluster's sidecar (same settings as bench_setup.sh), pinned.
#   start : starts the sidecar, prints the seconds from start to /readyz 200 and the RSS at ready
#   stop  : TERM to the recorded PID and waits for it to exit
#   drain LSN : from now, seconds until the mirror's applied LSN is at or past LSN (sidecar must be running)
. "$(dirname "$0")/env.sh"; . "$(dirname "$0")/run.sh"
BB=$BASE/bench; CPUS=${CPUS:-0-2}; BSTBY=5821; BHEALTH=5822; BPORT=5820
case "${1:-}" in
start)
  T1=$(date +%s.%N)
  su postgres -c "env -i PATH=/usr/bin:/bin QS_CLUSTER=qsbench QS_MODE=shadow QS_INGEST=logical QS_DUCKLAKE=on QS_DUCKLAKE_RETENTION=60s QS_SERVE_WITH_HALTED_TABLES=on QS_TABLES=public.orders,public.customers QS_SLOT=qs_bench QS_PUBLICATION=qs_bench QS_MIRROR_PATH=$BB/mirror QS_FRESHNESS_SLO=30s QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$BPORT QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=$BSTBY QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=qsbench-1 QS_HEALTH_ADDR=127.0.0.1:$BHEALTH ${EXTRA_ENV:-} setsid taskset -c $CPUS $BASE/qs-mirror >> $BB/mirror.log 2>&1 & echo \$! > $BB/mirror.pid"
  if [ -z "${NOWAIT:-}" ]; then for _ in $(seq 36000); do [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$BHEALTH/readyz)" = 200 ] && break; sleep 0.1; done; fi
  T2=$(date +%s.%N)
  echo "sidecar pid $(cat $BB/mirror.pid): start to ready $(python3 -c "print(round($T2-$T1,1))") s"
  echo "VmHWM $(awk '/VmHWM/{print $2" kB"}' /proc/"$(cat $BB/mirror.pid)"/status) VmRSS $(awk '/VmRSS/{print $2" kB"}' /proc/"$(cat $BB/mirror.pid)"/status)" ;;
stop)
  P=$(cat $BB/mirror.pid)
  echo "VmHWM before stop $(awk '/VmHWM/{print $2" kB"}' /proc/$P/status 2>/dev/null)"
  kill $P 2>/dev/null
  for _ in $(seq 600); do kill -0 $P 2>/dev/null || { echo "sidecar $P exited"; exit 0; }; sleep 0.1; done
  echo "sidecar $P did not exit"; exit 1 ;;
drain)
  python3 -I - "$2" <<'PY'
import sys, time, json, urllib.request, urllib.error
target = sys.argv[1]
def n(x):
    h, l = x.split("/")
    return (int(h, 16) << 32) | int(l, 16)
t0 = time.perf_counter()
while True:
    try:
        d = json.loads(urllib.request.urlopen("http://127.0.0.1:5822/readyz", timeout=10).read())
    except urllib.error.HTTPError as e:      # 503 while behind: the body is the same JSON
        d = json.loads(e.read())
    except OSError:
        time.sleep(0.1); continue
    a = d.get("applied_lsn") or ""
    if a and n(a) >= n(target):
        break
    time.sleep(0.05)
print(f"drain: applied LSN {a} reached {target} after {time.perf_counter()-t0:.2f} s; lag_bytes {d.get('lag_bytes')}")
PY
  ;;
*) echo "usage: $0 start|stop|drain LSN" >&2; exit 2 ;;
esac
