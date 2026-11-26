# Sourced by the scenario scripts after env.sh: the twelve tables of Part 1 and sidecar helpers.
ALLT=public.orders,public.customers,public.qs_types,ex.t_num,ex.t_str,ex.t_time,ex.t_doc,ex.customers,ex.orders,fv.events,fv.accounts,fv.lookup
export QS_TABLES_OVERRIDE=$ALLT
REPO=$(cd "$FV/../.." && pwd)
# sidecar_start / sidecar_stop / sidecar_kill9: the repo's sidecar.sh, with this run's tables
sidecar_start() { READY_TRIES=1200 bash $REPO/ext/pg_quicksilver/validate/real/sidecar.sh start; }
sidecar_stop() { bash $REPO/ext/pg_quicksilver/validate/real/sidecar.sh stop; }
sidecar_kill9() { bash $REPO/ext/pg_quicksilver/validate/real/sidecar.sh kill9; }
# wait_ready: /readyz answers 200 within 120 s
wait_ready() { for _ in $(seq 1200); do [ "$(curl -s -o /dev/null -w '%{http_code}' http://$HEALTH/readyz)" = 200 ] && return 0; sleep 0.1; done; return 1; }
