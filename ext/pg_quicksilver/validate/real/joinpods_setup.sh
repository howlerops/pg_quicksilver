#!/usr/bin/env bash
# Node 2 of a two-node pod pair, for the join-completeness check (item 5 of the merged-tree
# validation). Run it inside node 2's mount namespace, with node 2's mirror bind-mounted over
# $MIRROR (the caller does that): both sidecars then use the SAME mirror path, as two pods do,
# and each sees its own directory. Node 1 (the setup.sh standby + sidecar, in node 1's
# namespace) is the complete one; node 2's domain cap is 5 keys, so its dictionary trips while
# the tables load and its tables stay incomplete for the domain.
#   BASE, PPORT (primary), S2 (standby 2 port), H2 (health port 2), D2 (standby 2 directory)
set -uo pipefail
. "$(dirname "$0")/env.sh"
S2=${S2:-5853}; H2=${H2:-5854}; D2=${D2:-$BASE/nodeB/standby}
die() { echo "FAIL: $*" >&2; exit 1; }
psq $PPORT "SELECT pg_drop_replication_slot('pod_standby2') FROM pg_replication_slots WHERE slot_name = 'pod_standby2'" postgres >/dev/null 2>&1
psq $PPORT "SELECT pg_create_physical_replication_slot('pod_standby2', true)" postgres >/dev/null 2>&1 || die "physical slot 2"
rm -rf "$D2"; mkdir -p "$(dirname "$D2")"
su postgres -c "$PG/pg_basebackup -D $D2 -R -X stream -S pod_standby2 -c fast -d 'host=/tmp port=$PPORT user=postgres dbname=postgres'" >/dev/null 2>&1 || die basebackup2
{ echo "port = $S2"; echo "hot_standby_feedback = on"; echo "sync_replication_slots = on"; } >> "$D2/postgresql.conf"
chown -R postgres:postgres "$D2"
su postgres -c "$PG/pg_ctl -D $D2 -l $D2.log -w start" >/dev/null 2>&1 || { tail "$D2.log"; die "standby 2 start"; }
T=public.orders,public.customers,public.qs_types$(for t in $EX_TABLES; do printf ',ex.%s' $t; done)
su postgres -c "QS_CLUSTER=qsint QS_MODE=shadow QS_INGEST=logical QS_DUCKLAKE=on \
  QS_DUCKLAKE_RETENTION=60s QS_SERVE_WITH_HALTED_TABLES=on \
  QS_TABLES=$T QS_SLOT=qs_int2 QS_PUBLICATION=qs_int2 \
  QS_MIRROR_PATH=$MIRROR QS_FRESHNESS_SLO=30s \
  QS_JOIN_DOMAINS='cust=ex.jcust.id,ex.jord.cust' QS_JOIN_DOMAIN_MAX_KEYS=5 \
  QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$PPORT \
  QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=$S2 \
  QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=qsint-node2 \
  QS_HEALTH_ADDR=127.0.0.1:$H2 setsid $BASE/qs-mirror >> $D2.sidecar.log 2>&1 & echo \$! > $D2.sidecar.pid"
for _ in $(seq 1 600); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$H2/readyz" 2>/dev/null)" = 200 ] && { echo "node2 standby :$S2 sidecar ready"; exit 0; }
  sleep 0.5
done
tail -20 "$D2.sidecar.log"; die "sidecar 2 never ready"
