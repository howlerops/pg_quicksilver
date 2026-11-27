#!/usr/bin/env bash
# A second standby with its own sidecar, next to the one setup.sh made (JOIN_DOMAINS=on),
# for joinnodes.py. Node 2 has its own physical slot and its own logical slot and publication
# (the sidecar makes those), and its domain cap is small (QS_JOIN_DOMAIN_MAX_KEYS=5): its
# dictionary trips while the tables load, so node 2 stays incomplete for the domain and node 1
# stays complete.
#   S2 (default 5623) and H2 (5624) are its PostgreSQL and health ports; B2 its directory.
# NOT YET SHOWN: two live sidecars on one host. quicksilver.lakes holds one catalog path
# per cluster, and the last sidecar to sync writes its own; node 2 then cannot find node 1's
# schemas (or the reverse). joinnodes.py was run with node 1 alone; see the report.
set -uo pipefail
. "$(dirname "$0")/env.sh"
S2=${S2:-5623}; H2=${H2:-5624}; B2=${B2:-$BASE/node2}
die() { echo "FAIL: $*" >&2; exit 1; }
psq $PPORT "SELECT pg_drop_replication_slot('qsint_standby2') FROM pg_replication_slots WHERE slot_name = 'qsint_standby2'" postgres >/dev/null 2>&1
psq $PPORT "SELECT pg_create_physical_replication_slot('qsint_standby2', true)" postgres >/dev/null 2>&1 || die "physical slot 2"
rm -rf "$B2"
su postgres -c "$PG/pg_basebackup -D $B2 -R -X stream -S qsint_standby2 -c fast -d 'host=/tmp port=$PPORT user=postgres dbname=postgres'" >/dev/null 2>&1 || die basebackup2
{ echo "port = $S2"; echo "hot_standby_feedback = on"; echo "sync_replication_slots = on"; } >> "$B2/postgresql.conf"
su postgres -c "$PG/pg_ctl -D $B2 -l $B2.log -w start" >/dev/null 2>&1 || { tail "$B2.log"; die "standby 2 start"; }
T=public.orders,public.customers,public.qs_types$(for t in $EX_TABLES; do printf ',ex.%s' $t; done)
cp "$BASE/qs-mirror" "$B2/qs-mirror" && chown postgres:postgres "$B2/qs-mirror"
mkdir -p "$B2/mirror" && chown postgres:postgres "$B2/mirror"
su postgres -c "QS_CLUSTER=qsint QS_MODE=shadow QS_INGEST=logical QS_DUCKLAKE=on \
  QS_DUCKLAKE_RETENTION=60s QS_SERVE_WITH_HALTED_TABLES=on \
  QS_TABLES=$T QS_SLOT=qs_int2 QS_PUBLICATION=qs_int2 \
  QS_MIRROR_PATH=$B2/mirror QS_FRESHNESS_SLO=30s \
  QS_JOIN_DOMAINS='cust=ex.jcust.id,ex.jord.cust' QS_JOIN_DOMAIN_MAX_KEYS=5 \
  QS_PRIMARY_HOST=localhost QS_PRIMARY_PORT=$PPORT \
  QS_LOCAL_SOCKET_DIR=/tmp QS_LOCAL_PORT=$S2 \
  QS_DATABASE=$DB QS_PGUSER=postgres QS_POD_NAME=qsint-node2 \
  QS_HEALTH_ADDR=127.0.0.1:$H2 setsid $B2/qs-mirror >> $B2/mirror.log 2>&1 & echo \$! > $B2/mirror.pid"
for _ in $(seq 1 600); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$H2/readyz" 2>/dev/null)" = 200 ] && { echo "node2 :$S2 ready"; exit 0; }
  sleep 0.5
done
tail -20 "$B2/mirror.log"; die "sidecar 2 never ready"
