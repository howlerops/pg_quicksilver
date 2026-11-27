#!/usr/bin/env bash
# Stops everything setup.sh started and removes its data.
BASE=${BASE:-/var/lib/postgresql/qsrouter}
PG=/usr/lib/postgresql/17/bin
pkill -f "^$BASE/qs-router " 2>/dev/null
pkill -f "^$BASE/qs-mirror\$" 2>/dev/null   # only this cluster's
for d in standby primary; do
  [ -d "$BASE/$d" ] && su postgres -c "$PG/pg_ctl -D $BASE/$d -m immediate stop" >/dev/null 2>&1
done
# A postmaster whose data directory was removed under it does not answer pg_ctl.
pkill -f "postgres -D $BASE/" 2>/dev/null
sleep 1
rm -rf "$BASE"
exit 0
