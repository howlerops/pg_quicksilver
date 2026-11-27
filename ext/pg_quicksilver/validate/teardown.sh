#!/usr/bin/env bash
# Stops everything setup.sh started (by its own data directory) and removes it.
. "$(dirname "$0")/env.sh"
pkill -f "^$BASE/qs-mirror\$" 2>/dev/null
for d in standby primary; do
  [ -d "$BASE/$d" ] && su postgres -c "$PG/pg_ctl -D $BASE/$d -m immediate stop" >/dev/null 2>&1
done
pkill -f "postgres -D $BASE/" 2>/dev/null
sleep 1
rm -rf "$BASE"
exit 0
