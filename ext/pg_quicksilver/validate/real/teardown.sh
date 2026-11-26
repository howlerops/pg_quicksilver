#!/usr/bin/env bash
# Stops what setup.sh started, by PID, and removes it.
. "$(dirname "$0")/env.sh"
[ -f "$BASE/mirror.pid" ] && kill "$(cat "$BASE/mirror.pid")" 2>/dev/null
for d in standby primary; do
  [ -d "$BASE/$d" ] && su postgres -c "$PG/pg_ctl -D $BASE/$d -m immediate stop" >/dev/null 2>&1
done
sleep 1
case "$BASE" in /var/lib/postgresql/qsint*) rm -rf "$BASE" ;; esac
exit 0
