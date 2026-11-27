#!/usr/bin/env bash
# wait_replay.sh: returns when the standby (5861) has replayed everything the primary (5860)
# had written at the time of the call, and then until quicksilver.mirror_status() on the standby
# shows every mirror fresh (MIRROR_WAIT_SKIP=1 skips that half). Runs as root (through su) or as
# the postgres user itself (then psql is called directly, as it is from a psql \! command).
set -uo pipefail
PG=${PG:-/var/lib/postgresql/qsexp/pg17/usr/lib/postgresql/17/bin}
q() {
  if [ "$(id -u)" = 0 ]; then su postgres -c "$PG/psql -X -Atc \"$2\" -h /tmp -p $1 -U postgres -d app" 2>&1
  else "$PG/psql" -X -Atc "$2" -h /tmp -p "$1" -U postgres -d app 2>&1; fi
}
want=$(q 5860 "SELECT pg_current_wal_lsn()")
for _ in $(seq 1 600); do
  [ "$(q 5861 "SELECT pg_last_wal_replay_lsn() >= '$want'::pg_lsn")" = t ] && break
  sleep 0.1
done
[ "${MIRROR_WAIT_SKIP:-0}" = 1 ] && { echo "replayed to $want"; exit 0; }
for _ in $(seq 1 600); do
  n=$(q 5861 "SELECT count(*) FROM quicksilver.mirror_status() WHERE NOT fresh")
  [ "$n" = 0 ] && { echo "replayed to $want; all mirrors fresh"; exit 0; }
  sleep 0.1
done
echo "TIMEOUT: not fresh: $(q 5861 "SELECT string_agg(heap || ' ' || coalesce(stale_because,''), '; ') FROM quicksilver.mirror_status() WHERE NOT fresh")"; exit 1
