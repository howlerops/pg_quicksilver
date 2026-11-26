# Shared by the scripts in this directory: the extension, the sidecar and the
# DuckLake mirror wired together as they are deployed. Everything lives under
# $BASE and on ports 5580-5589; teardown.sh removes all of it, by PID.
PG=${PG:-/usr/lib/postgresql/17/bin}
BASE=${BASE:-/var/lib/postgresql/qsint}
PPORT=${PPORT:-5580}; SPORT=${SPORT:-5581}; HPORT=${HPORT:-5582}
HEALTH=127.0.0.1:$HPORT
DB=app
MIRROR=$BASE/mirror
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../../../.." && pwd)
EX_TABLES="t_num t_str t_time t_doc customers orders"
# JOIN_DOMAINS=on: the two uuid-keyed tables of corpus_join.sql, mirrored with their domain (join_tables.sql)
[ "${JOIN_DOMAINS:-off}" = on ] && EX_TABLES="$EX_TABLES jcust jord"
die() { echo "FAIL: $*" >&2; exit 1; }
# psq PORT SQL [DB]
psq() { su postgres -c "$PG/psql -X -h /tmp -p $1 -U postgres -d ${3:-$DB} -v ON_ERROR_STOP=1 -Atc \"$2\"" 2>&1; }
psqf() { su postgres -c "$PG/psql -X -h /tmp -p $PPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1 $*"; }
metric() { curl -s "http://$HEALTH/metrics" | awk -v m="$1" '$1 ~ "^"m {print $2; exit}'; }
# wait until the standby has replayed everything the primary has written
replayed() {
  local want; want=$(psq $PPORT "SELECT pg_current_wal_lsn()")
  for _ in $(seq 1 300); do [ "$(psq $SPORT "SELECT pg_last_wal_replay_lsn() >= '$want'")" = t ] && return 0; sleep 0.1; done
  return 1
}
