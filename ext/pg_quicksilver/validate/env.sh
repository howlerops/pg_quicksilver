# Shared by the validate scripts. PG may point at another build (the cassert one).
PG=${PG:-/usr/lib/postgresql/17/bin}
BASE=${BASE:-/var/lib/postgresql/qsext/c}
PPORT=${PPORT:-5541}; SPORT=${SPORT:-5542}; HPORT=${HPORT:-5543}
HEALTH=127.0.0.1:$HPORT
DB=app
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
EX_TABLES="t_num t_str t_time t_doc customers orders"
die() { echo "FAIL: $*" >&2; exit 1; }
psq() { su postgres -c "$PG/psql -X -h /tmp -p $1 -U postgres -d ${3:-$DB} -v ON_ERROR_STOP=1 -Atc \"$2\"" 2>&1; }
psqf() { su postgres -c "$PG/psql -X -h /tmp -p $PPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1 $*"; }
