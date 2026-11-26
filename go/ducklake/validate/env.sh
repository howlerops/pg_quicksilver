# Shared by the scripts in this directory. Everything they start lives under
# $BASE and on ports 5570-5572; teardown.sh removes all of it.
PG=/usr/lib/postgresql/17/bin
BASE=${BASE:-/var/lib/postgresql/qslake}
PPORT=${PPORT:-5570}; SPORT=${SPORT:-5571}; HEALTH=127.0.0.1:${HPORT:-5572}
ROWS=${ROWS:-2000000}
DB=app
MIRROR=$BASE/mirror
CATALOG=$MIRROR/_ducklake/catalog.sqlite
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
GO=$(cd "$HERE/../.." && pwd)
die() { echo "FAIL: $*" >&2; exit 1; }
psq() { su postgres -c "$PG/psql -h /tmp -p ${3:-$PPORT} -U postgres -d ${2:-$DB} -v ON_ERROR_STOP=1 -Atc \"$1\"" 2>&1; }
metric() { curl -s "http://$HEALTH/metrics" | awk -v m="$1" '$1 ~ "^"m {print $2; exit}'; }
