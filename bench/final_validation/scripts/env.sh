# Shared by the final-validation scripts. Sourced.
# The private PostgreSQL 17 tree (pg_duckdb 1.2.2 patched) and this run's ports and data.
PGTREE=${PGTREE:-/var/lib/postgresql/qspgd/pg17/usr/lib/postgresql/17}
export PG=$PGTREE/bin PG_LIBDIR=$PGTREE/lib
export LD_LIBRARY_PATH=$PGTREE/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
export BASE=${BASE:-/var/lib/postgresql/qsfinal} PPORT=${PPORT:-5800} SPORT=${SPORT:-5801} HPORT=${HPORT:-5802}
export HEALTH=127.0.0.1:$HPORT DB=app
SCRATCH=${SCRATCH:-/tmp/claude-0/-home-user-pg-quicksilver/58894071-fd3a-5a32-94ab-41a0530826ff/scratchpad}
FV=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)     # bench/final_validation
OUT=$FV/out
QSV=$BASE/qs-verify
# q PORT SQL [DB]: run SQL as postgres, tuples only.
q() { su postgres -c "LD_LIBRARY_PATH=$LD_LIBRARY_PATH $PG/psql -X -h /tmp -p $1 -U postgres -d ${3:-$DB} -v ON_ERROR_STOP=1 -Atc \"$2\"" 2>&1; }
# qf PORT FILE [DB]: run a SQL file.
qf() { su postgres -c "LD_LIBRARY_PATH=$LD_LIBRARY_PATH $PG/psql -X -h /tmp -p $1 -U postgres -d ${3:-$DB} -v ON_ERROR_STOP=1 -q -f $2" 2>&1; }
metric() { curl -s "http://$HEALTH/metrics" | awk -v m="$1" '$1 ~ "^"m {print $2; exit}'; }
# verify TABLE: qs-verify of one table against the primary, last line of output
# P, MD: the primary's port and the mirror directory (defaults: this run's primary and mirror)
verify() { timeout 900 $QSV -dsn "postgres://postgres@localhost:${P:-$PPORT}/$DB?sslmode=disable" -mirror ${MD:-$BASE/mirror} -table "$1" 2>&1 | tail -1; }
