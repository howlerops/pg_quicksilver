#!/usr/bin/env bash
# run.sh PORT USER FILE: runs FILE with psql as the OS user postgres, connected
# as USER to database app on PORT. psql -a echoes every input line, so the
# output holds the SQL next to its result. The database is app; no output is
# filtered.
#
#   PG=<copy>/bin LD_LIBRARY_PATH=<copy>/lib hardening/run.sh 5861 app hardening/a.sql
set -uo pipefail
PG=${PG:?PG}
port=$1; user=$2; file=$3
su postgres -c "$PG/psql -X -a -h /tmp -p $port -U $user -d app -f $file" 2>&1
