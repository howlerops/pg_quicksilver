#!/usr/bin/env bash
# race_mirror.sh: a deterministic reproducer of the registry load race (crash_backtrace.txt, part A).
#
#   PG=<prefix>/bin PPORT=5890 DB=app RACE_OUT=<dir> validate/real/hardening/race_mirror.sh
#
# Needs a running primary with pg_quicksilver and the customers registration (setup.sh), gdb, and
# the postgres OS user. The sidecar is stopped first (it would re-create the mirror view). A backend
# sleeps; the registry is invalidated while it sleeps; gdb stops it right after the existence check
# for customers, drops the customers mirror view from another session, forces the invalidation with
# AcceptInvalidationMessages(), and lets it go on. On the baseline build the backend dies with SIGSEGV
# in quote_identifier (qs_build_mirror); on a fixed build it answers from the heap and says so in
# quicksilver.mirror_status(). Prints the gdb output; the caller decides what is a pass.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
PG=${PG:?PG}
PPORT=${PPORT:?PPORT}
DB=${DB:-app}
OUT=${RACE_OUT:?RACE_OUT}
mkdir -p "$OUT"; chmod 777 "$OUT"
REAL=$(cd "$HERE/.." && pwd)
P="$PG/psql -X -h /tmp -p $PPORT -U postgres -d $DB -At"
su postgres -c "$P -c \"SELECT 1\"" >/dev/null || { echo "no primary on $PPORT" >&2; exit 1; }
"$REAL/sidecar.sh" stop
HEAP=$(su postgres -c "$P -c \"SELECT 'customers'::regclass::oid\"")
DROPSH=$OUT/race_drop.sh
printf '#!/bin/bash\n%s -c %s\n' "$PG/psql -X -h /tmp -p $PPORT -U postgres -d $DB" "'DROP VIEW quicksilver_mirror.\"public.customers\"' > $OUT/race_drop.txt 2>&1" > "$DROPSH"
chmod 755 "$DROPSH"
chmod 644 "$HERE/race_mirror.gdb"
su postgres -c "$P -c 'SELECT pg_sleep(4)' -c 'SELECT count(*) FROM customers'" > "$OUT/race_B.txt" 2>&1 &
sleep 1
su postgres -c "$P -c \"UPDATE quicksilver.mirrors SET max_lag_bytes = max_lag_bytes WHERE heap = 'customers'::regclass\"" >/dev/null 2>&1
PID=$(su postgres -c "$P -c \"SELECT pid FROM pg_stat_activity WHERE query LIKE 'SELECT pg_sleep%' AND datname = '$DB'\"" | head -1)
# the line after the existence check in qs_build_mirror: the lookup it made is held there
LINE=$(awk '/^qs_build_mirror\(QsMirror \*m\)/{f=1} f && /ReleaseSysCache\(tup\);/{print NR; exit}' "$HERE/../../../pg_quicksilver.c")
[ -n "$LINE" ] || { echo "no ReleaseSysCache in qs_build_mirror" >&2; exit 1; }
sed "s/HEAPOID/$HEAP/; s/LINE/$LINE/" "$HERE/race_mirror.gdb" > "$OUT/race.gdb"
RACE_DROP=$DROPSH gdb -p "$PID" -batch -x "$OUT/race.gdb" > "$OUT/race_gdb.txt" 2>&1 &
wait
grep -v "warning:\|libthread_db\|^\[Thread\|^\[Inferior" "$OUT/race_gdb.txt"
echo "--- drop: $(tail -1 "$OUT/race_drop.txt" 2>/dev/null) backend: $(cat "$OUT/race_B.txt" 2>/dev/null | tail -1)"
