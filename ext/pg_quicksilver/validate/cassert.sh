#!/usr/bin/env bash
# Everything again, on a PostgreSQL built with --enable-cassert.
#
#   CA=/path/to/prefix validate/cassert.sh
#
# CA is the prefix of a PostgreSQL 17 configured with --enable-cassert
# (docs/40 has the configure line). The stock pg_duckdb package is copied into
# it: the package is not a cassert build, but it loads, and every assertion in
# the server and in this extension is live. The extension is built against
# that prefix in a scratch copy of the source.
#
# Fails if the server log has a TRAP, a signal or a PANIC, or any WARNING other
# than the fuzz's own stray COMMIT/ROLLBACK.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
CA=${CA:?the prefix of an --enable-cassert PostgreSQL 17}
STOCK=${STOCK:-/usr/lib/postgresql/17}
export PG=$CA/bin BASE=${BASE:-/var/lib/postgresql/qsext/ca} PPORT=${PPORT:-5544} SPORT=${SPORT:-5545} HPORT=${HPORT:-5546} PUBLISH_WAIT=1
N=${N:-4000}; SEED=${SEED:-3}
[ "$($PG/pg_config --configure | grep -c enable-cassert)" = 1 ] || { echo "$CA is not a cassert build" >&2; exit 1; }
cp "$STOCK/lib/pg_duckdb.so" "$STOCK/lib/libduckdb.so" "$CA/lib/" || exit 1
cp /usr/share/postgresql/17/extension/pg_duckdb* "$CA/share/extension/" || exit 1
B=/var/lib/postgresql/qsext/cabuild
rm -rf "$B"; mkdir -p "$B"
cp -r "$HERE"/../{pg_quicksilver.c,pg_quicksilver.control,pg_quicksilver--0.1.sql,Makefile,sql,expected} "$B/"
( cd "$B" && make PG_CONFIG=$PG/pg_config with_llvm=no >/dev/null && make PG_CONFIG=$PG/pg_config with_llvm=no install >/dev/null ) || exit 1
chown -R postgres:postgres "$B"
"$HERE/setup.sh" >/dev/null || exit 1
. "$HERE/env.sh"
echo "debug_assertions = $(psq $PPORT 'SHOW debug_assertions')"
rc=0
su postgres -c "cd $B && make PG_CONFIG=$PG/pg_config with_llvm=no installcheck PPORT=$PPORT 2>&1" | grep -E "^not ok|tests (passed|failed)" || rc=1
python3 "$HERE/equality.py" --port $PPORT --out "$BASE/equality.json" | grep -E '"(statements|routed_to_mirror|routed_different|heap_different)"' | tr -d '\n'; echo
python3 "$HERE/equality.py" --port $PPORT --out "$BASE/equality2.json" --set quicksilver.tolerate_lost_errors=on | grep -E '"(statements|routed_to_mirror|routed_different|heap_different)"' | tr -d '\n'; echo
python3 "$HERE/psycopg_check.py" --port $PPORT 2>/dev/null | tail -1
"$HERE/standby_check.sh" | grep -c '^ok' | sed 's/$/ standby checks ok/'
python3 "$HERE/fuzz.py" --port $PPORT --n $N --seed $SEED --log "$BASE/primary.log" | grep -vE "no transaction in progress|^ +[0-9]+ +[0-9A-Z?]"
for l in primary standby; do
  crash=$(grep -cE "TRAP:|terminated by signal|PANIC:" "$BASE/$l.log")
  warn=$(grep "WARNING:" "$BASE/$l.log" | grep -vc "there is no transaction in progress")
  echo "$l.log: $crash crash lines, $warn other WARNING lines, $(grep -c 'ERROR:' "$BASE/$l.log") ERROR lines"
  [ "$crash" = 0 ] && [ "$warn" = 0 ] || rc=1
done
"$HERE/teardown.sh"; rm -rf "$B"
exit $rc
