#!/usr/bin/env bash
# The real path on a PostgreSQL built with --enable-cassert.
#
#   CA=/path/to/prefix validate/real/cassert.sh [seconds of concurrency, default 300]
#
# CA is the prefix of a PostgreSQL 17 configured with --enable-cassert
# (./configure --enable-cassert --enable-debug --with-icu --with-openssl
# --with-lz4 --with-zstd). The stock pg_duckdb package is copied into it (it is
# not a cassert build, but it loads, and every assertion in the server and in
# pg_quicksilver is live); the extension is built against the prefix in a
# scratch copy of the source.
#
# Runs the regression tests (errors.sql forces the mirror to fail under kept
# plans, cursors and transaction blocks) and then concurrency.py, and fails on
# a TRAP, a signal, a PANIC or a leaked-resource WARNING in either server log.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
CA=${CA:?the prefix of an --enable-cassert PostgreSQL 17}
STOCK=${STOCK:-/usr/lib/postgresql/17}
SECS=${1:-300}
export PG=$CA/bin
[ "$($PG/pg_config --configure | grep -c enable-cassert)" = 1 ] || { echo "$CA is not a cassert build" >&2; exit 1; }
cp "$STOCK/lib/pg_duckdb.so" "$STOCK/lib/libduckdb.so" "$CA/lib/" || exit 1
cp /usr/share/postgresql/17/extension/pg_duckdb* "$CA/share/extension/" || exit 1
B=/var/lib/postgresql/qsint-cabuild
rm -rf "$B"; mkdir -p "$B"
cp -r "$HERE"/../../{pg_quicksilver.c,pg_quicksilver.control,pg_quicksilver--0.1.sql,Makefile,sql,expected} "$B/"
( cd "$B" && make PG_CONFIG=$PG/pg_config with_llvm=no >/dev/null && make PG_CONFIG=$PG/pg_config with_llvm=no install >/dev/null ) || exit 1
chown -R postgres:postgres "$B"
DB_ROUTE=none "$HERE/setup.sh" >/dev/null || exit 1
. "$HERE/env.sh"
echo "debug_assertions = $(psq $PPORT 'SHOW debug_assertions') (primary), $(psq $SPORT 'SHOW debug_assertions') (standby)"
rc=0
"$HERE/primary_report.sh" >/dev/null || rc=1
su postgres -c "cd $B && make PG_CONFIG=$PG/pg_config with_llvm=no installcheck PPORT=$PPORT 2>&1" | grep -E "^not ok|tests (passed|failed)" || rc=1
[ -s "$B/regression.diffs" ] && { head -40 "$B/regression.diffs"; rc=1; }
# primary_report.sh vouched for the lake on the primary so that the tests could
# route there. That is over: the primary has no mirror, as in production.
psq $PPORT "SELECT count(quicksilver.report_applied_lsn(heap, '0/0')::text) FROM quicksilver.mirrors" >/dev/null
psq $PPORT "ALTER DATABASE $DB SET quicksilver.route = auto" >/dev/null
python3 "$HERE/concurrency.py" --seconds "$SECS" || rc=1
for l in primary standby; do
  crash=$(grep -cE "TRAP:|terminated by signal|PANIC:" "$BASE/$l.log")
  leak=$(grep -E "WARNING:" "$BASE/$l.log" | grep -ciE "leak|still (held|referenced|open)|not closed|resource was not")
  echo "$l.log: $crash crash lines, $leak leaked-resource warnings, $(grep -c 'WARNING:' "$BASE/$l.log") WARNING lines in all, $(grep -c 'ERROR:' "$BASE/$l.log") ERROR lines"
  [ "$crash" = 0 ] && [ "$leak" = 0 ] || rc=1
done
rm -rf "$B"
exit $rc
