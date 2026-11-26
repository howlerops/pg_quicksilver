#!/usr/bin/env bash
# After `make install`: restart the primary on the new library, recreate the
# extension (so the seed allowlist is the file's) and republish the mirror.
set -uo pipefail
. "$(dirname "$0")/env.sh"
psq $PPORT "DROP EXTENSION IF EXISTS pg_quicksilver CASCADE" >/dev/null
su postgres -c "$PG/pg_ctl -D $BASE/primary -l $BASE/primary.log -w restart" >/dev/null || die restart
for _ in $(seq 1 30); do psq $PPORT "SELECT 1" >/dev/null 2>&1 && break; sleep 1; done
psq $PPORT "CREATE EXTENSION pg_quicksilver" >/dev/null || die "CREATE EXTENSION"
# the standby loaded the old library too
su postgres -c "$PG/pg_ctl -D $BASE/standby -l $BASE/standby.log -w restart" >/dev/null || die "standby restart"
PUBLISH_WAIT=${PUBLISH_WAIT:-1} "$HERE/publish.sh" 2>&1 | grep -v "deltas still pending"
psq $PPORT "SELECT heap, usable, fresh, coalesce(reason, unusable_columns, '') FROM quicksilver.mirror_status() ORDER BY 1"
psq $PPORT "SELECT * FROM quicksilver.allowlist_problems()"
