#!/usr/bin/env bash
# For the regression tests only. They create and register tables, so they run
# on the PRIMARY, and in production the primary has no mirror: the sidecar
# reports applied LSNs to the instance it sits next to, and on a primary it
# stands down. In this one-host cluster the lake's files are on the same
# filesystem, so the primary can read them once somebody vouches for an LSN.
# This does, after waiting for the mirror to have applied everything, and it is
# true only while nothing writes to the mirrored tables: which the tests do not.
set -uo pipefail
. "$(dirname "$0")/env.sh"
want=$(psq $PPORT "SELECT pg_current_wal_lsn()")
for _ in $(seq 1 300); do
  n=$(psq $SPORT "SELECT count(*) FROM quicksilver.mirror_status() WHERE applied_lsn IS NULL OR applied_lsn < '$want'")
  [ "$n" = 0 ] && break
  sleep 0.2
done
[ "$n" = 0 ] || die "the mirror never reached $want"
psq $PPORT "SELECT count(quicksilver.report_applied_lsn(heap, pg_current_wal_insert_lsn() + 1000000000)::text) FROM quicksilver.mirrors" >/dev/null || die report
psq $PPORT "SELECT count(*) || ' tables reported on the primary' FROM quicksilver.mirror_status() WHERE fresh"
