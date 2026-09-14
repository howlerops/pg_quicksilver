#!/usr/bin/env bash
# Empirically validate the physical-WAL claims in docs/03-wal-ingestion.md.
#
# Controlled experiment: identical INSERT / UPDATE(one small column) / DELETE
# against a NARROW row (~30 B payload) and a WIDE row (~2000 B payload).
# If a WAL record carries the row, its size tracks row width. If it doesn't,
# the record size is identical for both.
#
# Usage: wal_evidence.sh [outdir]
set -euo pipefail
PSQL="psql -h /tmp -p 5433 -U postgres -X -q"
PGB=/usr/lib/postgresql/16/bin
BASE=/var/lib/postgresql/qsbench
OUT=${1:-$BASE/wal_evidence}; mkdir -p "$OUT"

restart_with () {   # $1 = wal_level
  sed -i "s/^wal_level = .*/wal_level = $1/" $BASE/pgdata/postgresql.conf
  su postgres -c "$PGB/pg_ctl -D $BASE/pgdata -l $BASE/pg.log -w restart" >/dev/null 2>&1
}

run_case () {       # $1 = label
  $PSQL -tAc "SELECT pg_switch_wal()" >/dev/null; $PSQL -tAc "CHECKPOINT" >/dev/null
  local s; s=$($PSQL -tAc "SELECT pg_current_wal_lsn()")
  $PSQL -tAc "INSERT INTO walcase VALUES (1, repeat('n',30),  'a')" >/dev/null
  $PSQL -tAc "INSERT INTO walcase VALUES (2, repeat('w',2000),'a')" >/dev/null
  $PSQL -tAc "UPDATE walcase SET note='b' WHERE id=1" >/dev/null
  $PSQL -tAc "UPDATE walcase SET note='b' WHERE id=2" >/dev/null
  $PSQL -tAc "DELETE FROM walcase WHERE id=1" >/dev/null
  $PSQL -tAc "DELETE FROM walcase WHERE id=2" >/dev/null
  local e; e=$($PSQL -tAc "SELECT pg_current_wal_lsn()")
  echo "### $1"
  echo "    total WAL for the 6 statements: $($PSQL -tAc "SELECT pg_size_pretty(('$e'::pg_lsn - '$s'::pg_lsn))")"
  su postgres -c "$PGB/pg_waldump -p $BASE/pgdata/pg_wal -s $s -e $e 2>/dev/null" \
    | grep -E 'rmgr: Heap ' | grep -Ev 'INPLACE|LOCK|FREEZE' \
    | sed -E 's/^rmgr: Heap +len \(rec\/tot\): +([0-9]+)\/ *([0-9]+).*desc: ([A-Z_+]+).*/    record=\3  size=\1B/' || true
  echo
}

setup_table () {
  $PSQL -tAc "DROP TABLE IF EXISTS walcase" >/dev/null 2>&1
  $PSQL -tAc "CREATE TABLE walcase(id int primary key, pad text, note text) WITH (fillfactor=50)" >/dev/null
  $PSQL -tAc "ALTER TABLE walcase ALTER COLUMN pad SET STORAGE PLAIN" >/dev/null
}

{
echo "================================================================"
echo "WAL record evidence — PostgreSQL $($PSQL -tAc 'SHOW server_version' | cut -d' ' -f1)"
echo "Narrow row ~30 B payload | Wide row ~2000 B payload (67x wider)"
echo "Identical ops on both. Record order per case: INSERT(narrow),"
echo "INSERT(wide), UPDATE(narrow), UPDATE(wide), DELETE(narrow), DELETE(wide)"
echo "================================================================"; echo

restart_with replica; setup_table
run_case "CASE 1 — wal_level=replica, REPLICA IDENTITY DEFAULT   [the physical-WAL case]"

restart_with logical; setup_table
run_case "CASE 2 — wal_level=logical, REPLICA IDENTITY DEFAULT   [standard logical replication]"

setup_table; $PSQL -tAc "ALTER TABLE walcase REPLICA IDENTITY FULL" >/dev/null
run_case "CASE 3 — wal_level=logical, REPLICA IDENTITY FULL      [required for tables with no PK]"

restart_with replica
} | tee "$OUT/wal_evidence.txt"
