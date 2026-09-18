#!/usr/bin/env bash
# Drop a database without taking the standby down with it.
#
# On PostgreSQL 17, dropping a database that has a FAILOVER logical slot which
# the standby has SYNCHRONISED kills the standby outright. Its startup process
# cannot replay the DROP while the slot-sync worker holds the synced copy:
#
#   FATAL:  replication slot "rc_slot" is active for PID 4122
#   CONTEXT:  WAL redo at 0/2A24BB20 for Database/DROP: dir 1663/16384
#   LOG:  startup process (PID 4120) exited with exit code 1
#   LOG:  shutting down due to startup process failure
#
# That is not a benchmark-only hazard. It is the exact configuration this
# project REQUIRES: failover slots plus sync_replication_slots = on are how a
# logical slot survives a failover on 17, which is why the plugin refuses to run
# on 16 (docs/14, docs/15). Every Quicksilver deployment has it. See
# docs/30-dropping-a-database-kills-the-standby.md.
#
# Reproduced deliberately and mitigated deliberately: drop the SLOT first, wait
# for the standby to forget its synced copy — two seconds, measured — and only
# then drop the database. The standby survives.
#
#   source bench/scripts/lib_dropdb.sh
#   qs_drop_database 5443 mydb            # standby assumed on 5444
#   qs_drop_database 5443 mydb 5444

QS_PGBIN=${QS_PGBIN:-/usr/lib/postgresql/17/bin}

qs_drop_database() {
  local port="$1" db="$2" standby="${3:-5444}"
  local q sq
  q()  { su postgres -c "$QS_PGBIN/psql -h /tmp -p $port -U postgres -d ${2:-postgres} -Atc \"$1\"" 2>/dev/null; }
  sq() { su postgres -c "$QS_PGBIN/psql -h /tmp -p $standby -U postgres -d postgres -Atc \"$1\"" 2>/dev/null; }

  # Nothing to do, and asking about slots on a database that is not there
  # returns nothing useful anyway.
  [ "$(q "SELECT 1 FROM pg_database WHERE datname='$db'")" = "1" ] || {
    q "DROP DATABASE IF EXISTS $db" >/dev/null
    return 0
  }

  # Every slot belonging to this database, active or not. A slot still held by
  # a walsender has to have that walsender terminated first, or the drop fails
  # and we are back to the crash this exists to prevent.
  local slots
  slots=$(q "SELECT slot_name FROM pg_replication_slots
             WHERE database = '$db'")
  local s
  for s in $slots; do
    q "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
        WHERE slot_name = '$s' AND active" >/dev/null
    local i
    for i in $(seq 30); do
      q "SELECT pg_drop_replication_slot('$s')" >/dev/null && break
      sleep 1
    done
  done

  # Then wait for the standby to forget the synced copies. Measured at two
  # seconds; thirty is given because the cost of being wrong is a dead standby
  # and the cost of waiting is a benchmark that takes a moment longer.
  if [ -n "$slots" ]; then
    local i n
    for i in $(seq 30); do
      n=$(sq "SELECT count(*) FROM pg_replication_slots WHERE database = '$db'")
      [ "${n:-0}" = "0" ] && break
      sleep 1
    done
  fi

  q "DROP DATABASE IF EXISTS $db" >/dev/null
}
