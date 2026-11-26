#!/bin/bash
# Item 1 reset: stop the item-1 sidecar (by its recorded PID), drop its slot and
# publication, recreate database app from item1_seed.sql, wipe the mirror.
. "$(dirname "$0")/qs2_env.sh"
sc_stop item1
psq $PPORT postgres "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'qs2_item1%' AND NOT active" >/dev/null 2>&1
psq $PPORT app "DROP PUBLICATION IF EXISTS qs2_pub" >/dev/null 2>&1
psq $PPORT postgres "DROP DATABASE IF EXISTS app" >/dev/null || die "drop database"
psq $PPORT postgres "CREATE DATABASE app" >/dev/null || die "create database"
su postgres -s /bin/bash -c "$PG/psql -X -q -v ON_ERROR_STOP=1 -h $SOCK -p $PPORT -U postgres -d app -f $HERE/item1_seed.sql" || die "seed"
rm -rf "$BASE/mirror/item1"
echo "reset: primary $PPORT database app seeded, mirror wiped"
