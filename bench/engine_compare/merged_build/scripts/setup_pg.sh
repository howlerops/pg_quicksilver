#!/bin/bash
set -u; . "$(dirname "$0")/lib.sh"
install -d -o postgres -g postgres $BASE $BASE/sock
su postgres -c "$PG/initdb -D $BASE/primary -U postgres -A trust --locale=C.UTF-8 -E UTF8" >/dev/null
cat >> $BASE/primary/postgresql.conf <<C
port = $PP
listen_addresses = 'localhost'
unix_socket_directories = '$BASE/sock'
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
shared_buffers = 256MB
max_wal_size = 4GB
C
su postgres -c "taskset -c 2-3 $PG/pg_ctl -D $BASE/primary -l $BASE/primary.log -w start" >/dev/null
su postgres -c "$PG/pg_basebackup -h $BASE/sock -p $PP -U postgres -D $BASE/standby -R -X stream"
echo "port = $SP" >> $BASE/standby/postgresql.conf
echo "shared_buffers = 32MB" >> $BASE/standby/postgresql.conf
su postgres -c "taskset -c 2-3 $PG/pg_ctl -D $BASE/standby -l $BASE/standby.log -w start" >/dev/null
psq "CREATE DATABASE $DB" $PP postgres
psq "SELECT pg_is_in_recovery()" $SP postgres
su postgres -c "$PG/psql -X -q -h $BASE/sock -p $PP -d $DB" < $E/scripts/load_small.sql
psq "select count(*), count(amount), count(payload), count(note) from items"
