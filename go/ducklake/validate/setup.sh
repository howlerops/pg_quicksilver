#!/usr/bin/env bash
# A PostgreSQL 17 primary with pg_duckdb, a hot standby, and $ROWS rows of
# public.orders (the table the prototype measured: uuid key, numeric(12,2),
# timestamptz, status, ~200 bytes of text). The sidecar is started separately
# (sidecar.sh) so that the same cluster serves a run with QS_DUCKLAKE on and
# one with it off.
set -uo pipefail
. "$(dirname "$0")/env.sh"
[ -f "$($PG/pg_config --pkglibdir)/pg_duckdb.so" ] || die "pg_duckdb is not installed for PostgreSQL 17"
"$HERE/teardown.sh"
mkdir -p "$BASE"; chown postgres:postgres "$BASE"
su postgres -c "$PG/initdb -D $BASE/primary -A trust" >/dev/null 2>&1 || die initdb
cat >> "$BASE/primary/postgresql.conf" <<CONF
port = $PPORT
listen_addresses = 'localhost'
unix_socket_directories = '/tmp'
wal_level = logical
shared_preload_libraries = 'pg_duckdb'
shared_buffers = 512MB
max_wal_size = 2GB
CONF
su postgres -c "$PG/pg_ctl -D $BASE/primary -l $BASE/primary.log -w start" >/dev/null 2>&1 || { tail "$BASE/primary.log"; die "primary start"; }
psq "CREATE DATABASE $DB" postgres >/dev/null
psq "CREATE EXTENSION pg_duckdb" >/dev/null || die "CREATE EXTENSION"
su postgres -c "$PG/psql -h /tmp -p $PPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1 -v rows=$ROWS" <<'SQL' || die seed
CREATE TABLE orders(
  id uuid PRIMARY KEY,
  seq bigint NOT NULL,
  amount numeric(12,2) NOT NULL,
  created_at timestamptz NOT NULL,
  status text NOT NULL,
  note text);
INSERT INTO orders
SELECT gen_random_uuid(), g, (random()*100000)::numeric(12,2),
       timestamptz '2023-01-01 00:00:00+00' + (random()*3*365*86400 || ' seconds')::interval,
       (ARRAY['new','paid','shipped','delivered','cancelled','refunded'])[1+(random()*5)::int],
       repeat(md5(g::text), 6)
FROM generate_series(1, :rows) g;
CREATE UNIQUE INDEX orders_seq ON orders(seq);
CREATE SEQUENCE orders_seq_next START :rows INCREMENT 1;
SELECT nextval('orders_seq_next');
VACUUM ANALYZE;
SQL
psq "SELECT pg_create_physical_replication_slot('lake_standby', true)" postgres >/dev/null
su postgres -c "$PG/pg_basebackup -D $BASE/standby -R -X stream -S lake_standby -c fast -d 'host=/tmp port=$PPORT user=postgres dbname=postgres'" >/dev/null 2>&1 || die basebackup
{ echo "port = $SPORT"; echo "hot_standby_feedback = on"; echo "sync_replication_slots = on"; } >> "$BASE/standby/postgresql.conf"
su postgres -c "$PG/pg_ctl -D $BASE/standby -l $BASE/standby.log -w start" >/dev/null 2>&1 || { tail "$BASE/standby.log"; die "standby start"; }
( cd "$GO" && for b in qs-mirror qs-query qs-verify; do go build -o "$BASE/$b" ./cmd/$b || exit 1; done ) || die build
chown -R postgres:postgres "$BASE"
echo "primary :$PPORT standby :$SPORT rows $(psq 'SELECT count(*) FROM orders')"
