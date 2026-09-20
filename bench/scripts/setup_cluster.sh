#!/usr/bin/env bash
# Build the PostgreSQL 17 primary and standby every other script assumes.
#
# Until this existed, nothing in the repository created that cluster. It was
# made by hand once, and from then on every script under bench/scripts opened
# with `psq 5443 "SELECT 1" || skip "primary would not start"` — so on any
# machine that was not THAT machine, the whole database half of the suite
# reported INCOMPLETE and the suite exited 2. Which is honest, and is also a
# test suite that cannot run anywhere.
#
# That is the gap CI found: a fresh runner is exactly the case the scripts could
# not handle.
#
#   bash bench/scripts/setup_cluster.sh          # build if absent
#   FORCE=1 bash bench/scripts/setup_cluster.sh  # destroy and rebuild
#
# Run as root: it creates data directories owned by postgres and drives pg_ctl
# through `su postgres`, exactly as the other scripts do.
set -uo pipefail

PG=${PG:-/usr/lib/postgresql/17/bin}
BASE=${BASE:-/var/lib/postgresql/qs17}
PRIMARY=$BASE/primary
STANDBY=$BASE/standby
PRIMARY_PORT=${PRIMARY_PORT:-5443}
STANDBY_PORT=${STANDBY_PORT:-5444}
FORCE=${FORCE:-0}

say()  { printf '\n=== %s ===\n' "$*"; }
ok()   { printf '  %s\n' "$*"; }
die()  { printf '\nFAILED: %s\n' "$*"; exit 1; }

[ "$(id -u)" = "0" ] || die "run as root: this creates directories owned by postgres"
[ -x "$PG/initdb" ] || die "no PostgreSQL 17 at $PG.
  This project REQUIRES 17 and will not fall back. On 16 a failover loses the
  logical slot outright (docs/14, docs/15), which is why the plugin refuses
  ingest: logical below 17 — testing on 16 would test a configuration the
  product rejects."

id postgres >/dev/null 2>&1 || die "no postgres user"

say "cluster"
if [ -d "$PRIMARY" ] && [ "$FORCE" != "1" ]; then
  if su postgres -c "$PG/pg_ctl -D $PRIMARY status" >/dev/null 2>&1; then
    ok "primary already running on $PRIMARY_PORT"
  else
    su postgres -c "$PG/pg_ctl -D $PRIMARY -l $BASE/primary.log -w start" >/dev/null 2>&1 \
      || die "an existing primary at $PRIMARY would not start; FORCE=1 to rebuild"
    ok "started the existing primary on $PRIMARY_PORT"
  fi
else
  # Stop anything holding the old directories before removing them, or the
  # rebuilt cluster races a postmaster still writing to files that are gone.
  su postgres -c "$PG/pg_ctl -D $STANDBY stop -m immediate" >/dev/null 2>&1
  su postgres -c "$PG/pg_ctl -D $PRIMARY stop -m immediate" >/dev/null 2>&1
  rm -rf "$PRIMARY" "$STANDBY"
  install -d -o postgres -g postgres "$BASE"

  su postgres -c "$PG/initdb -D $PRIMARY -U postgres --locale=C.UTF-8 -E UTF8" \
    >/dev/null 2>&1 || die "initdb failed"

  # wal_level = logical is the whole point; the rest is what the benchmarks and
  # the E2E scripts need to exist at all.
  #
  # fsync = off is deliberate and is ONLY safe because this cluster is
  # disposable. It is a benchmark fixture, not a database: every script here
  # drops and recreates its own data, and a crash means re-running the script.
  # Never carry this setting anywhere real.
  cat >> "$PRIMARY/postgresql.conf" <<CONF

# --- quicksilver test fixture -------------------------------------------------
port = $PRIMARY_PORT
unix_socket_directories = '/tmp'
listen_addresses = 'localhost'
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
max_worker_processes = 16
hot_standby = on
hot_standby_feedback = on
sync_replication_slots = on
shared_buffers = 256MB
fsync = off
# Bound what one slot may pin. This is the R7 mitigation (docs/28), and it is
# the setting the chart tells operators is not optional, so the fixture that
# tests the product should not be running without it.
max_slot_wal_keep_size = 4GB
CONF

  # Local connections only, trust: no network listener beyond localhost and no
  # password to leak into a script or a log.
  cat >> "$PRIMARY/pg_hba.conf" <<'HBA'
local   all             all                                     trust
host    all             all             127.0.0.1/32            trust
local   replication     all                                     trust
host    replication     all             127.0.0.1/32            trust
HBA

  su postgres -c "$PG/pg_ctl -D $PRIMARY -l $BASE/primary.log -w -t 120 start" >/dev/null 2>&1 \
    || { tail -20 "$BASE/primary.log" 2>/dev/null; die "primary would not start"; }
  ok "primary initialised on $PRIMARY_PORT"
fi

say "standby"
psq() { su postgres -c "$PG/psql -h /tmp -p $PRIMARY_PORT -U postgres -d postgres -Atc \"$1\""; }

# Running is not the same as being a standby, and the difference is the whole
# reason this script could not run twice.
#
# e2e_mirror.sh PROMOTES the standby — that is the point of it, and the suite
# runs it last for exactly that reason. What it leaves behind is a perfectly
# healthy postmaster on the standby's port that is a PRIMARY on a diverged
# timeline. `pg_ctl status` says it is up, so the reuse branch took it, and the
# run then died forty lines later at "standby on 5444 is not in recovery" —
# which is true, unhelpful, and not something the script tried to fix.
#
# So the reuse condition asks the server what it IS, not whether it is there.
# Anything that is not in recovery falls through to the rebuild below, which is
# correct for a promoted node anyway: its timeline has diverged from the
# primary's and only a fresh basebackup can reconcile that.
standby_in_recovery() {
  [ "$(su postgres -c "$PG/psql -h /tmp -p $STANDBY_PORT -U postgres -d postgres \
      -Atc 'SELECT pg_is_in_recovery()'" 2>/dev/null)" = "t" ]
}

if [ -d "$STANDBY" ] && [ "$FORCE" != "1" ] && \
   su postgres -c "$PG/pg_ctl -D $STANDBY status" >/dev/null 2>&1 && \
   standby_in_recovery; then
  ok "standby already running on $STANDBY_PORT, and in recovery"
else
  if su postgres -c "$PG/pg_ctl -D $STANDBY status" >/dev/null 2>&1 && ! standby_in_recovery; then
    ok "the server on $STANDBY_PORT was promoted (e2e does this); rebuilding it as a standby"
  fi
  # Stop the standby BEFORE dropping its slot: PostgreSQL will not drop an
  # active slot, so with it still streaming the drop silently fails and the
  # rebuild reuses a slot carrying the previous run's restart_lsn.
  su postgres -c "$PG/pg_ctl -D $STANDBY stop -m immediate" >/dev/null 2>&1
  psq "SELECT pg_drop_replication_slot('qs_standby') FROM pg_replication_slots
       WHERE slot_name='qs_standby'" >/dev/null 2>&1
  psq "SELECT pg_create_physical_replication_slot('qs_standby', true)" >/dev/null 2>&1
  rm -rf "$STANDBY"

  su postgres -c "$PG/pg_basebackup -D $STANDBY -R -X stream -S qs_standby -c fast \
    -d 'host=/tmp port=$PRIMARY_PORT user=postgres dbname=postgres'" >/dev/null 2>&1 \
    || die "pg_basebackup failed"
  { echo "port = $STANDBY_PORT"
    echo "sync_replication_slots = on"
    echo "hot_standby_feedback = on"; } >> "$STANDBY/postgresql.auto.conf"

  su postgres -c "$PG/pg_ctl -D $STANDBY -l $BASE/standby.log -w -t 120 start" >/dev/null 2>&1 \
    || { tail -20 "$BASE/standby.log" 2>/dev/null; die "standby would not start"; }
  ok "standby initialised on $STANDBY_PORT"
fi

say "check"
[ "$(psq 'SELECT 1')" = "1" ] || die "primary does not answer on $PRIMARY_PORT"
ok "primary answers on $PRIMARY_PORT"

rec=$(su postgres -c "$PG/psql -h /tmp -p $STANDBY_PORT -U postgres -d postgres -Atc 'SELECT pg_is_in_recovery()'" 2>/dev/null)
[ "$rec" = "t" ] || die "standby on $STANDBY_PORT is not in recovery"
ok "standby answers on $STANDBY_PORT and is in recovery"

wal=$(psq "SHOW wal_level")
[ "$wal" = "logical" ] || die "wal_level is $wal, not logical — nothing can decode"
ok "wal_level = logical"

ver=$(psq "SHOW server_version_num")
[ "$ver" -ge 170000 ] || die "server_version_num is $ver; this project requires 17"
ok "PostgreSQL $(psq 'SHOW server_version')"

printf '\nREADY — primary %s, standby %s\n' "$PRIMARY_PORT" "$STANDBY_PORT"
