#!/usr/bin/env bash
# DuckDB's `ducklake` and `sqlite` extensions, put into an image so that an
# instance never has to fetch them.
#
#   duckdb-extensions.sh install <dir>   needs the network; fills <dir>
#   duckdb-extensions.sh verify  <dir>   needs none; proves <dir> is enough
#
# Why this is a script that starts a PostgreSQL rather than two curl lines.
# pg_duckdb's DuckDB looks for an extension at
#
#   <duckdb.extension_directory>/<duckdb version>/<platform>/<name>.duckdb_extension
#
# and the version and platform are properties of the libduckdb.so inside the
# pg_duckdb package, not of anything this repository pins. Asking that DuckDB
# to install them is the only way to land the files where the same DuckDB will
# look, and it stays right when the package is bumped.
#
# Why <dir> has to be named in the Cluster. pg_duckdb defaults
# duckdb.extension_directory to $PGDATA/pg_duckdb/extensions. PGDATA is a
# volume, so nothing in an image can be there: the image carries the files
# somewhere else and the Cluster sets
#
#   postgresql.parameters:  duckdb.extension_directory: <dir>
#
# `verify` is the claim docs/16 could not make ("this was not tested without
# network access"): a second, empty PostgreSQL, the directory not writable by
# the server, and the statement the sidecar itself runs on a new cluster
# (duckdb.install_extension) followed by a DuckLake on a SQLite catalog being
# created, written and read. deploy/Dockerfile runs it under
# `RUN --network=none`, so an image that would have needed the network does
# not build.
#
# PRELOAD is the scratch server's shared_preload_libraries. With
# pg_quicksilver in it (after pg_duckdb, which is the only order that works)
# `verify` also creates that extension, so an image whose pg_quicksilver.so was
# built against the wrong headers, or left out, does not build either.
#
# Runs as the postgres OS user (initdb refuses root). PG_BIN names the server
# binaries; the default is the Debian layout the CloudNativePG images use.
set -euo pipefail

mode=${1:-}
dir=${2:-}
PG_BIN=${PG_BIN:-/usr/lib/postgresql/${PG_MAJOR:-17}/bin}
EXTENSIONS=${EXTENSIONS:-ducklake sqlite}
PRELOAD=${PRELOAD:-pg_duckdb}

case "$mode" in install|verify) ;; *) echo "usage: $0 install|verify <dir>" >&2; exit 2 ;; esac
[ -n "$dir" ] || { echo "usage: $0 install|verify <dir>" >&2; exit 2; }
[ "$(id -u)" != 0 ] || { echo "run this as the postgres user, not root: initdb refuses root" >&2; exit 2; }

work=$(mktemp -d)
log=$work/postgres.log
stop() {
  rc=$?
  [ -f "$work/data/postmaster.pid" ] && "$PG_BIN/pg_ctl" -D "$work/data" -m immediate stop >/dev/null 2>&1
  if [ $rc -ne 0 ] && [ -f "$log" ]; then
    echo "--- the scratch server's log ---" >&2
    tail -40 "$log" >&2
  fi
  rm -rf "$work"
  exit $rc
}
trap stop EXIT

"$PG_BIN/initdb" -D "$work/data" -U postgres -A trust -E UTF8 --locale=C.UTF-8 >/dev/null
# No TCP listener: this server exists for a few statements and must not be
# reachable, or collide with anything, while it does.
"$PG_BIN/pg_ctl" -D "$work/data" -l "$log" -w start -o \
  "-c listen_addresses='' -c unix_socket_directories='$work' \
   -c shared_preload_libraries='$PRELOAD' -c duckdb.extension_directory='$dir' \
   -c shared_buffers=32MB -c max_connections=10 -c fsync=off" >/dev/null

# NOTICEs off: duckdb.raw_query reports its result as one.
q() { PGOPTIONS="-c client_min_messages=warning" "$PG_BIN/psql" -X -h "$work" -U postgres -d postgres -v ON_ERROR_STOP=1 -Atq "$@"; }
q -c "CREATE EXTENSION pg_duckdb"
printf 'pg_duckdb %s, DuckDB %s, %s, extension_directory=%s\n' \
  "$(q -c "SELECT extversion FROM pg_extension WHERE extname='pg_duckdb'")" \
  "$(q -c "SELECT r['v'] FROM duckdb.query('SELECT version() AS v') r")" \
  "$(q -c "SELECT r['platform'] FROM duckdb.query('SELECT platform FROM pragma_platform()') r")" \
  "$(q -c "SHOW duckdb.extension_directory")"

case ",$PRELOAD," in
  *,pg_quicksilver,*)
    q -c "CREATE EXTENSION pg_quicksilver"
    printf 'pg_quicksilver %s, preloaded as %s, quicksilver.route=%s\n' \
      "$(q -c "SELECT extversion FROM pg_extension WHERE extname='pg_quicksilver'")" \
      "$(q -c "SHOW shared_preload_libraries")" "$(q -c "SHOW quicksilver.route")" ;;
esac

if [ "$mode" = verify ]; then
  # The server must not be able to repair a missing file by writing one.
  if [ -w "$dir" ]; then
    echo "FAIL: $dir is writable by $(id -un); the image must carry it read-only, as a Pod with a read-only root filesystem will see it" >&2
    exit 1
  fi
  for x in $EXTENSIONS; do
    n=$(find "$dir" -name "${x}*.duckdb_extension" | wc -l)
    [ "$n" -ge 1 ] || { echo "FAIL: no ${x}*.duckdb_extension under $dir" >&2; exit 1; }
  done
fi

# What the sidecar runs on a cluster that has never had them
# (go/cmd/qs-mirror/extension.go): a row in duckdb.extensions, which every
# backend of every instance then loads.
for x in $EXTENSIONS; do
  q -c "SELECT duckdb.install_extension('$x')" >/dev/null
done

# And what a routed session does with them: a DuckLake on a SQLite catalog. A
# new session, so that the load is the one a backend does at start and not
# something the install above left behind.
lake=$work/lake
mkdir -p "$lake/data"
q >/dev/null <<SQL
SELECT duckdb.raw_query(\$qs\$ ATTACH 'ducklake:sqlite:$lake/catalog.sqlite' AS lake (DATA_PATH '$lake/data') \$qs\$);
SELECT duckdb.raw_query(\$qs\$ CREATE TABLE lake.t AS SELECT range AS id FROM range(1000) \$qs\$);
SQL
got=$(q <<SQL
SELECT duckdb.raw_query(\$qs\$ ATTACH IF NOT EXISTS 'ducklake:sqlite:$lake/catalog.sqlite' AS lake (READ_ONLY) \$qs\$);
SELECT r['n'] FROM duckdb.query(\$qs\$ SELECT count(*) AS n FROM lake.t \$qs\$) r;
SQL
)
got=$(printf '%s\n' "$got" | tail -1)
[ "$got" = 1000 ] || { echo "FAIL: a DuckLake on a SQLite catalog read back '$got', not 1000" >&2; exit 1; }

echo "files:"
find "$dir" -type f -printf '  %10s  %P\n' | sort -k2
case "$mode" in
  install) echo "installed: $EXTENSIONS" ;;
  verify)  echo "verified: $EXTENSIONS load from $dir with no network and no write access, and a ducklake:sqlite: lake reads back" ;;
esac
