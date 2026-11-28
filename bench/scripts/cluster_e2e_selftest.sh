#!/usr/bin/env bash
# The cluster test's own SQL, run where there is no cluster.
#
# bench/scripts/cluster_e2e.sh can only run on Kubernetes, takes forty minutes,
# and half of what it asserts is not about Kubernetes at all: which slots
# exist, what qs-verify prints, what EXPLAIN says when a read is routed, what
# /readyz says about a halted table, whether REBUILD brings it back. A mistake
# in any of those would otherwise be found by a cluster run failing for a
# reason that is the test's and not the product's.
#
# So this sources that script for its section functions and runs four of them
# (slots, data, routing, halt and REBUILD) against a real PostgreSQL 17
# primary, a streaming standby and the production sidecar, started by
# ext/pg_quicksilver/validate/real. `kubectl` is a shim, written below, that
# turns the handful of calls those sections make into the local equivalent:
#
#   kubectl exec <pod> -c postgres -- psql -h <host> ...      psql on that port
#   kubectl exec <pod> -c quicksilver-mirror -- /qs-verify    the built qs-verify
#   kubectl get --raw .../pods/<pod>:9188/proxy/<path>        curl the sidecar
#   kubectl delete pod <replica>                              restart the sidecar
#
# What this does NOT show, and the cluster run does: anything about Pods, the
# operator, the plugin's admission, images, Services, a second replica, a
# switchover or a scale-down. One standby stands in for "each replica".
#
# Needs root (the servers run as the postgres OS user), PostgreSQL 17 with
# pg_duckdb, and pg_quicksilver installed (`make -C ext/pg_quicksilver install`).
# Exits 2, INCOMPLETE, when one of those is missing.
#
#   sudo bash bench/scripts/cluster_e2e_selftest.sh
#   DUCKDB_EXT_DIR=/usr/lib/quicksilver/duckdb-extensions sudo -E bash ...
#
# With DUCKDB_EXT_DIR set the servers use that directory for DuckDB's
# extensions, as a Cluster on the operand image does, and section 6's "nothing
# was fetched into PGDATA" is asserted for real. Without it that one check is
# reported as not run.
set -uo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
REAL=$REPO/ext/pg_quicksilver/validate/real
export PG=${PG:-/usr/lib/postgresql/17/bin}
export BASE=${BASE:-/var/lib/postgresql/qsint-e2e}
export PPORT=${PPORT:-5583} SPORT=${SPORT:-5584} HPORT=${HPORT:-5585}
KEEP=${KEEP:-0}

incomplete() { printf '\nINCOMPLETE — cannot run, which is NOT a pass: %s\n' "$*"; exit 2; }
[ "$(id -u)" = 0 ] || incomplete "must run as root: the servers run as the postgres OS user"
[ -x "$PG/postgres" ] || incomplete "no PostgreSQL 17 at $PG"
lib=$("$PG/pg_config" --pkglibdir 2>/dev/null)
for so in pg_duckdb pg_quicksilver; do
  [ -f "$lib/$so.so" ] || incomplete "$so is not installed in $lib"
done
command -v go >/dev/null || incomplete "no go on PATH (under sudo, pass it: sudo env \"PATH=\$PATH\" ...)"

# shellcheck source=/dev/null
. "$REAL/env.sh"

TABLES=public.events,public.accounts
export QS_TABLES_OVERRIDE=$TABLES
# The slot prefix a Cluster named `app` gets from the plugin, so that the
# cluster script's own idea of the slot's name is what is being tested.
export SIDECAR_ENV="QS_SLOT=quicksilver_app"
if [ -n "${DUCKDB_EXT_DIR:-}" ]; then
  [ -d "$DUCKDB_EXT_DIR" ] || incomplete "DUCKDB_EXT_DIR=$DUCKDB_EXT_DIR does not exist"
  export EXTRA_CONF="duckdb.extension_directory = '$DUCKDB_EXT_DIR'"
fi

cleanup() {
  rc=$?
  if [ "$KEEP" = 1 ]; then echo "KEEP=1: left running under $BASE (ports $PPORT, $SPORT, $HPORT)"
  else "$REAL/teardown.sh" >/dev/null 2>&1; rm -rf "$SHIM"; fi
  exit $rc
}
SHIM=$(mktemp -d)
trap cleanup EXIT

echo "== a primary, a standby and the sidecar =="
# setup.sh starts the sidecar on its own tables; it is restarted below on the
# two this test uses, which do not exist until then.
QS_TABLES_OVERRIDE='' ROWS=1000 DB_ROUTE=none "$REAL/setup.sh" || incomplete "validate/real/setup.sh failed"
"$REAL/sidecar.sh" stop
# The Cluster's postInitApplicationSQL, word for word.
psq "$PPORT" "CREATE TABLE IF NOT EXISTS public.events (id bigint PRIMARY KEY, sku text, amount numeric(12,2), ts timestamptz DEFAULT now())" >/dev/null
psq "$PPORT" "INSERT INTO public.events SELECT g, 'SKU-'||(g%1000), (g%997)/7.0, now() FROM generate_series(1,200000) g ON CONFLICT DO NOTHING" >/dev/null
psq "$PPORT" "CREATE TABLE IF NOT EXISTS public.accounts (id bigint PRIMARY KEY, owner text, balance numeric(12,2))" >/dev/null
psq "$PPORT" "INSERT INTO public.accounts SELECT g, 'owner-'||g, g FROM generate_series(1,1000) g ON CONFLICT DO NOTHING" >/dev/null
"$REAL/sidecar.sh" start || { tail -20 "$BASE/mirror.log"; incomplete "the sidecar did not become ready on $TABLES"; }

# ---------------------------------------------------------------- the shim
echo 1 > "$SHIM/uid.qsint-2"; echo 1 > "$SHIM/uid.qsint-1"
cat > "$SHIM/kubectl" <<'SHIM_EOF'
#!/usr/bin/env bash
# kubectl, for bench/scripts/cluster_e2e_selftest.sh: qsint-1 is the primary,
# qsint-2 the one replica. Anything it is not asked in that test is an error,
# so that a new kubectl call in a section fails here instead of passing blind.
set -uo pipefail
[ "${1:-}" = -n ] && shift 2
cmd=${1:-}; shift || true
port_of() {
  case "$1" in
    qsint-1|qsint-1.pod|app-rw) echo "$PPORT" ;;
    qsint-2|qsint-2.pod|app-ro) echo "$SPORT" ;;
    *) echo "shim: no such host $1" >&2; exit 64 ;;
  esac
}
here() { # paths in a Pod -> paths here
  printf '%s' "$1" | sed "s|^/var/lib/postgresql/data/quicksilver|$MIRROR|"
}
jp() { local a; for a in "$@"; do case "$a" in -o) ;; jsonpath=*) printf '%s' "${a#jsonpath=}" ;; esac; done; }
case "$cmd" in
  get)
    what=${1:-}; shift || true
    case "$what" in
      --raw) curl -s "http://$HEALTH${1##*/proxy}" ;;
      pods)  printf 'qsint-1\nqsint-2\n' ;;
      cluster)
        case "$(jp "$@")" in
          *currentPrimary*) printf 'qsint-1' ;;
          *spec.instances*|*readyInstances*) printf '2' ;;
          *) echo "shim: get cluster $*" >&2; exit 64 ;;
        esac ;;
      pod)
        name=$1; shift
        case "$(jp "$@")" in
          *podIP*) printf '%s.pod' "$name" ;;
          *uid*)   cat "$SHIM/uid.$name" ;;
          *) echo "shim: get pod $*" >&2; exit 64 ;;
        esac ;;
      secret) printf 'x' | base64 ;;
      *) echo "shim: get $what $*" >&2; exit 64 ;;
    esac ;;
  wait) exit 0 ;;
  delete)
    [ "${1:-}" = pod ] && [ "${2:-}" = qsint-2 ] || { echo "shim: delete $*" >&2; exit 64; }
    "$REAL/sidecar.sh" stop
    echo $(( $(cat "$SHIM/uid.qsint-2") + 1 )) > "$SHIM/uid.qsint-2"
    "$REAL/sidecar.sh" start ;;
  exec)
    [ "${1:-}" = -i ] && shift
    pod=$1; shift
    [ "$1" = -c ] || { echo "shim: exec without -c" >&2; exit 64; }
    container=$2; shift 2
    [ "$1" = -- ] && shift
    if [ "$container" = quicksilver-mirror ]; then
      [ "$1" = /qs-verify ] || { echo "shim: exec in the mirror: $*" >&2; exit 64; }
      table=""; while [ $# -gt 0 ]; do [ "$1" = -table ] && table=$2; shift; done
      exec "$BASE/qs-verify" -dsn "host=/tmp port=$PPORT user=postgres dbname=app" -mirror "$MIRROR" -table "$table"
    fi
    if [ "$1" = env ]; then
      shift
      while [ $# -gt 0 ] && [[ $1 == *=* ]]; do export "${1?}"; shift; done
    fi
    case "$1" in
      psql)
        shift; args=()
        while [ $# -gt 0 ]; do
          if [ "$1" = -h ]; then
            h=$2; shift 2
            [ "$h" = /controller/run ] && h=$pod
            args+=(-h /tmp -p "$(port_of "$h")")
          else args+=("$1"); shift; fi
        done
        exec "$PG/psql" "${args[@]}" ;;
      sh)    PGDATA=$BASE/standby exec "$@" ;;
      touch) exec touch "$(here "$2")" ;;
      test)  exec test "$2" "$(here "$3")" ;;
      *) echo "shim: exec in postgres: $*" >&2; exit 64 ;;
    esac ;;
  *) echo "shim: $cmd $*" >&2; exit 64 ;;
esac
SHIM_EOF
chmod +x "$SHIM/kubectl"
export SHIM REAL HEALTH MIRROR
export PATH="$SHIM:$PATH"

# ---------------------------------------------------------------- the sections
# The operator's settings are not this PostgreSQL's: section 4 would otherwise
# assert the plugin's enriched parameters on a server nobody enriched.
export E2E_LIB=1 SCOPE=full CLUSTER=app NS=selftest ARTIFACTS="$SHIM/artifacts" E2E_ENRICHED_CHECK=0
# shellcheck source=bench/scripts/cluster_e2e.sh
. "$REPO/bench/scripts/cluster_e2e.sh"
TIMEOUT=120

say "4. the replica has a slot of its own (one replica here; the cluster has two)"
if slots_exact 60; then ok "$(replicas) streams from $(slot_of "$(replicas)"), active, and it is the only Quicksilver slot"
else bad "the slots on the primary are not exactly the replica's"; fi

[ "$FAIL" -ne 0 ] || section_data
if [ -z "${EXTRA_CONF:-}" ]; then
  # Without an extension directory these servers fetched DuckDB's extensions
  # into PGDATA, which is the thing that check exists to refuse.
  export E2E_SKIP_CARRIED=1
  notrun "DuckDB's extensions carried, not fetched: DUCKDB_EXT_DIR was not given, so these servers downloaded them"
fi
[ "$FAIL" -ne 0 ] || section_routing
[ "$FAIL" -ne 0 ] || section_halt

say "result"
for n in "${NOTRUN[@]:-}"; do [ -z "$n" ] || printf 'NOT RUN: %s\n' "$n"; done
if [ "$FAIL" -eq 0 ]; then echo "PASS — the cluster test's slot, verify, routing, halt and REBUILD assertions hold against a real primary, standby and sidecar."
else echo "FAIL"; tail -30 "$BASE/mirror.log"; fi
exit "$FAIL"
