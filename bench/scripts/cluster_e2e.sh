#!/usr/bin/env bash
# What only a cluster can show.
#
# No sandbox this project is developed in can create a Pod: a kubelet needs
# CAP_SYS_RESOURCE (runc sets a sandbox's oom_score_adj to -998 and a process
# may only RAISE that without the capability; docs/21 chased it to the bottom).
# So this script assumes a cluster and asserts, on it, every claim the
# repository makes about one:
#
#    2  the plugin loads and VALIDATES: a Cluster with a misspelled parameter,
#       and one asking for takeover without the acknowledgement, are refused
#       at admission by the plugin's own message.
#    3  a three-instance Cluster comes up with the sidecar in EVERY instance
#       Pod, each image pulled, and in shadow mode no readiness probe on it.
#    4  each replica has a replication slot of its OWN, active, and the primary
#       has none.
#    5  rows loaded on the primary appear in BOTH mirrors, and qs-verify, run
#       inside each replica Pod from the mirror image, says MATCH.
#    6  with DuckLake and pg_quicksilver: an aggregate sent to the -ro Service
#       is ROUTED (EXPLAIN says so) and equals the primary's answer; a key
#       lookup stays on the heap; nothing was fetched from DuckDB's extension
#       repository.
#    7  a bad value HALTS one table: that table is unroutable and the other
#       still routes, on the same Pods; the documented REBUILD recovers it.
#    8  a SWITCHOVER: both mirrors match afterwards, and no orphaned slot is
#       left pinning WAL on the new primary.
#    9  scaling 3 -> 2 leaves no inactive Quicksilver slot retaining WAL.
#   10  changing sidecarImage ROLLS the instances. go/fidelity asserts CNPG's
#       comparison reports a difference; this asserts the operator acts on it.
#   11  in mode: takeover, a replica whose mirror is behind freshnessSLO LEAVES
#       the -ro endpoints.
#
# Sections 10 and 11 are the original two, unchanged in what they assert. They
# run last because takeover leaves the Cluster deliberately unready.
#
# Run it anywhere there is a cluster:
#
#   KUBECONFIG=... bash bench/scripts/cluster_e2e.sh
#   VERSION=0.0.3 SCOPE=legacy GHCR_USER=me GHCR_TOKEN=ghp_... bash bench/scripts/cluster_e2e.sh
#
# In CI, .github/workflows/cluster-e2e.yml creates a kind cluster. On a pull
# request it builds the checkout's three images and side-loads them; after a
# release it pulls the published ones with the workflow's GITHUB_TOKEN, so the
# pull is a real authenticated registry pull.
#
# Exit status: 0 every section agreed; 1 a section ran and disagreed; 2
# INCOMPLETE, a precondition was missing and nothing was learned.
set -uo pipefail

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
# shellcheck source=bench/scripts/lib_slotname.sh
. "$REPO/bench/scripts/lib_slotname.sh"

NS=${NS:-qs-e2e}
CNPG_NS=${CNPG_NS:-cnpg-system}
CLUSTER=${CLUSTER:-app}
DB=app
# The default is the version a run with no VERSION should be testing, which is
# the newest good PUBLISHED one. It was 0.0.2 — the build whose sidecar takes
# :9187 and stops PostgreSQL starting — so a hand-run in published mode tested
# the broken release and, worse, would have been RIGHT to fail. CI always
# passes VERSION, so nothing caught it.
VERSION=${VERSION:-0.0.3}
REGISTRY=${REGISTRY:-ghcr.io/howlerops}
GHCR_USER=${GHCR_USER:-}
GHCR_TOKEN=${GHCR_TOKEN:-}
# Side-load locally built images instead of pulling. It does NOT prove the pull
# path, and the summary says which ran.
LOCAL_IMAGES=${LOCAL_IMAGES:-0}

# WHAT IS UNDER TEST, and it is not one question.
#
#   full    everything above. Needs images that have per-Pod slots, DuckLake
#           and the extension's half of the sidecar, and the operand image.
#   legacy  sections 0-3, 10 and 11 on a two-instance Cluster with the stock
#           PostgreSQL image: what 0.0.3 and earlier can be asked. Asking 0.0.3
#           for `ducklake: "true"` gets "unknown Quicksilver parameter" from
#           its plugin, which is 0.0.3 being right, not this harness finding
#           anything.
#
# The default follows the images: a release that publishes no operand image
# cannot be asked to route.
SCOPE=${SCOPE:-}
if [ -z "$SCOPE" ]; then
  case "$VERSION" in 0.0.[0-3]) SCOPE=legacy ;; *) SCOPE=full ;; esac
fi
case "$SCOPE" in full|legacy) ;; *) echo "SCOPE must be full or legacy" >&2; exit 2 ;; esac

STOCK_PG_IMAGE=ghcr.io/cloudnative-pg/postgresql:17.2-standard-bookworm
# The operand image: CloudNativePG's PostgreSQL 17 plus pg_duckdb,
# pg_quicksilver and DuckDB's extensions (deploy/Dockerfile, target postgres).
#
# The tag is 17-<version>, never the bare version: CloudNativePG reads the
# PostgreSQL major from the front of an image tag, so "dev" is refused at
# admission and "0.0.4" would be PostgreSQL 0.
PG_IMAGE=${PG_IMAGE:-$REGISTRY/pg_quicksilver-postgres:17-$VERSION}
# Where that image keeps DuckDB's extensions; the image's own label says the
# same. See deploy/duckdb-extensions.sh for why a Cluster has to name it.
DUCKDB_EXT_DIR=${DUCKDB_EXT_DIR:-/usr/lib/quicksilver/duckdb-extensions}
if [ "$SCOPE" = full ]; then WANT_INSTANCES=${INSTANCES:-3}; else WANT_INSTANCES=${INSTANCES:-2}; PG_IMAGE=$STOCK_PG_IMAGE; fi

# The image section 10 rolls TO. It has to be a different reference from the
# one the Pods are already running — that difference is the whole stimulus —
# and it has to be an image the kubelet can actually obtain, or the Pods go to
# ImagePullBackOff and the section reports "no Pod was replaced" after ten
# minutes of waiting for something that could never happen. That is exactly
# what it did: :latest exists on ghcr, so the published path was fine, and
# nothing had ever loaded :latest onto a kind node.
ROLL_IMAGE=${ROLL_IMAGE:-$REGISTRY/pg_quicksilver-mirror:latest}
# WHICH CHART, and it is not the same question as which images.
#
# In published mode this run stands in for somebody following the README, and
# what the README tells them to run is `helm install oci://...`. Installing the
# CHECKOUT's chart there would let a broken published chart pass a run whose
# entire purpose is to catch that — the chart is packaged by a separate job,
# after the images, and it is the piece with no other test.
#
# In source mode there is nothing published to install, so the checkout is the
# only answer and also the right one: the question a pull request asks is
# whether THIS tree works.
#
# This is not hypothetical. Rendering the PUBLISHED 0.0.2 chart with the exact
# flags below shows it ignores `image.pullSecrets` entirely — that value did not
# exist in 0.0.2's templates, it was added later. So published mode was passing
# PULL_ARGS the chart under test would have dropped on the floor, and the
# checkout's chart honoured them, so the run looked like it had proved the
# credential path. It had proved the checkout's.
CHART=${CHART:-}
if [ -z "$CHART" ]; then
  if [ "$LOCAL_IMAGES" = "1" ]; then CHART="$REPO/charts/quicksilver"
  else CHART="oci://$REGISTRY/charts/quicksilver"; fi
fi
case "$CHART" in
  oci://*) CHART_VERSION_ARGS=(--version "$VERSION") ;;
  *)       CHART_VERSION_ARGS=() ;;
esac
TIMEOUT=${TIMEOUT:-600}
# How long a slot is given to settle after a switchover or a scale-down before
# its state is asserted: a sidecar has to notice its role, reconnect, and
# either take its slot or drop it.
SLOT_SETTLE=${SLOT_SETTLE:-180}
# The slot janitor, when the build under test has one. JANITOR_PARAM names the
# plugin parameter that sets its period; empty means "ask the running plugin",
# which is done in section 3 by reading the parameters it wrote back into the
# Cluster. JANITOR_PERIOD is what the test sets it to.
JANITOR_PARAM=${JANITOR_PARAM:-}
JANITOR_PERIOD=${JANITOR_PERIOD:-20s}
# Everything a failed run needs is written here whether it failed or not; the
# workflow uploads it.
ARTIFACTS=${ARTIFACTS:-/tmp/qs-cluster-e2e}
mkdir -p "$ARTIFACTS"
FAIL=0
NOTRUN=()

say()  { printf '\n== %s ==\n' "$*"; }
ok()   { printf '  ok: %s\n' "$*"; }
bad()  { printf '  FAIL: %s\n' "$*"; FAIL=1
         [ -n "${GITHUB_ACTIONS:-}" ] && printf '::error title=cluster-e2e::%s\n' "$*"; return 0; }
skip() { printf '\nINCOMPLETE — skipped, which is NOT a pass: %s\n' "$*"; exit 2; }
# A check that did not run. Not a failure and NOT a pass: it is counted,
# repeated in the result, and annotated on the workflow run.
notrun() { printf '  NOT RUN, which is not a pass: %s\n' "$*"; NOTRUN+=("$*")
           [ -n "${GITHUB_ACTIONS:-}" ] && printf '::warning title=cluster-e2e check not run::%s\n' "$*"; return 0; }
# seconds since the epoch, and a deadline loop that reads as one
now() { date +%s; }

# ---------------------------------------------------------------- helpers
kc() { kubectl -n "$NS" "$@"; }

# INSTANCE Pods, not every Pod the Cluster owns. cnpg.io/cluster also matches
# the initdb Job's Pod, which has no mirror container in it and never will —
# asking for one there fails with "container quicksilver-mirror is not valid
# for pod app-1-initdb-xxxxx", which reads like the injection is broken.
INSTANCES_SEL="cnpg.io/podRole=instance,cnpg.io/cluster=$CLUSTER"

instances() { kc get pods -l "$INSTANCES_SEL" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | sort; }
primary()   { kc get cluster "$CLUSTER" -o jsonpath='{.status.currentPrimary}' 2>/dev/null; }
replicas()  { local p; p=$(primary); instances | grep -vx "$p"; }
pod_ip()    { kc get pod "$1" -o jsonpath='{.status.podIP}' 2>/dev/null; }
uid_of()    { kc get pod "$1" -o jsonpath='{.metadata.uid}' 2>/dev/null; }
sidecar_field() { # <pod> <jsonpath under the sidecar's container>
  kc get pod "$1" -o jsonpath="{.spec.initContainers[?(@.name==\"quicksilver-mirror\")]$2}" 2>/dev/null
}

# SQL on an instance, over its local socket, as the superuser. One statement
# per call: psql runs several in one -c as ONE transaction, and pg_quicksilver
# does not route inside a transaction block.
sql() { # <pod> <statement>
  kc exec -i "$1" -c postgres -- psql -X -h /controller/run -U postgres -d "$DB" \
    -v ON_ERROR_STOP=1 -Atq -c "$2" 2>&1
}
SUPERPW=""
superpw() {
  [ -n "$SUPERPW" ] || SUPERPW=$(kc get secret "$CLUSTER-superuser" -o jsonpath='{.data.password}' 2>/dev/null | base64 -d)
  printf '%s' "$SUPERPW"
}
# SQL over the NETWORK, the way a client reaches a Service: from inside the
# primary's Pod (which has psql and cluster DNS), with a password, to <host>.
# Each further argument is one statement in the same session.
# ROUTE_OPTS is what an application that wants routing sets, per role or per
# database; here per session so that nothing else in this script is routed by
# surprise. freshness_wait because freshness is strict by default: a statement
# is routed only when the mirror has reached what the statement can see, and
# this gives a mirror that is a few milliseconds behind the time to get there.
ROUTE_OPTS="-c quicksilver.route=auto -c quicksilver.freshness_wait=10s"
netsql() { # <host> <extra PGOPTIONS> <statement>...
  local host=$1 opts=$2; shift 2
  local args=() s
  for s in "$@"; do args+=(-c "$s"); done
  kc exec -i "$(primary)" -c postgres -- env PGPASSWORD="$(superpw)" PGSSLMODE=require \
    PGCONNECT_TIMEOUT=10 PGOPTIONS="$ROUTE_OPTS $opts" \
    psql -X -h "$host" -U postgres -d "$DB" -v ON_ERROR_STOP=1 -Atq "${args[@]}" 2>&1
}

# The sidecar's own HTTP endpoints, through the API server: the mirror image is
# distroless and the PostgreSQL image has no curl, so there is nothing in the
# Pod to ask with.
sidecar_http() { # <pod> <path>
  kubectl get --raw "/api/v1/namespaces/$NS/pods/$1:9188/proxy$2" 2>&1
}
halted_tables() { sidecar_http "$1" /metrics | awk '$1 == "quicksilver_halted_tables" {print int($2); f=1} END {if (!f) print "?"}'; }

# qs-verify, from the mirror image, inside the replica Pod, against the
# primary. The image has no shell: this is exec of the binary itself.
verify_once() { # <pod> <table>
  kc exec "$1" -c quicksilver-mirror -- /qs-verify \
    -dsn "host=$CLUSTER-rw port=5432 user=postgres password=$(superpw) dbname=$DB sslmode=require" \
    -mirror /var/lib/postgresql/data/quicksilver -table "$2" 2>&1
}
# A mirror is behind its source by design, so MATCH is waited for: the claim is
# "it gets there", and DIVERGED after the deadline is the failure.
verify_match() { # <pod> <table> [seconds]
  local pod=$1 table=$2 deadline=$(( $(now) + ${3:-180} )) out=""
  while [ "$(now)" -lt "$deadline" ]; do
    out=$(verify_once "$pod" "$table")
    if printf '%s\n' "$out" | tail -1 | grep -qx MATCH; then
      ok "$pod $table: $(printf '%s\n' "$out" | grep -m1 '^mirror ') — MATCH"
      return 0
    fi
    sleep 5
  done
  printf '%s\n' "$out" | tail -15 | sed 's/^/    /'
  bad "qs-verify in $pod never reported MATCH for $table"
  return 1
}
verify_all_replicas() { # [seconds]
  local r t rc=0
  for r in $(replicas); do
    for t in public.events public.accounts; do verify_match "$r" "$t" "${1:-180}" || rc=1; done
  done
  return "$rc"
}

# Quicksilver's logical slots as the primary sees them:
#   name|active|wal_status|bytes of WAL the slot is holding back
slots() {
  sql "$(primary)" "SELECT slot_name, active, coalesce(wal_status,'-'), coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint FROM pg_replication_slots WHERE slot_type = 'logical' AND slot_name LIKE 'quicksilver\\_%' ORDER BY 1"
}
slot_of() { qs_pod_slot "quicksilver_$CLUSTER" "$1"; }
# The whole of what "no orphaned slot" means, in one comparison: on the current
# primary, the Quicksilver slots are EXACTLY one per current replica, each
# named for its Pod, each active. Anything else on the list is a slot nobody
# is reading, and an unread slot pins WAL until the disk is full.
slots_exact() { # [seconds] -> 0 when the set is exact
  local deadline=$(( $(now) + ${1:-$SLOT_SETTLE} )) want have
  want=$(for r in $(replicas); do printf '%s|t\n' "$(slot_of "$r")"; done | sort)
  while :; do
    have=$(slots | awk -F'|' 'NF >= 2 {print $1 "|" $2}' | sort)
    [ "$have" = "$want" ] && return 0
    [ "$(now)" -lt "$deadline" ] || break
    sleep 5
  done
  echo "    on primary $(primary) — name|active|wal_status|retained_bytes:"
  slots | sed 's/^/      /'
  echo "    expected exactly (name|active):"
  printf '%s\n' "$want" | sed 's/^/      /'
  return 1
}

# A wait that actually waits. `kubectl wait` does NOT wait for a resource to be
# CREATED: with nothing matching the selector it returns an error immediately,
# which this read as "the Pods never became Ready" 39 seconds into a 10-minute
# budget, while CNPG was still running initdb.
#
# ALL of them, not "at least one", and EXACTLY the number the spec asks for.
# This used to accept n>=1, and on a two-instance Cluster it passed the moment
# app-1 was Ready — while CNPG was still creating app-2. Everything below then
# raced the cluster into existence, and the roll section mistook that second
# Pod's FIRST appearance for a rollout. A check that starts before the fixture
# is finished is not measuring the fixture. Exactly, because after a scale-down
# "at least two" is true while the third Pod is still there.
wait_instances_ready() { # [seconds]
  local want deadline n rdy
  want=$(kc get cluster "$CLUSTER" -o jsonpath='{.spec.instances}' 2>/dev/null); want=${want:-1}
  deadline=$(( $(now) + ${1:-$TIMEOUT} ))
  while [ "$(now)" -lt "$deadline" ]; do
    n=$(kc get pods -l "$INSTANCES_SEL" --no-headers 2>/dev/null | wc -l)
    rdy=$(kc get cluster "$CLUSTER" -o jsonpath='{.status.readyInstances}' 2>/dev/null)
    if [ "$n" -eq "$want" ] && [ "${rdy:-0}" -eq "$want" ] 2>/dev/null \
       && kc wait --for=condition=Ready pod -l "$INSTANCES_SEL" --timeout=30s >/dev/null 2>&1 \
       && [ -n "$(primary)" ]; then
      return 0
    fi
    sleep 10
  done
  return 1
}
why_not_ready() {
  kc get pods -o wide
  kc get cluster "$CLUSTER" -o jsonpath='{.status.phase}: {.status.phaseReason}{"\n"}' 2>/dev/null
  kc describe pods -l "$INSTANCES_SEL" | grep -A 8 "Events:" | tail -20
  # The reason a container will not start, which is usually the whole answer
  # and is not in the Events list: CreateContainerConfigError names a missing
  # Secret, ImagePullBackOff names the registry.
  kc get pods -l "$INSTANCES_SEL" -o jsonpath=\
'{range .items[*]}{.metadata.name}{"\n"}{range .status.initContainerStatuses[*]}  init/{.name}: {.state.waiting.reason} {.state.waiting.message}{"\n"}{end}{range .status.containerStatuses[*]}  {.name}: {.state.waiting.reason} {.state.waiting.message}{"\n"}{end}{end}' 2>/dev/null
}

# The plugin's parameters, as one merge patch. A merge patch REPLACES the
# plugins list, so every patch has to carry every parameter: a patch that named
# only sidecarImage would silently turn DuckLake off and the roll it caused
# would be for the wrong reason.
declare -A PARAMS
patch_params() { # [key=value ...]  (value empty removes the key)
  local kv
  for kv in "$@"; do
    if [ -z "${kv#*=}" ]; then unset "PARAMS[${kv%%=*}]"; else PARAMS[${kv%%=*}]=${kv#*=}; fi
  done
  local body
  body=$(for k in "${!PARAMS[@]}"; do printf '%s\t%s\n' "$k" "${PARAMS[$k]}"; done | python3 -c '
import json, sys
p = dict(l.rstrip("\n").split("\t", 1) for l in sys.stdin if l.strip())
print(json.dumps({"spec": {"plugins": [{"name": "quicksilver.howlerops.io", "parameters": p}]}}))')
  kc patch cluster "$CLUSTER" --type merge -p "$body" 2>&1
}
params_yaml() { # the same parameters, as the YAML block of a new Cluster
  local k
  for k in $(printf '%s\n' "${!PARAMS[@]}" | sort); do printf '        %s: "%s"\n' "$k" "${PARAMS[$k]}"; done
}

# What a failed run needs, written down while the cluster still exists. Run on
# every exit: a pass with its evidence attached costs a few seconds.
collect() {
  local d=$ARTIFACTS p c
  {
    kubectl get nodes -o wide
    kubectl get pods -A -o wide
    kc get cluster "$CLUSTER" -o yaml
    kc get events --sort-by=.lastTimestamp | tail -80
  } > "$d/cluster.txt" 2>&1
  kc describe pods -l "$INSTANCES_SEL" > "$d/describe-instances.txt" 2>&1
  kubectl -n "$CNPG_NS" logs deploy/quicksilver --tail=2000 > "$d/plugin.log" 2>&1
  kubectl -n "$CNPG_NS" logs deploy/cnpg-controller-manager --tail=3000 > "$d/operator.log" 2>&1
  for p in $(instances); do
    for c in postgres quicksilver-mirror; do
      kc logs "$p" -c "$c" --tail=5000 > "$d/$p.$c.log" 2>&1
      # A crash loop means the useful output is in the PREVIOUS container,
      # which is gone from the running one.
      kc logs "$p" -c "$c" --previous --tail=2000 > "$d/$p.$c.previous.log" 2>/dev/null \
        || rm -f "$d/$p.$c.previous.log"
    done
    sidecar_http "$p" /metrics > "$d/$p.metrics.txt" 2>&1
    sidecar_http "$p" /readyz > "$d/$p.readyz.json" 2>&1
    if [ "$SCOPE" = full ]; then
      sql "$p" "SELECT heap, mirror, usable, reason, applied_lsn, current_lsn, fresh, stale_because FROM quicksilver.mirror_status()" \
        > "$d/$p.mirror_status.txt" 2>&1
    fi
  done
  if [ -n "$(primary)" ]; then
    sql "$(primary)" "SELECT slot_name, slot_type, active, wal_status, restart_lsn, confirmed_flush_lsn, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) FROM pg_replication_slots ORDER BY 1" \
      > "$d/slots.txt" 2>&1
  fi
  echo "  diagnostics are in $d"
}

# ---------------------------------------------------------------- the sections
# Sections 4 to 9, as functions, defined before anything runs so that
# bench/scripts/cluster_e2e_selftest.sh can source this file and run the ones
# that are only SQL against a PostgreSQL it can actually start. A cluster is
# the only place they mean anything; a typo in one of them should not cost a
# forty-minute run to find.
section_slots() {
  say "4. each replica has a slot of its OWN"
  # A slot is one cursor. Two sidecars sharing one meant the second silently
  # missed every row written while the first held it, with nothing in either
  # log; this is the first place two sidecars have ever run against one primary.
  local n; n=$(replicas | wc -l)
  [ "$n" -ge 2 ] || { bad "only $n replica(s); two are needed to tell one slot each from one slot shared"; return; }
  if slots_exact "$TIMEOUT"; then
    for r in $(replicas); do ok "$r streams from its own slot $(slot_of "$r"), active"; done
    ok "the primary $(primary) holds no slot for itself, and there is no shared quicksilver_$CLUSTER slot"
  else
    bad "the Quicksilver slots on the primary are not exactly one active slot per replica"
  fi
}

section_data() {
  say "5. rows written on the primary reach BOTH mirrors"
  local p; p=$(primary)
  # After the snapshot, through the stream: an insert, an update and a delete,
  # on both tables, so MATCH below is about replication and not about the
  # initial copy.
  sql "$p" "INSERT INTO public.events SELECT g, 'SKU-'||(g%1000), (g%997)/7.0, now() FROM generate_series(200001,205000) g" >/dev/null \
    || bad "insert on the primary failed"
  sql "$p" "UPDATE public.events SET amount = amount + 1 WHERE id % 100 = 0" >/dev/null || bad "update on the primary failed"
  sql "$p" "DELETE FROM public.events WHERE id % 1000 = 1" >/dev/null || bad "delete on the primary failed"
  sql "$p" "UPDATE public.accounts SET balance = balance + 0.5 WHERE id % 10 = 0" >/dev/null || bad "update on the primary failed"
  echo "  primary: $(sql "$p" "SELECT count(*) || ' events, sum ' || sum(amount) FROM public.events")"
  verify_all_replicas 300
}

# EXPLAIN on <host> until it says the statement is routed, or the deadline.
routed_on() { # <host> <label> <statement> [extra PGOPTIONS] [seconds]
  local host=$1 label=$2 stmt=$3 opts=${4:-} deadline=$(( $(now) + ${5:-240} )) out=""
  while [ "$(now)" -lt "$deadline" ]; do
    out=$(netsql "$host" "$opts" "EXPLAIN (COSTS OFF) $stmt")
    if printf '%s\n' "$out" | grep -q 'Quicksilver: mirror'; then
      ok "$label: EXPLAIN says ROUTED — $(printf '%s\n' "$out" | grep -m1 'Scan ─' | tr -d '╭─╮ ' || true)"
      return 0
    fi
    sleep 5
  done
  printf '%s\n' "$out" | grep -E 'Quicksilver|ERROR|FATAL|psql' | head -5 | sed 's/^/    /'
  echo "    mirror_status on that host:"
  netsql "$host" "" "SELECT heap, usable, fresh, reason, stale_because, applied_lsn, current_lsn FROM quicksilver.mirror_status()" | sed 's/^/      /'
  bad "$label: the statement was never routed"
  return 1
}
# The answer through <host>, routed, against the primary's own. Routing falls
# back to the heap when the mirror is behind, which gives the right answer for
# the wrong reason, so the session's last decision is read with it: equal AND
# from the mirror is the claim.
routed_equals_primary() { # <host> <label> <statement>
  local host=$1 label=$2 stmt=$3 want got side
  for _ in 1 2 3 4 5 6; do
    want=$(sql "$(primary)" "$stmt")
    got=$(netsql "$host" "" "$stmt" "SELECT side || ' (' || reason || ')' FROM quicksilver.last_decision()")
    side=$(printf '%s\n' "$got" | tail -1); got=$(printf '%s\n' "$got" | head -1)
    case "$side" in mirror*) [ "$got" = "$want" ] && { ok "$label: answered from the MIRROR and equal to the primary — $got"; return 0; } ;; esac
    sleep 5
  done
  bad "$label: primary says '$want', $host said '$got', decided: $side"
  return 1
}

AGG="SELECT count(*), sum(amount) FROM public.events"
AGG_ACCOUNTS="SELECT count(*), sum(balance) FROM public.accounts"
# accounts is 1,000 rows, under the floor that keeps small reads on the heap.
# The floor is not what section 7 is about, so it is lowered for that table's
# statements and left alone everywhere else.
NO_FLOOR="-c quicksilver.min_cost=0 -c quicksilver.min_rows=0"

section_routing() {
  say "6. a read sent to -ro is ROUTED to the mirror, and a key lookup is not"
  local p r out ext; p=$(primary)
  for ext in pg_duckdb pg_quicksilver; do
    [ "$(sql "$p" "SELECT count(*) FROM pg_extension WHERE extname = '$ext'")" = 1 ] \
      || { bad "$ext is not installed in $DB: the operand image or shared_preload_libraries is wrong"; return; }
  done
  ok "pg_duckdb and pg_quicksilver are installed, preloaded as: $(sql "$p" "SHOW shared_preload_libraries")"
  # The planner's row estimates decide "expensive enough". Statistics are
  # replicated, so once on the primary is once everywhere.
  sql "$p" "ANALYZE public.events" >/dev/null; sql "$p" "ANALYZE public.accounts" >/dev/null

  # The sidecar's half: one mirror relation per table, registered on the
  # primary. Both sidecars reconcile the same registry.
  local deadline=$(( $(now) + 240 )) n=0
  while [ "$(now)" -lt "$deadline" ]; do
    n=$(sql "$p" "SELECT count(*) FROM quicksilver.mirrors WHERE enabled")
    [ "$n" = 2 ] && break
    sleep 5
  done
  if [ "$n" = 2 ]; then ok "the sidecars registered both tables in quicksilver.mirrors"
  else sql "$p" "SELECT * FROM quicksilver.mirrors" | sed 's/^/    /'
       bad "quicksilver.mirrors has $n enabled rows, not 2: the sidecars did not register the mirror relations"; return; fi

  # Each replica by its own address first: -ro picks one of them per
  # connection, so a run that only asked the Service could pass on one replica
  # and never have asked the other.
  for r in $(replicas); do
    routed_on "$(pod_ip "$r")" "$r" "$AGG" || return
    routed_equals_primary "$(pod_ip "$r")" "$r" "$AGG"
  done
  # Then the Service, which is what a client uses.
  routed_on "$CLUSTER-ro" "$CLUSTER-ro" "$AGG" && routed_equals_primary "$CLUSTER-ro" "$CLUSTER-ro" "$AGG"

  # A key lookup is what the heap is for, and the measured cost of getting this
  # wrong is a median 935x (docs/11).
  out=$(netsql "$CLUSTER-ro" "" "EXPLAIN (COSTS OFF) SELECT * FROM public.events WHERE id = 4242")
  if printf '%s\n' "$out" | grep -q 'Quicksilver: heap' && ! printf '%s\n' "$out" | grep -q 'Quicksilver: mirror'; then
    ok "a key lookup stays on the heap — $(printf '%s\n' "$out" | grep 'Quicksilver:' | sed 's/^ *//')"
  else
    printf '%s\n' "$out" | head -8 | sed 's/^/    /'
    bad "a key lookup through $CLUSTER-ro was not kept on the heap"
  fi
  # The primary has no mirror: the same aggregate, with routing asked for, is
  # answered from its heap.
  out=$(netsql "$CLUSTER-rw" "" "EXPLAIN (COSTS OFF) $AGG")
  if printf '%s\n' "$out" | grep -q 'Quicksilver: mirror'; then
    bad "the PRIMARY routed a read to a mirror it does not have"
  else
    ok "the primary answers from its heap — $(printf '%s\n' "$out" | grep 'Quicksilver:' | sed 's/^ *//')"
  fi

  [ "${E2E_SKIP_CARRIED:-0}" = 1 ] || extensions_carried
}

# Nothing was fetched. The operand image carries DuckDB's extensions and the
# Cluster names the directory; if either were wrong pg_duckdb would have
# downloaded them into PGDATA, which a kind node allows and a production
# network may not. (That they load with NO network is proved where it can be:
# the image build runs deploy/duckdb-extensions.sh verify with --network=none.)
extensions_carried() {
  local r out dir
  for r in $(replicas); do
    # shellcheck disable=SC2016  # expanded by the shell in the Pod, not this one
    out=$(kc exec "$r" -c postgres -- sh -c 'd=${PGDATA:-/var/lib/postgresql/data/pgdata}/pg_duckdb/extensions; find "$d" -name "*.duckdb_extension" 2>/dev/null | wc -l' 2>&1)
    dir=$(sql "$r" "SHOW duckdb.extension_directory")
    if [ "$out" = 0 ] && [ "$dir" = "$DUCKDB_EXT_DIR" ]; then
      ok "$r: DuckDB's extensions came from the image ($dir); none were downloaded into PGDATA"
    else
      bad "$r: duckdb.extension_directory=$dir and $out extension file(s) under PGDATA: they were fetched, not carried"
    fi
  done
}

section_halt() {
  say "7. a bad value halts ONE table, and REBUILD brings it back"
  local p r out h; p=$(primary)
  # NaN is a legal numeric(12,2) in PostgreSQL and has no DECIMAL(12,2). The
  # mirror stops that table rather than store something else (docs/16).
  sql "$p" "UPDATE public.accounts SET balance = 'NaN' WHERE id = 1" >/dev/null || { bad "could not write the bad value"; return; }
  for r in $(replicas); do
    local deadline=$(( $(now) + 120 ))
    while [ "$(now)" -lt "$deadline" ]; do h=$(halted_tables "$r"); [ "$h" = 1 ] && break; sleep 3; done
    if [ "$h" = 1 ]; then
      ok "$r: quicksilver_halted_tables is 1 — $(sidecar_http "$r" /metrics | grep -m1 '^quicksilver_table_halted' | cut -c1-150)"
    else
      bad "$r: quicksilver_halted_tables is '$h', not 1, two minutes after a NaN was written to public.accounts"; continue
    fi
    # serveWithHaltedTables: the Pod stays in service on its healthy table and
    # says so, rather than leaving -ro for a table the heap can answer.
    out=$(sidecar_http "$r" /readyz)
    if printf '%s' "$out" | grep -q '"ready":true' && printf '%s' "$out" | grep -q 'DEGRADED'; then
      ok "$r: /readyz is ready and says DEGRADED, naming the halted table"
    else
      printf '%s\n' "$out" | cut -c1-300 | sed 's/^/    /'
      bad "$r: /readyz is not 'ready, DEGRADED' with one table halted and serveWithHaltedTables on"
    fi
    # ONLY that table. The halted one must not be routed...
    local unrouted=0
    deadline=$(( $(now) + 120 ))
    while [ "$(now)" -lt "$deadline" ]; do
      out=$(netsql "$(pod_ip "$r")" "$NO_FLOOR" "EXPLAIN (COSTS OFF) $AGG_ACCOUNTS")
      if ! printf '%s\n' "$out" | grep -q 'Quicksilver: mirror' && printf '%s\n' "$out" | grep -q 'accounts'; then unrouted=1; break; fi
      sleep 5
    done
    if [ "$unrouted" = 1 ]; then ok "$r: public.accounts is answered from the heap while halted"
    else bad "$r: public.accounts is STILL routed to a mirror that halted"; fi
    # ...and the answer for it is still the primary's, NaN and all.
    if [ "$(netsql "$(pod_ip "$r")" "$NO_FLOOR" "$AGG_ACCOUNTS")" = "$(sql "$p" "$AGG_ACCOUNTS")" ]; then
      ok "$r: and its answer equals the primary's"
    else
      bad "$r: the halted table's answer differs from the primary's"
    fi
    # ...while the other one still is.
    routed_on "$(pod_ip "$r")" "$r, public.events with accounts halted" "$AGG" "" 60
  done

  # The documented recovery (docs/16, "Rebuilding one table"): fix the cause at
  # the source, ask with a file in that table's mirror directory, restart the
  # sidecar. One replica at a time, as an operator would.
  sql "$p" "UPDATE public.accounts SET balance = 1 WHERE id = 1" >/dev/null || { bad "could not fix the bad value"; return; }
  for r in $(replicas); do
    local before; before=$(uid_of "$r")
    kc exec "$r" -c postgres -- touch /var/lib/postgresql/data/quicksilver/public.accounts/REBUILD \
      || { bad "$r: could not create the REBUILD file"; continue; }
    kc delete pod "$r" --wait=true >/dev/null 2>&1
    local deadline=$(( $(now) + TIMEOUT )) back=0
    while [ "$(now)" -lt "$deadline" ]; do
      if [ -n "$(uid_of "$r")" ] && [ "$(uid_of "$r")" != "$before" ] && wait_instances_ready 20; then back=1; break; fi
      sleep 5
    done
    [ "$back" = 1 ] || { why_not_ready; bad "$r did not come back after its Pod was deleted"; continue; }
    deadline=$(( $(now) + 180 ))
    while [ "$(now)" -lt "$deadline" ]; do h=$(halted_tables "$r"); [ "$h" = 0 ] && break; sleep 3; done
    if [ "$h" = 0 ]; then ok "$r: restarted with REBUILD, and no table is halted"
    else bad "$r: quicksilver_halted_tables is '$h' after REBUILD and a restart"; continue; fi
    # The request is spent once honoured.
    if kc exec "$r" -c postgres -- test -e /var/lib/postgresql/data/quicksilver/public.accounts/REBUILD 2>/dev/null; then
      bad "$r: the REBUILD file is still there after the rebuild"
    fi
    verify_match "$r" public.accounts 180
    verify_match "$r" public.events 180
    routed_on "$(pod_ip "$r")" "$r, public.accounts after the rebuild" "$AGG_ACCOUNTS" "$NO_FLOOR" 240
  done
}

# NOT RUN unless the build has a janitor. See the note where JANITOR_PARAM is
# detected. With one, an orphaned slot must be gone within a few periods.
janitor_check() { # <what happened>
  if [ -z "$JANITOR_PARAM" ]; then
    notrun "the slot janitor after $1: this build's plugin reports no janitor parameter. When it has one, this step sets it to $JANITOR_PERIOD and requires every orphaned Quicksilver slot to be gone within a few periods."
    return 2
  fi
  local secs; secs=$(python3 -c '
import re, sys
m = re.fullmatch(r"(\d+)(ms|s|m|h)", sys.argv[1]); n, u = int(m.group(1)), m.group(2)
print(max(1, int(n * {"ms": 0.001, "s": 1, "m": 60, "h": 3600}[u])))' "$JANITOR_PERIOD" 2>/dev/null || echo 20)
  if slots_exact $(( secs * 4 + 60 )); then
    ok "the janitor ($JANITOR_PARAM=$JANITOR_PERIOD) left exactly one active slot per replica after $1"
    return 0
  fi
  bad "an orphaned Quicksilver slot is still on the primary $(( secs * 4 + 60 ))s after $1, with $JANITOR_PARAM=$JANITOR_PERIOD"
  return 1
}

section_switchover() {
  say "8. a switchover: both mirrors match afterwards and no slot is orphaned"
  local old target; old=$(primary); target=$(replicas | head -1)
  [ -n "$target" ] || { bad "no replica to promote"; return; }
  echo "  $old -> $target"
  # The plugin when it is there, and otherwise exactly what it does: it writes
  # the target into the Cluster's status and the operator does the rest.
  if kubectl cnpg version >/dev/null 2>&1; then
    kubectl cnpg promote -n "$NS" "$CLUSTER" "$target" >/dev/null 2>&1 || bad "kubectl cnpg promote failed"
  else
    kc patch cluster "$CLUSTER" --subresource=status --type=merge -p \
      "{\"status\":{\"targetPrimary\":\"$target\",\"phase\":\"Switchover in progress\",\"phaseReason\":\"Switching over to $target\"}}" >/dev/null \
      || bad "could not set .status.targetPrimary (kubectl 1.24+ is needed for --subresource)"
  fi
  local deadline=$(( $(now) + TIMEOUT )) done_=0
  while [ "$(now)" -lt "$deadline" ]; do
    if [ "$(primary)" = "$target" ] && wait_instances_ready 20; then done_=1; break; fi
    sleep 5
  done
  if [ "$done_" = 1 ]; then ok "$target is the primary and all $(instances | wc -l) instances are Ready"
  else why_not_ready; bad "the switchover to $target did not complete within ${TIMEOUT}s"; return; fi

  # Written on the NEW primary, so MATCH is about the stream that exists now:
  # $old's sidecar has just started mirroring, and the other replica's has had
  # its source change under it.
  sql "$target" "INSERT INTO public.events SELECT g, 'SKU-'||(g%1000), (g%997)/7.0, now() FROM generate_series(300001,305000) g" >/dev/null \
    || bad "insert on the new primary failed"
  sql "$target" "UPDATE public.accounts SET balance = balance + 1 WHERE id % 7 = 0" >/dev/null || bad "update on the new primary failed"
  verify_all_replicas 420

  # Slot state. The sidecar on $target was mirroring until a moment ago and
  # had a slot; docs/16 says a promoted Pod drops its own slot when it stands
  # down. If it does not, that slot sits on the new primary with no reader.
  if slots_exact "$SLOT_SETTLE"; then
    ok "after the switchover the primary holds exactly one active slot per replica: $(replicas | while read -r r; do slot_of "$r"; done | paste -sd' ')"
    ok "the promoted Pod's own slot, $(slot_of "$target"), is gone"
  else
    bad "after the switchover the Quicksilver slots are not exactly one active slot per replica (above): an orphan pins WAL on the new primary"
  fi
  janitor_check "a switchover"
  # Not asserted, because nobody has claimed it: whether the router still
  # routes on -ro straight after a switchover. Printed, so that the first time
  # it does not, the log already has it.
  echo "  for the record, -ro after the switchover: $(netsql "$CLUSTER-ro" "" "EXPLAIN (COSTS OFF) $AGG" | grep -m1 'Quicksilver:' | sed 's/^ *//')"
}

section_scale_down() {
  say "9. scaling 3 -> 2 leaves no inactive slot retaining WAL"
  local before after gone orphan
  before=$(instances)
  kc patch cluster "$CLUSTER" --type merge -p '{"spec":{"instances":2}}' >/dev/null || { bad "could not scale the Cluster"; return; }
  if wait_instances_ready "$TIMEOUT"; then ok "the Cluster is at 2 instances, all Ready"
  else why_not_ready; bad "the Cluster did not settle at 2 instances"; return; fi
  after=$(instances)
  gone=$(comm -23 <(printf '%s\n' "$before") <(printf '%s\n' "$after") | head -1)
  echo "  removed: ${gone:-?} (slot $(slot_of "${gone:-?}"))"

  # Nothing is running in the removed Pod to drop its slot, which is the case
  # docs/16 lists as NOT handled without a janitor: the slot stays, inactive,
  # holding every WAL segment since its last confirmation.
  if [ -n "$JANITOR_PARAM" ]; then
    janitor_check "a scale-down"
  else
    orphan=$(slots | awk -F'|' -v s="$(slot_of "$gone")" '$1 == s {print $0}')
    if [ -z "$orphan" ]; then
      ok "the removed Pod's slot is not on the primary"
    else
      echo "    the removed Pod's slot, name|active|wal_status|retained_bytes: $orphan"
      janitor_check "a scale-down"
      # What an operator is told to do until there is a janitor, done here so
      # that the claim "the documented recovery works" is at least checked and
      # the sections below start from a clean primary.
      if sql "$(primary)" "SELECT pg_drop_replication_slot('$(slot_of "$gone")')" >/dev/null; then
        echo "    dropped it by hand, as docs/16 says to"
      else
        bad "the documented pg_drop_replication_slot of the orphaned slot failed"
      fi
    fi
  fi
  if slots_exact "$SLOT_SETTLE"; then
    ok "the primary holds exactly one active Quicksilver slot, for the remaining replica; nothing inactive retains WAL"
  else
    bad "after the scale-down there is still a Quicksilver slot that is not an active replica's"
  fi
  verify_all_replicas 240
}

# Sourced by the self-test: everything above is definitions.
if [ "${E2E_LIB:-0}" = 1 ]; then return 0 2>/dev/null || exit 0; fi

command -v kubectl >/dev/null || skip "no kubectl"
command -v helm    >/dev/null || skip "no helm"
command -v python3 >/dev/null || skip "no python3"
kubectl cluster-info >/dev/null 2>&1 || skip "no reachable cluster (set KUBECONFIG)"
trap 'rc=$?; collect >/dev/null 2>&1; exit $rc' EXIT

# ---------------------------------------------------------------- prerequisites
say "0. the cluster, cert-manager and CloudNativePG"
echo "  scope: $SCOPE — $WANT_INSTANCES instances, PostgreSQL image $PG_IMAGE"
kubectl get nodes -o wide | tail -n +1 | head -3

if ! kubectl get crd certificates.cert-manager.io >/dev/null 2>&1; then
  kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml >/dev/null \
    || skip "could not install cert-manager"
fi
kubectl -n cert-manager rollout status deploy/cert-manager-webhook --timeout=300s >/dev/null 2>&1 \
  || skip "cert-manager webhook never became ready"
ok "cert-manager ready"

# CloudNativePG 1.26 is a FLOOR, not a preference, and this is where it was
# found. On 1.25.1 the roll section could never pass, for a structural reason:
#
#   1.25  specs.PodWithExistingStorage() builds the target Pod with no plugin
#         client at all, and writes that UN-PATCHED spec into the
#         cnpg.io/podSpec annotation. The sidecar is therefore absent from BOTH
#         sides of checkPodSpecIsOutdated's comparison, so no plugin parameter
#         — sidecarImage, mode, freshnessSLO, tables — can ever produce a diff,
#         and running instances are never rolled for one.
#
#   1.26+ specs.NewInstance() calls the lifecycle hook with
#         OperationVerbEvaluate before the annotation is written, so the
#         sidecar is on both sides and a change rolls the Pods.
#
# go/fidelity builds against the version in its go.mod (1.30) and its rollout
# test passes there, which is exactly how this hid: the unit test proved a
# property of an operator this script did not install.
CNPG_VERSION=${CNPG_VERSION:-1.26.0}
if ! kubectl get crd clusters.postgresql.cnpg.io >/dev/null 2>&1; then
  kubectl apply --server-side -f \
    "https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-${CNPG_VERSION%.*}/releases/cnpg-${CNPG_VERSION}.yaml" >/dev/null \
    || skip "could not install CloudNativePG $CNPG_VERSION"
fi
kubectl -n "$CNPG_NS" rollout status deploy/cnpg-controller-manager --timeout=300s >/dev/null 2>&1 \
  || skip "the CNPG operator never became ready"

# Assert the floor against what is RUNNING, not against what was asked for: a
# standing cluster may already have an older operator, in which case the install
# above was skipped entirely by the CRD check.
opimg=$(kubectl -n "$CNPG_NS" get deploy cnpg-controller-manager \
  -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null)
opver=${opimg##*:}
case "${opver#v}" in
  1.25.*|1.24.*|1.23.*|1.22.*|1.21.*|1.2[0-5])
    skip "CloudNativePG $opver is below the 1.26 floor: it builds the target Pod without the plugin, so a plugin parameter change can never roll an instance and the roll and takeover sections cannot pass. See docs/37." ;;
esac
ok "CloudNativePG ready in $CNPG_NS (operator $opver, floor is 1.26)"

kubectl create namespace "$NS" >/dev/null 2>&1
kubectl create namespace "$CNPG_NS" >/dev/null 2>&1

# ---------------------------------------------------------------- the pull path
say "1. credentials for the pull, in BOTH namespaces"
# This is the half the chart README could only describe. The plugin image is
# pulled in the operator's namespace and the mirror and operand images in the
# Cluster's, and a secret in one does nothing for the other.
if [ "$LOCAL_IMAGES" = "1" ]; then
  ok "LOCAL_IMAGES=1 — side-loading, which does NOT exercise the registry"
  # Checked HERE, long before the roll section would need it. Nothing
  # side-loads :latest, so the default roll target cannot be pulled in this
  # mode, and the failure it produces — "no Pod was replaced within 10 minutes"
  # — reads like the operator ignoring a spec change rather than a tag that was
  # never on the node.
  if [ "$ROLL_IMAGE" = "$REGISTRY/pg_quicksilver-mirror:latest" ]; then
    skip "LOCAL_IMAGES=1 and ROLL_IMAGE is still the default :latest, which is not loaded onto the node. Load a second tag and pass ROLL_IMAGE=<that tag>."
  fi
  ok "the roll section will roll to $ROLL_IMAGE"
  PULL_ARGS=()
  CLUSTER_PULL_SECRETS=""
elif [ -n "$GHCR_TOKEN" ]; then
  for ns in "$CNPG_NS" "$NS"; do
    kubectl -n "$ns" delete secret ghcr >/dev/null 2>&1
    kubectl -n "$ns" create secret docker-registry ghcr \
      --docker-server=ghcr.io --docker-username="${GHCR_USER:-x}" \
      --docker-password="$GHCR_TOKEN" >/dev/null || skip "could not create the pull secret in $ns"
  done
  ok "imagePullSecret 'ghcr' created in $CNPG_NS and $NS"
  # The Pods' credential is not helm's. Section 2 may install the chart from
  # oci://, which helm pulls with its own registry config, and a token that
  # works for the kubelet says nothing about that.
  case "$CHART" in
    oci://*) if printf '%s' "$GHCR_TOKEN" \
               | helm registry login ghcr.io -u "${GHCR_USER:-x}" --password-stdin >/dev/null 2>&1; then
               ok "helm logged in to ghcr.io for the chart pull"
             else
               echo "  note: helm registry login failed; the chart is public, so trying anonymously"
             fi ;;
  esac
  PULL_ARGS=(--set "image.pullSecrets[0].name=ghcr")
  CLUSTER_PULL_SECRETS=$'  imagePullSecrets:\n    - name: ghcr'
else
  skip "no GHCR_TOKEN and LOCAL_IMAGES=0: nothing would be pulled, so nothing would be proven"
fi

# ---------------------------------------------------------------- the plugin
say "2. install the plugin, and see it REFUSE what it should"
echo "  chart: $CHART${CHART_VERSION_ARGS[*]:+ ${CHART_VERSION_ARGS[*]}}"
helm upgrade --install quicksilver "$CHART" "${CHART_VERSION_ARGS[@]}" \
  --namespace "$CNPG_NS" \
  --set operatorNamespace="$CNPG_NS" \
  --set image.plugin="$REGISTRY/pg_quicksilver-plugin:$VERSION" \
  --set image.mirror="$REGISTRY/pg_quicksilver-mirror:$VERSION" \
  "${PULL_ARGS[@]}" --wait --timeout 5m >/dev/null \
  || { kubectl -n "$CNPG_NS" get pods
       # Which outcome this is depends on whose chart it was. A checkout that
       # will not install is a broken harness — INCOMPLETE, nothing was tested.
       # A PUBLISHED chart that will not install is the defect this run exists
       # to find, and calling it "could not run" would hide it.
       case "$CHART" in
         oci://*) bad "the published chart $CHART $VERSION would not install"; exit 1 ;;
         *)       skip "the chart would not install" ;;
       esac; }

kubectl -n "$CNPG_NS" rollout status deploy/quicksilver --timeout=300s >/dev/null 2>&1 \
  || { kubectl -n "$CNPG_NS" describe pods -l app.kubernetes.io/name=quicksilver | tail -25
       bad "the plugin Deployment never became ready"; }
ok "plugin running — the -plugin image was PULLED and STARTED"

# Running is not loaded. The operator finds a plugin by a Service label and
# dials it over mTLS, and a plugin it never reached admits everything, because
# nobody asked it. So the proof that it loaded is a refusal only it can make:
# these two Clusters are valid CloudNativePG and are refused by words that
# exist nowhere but in go/internal/plugin/config.go. --dry-run=server goes
# through the real admission webhooks and creates nothing.
#
# The operator learns of a plugin Service on its own schedule, so the first
# refusal is waited for; an admission that keeps succeeding is the failure.
refused_with() { # <label> <expected text> <parameters, as YAML lines>
  local label=$1 want=$2 params=$3 out="" deadline=$(( $(now) + 180 ))
  while [ "$(now)" -lt "$deadline" ]; do
    out=$(cat <<YAML | kc apply --dry-run=server -f - 2>&1
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: refused
spec:
  instances: 2
  imageName: $STOCK_PG_IMAGE
  storage:
    size: 1Gi
  plugins:
    - name: quicksilver.howlerops.io
      parameters:
$params
YAML
)
    if printf '%s' "$out" | grep -q "$want"; then
      ok "refused at admission: $label"
      return 0
    fi
    sleep 10
  done
  printf '%s\n' "$out" | tail -5 | sed 's/^/    /'
  bad "a Cluster with $label was not refused with \"$want\": the plugin is running but the operator is not asking it"
  return 1
}
refused_with "a misspelled parameter (tabels:)" "unknown Quicksilver parameter" \
$'        tables: public.events\n        tabels: public.events'
refused_with "mode: takeover without the acknowledgement" "acknowledgeOLTPRegression" \
$'        tables: public.events\n        mode: takeover'

# ---------------------------------------------------------------- the Cluster
say "3. a $WANT_INSTANCES-instance Cluster that names the plugin"
PARAMS=([tables]="public.events,public.accounts" [freshnessSLO]="30s")
if [ "$SCOPE" = full ]; then
  PARAMS[ducklake]="true"
  # With the extension a halted table is answered from the heap on that Pod, so
  # the Pod stays in service; section 7 depends on it.
  PARAMS[serveWithHaltedTables]="true"
  # Not the 1Gi default: three sidecars at 1Gi each are requests a CI node may
  # not have, and these tables are 200,000 rows (300 bytes a row is 60 MB).
  PARAMS[sidecarMemory]="512Mi"
  [ -z "$JANITOR_PARAM" ] || PARAMS[$JANITOR_PARAM]="$JANITOR_PERIOD"
  PG_BLOCK=$(cat <<YAML
  postgresql:
    # In this order: pg_quicksilver calls into pg_duckdb when it loads.
    shared_preload_libraries:
      - pg_duckdb
      - pg_quicksilver
    parameters:
      max_slot_wal_keep_size: "1GB"
      # Where the operand image keeps DuckDB's ducklake and sqlite extensions.
      # Without it pg_duckdb looks under PGDATA, finds nothing, and fetches.
      duckdb.extension_directory: "$DUCKDB_EXT_DIR"
YAML
)
  EXT_SQL=$(cat <<'YAML'
        # The sidecar looks for pg_quicksilver in the database and does the
        # rest itself. Created here so it is there before the first sidecar
        # looks, rather than a minute later.
        - CREATE EXTENSION IF NOT EXISTS pg_duckdb
        - CREATE EXTENSION IF NOT EXISTS pg_quicksilver
YAML
)
else
  PG_BLOCK=$(cat <<'YAML'
  postgresql:
    parameters:
      max_slot_wal_keep_size: "1GB"
YAML
)
  EXT_SQL=""
fi

cat <<YAML | kc apply -f - >/dev/null || bad "the Cluster was not admitted"
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: $CLUSTER
spec:
  # Three in full scope, so that there are TWO mirroring replicas: one mirror
  # cannot show that two sidecars do not share a slot, and a switchover with
  # one replica leaves nothing to compare the survivor with.
  instances: $WANT_INSTANCES
  imageName: $PG_IMAGE
  # Not optional, and not obvious. The sidecar reads its credentials from
  # <cluster>-superuser by default (config.go), and CloudNativePG has defaulted
  # enableSuperuserAccess to FALSE since 1.21 — so that Secret simply does not
  # exist unless asked for. The sidecar references it through secretKeyRef, and
  # a container whose secretKeyRef cannot resolve never starts:
  # CreateContainerConfigError, and a Pod that stays not-Ready with no mention
  # of the mirror anywhere. examples/cluster-shadow.yaml has always set this;
  # this test did not.
  enableSuperuserAccess: true
$CLUSTER_PULL_SECRETS
  storage:
    size: 2Gi
  # The mirror is told to follow these tables. Nothing else here creates them,
  # and a mirror pointed at a table that does not exist cannot snapshot, so it
  # never reaches ready and the Pod never reaches Ready — the same symptom as
  # the missing Secret, from a different cause. postInitApplicationSQL runs in
  # the application database once initdb finishes, before the sidecar has
  # anything to ask for.
  #
  # A single-column primary key because the plugin refuses anything else, and
  # that refusal is tested in go/internal/plugin.
  #
  # 200,000 rows in events because routing has a floor: a statement is sent to
  # the mirror when it reads 50,000 rows of mirrored tables or costs 10,000,
  # and below that the heap is the right answer and EXPLAIN says so.
  # accounts is the table section 7 halts: numeric(12,2) accepts NaN and the
  # mirror's DECIMAL(12,2) cannot hold it.
  bootstrap:
    initdb:
      postInitApplicationSQL:
        - CREATE TABLE IF NOT EXISTS public.events (id bigint PRIMARY KEY, sku text, amount numeric(12,2), ts timestamptz DEFAULT now())
        - INSERT INTO public.events SELECT g, 'SKU-'||(g%1000), (g%997)/7.0, now() FROM generate_series(1,200000) g ON CONFLICT DO NOTHING
        - CREATE TABLE IF NOT EXISTS public.accounts (id bigint PRIMARY KEY, owner text, balance numeric(12,2))
        - INSERT INTO public.accounts SELECT g, 'owner-'||g, g FROM generate_series(1,1000) g ON CONFLICT DO NOTHING
$EXT_SQL
$PG_BLOCK
  plugins:
    - name: quicksilver.howlerops.io
      parameters:
$(params_yaml)
YAML

# The real assertion of this whole script: Pods, running, with our sidecar in
# them, having pulled our images.
if wait_instances_ready "$TIMEOUT"; then
  ok "all $WANT_INSTANCES instance Pods are Ready; primary is $(primary)"
else
  why_not_ready
  bad "instance Pods never became Ready within ${TIMEOUT}s"
fi
[ -n "$(instances)" ] || skip "no instance Pod at all"

# In EVERY instance Pod, not the first one `kubectl get` happens to list. One
# Pod with a sidecar says the injection can work; all of them says it does.
for pod in $(instances); do
  img=$(sidecar_field "$pod" .image)
  if [ -n "$img" ]; then
    ok "$pod: the mirror sidecar is in the Pod, image=$img"
  else
    kc get pod "$pod" -o jsonpath='{.spec.initContainers[*].name}{"\n"}'
    bad "$pod: no quicksilver-mirror container in the instance Pod"
  fi
  # Pulled, not merely referenced: the Pod's status carries the imageID it ran.
  imgid=$(kc get pod "$pod" \
    -o jsonpath='{.status.initContainerStatuses[?(@.name=="quicksilver-mirror")].imageID}')
  if [ -n "$imgid" ]; then ok "$pod: PULLED: imageID=$imgid"
  else bad "$pod: the sidecar has no imageID, so nothing was pulled"; fi
  # Shadow is "-ro untouched". The sidecar is a native sidecar, so a readiness
  # probe on it gates the whole Pod, PostgreSQL included: in shadow there must
  # be none (docs/33), and section 11 shows the other half, in takeover.
  probe=$(sidecar_field "$pod" .readinessProbe.httpGet.path)
  mode=$(sidecar_field "$pod" '.env[?(@.name=="QS_MODE")].value')
  if [ "$mode" = shadow ] && [ -z "$probe" ]; then
    ok "$pod: QS_MODE=shadow and no readiness probe on the sidecar — the mirror cannot take PostgreSQL out of -ro"
  else
    bad "$pod: QS_MODE=$mode with readiness probe '$probe'; in shadow there must be none"
  fi
done

# Loaded, as the operator records it: the handshake completed and the plugin's
# capabilities are in the Cluster's status.
pstat=$(kc get cluster "$CLUSTER" -o jsonpath='{.status.pluginStatus[?(@.name=="quicksilver.howlerops.io")].version}' 2>/dev/null)
if [ -n "$pstat" ]; then ok "the operator records the plugin in .status.pluginStatus (version $pstat)"
else bad "quicksilver.howlerops.io is not in the Cluster's .status.pluginStatus"; fi

# Defaults are written back into the Cluster by the plugin's MutateCluster, so
# the parameters on the admitted object are the running plugin saying what it
# knows. That is how the slot janitor is detected: by the binary under test
# reporting a parameter for it, not by this script guessing at a name.
if [ "$SCOPE" = full ] && [ -z "$JANITOR_PARAM" ]; then
  JANITOR_PARAM=$(kc get cluster "$CLUSTER" -o json 2>/dev/null | python3 -c '
import json, sys
try:
    p = json.load(sys.stdin)["spec"]["plugins"][0].get("parameters", {})
except Exception:
    p = {}
print(next((k for k in sorted(p) if "janitor" in k.lower()), ""))')
  if [ -n "$JANITOR_PARAM" ]; then
    echo "  the plugin reports a janitor parameter: $JANITOR_PARAM (will be set to $JANITOR_PERIOD)"
  else
    echo "  the plugin reports no janitor parameter; sections 8 and 9 assert on slot state alone"
  fi
fi

# ---------------------------------------------------------------- full scope
if [ "$SCOPE" = full ]; then
  for s in section_slots section_data section_routing section_halt section_switchover section_scale_down; do
    # Each of these needs what the ones before it left behind. After a failure
    # the rest would report their own timeouts about a cluster that is already
    # known to be wrong, which is twenty minutes of noise on top of the answer.
    if [ $FAIL -ne 0 ]; then
      say "${s#section_}: skipped, because a section above failed"
      continue
    fi
    $s
  done
else
  say "4-9. NOT RUN in legacy scope"
  notrun "per-Pod slots, both mirrors matching, routing, halt and REBUILD, switchover, scale-down: $VERSION predates them (SCOPE=legacy)"
fi

# ---------------------------------------------------------------- the rollout
say "10. changing sidecarImage ROLLS the instances"
# The last two sections both watch instance Pods. If there are none, they
# cannot report anything except their own timeouts — 20 further minutes of
# waiting to restate what a section above already said.
if [ $FAIL -ne 0 ]; then
  echo "  skipped: no healthy instance Pods to roll (see above)"
else
# go/fidelity proves CNPG's own comparison reports a difference. Only a cluster
# shows the operator acting on it.
#
# PER INSTANCE NAME, not the set of uids. Comparing the whole set answers "did
# anything about the Pod list change", and a SECOND INSTANCE BEING CREATED
# changes it exactly like a replacement does. That is what happened: section 3
# passed with one of two Pods Ready, CNPG created app-2 in the ordinary course
# of reaching two instances, the uid set changed, and this reported a rollout it
# had not seen. app-1 was still the original Pod, still running the original
# image, which is what gave it away afterwards.
#
# The property is: a Pod that existed BEFORE, under the same instance name, now
# has a different uid. A new name appearing proves nothing, and neither does a
# name going away.
names=$(instances)
declare -A before_uid
for n in $names; do before_uid[$n]=$(uid_of "$n"); done
echo "  before: $(for n in $names; do printf '%s=%s ' "$n" "${before_uid[$n]:0:8}"; done)"

patch_params "sidecarImage=$ROLL_IMAGE" >/dev/null
echo "  patched sidecarImage to $ROLL_IMAGE"

rolled=0
for _ in $(seq 60); do
  for n in $names; do
    cur=$(uid_of "$n")
    # Replaced, not merely absent: a uid that is gone could be a Pod being
    # deleted for any reason, and one that came back different is the thing
    # being claimed.
    if [ -n "$cur" ] && [ "$cur" != "${before_uid[$n]}" ]; then
      echo "  $n replaced: ${before_uid[$n]:0:8} -> ${cur:0:8}"
      rolled=1; break 2
    fi
  done
  sleep 10
done
if [ "$rolled" = "1" ]; then
  ok "the operator rolled the instances after sidecarImage changed"
else
  # "No Pod was replaced" has two very different causes — the operator did not
  # act, or it acted and the replacement cannot start — and this said neither.
  # The replacement's waiting reason tells them apart in one line.
  kc get pods -l "$INSTANCES_SEL" -o jsonpath=\
'{range .items[*]}{.metadata.name}{" "}{.status.phase}{"\n"}{range .status.initContainerStatuses[*]}    init/{.name} {.image} {.state.waiting.reason} {.state.waiting.message}{"\n"}{end}{end}' 2>/dev/null
  bad "no Pod was replaced within 10 minutes of changing sidecarImage to $ROLL_IMAGE"
fi

fi

# ---------------------------------------------------------------- the probe
say "11. in takeover, a stale mirror LEAVES the -ro endpoints"
if [ $FAIL -ne 0 ]; then
  echo "  skipped: no healthy instance Pods to remove from an endpoint (see above)"
else
# The property docs/33 reasons about from the Kubernetes contract, never
# observed. freshnessSLO is set absurdly low so the mirror cannot satisfy it,
# which is the only deterministic way to make the probe fail on demand.
patch_params "mode=takeover" "acknowledgeOLTPRegression=true" "freshnessSLO=1s" "sidecarImage=" >/dev/null \
  || bad "the Cluster would not accept mode: takeover with the acknowledgement"

# FIRST: did the Pods actually pick takeover up?
#
# Patching the Cluster changes the SPEC. Whether that reaches a running Pod is a
# separate question, and conflating the two cost seventeen minutes of waiting
# and a failure message that pointed at the probe. The Cluster read
# `mode: takeover, freshnessSLO: 1s` at generation 3 while both Pods were still
# running QS_MODE=shadow, QS_FRESHNESS_SLO=30s and no readiness probe at all —
# and a container with no readiness probe reports ready=true unconditionally, so
# the state below could never occur no matter how long it was waited for.
#
# This asserts the precondition separately, so "the roll never happened" and
# "the probe never fired" stop looking like the same failure.
took=0
for _ in $(seq 60); do
  stale=0
  for pod in $(instances); do
    [ "$(sidecar_field "$pod" '.env[?(@.name=="QS_MODE")].value')" = "takeover" ] || stale=1
  done
  [ "$stale" = "0" ] && { took=1; break; }
  sleep 10
done
if [ "$took" = "1" ]; then
  ok "every instance Pod now runs QS_MODE=takeover"
else
  kc get pods -l "$INSTANCES_SEL" -o jsonpath=\
'{range .items[*]}{.metadata.name}{" mode="}{.spec.initContainers[?(@.name=="quicksilver-mirror")].env[?(@.name=="QS_MODE")].value}{" slo="}{.spec.initContainers[?(@.name=="quicksilver-mirror")].env[?(@.name=="QS_FRESHNESS_SLO")].value}{" probe="}{.spec.initContainers[?(@.name=="quicksilver-mirror")].readinessProbe.httpGet.path}{"\n"}{end}' 2>/dev/null
  echo "  cluster spec says: $(kc get cluster "$CLUSTER" -o jsonpath='{.spec.plugins[0].parameters.mode}/{.spec.plugins[0].parameters.freshnessSLO}')"
  bad "the Cluster is on mode: takeover but the Pods never picked it up, so the readiness probe was never added and nothing below can be observed"
fi

# "The endpoint emptied" is NOT the assertion, though it was the first one
# written. Switching to takeover ADDS the readiness probe, which changes the Pod
# spec, which makes CNPG roll the instances — and -ro empties during any
# rollout. That check would have reported "mirror freshness gates serving" while
# observing nothing but Pods restarting.
#
# The state below cannot be produced by a rollout. A Pod that is Running, whose
# POSTGRES container is ready, whose MIRROR container is not, and which is
# therefore absent from -ro, is the freshness probe doing precisely the job
# docs/33 describes: PostgreSQL is fine, the mirror is behind, and the node is
# out of service because of the mirror.
gated=0
for _ in $(seq 90); do
  eps=$(kc get endpoints "$CLUSTER-ro" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null)
  for pod in $(instances); do
    phase=$(kc get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null)
    pgready=$(kc get pod "$pod" -o jsonpath='{.status.containerStatuses[?(@.name=="postgres")].ready}' 2>/dev/null)
    mready=$(kc get pod "$pod" -o jsonpath='{.status.initContainerStatuses[?(@.name=="quicksilver-mirror")].ready}' 2>/dev/null)
    ip=$(pod_ip "$pod")
    if [ "$phase" = "Running" ] && [ "$pgready" = "true" ] && [ "$mready" = "false" ] \
       && [ -n "$ip" ] && ! printf '%s' "$eps" | tr ' ' '\n' | grep -qx "$ip"; then
      gated=1
      printf '  %s: postgres ready, mirror NOT ready, and absent from %s-ro\n' "$pod" "$CLUSTER"
      break 2
    fi
  done
  sleep 10
done
if [ "$gated" = "1" ]; then
  ok "mirror freshness gated the endpoint, with PostgreSQL itself healthy — docs/33"
else
  kc get endpoints "$CLUSTER-ro" -o wide 2>/dev/null
  kc get pods -l "$INSTANCES_SEL" -o jsonpath=\
'{range .items[*]}{.metadata.name} phase={.status.phase} ip={.status.podIP}{"\n"}{range .status.initContainerStatuses[*]}  init/{.name} ready={.ready}{"\n"}{end}{range .status.containerStatuses[*]}  {.name} ready={.ready}{"\n"}{end}{end}' 2>/dev/null
  bad "never saw a Pod with PostgreSQL ready, the mirror not ready, and out of -ro"
fi

fi

say "result"
if [ ${#NOTRUN[@]} -gt 0 ]; then
  echo "NOT RUN — these told us nothing, and a green run does not include them:"
  for n in "${NOTRUN[@]}"; do printf '  - %s\n' "$n"; done
  echo
fi
if [ $FAIL -eq 0 ]; then
  if [ "$SCOPE" = full ]; then
    echo "PASS — the plugin refused what it should; three instances ran with a sidecar"
    echo "each; each replica had its own slot; both mirrors matched their source; -ro"
    echo "routed an aggregate to the mirror and kept a key lookup on the heap; a halted"
    echo "table left routing alone and REBUILD brought it back; a switchover and a"
    echo "scale-down left no orphaned slot; a sidecarImage change rolled the Pods; and"
    echo "mirror freshness gated the -ro endpoint."
  else
    echo "PASS (legacy scope) — Pods pulled the images and ran, a sidecarImage change"
    echo "rolled them, and mirror freshness gated the -ro endpoint."
  fi
else
  echo "FAIL"
fi
exit $FAIL
