#!/usr/bin/env bash
# The cluster test, on a kind cluster built from Docker on THIS host.
#
# The same run .github/workflows/cluster-e2e.yml makes on a pull request, for a
# machine that has Docker and for the days GitHub Actions does not allocate
# runners (it has happened: HANDOFF.md). It builds this checkout's THREE images
# (plugin, mirror, and the PostgreSQL operand image with pg_duckdb and
# pg_quicksilver), side-loads them, and runs bench/scripts/cluster_e2e.sh in
# full scope: three instances, per-Pod slots, both mirrors verified, routing
# through -ro, a halt and its REBUILD, a switchover, a scale-down, the roll and
# the takeover gate.
#
# All of it needs a kubelet that can create pod sandboxes, which nothing in the
# sandbox this was developed in can.
#
#   bash bench/docker/cluster_kind.sh              # build this checkout, run it
#   KEEP=1 bash bench/docker/cluster_kind.sh       # leave the cluster up
#   CLUSTER_NAME=qs bash bench/docker/cluster_kind.sh
#
# Roughly 45 minutes: about ten for the images the first time (the operand
# image downloads pg_duckdb and DuckDB's extensions), the rest waiting on
# CloudNativePG.
set -uo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
cd "$REPO" || exit 2

CLUSTER_NAME=${CLUSTER_NAME:-qs}
KEEP=${KEEP:-0}
REGISTRY=localhost/qs
TAG=dev
ROLL_TAG=dev-rolled

say()  { printf '\n=== %s ===\n' "$*"; }
die()  { printf '\nFAIL: %s\n' "$*"; exit 1; }
skip() { printf '\nINCOMPLETE — cannot run, which is NOT a pass: %s\n' "$*"; exit 2; }

# ---------------------------------------------------------------- preflight
#
# Checked HERE, before twenty-five minutes of work, because every one of these
# presents later as something else. The capability check in particular: without
# CAP_SYS_RESOURCE runc cannot set a pod sandbox's oom_score_adj to -998, no pod
# sandbox can be created at all, and the failure surfaces as Pods stuck Pending
# with an error that names neither runc nor the capability. docs/21 has the full
# diagnosis.
say "0. can this host do it at all"

engine=$(command -v docker) || skip "no docker on PATH; kind needs Docker specifically"
$engine info >/dev/null 2>&1 || skip "'docker info' fails — the daemon is not reachable"
printf '  docker: %s\n' "$($engine --version)"

printf '  cgroup: %s\n' "$(stat -fc %T /sys/fs/cgroup 2>/dev/null)"
if ! python3 - <<'PY'
import sys
bnd = int(open('/proc/self/status').read().split('CapBnd:')[1].split()[0], 16)
ok = bnd >> 24 & 1
print(f"  CAP_SYS_RESOURCE: {'present' if ok else 'ABSENT'}")
sys.exit(0 if ok else 1)
PY
then
  skip "CAP_SYS_RESOURCE is absent, so runc cannot create a pod sandbox and kind
  will produce Pods that never start. This is the sandbox limitation in docs/21;
  run this on a host with full capabilities."
fi

for t in kind kubectl helm; do
  command -v "$t" >/dev/null || skip "no $t on PATH (need kind, kubectl and helm)"
  printf '  %-8s %s\n' "$t" "$(command -v $t)"
done

# Assigned, NOT piped into a block. The right-hand side of a pipe runs in a
# subshell, so skip()'s exit would have ended the subshell and left the script
# running — a precondition that reports failure and then proceeds anyway is
# worse than no precondition.
avail=$(df -BG --output=avail / 2>/dev/null | tail -1 | tr -dc '0-9')
if [ -n "$avail" ] && [ "$avail" -lt 14 ]; then
  skip "${avail}G free on /, and a kind cluster with two operators, the operand image and three instances needs about 14G"
fi
printf '  disk: %sG free\n' "${avail:-unknown}"

# ---------------------------------------------------------------- the cluster
say "1. a kind cluster named $CLUSTER_NAME"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo "  reusing the existing cluster"
else
  kind create cluster --name "$CLUSTER_NAME" --wait 120s || die "kind create cluster"
fi
kubectl cluster-info --context "kind-$CLUSTER_NAME" >/dev/null 2>&1 \
  || die "the kind cluster is not reachable"
kubectl config use-context "kind-$CLUSTER_NAME" >/dev/null

cleanup() {
  if [ "$KEEP" = "1" ]; then
    printf '\nKEEP=1 — leaving cluster %s up. Delete it with:\n  kind delete cluster --name %s\n' \
      "$CLUSTER_NAME" "$CLUSTER_NAME"
  else
    printf '\ndeleting cluster %s (KEEP=1 to keep it)\n' "$CLUSTER_NAME"
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1
  fi
}
trap cleanup EXIT

# ---------------------------------------------------------------- the images
say "2. build this checkout and load it into the node"
$engine build --target plugin -t "$REGISTRY/pg_quicksilver-plugin:$TAG" -f deploy/Dockerfile . \
  || die "building the plugin image"
$engine build --target mirror -t "$REGISTRY/pg_quicksilver-mirror:$TAG" -f deploy/Dockerfile . \
  || die "building the mirror image"
# The operand image. Tagged 17-<tag>: CloudNativePG reads the PostgreSQL major
# from the front of an image tag and refuses one that does not start with a
# number. Its build is also a test: the last stage starts a
# PostgreSQL with the network OFF and requires both extensions to load and a
# DuckLake on a SQLite catalog to read back (deploy/duckdb-extensions.sh), so a
# failure here is usually that, and the build log says which statement.
$engine build --target postgres -t "$REGISTRY/pg_quicksilver-postgres:17-$TAG" -f deploy/Dockerfile . \
  || die "building the PostgreSQL operand image (its last stage is an offline self-check; read the log above)"

# A SECOND tag on the same image, for the roll. Section 4 proves the operator
# replaces Pods when sidecarImage changes, so it needs a reference that differs
# from the running one AND that the kubelet can obtain. Deliberately not
# :latest — Kubernetes defaults that tag's imagePullPolicy to Always, which
# ignores the node's copy and goes to a registry that is not there.
$engine tag "$REGISTRY/pg_quicksilver-mirror:$TAG" "$REGISTRY/pg_quicksilver-mirror:$ROLL_TAG"

kind load docker-image --name "$CLUSTER_NAME" \
  "$REGISTRY/pg_quicksilver-plugin:$TAG" \
  "$REGISTRY/pg_quicksilver-mirror:$TAG" \
  "$REGISTRY/pg_quicksilver-mirror:$ROLL_TAG" \
  "$REGISTRY/pg_quicksilver-postgres:17-$TAG" || die "kind load"
echo "  loaded $TAG and $ROLL_TAG, and the operand image"

# ---------------------------------------------------------------- the run
say "3. the cluster test, full scope"
# LOCAL_IMAGES=1 says plainly that the registry path is NOT being exercised
# here: the images are side-loaded onto the node. The published-pull half is
# what the release workflow is for, and claiming it from a side-load would be
# claiming something weaker than it sounds.
env LOCAL_IMAGES=1 \
SCOPE=full \
VERSION="$TAG" \
REGISTRY="$REGISTRY" \
PG_IMAGE="$REGISTRY/pg_quicksilver-postgres:17-$TAG" \
ROLL_IMAGE="$REGISTRY/pg_quicksilver-mirror:$ROLL_TAG" \
bash bench/scripts/cluster_e2e.sh
rc=$?

say "result"
case $rc in
  0) echo "PASS — the cluster agreed on every section." ;;
  2) echo "INCOMPLETE — a precondition was not met. Not a failure and not a pass;"
     echo "the last line above says which." ;;
  *) echo "FAIL — a section ran and disagreed (exit $rc)."
     echo
     echo "What the cluster looked like:"
     kubectl get pods -A -o wide 2>/dev/null | head -30
     kubectl -n qs-e2e describe pod -l cnpg.io/podRole=instance 2>/dev/null \
       | grep -E "Name:|Image:|Ready:|State:|Reason:|QS_MODE|QS_FRESHNESS|Readiness:" | head -40
     echo
     echo "Logs, slots, mirror_status and the sidecars' /metrics: ${ARTIFACTS:-/tmp/qs-cluster-e2e}"
     ;;
esac
exit $rc
