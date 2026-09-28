#!/usr/bin/env bash
# The cluster test, on a kind cluster built from Docker on THIS host.
#
# This is the replacement for .github/workflows/cluster-e2e.yml, and it is the
# only thing in the repository that can settle the last two open claims:
#
#   - a Pod being ROLLED because a plugin parameter changed
#   - a Pod being REMOVED from the -ro Service because the mirror's readiness
#     probe failed
#
# Both need a kubelet that can create pod sandboxes. Nothing in the sandbox this
# was developed in can, and as of this writing GitHub Actions is not allocating
# runners for the account, so this script is how the answer gets produced.
#
#   bash bench/docker/cluster_kind.sh              # build this checkout, run it
#   KEEP=1 bash bench/docker/cluster_kind.sh       # leave the cluster up
#   CLUSTER_NAME=qs bash bench/docker/cluster_kind.sh
#
# Roughly 25 minutes: a few for the images, the rest waiting on CloudNativePG.
set -uo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
cd "$REPO"

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
if [ -n "$avail" ] && [ "$avail" -lt 12 ]; then
  skip "${avail}G free on /, and a kind cluster with two operators and two PostgreSQL images needs about 12G"
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

# A SECOND tag on the same image, for the roll. Section 4 proves the operator
# replaces Pods when sidecarImage changes, so it needs a reference that differs
# from the running one AND that the kubelet can obtain. Deliberately not
# :latest — Kubernetes defaults that tag's imagePullPolicy to Always, which
# ignores the node's copy and goes to a registry that is not there.
$engine tag "$REGISTRY/pg_quicksilver-mirror:$TAG" "$REGISTRY/pg_quicksilver-mirror:$ROLL_TAG"

kind load docker-image --name "$CLUSTER_NAME" \
  "$REGISTRY/pg_quicksilver-plugin:$TAG" \
  "$REGISTRY/pg_quicksilver-mirror:$TAG" \
  "$REGISTRY/pg_quicksilver-mirror:$ROLL_TAG" || die "kind load"
echo "  loaded $TAG and $ROLL_TAG"

# ---------------------------------------------------------------- the run
say "3. pull, run, roll and gate"
# LOCAL_IMAGES=1 says plainly that the registry path is NOT being exercised
# here: the images are side-loaded onto the node. The published-pull half is
# what the release workflow is for, and claiming it from a side-load would be
# claiming something weaker than it sounds.
LOCAL_IMAGES=1 \
VERSION="$TAG" \
REGISTRY="$REGISTRY" \
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
     ;;
esac
exit $rc
