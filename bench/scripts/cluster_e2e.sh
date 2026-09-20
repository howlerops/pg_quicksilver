#!/usr/bin/env bash
# The two things no sandbox here can prove: that a Pod PULLS the published
# images, and that a Pod is ROLLED and REMOVED by the plugin's own signals.
#
# Both need a kubelet that can create pod sandboxes, which needs CAP_SYS_RESOURCE
# — runc sets a sandbox's oom_score_adj to -998 and a process may only RAISE that
# without the capability. docs/21 chased this to the bottom. It is not a
# permissions puzzle to be worked around; it is a property of the machine.
#
# So this script assumes a cluster and asserts what only a cluster can show:
#
#   1. instance Pods reach Ready with the mirror sidecar in them, having PULLED
#      both images from ghcr through an imagePullSecret — which also tests the
#      two-namespace pull instructions in the chart README, the half nothing
#      could check before.
#   2. changing sidecarImage ROLLS the instances. go/fidelity asserts CNPG's
#      comparison reports a difference; this asserts the operator acts on it.
#   3. in mode: takeover, a replica whose mirror is behind freshnessSLO LEAVES
#      the -ro endpoints, and returns when it catches up.
#
# Run it anywhere there is a cluster:
#
#   KUBECONFIG=... bash bench/scripts/cluster_e2e.sh
#   VERSION=0.0.2 GHCR_USER=me GHCR_TOKEN=ghp_... bash bench/scripts/cluster_e2e.sh
#
# In CI, .github/workflows/cluster-e2e.yml creates a kind cluster and passes the
# workflow's own GITHUB_TOKEN, so the pull is a real authenticated registry pull
# rather than a local image side-loaded into the node.
set -uo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
NS=${NS:-qs-e2e}
CNPG_NS=${CNPG_NS:-cnpg-system}
CLUSTER=${CLUSTER:-app}
VERSION=${VERSION:-0.0.2}
REGISTRY=${REGISTRY:-ghcr.io/howlerops}
GHCR_USER=${GHCR_USER:-}
GHCR_TOKEN=${GHCR_TOKEN:-}
# Side-load locally built images instead of pulling. Useful on a laptop; it does
# NOT prove the pull path, so CI leaves it off and the summary says which ran.
LOCAL_IMAGES=${LOCAL_IMAGES:-0}
TIMEOUT=${TIMEOUT:-600}
FAIL=0

say()  { printf '\n== %s ==\n' "$*"; }
ok()   { printf '  ok: %s\n' "$*"; }
bad()  { printf '  FAIL: %s\n' "$*"; FAIL=1; }
skip() { printf '\nINCOMPLETE — skipped, which is NOT a pass: %s\n' "$*"; exit 2; }

command -v kubectl >/dev/null || skip "no kubectl"
command -v helm    >/dev/null || skip "no helm"
kubectl cluster-info >/dev/null 2>&1 || skip "no reachable cluster (set KUBECONFIG)"

# ---------------------------------------------------------------- prerequisites
say "0. the cluster, cert-manager and CloudNativePG"
kubectl get nodes -o wide | tail -n +1 | head -3

if ! kubectl get crd certificates.cert-manager.io >/dev/null 2>&1; then
  kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml >/dev/null \
    || skip "could not install cert-manager"
fi
kubectl -n cert-manager rollout status deploy/cert-manager-webhook --timeout=300s >/dev/null 2>&1 \
  || skip "cert-manager webhook never became ready"
ok "cert-manager ready"

if ! kubectl get crd clusters.postgresql.cnpg.io >/dev/null 2>&1; then
  kubectl apply --server-side -f \
    https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.25/releases/cnpg-1.25.1.yaml >/dev/null \
    || skip "could not install CloudNativePG"
fi
kubectl -n "$CNPG_NS" rollout status deploy/cnpg-controller-manager --timeout=300s >/dev/null 2>&1 \
  || skip "the CNPG operator never became ready"
ok "CloudNativePG ready in $CNPG_NS"

kubectl create namespace "$NS" >/dev/null 2>&1
kubectl create namespace "$CNPG_NS" >/dev/null 2>&1

# ---------------------------------------------------------------- the pull path
say "1. credentials for the pull, in BOTH namespaces"
# This is the half the chart README could only describe. The plugin image is
# pulled in the operator's namespace and the mirror image in the Cluster's, and
# a secret in one does nothing for the other.
if [ "$LOCAL_IMAGES" = "1" ]; then
  ok "LOCAL_IMAGES=1 — side-loading, which does NOT exercise the registry"
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
  PULL_ARGS=(--set image.pullSecrets[0].name=ghcr)
  CLUSTER_PULL_SECRETS=$'  imagePullSecrets:\n    - name: ghcr'
else
  skip "no GHCR_TOKEN and LOCAL_IMAGES=0: nothing would be pulled, so nothing would be proven"
fi

# ---------------------------------------------------------------- the plugin
say "2. install the plugin"
helm upgrade --install quicksilver "$REPO/charts/quicksilver" \
  --namespace "$CNPG_NS" \
  --set operatorNamespace="$CNPG_NS" \
  --set image.plugin="$REGISTRY/pg_quicksilver-plugin:$VERSION" \
  --set image.mirror="$REGISTRY/pg_quicksilver-mirror:$VERSION" \
  "${PULL_ARGS[@]}" --wait --timeout 5m >/dev/null \
  || { kubectl -n "$CNPG_NS" get pods; skip "the chart would not install"; }

kubectl -n "$CNPG_NS" rollout status deploy/quicksilver --timeout=300s >/dev/null 2>&1 \
  || { kubectl -n "$CNPG_NS" describe pods -l app.kubernetes.io/name=quicksilver | tail -25
       bad "the plugin Deployment never became ready"; }
ok "plugin running — the -plugin image was PULLED and STARTED"

# ---------------------------------------------------------------- the Cluster
say "3. a Cluster that names the plugin"
cat <<YAML | kubectl -n "$NS" apply -f - >/dev/null
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: $CLUSTER
spec:
  instances: 2
  imageName: ghcr.io/cloudnative-pg/postgresql:17.2-standard-bookworm
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
  # The mirror is told to follow public.events. Nothing else here creates it,
  # and a mirror pointed at a table that does not exist cannot snapshot, so it
  # never reaches ready and the Pod never reaches Ready — the same symptom as
  # the missing Secret, from a different cause. postInitApplicationSQL runs in
  # the application database once initdb finishes, before the sidecar has
  # anything to ask for.
  #
  # A single-column primary key because the plugin refuses anything else, and
  # that refusal is tested in go/internal/plugin.
  bootstrap:
    initdb:
      postInitApplicationSQL:
        - CREATE TABLE IF NOT EXISTS public.events (id bigint PRIMARY KEY, sku text, amount numeric(12,2), ts timestamptz DEFAULT now())
        - INSERT INTO public.events SELECT g, 'SKU-'||g, (g%997)/7.0, now() FROM generate_series(1,1000) g ON CONFLICT DO NOTHING
  postgresql:
    parameters:
      max_slot_wal_keep_size: "1GB"
  plugins:
    - name: quicksilver.howlerops.io
      parameters:
        tables: public.events
        freshnessSLO: 30s
YAML

# The real assertion of this whole script: a Pod, running, with our sidecar in
# it, having pulled our image.
#
# INSTANCE Pods, not every Pod the Cluster owns. cnpg.io/cluster also matches
# the initdb Job's Pod, which has no mirror container in it and never will —
# asking for one there fails with "container quicksilver-mirror is not valid
# for pod app-1-initdb-xxxxx", which reads like the injection is broken.
INSTANCES="cnpg.io/podRole=instance,cnpg.io/cluster=$CLUSTER"

# And a wait that actually waits. `kubectl wait` does NOT wait for a resource to
# be CREATED: with nothing matching the selector it returns an error
# immediately, which this read as "the Pods never became Ready" 39 seconds into
# a 10-minute budget, while CNPG was still running initdb.
deadline=$(( $(date +%s) + TIMEOUT ))
ready=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  n=$(kubectl -n "$NS" get pods -l "$INSTANCES" --no-headers 2>/dev/null | wc -l)
  if [ "$n" -ge 1 ] && kubectl -n "$NS" wait --for=condition=Ready pod -l "$INSTANCES" \
       --timeout=30s >/dev/null 2>&1; then
    ready=1; break
  fi
  sleep 10
done
if [ "$ready" = "1" ]; then
  ok "instance Pods are Ready ($(kubectl -n "$NS" get pods -l "$INSTANCES" --no-headers | wc -l) of them)"
else
  kubectl -n "$NS" get pods
  kubectl -n "$NS" get cluster "$CLUSTER" -o jsonpath='{.status.phase}: {.status.phaseReason}{"\n"}' 2>/dev/null
  kubectl -n "$NS" describe pods -l "$INSTANCES" | grep -A 8 "Events:" | tail -20
  # The reason a container will not start, which is usually the whole answer
  # and is not in the Events list: CreateContainerConfigError names a missing
  # Secret, ImagePullBackOff names the registry.
  kubectl -n "$NS" get pods -l "$INSTANCES" -o jsonpath=\
'{range .items[*]}{.metadata.name}{"\n"}{range .status.initContainerStatuses[*]}  init/{.name}: {.state.waiting.reason} {.state.waiting.message}{"\n"}{end}{range .status.containerStatuses[*]}  {.name}: {.state.waiting.reason} {.state.waiting.message}{"\n"}{end}{end}' 2>/dev/null
  bad "instance Pods never became Ready within ${TIMEOUT}s"
fi

POD=$(kubectl -n "$NS" get pods -l "$INSTANCES" -o name | head -1)
[ -n "$POD" ] || skip "no instance Pod at all"

img=$(kubectl -n "$NS" get "$POD" -o jsonpath='{.spec.initContainers[?(@.name=="quicksilver-mirror")].image}')
if [ -n "$img" ]; then
  ok "the mirror sidecar is in the Pod, image=$img"
else
  kubectl -n "$NS" get "$POD" -o jsonpath='{.spec.initContainers[*].name}{"\n"}'
  bad "no quicksilver-mirror container in the instance Pod"
fi

# Pulled, not merely referenced: the Pod's status carries the imageID it ran.
imgid=$(kubectl -n "$NS" get "$POD" \
  -o jsonpath='{.status.initContainerStatuses[?(@.name=="quicksilver-mirror")].imageID}')
if [ -n "$imgid" ]; then
  ok "PULLED: imageID=$imgid"
else
  bad "the sidecar has no imageID, so nothing was pulled"
fi

# ---------------------------------------------------------------- the rollout
say "4. changing sidecarImage ROLLS the instances"
# Sections 4 and 5 both watch instance Pods. If there are none, they cannot
# report anything except their own timeouts — 20 further minutes of waiting to
# restate what section 3 already said.
if [ $FAIL -ne 0 ]; then
  echo "  skipped: no healthy instance Pods to roll (see above)"
else
# go/fidelity proves CNPG's own comparison reports a difference. Only a cluster
# shows the operator acting on it.
before=$(kubectl -n "$NS" get pods -l "$INSTANCES" \
  -o jsonpath='{range .items[*]}{.metadata.uid}{" "}{end}')
kubectl -n "$NS" patch cluster "$CLUSTER" --type merge -p \
  "{\"spec\":{\"plugins\":[{\"name\":\"quicksilver.howlerops.io\",\"parameters\":{\"tables\":\"public.events\",\"freshnessSLO\":\"30s\",\"sidecarImage\":\"$REGISTRY/pg_quicksilver-mirror:latest\"}}]}}" >/dev/null

rolled=0
for _ in $(seq 60); do
  after=$(kubectl -n "$NS" get pods -l "$INSTANCES" \
    -o jsonpath='{range .items[*]}{.metadata.uid}{" "}{end}')
  [ "$after" != "$before" ] && { rolled=1; break; }
  sleep 10
done
if [ "$rolled" = "1" ]; then
  ok "the operator rolled the instances after sidecarImage changed"
else
  bad "no Pod was replaced within 10 minutes of changing sidecarImage"
fi

fi

# ---------------------------------------------------------------- the probe
say "5. in takeover, a stale mirror LEAVES the -ro endpoints"
if [ $FAIL -ne 0 ]; then
  echo "  skipped: no healthy instance Pods to remove from an endpoint (see above)"
else
# The property docs/33 reasons about from the Kubernetes contract, never
# observed. freshnessSLO is set absurdly low so the mirror cannot satisfy it,
# which is the only deterministic way to make the probe fail on demand.
kubectl -n "$NS" patch cluster "$CLUSTER" --type merge -p \
  '{"spec":{"plugins":[{"name":"quicksilver.howlerops.io","parameters":{"tables":"public.events","mode":"takeover","acknowledgeOLTPRegression":"true","freshnessSLO":"1s"}}]}}' >/dev/null \
  || bad "the Cluster would not accept mode: takeover with the acknowledgement"

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
  eps=$(kubectl -n "$NS" get endpoints "$CLUSTER-ro" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null)
  for pod in $(kubectl -n "$NS" get pods -l "$INSTANCES" -o name 2>/dev/null); do
    phase=$(kubectl -n "$NS" get "$pod" -o jsonpath='{.status.phase}' 2>/dev/null)
    pgready=$(kubectl -n "$NS" get "$pod" -o jsonpath='{.status.containerStatuses[?(@.name=="postgres")].ready}' 2>/dev/null)
    mready=$(kubectl -n "$NS" get "$pod" -o jsonpath='{.status.initContainerStatuses[?(@.name=="quicksilver-mirror")].ready}' 2>/dev/null)
    ip=$(kubectl -n "$NS" get "$pod" -o jsonpath='{.status.podIP}' 2>/dev/null)
    if [ "$phase" = "Running" ] && [ "$pgready" = "true" ] && [ "$mready" = "false" ] \
       && [ -n "$ip" ] && ! printf '%s' "$eps" | tr ' ' '\n' | grep -qx "$ip"; then
      gated=1
      printf '  %s: postgres ready, mirror NOT ready, and absent from %s-ro\n' \
        "${pod#pod/}" "$CLUSTER"
      break 2
    fi
  done
  sleep 10
done
if [ "$gated" = "1" ]; then
  ok "mirror freshness gated the endpoint, with PostgreSQL itself healthy — docs/33"
else
  kubectl -n "$NS" get endpoints "$CLUSTER-ro" -o wide 2>/dev/null
  kubectl -n "$NS" get pods -l "$INSTANCES" -o jsonpath=\
'{range .items[*]}{.metadata.name} phase={.status.phase} ip={.status.podIP}{"\n"}{range .status.initContainerStatuses[*]}  init/{.name} ready={.ready}{"\n"}{end}{range .status.containerStatuses[*]}  {.name} ready={.ready}{"\n"}{end}{end}' 2>/dev/null
  bad "never saw a Pod with PostgreSQL ready, the mirror not ready, and out of -ro"
fi

fi

say "result"
if [ $FAIL -eq 0 ]; then
  echo "PASS — Pods pulled both images and ran, a sidecarImage change rolled them,"
  echo "and mirror freshness gated the -ro endpoint."
else
  echo "FAIL"
fi
exit $FAIL
