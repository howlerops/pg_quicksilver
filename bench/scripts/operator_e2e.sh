#!/usr/bin/env bash
# Run the plugin against the REAL CloudNativePG operator.
#
# Everything the plugin knew about CNPG-I until now was checked against the
# CONTRACT — generated stubs, envtest, the machinery's own helpers (docs/17).
# The gap between "matches the interface" and "the operator actually calls it"
# is where the TYPE_EVALUATE bug lived: capabilities that satisfied every type
# check and made CNPG skip the hook silently.
#
# This closes that gap. What it proves, in order:
#
#   1. the operator DISCOVERS the plugin from the Service label alone
#   2. a Cluster naming it is admitted by the real admission webhooks
#   3. the CNPG-I handshake completes over mTLS and the operator records our
#      capabilities in .status.pluginStatus
#   4. the operator calls the lifecycle hook while building the instance Pod
#   5. the sidecar is IN that Pod, as a native sidecar, with every value
#      rendered from the Cluster spec
#
# ---------------------------------------------------------------------------
# The substitution, stated plainly rather than hidden
#
# This sandbox does not grant CAP_SYS_RESOURCE, and without it a process cannot
# LOWER its oom_score_adj. runc sets a pod sandbox's to -998, so every pod fails
# at nsexec with "Permission denied" — identically under kind and under k3s.
# Kubernetes' control plane runs here; its PODS do not.
#
# So the API server is real (k3s), the CRDs are real (CNPG 1.30), the operator
# is the real binary from the released image, and the plugin is ours — but the
# operator and the plugin run as HOST PROCESSES, and there is no kubelet. Pods
# are created and stay Pending. This script stands in for the kubelet at exactly
# one point, marking the initdb Job complete, and says so where it does it.
#
# Everything up to and including the Pod SPEC the operator builds is real, which
# is precisely where a plugin lives.
#
# Prerequisites, all of which bench/scripts/operator_setup.sh provides:
#   - k3s running with --kubelet-arg=fail-cgroupv1=false
#   - CNPG 1.30 CRDs and webhook configurations installed
#   - /tmp/cnpg-manager and /operator/manager_amd64 from the released image
#
#   bash bench/scripts/operator_e2e.sh
set -uo pipefail
cd "$(dirname "$0")/../.."

CNPG_NS=cnpg-system
APP_NS=qs-e2e
PLUGIN_SVC=quicksilver
PLUGIN_PORT=9090
WORK=/tmp/qs-operator-e2e
CERTS=$WORK/webhook-certs
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
FAIL=0

say()  { printf '\n=== %s ===\n' "$*"; }
ok()   { printf '  ok: %s\n' "$*"; }
note() { printf '  -- %s\n' "$*"; }
bad()  { printf '  FAIL: %s\n' "$*"; FAIL=1; }
skip() { printf '\nINCOMPLETE — skipped, which is NOT a pass: %s\n' "$*"; exit 2; }

# PIDs, not pkill by name: "quicksilver-plugin" is longer than the 15 characters
# pgrep -x matches on, and pkill -f matches this script's own command line. Both
# traps this project has already paid for once (docs/19).
PLUGIN_PID=""; OPERATOR_PID=""
cleanup() {
  [ -n "$PLUGIN_PID" ] && kill "$PLUGIN_PID" 2>/dev/null
  [ -n "$OPERATOR_PID" ] && kill "$OPERATOR_PID" 2>/dev/null
  return 0
}
trap cleanup EXIT

# The plugin's name comes from the chart, which takes it from the plugin.
# Retyping it is how a registration silently fails to match: the Service label
# and .spec.plugins[].name must agree EXACTLY, and a mismatch shows up as "the
# plugin was never called" rather than as an error anywhere.
PLUGIN_NAME=$(sed -n 's/.*define "quicksilver.pluginName" -}}\([^{]*\){{-.*/\1/p' \
  charts/quicksilver/templates/_helpers.tpl)
[ -n "$PLUGIN_NAME" ] || skip "could not read the plugin name from the chart"

command -v kubectl >/dev/null || skip "kubectl not installed"
kubectl get nodes >/dev/null 2>&1 || skip "no reachable API server — run operator_setup.sh"
[ -x /tmp/cnpg-manager ] || skip "operator binary missing — run operator_setup.sh"
[ -f /operator/manager_amd64 ] || skip "/operator/manager_amd64 missing — run operator_setup.sh"

say "the cluster and the real CNPG CRDs"
kubectl get crd clusters.postgresql.cnpg.io >/dev/null 2>&1 || skip "CNPG CRDs are not installed"
ok "$(kubectl version -o json 2>/dev/null | python3 -c 'import json,sys;print("API server "+json.load(sys.stdin)["serverVersion"]["gitVersion"])' 2>/dev/null || echo 'API server reachable')"
ok "$(kubectl get crd -o name | grep -c cnpg.io) cnpg.io CRDs, operator $(/tmp/cnpg-manager version 2>&1 | sed 's/.*Version:\([^ ]*\).*/\1/')"
kubectl delete ns $APP_NS --ignore-not-found --wait=true >/dev/null 2>&1
kubectl create namespace $APP_NS >/dev/null 2>&1

say "mTLS material, exactly as the chart provisions it"
rm -rf $WORK; mkdir -p $CERTS
cat > $WORK/openssl.cnf <<EOF
[req]
distinguished_name = dn
[dn]
[server_ext]
subjectAltName = DNS:$PLUGIN_SVC, DNS:$PLUGIN_SVC.$CNPG_NS, DNS:$PLUGIN_SVC.$CNPG_NS.svc, DNS:localhost, IP:127.0.0.1
extendedKeyUsage = serverAuth
[client_ext]
extendedKeyUsage = clientAuth
EOF
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=qs-ca" \
  -keyout $WORK/ca.key -out $WORK/ca.crt >/dev/null 2>&1
for who in server client; do
  openssl req -newkey rsa:2048 -nodes -subj "/CN=$who" \
    -keyout $WORK/$who.key -out $WORK/$who.csr >/dev/null 2>&1
  openssl x509 -req -in $WORK/$who.csr -CA $WORK/ca.crt -CAkey $WORK/ca.key \
    -CAcreateserial -days 1 -extfile $WORK/openssl.cnf -extensions ${who}_ext \
    -out $WORK/$who.crt >/dev/null 2>&1
done
[ -s $WORK/server.crt ] && [ -s $WORK/client.crt ] || skip "certificate generation failed"
kubectl create secret generic quicksilver-server-tls -n $CNPG_NS \
  --from-file=tls.crt=$WORK/server.crt --from-file=tls.key=$WORK/server.key \
  --from-file=ca.crt=$WORK/ca.crt --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl create secret generic quicksilver-client-tls -n $CNPG_NS \
  --from-file=tls.crt=$WORK/client.crt --from-file=tls.key=$WORK/client.key \
  --from-file=ca.crt=$WORK/ca.crt --dry-run=client -o yaml | kubectl apply -f - >/dev/null
ok "CA, server cert (SAN $PLUGIN_SVC.$CNPG_NS.svc), client cert, both secrets"

say "the Service that IS the registration"
# No CRD and no operator config: CNPG lists Services in its OWN namespace, and
# any that carries cnpg.io/pluginName becomes a plugin a Cluster may name.
kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Service
metadata:
  name: $PLUGIN_SVC
  namespace: $CNPG_NS
  labels:
    cnpg.io/pluginName: $PLUGIN_NAME
  annotations:
    cnpg.io/pluginPort: "$PLUGIN_PORT"
    cnpg.io/pluginClientSecret: quicksilver-client-tls
    cnpg.io/pluginServerSecret: quicksilver-server-tls
spec:
  ports:
    - name: grpc
      port: $PLUGIN_PORT
      targetPort: $PLUGIN_PORT
EOF
# The operator dials the Service by DNS. Running on the host it uses /etc/hosts,
# so the plugin is reachable at the name CNPG will actually use.
for n in "$PLUGIN_SVC" "$PLUGIN_SVC.$CNPG_NS" "$PLUGIN_SVC.$CNPG_NS.svc" \
         "$PLUGIN_SVC.$CNPG_NS.svc.cluster.local"; do
  grep -qE "^127\.0\.0\.1[[:space:]]+$n\$" /etc/hosts || echo "127.0.0.1 $n" >> /etc/hosts
done
ok "Service labelled $PLUGIN_NAME, resolvable on this host"

say "the plugin, listening"
go -C go build -o /tmp/quicksilver-plugin ./cmd/quicksilver-plugin || skip "plugin build failed"
/tmp/quicksilver-plugin \
  --server-cert=$WORK/server.crt --server-key=$WORK/server.key \
  --client-cert=$WORK/ca.crt --server-address=:$PLUGIN_PORT \
  > $WORK/plugin.log 2>&1 &
PLUGIN_PID=$!
sleep 3
kill -0 "$PLUGIN_PID" 2>/dev/null || { tail -10 $WORK/plugin.log; skip "plugin did not start"; }
# Prove the mTLS the operator is about to attempt, BEFORE blaming the operator
# for failing it.
if timeout 15 openssl s_client -connect 127.0.0.1:$PLUGIN_PORT -cert $WORK/client.crt \
     -key $WORK/client.key -CAfile $WORK/ca.crt -alpn h2 </dev/null 2>&1 \
     | grep -q "Verify return code: 0"; then
  ok "plugin on :$PLUGIN_PORT — mTLS verified, ALPN h2 negotiated"
else
  bad "the plugin's own mTLS does not verify with the client cert CNPG will use"
fi

say "the webhook path, which the operator will not start without"
# The operator's webhooks are normally served by the operator POD behind a
# Service. Neither exists here, so the Service is pointed at this host. The
# endpoint port is UNNAMED because the Service's port is unnamed: the API server
# matches endpoint ports to service ports BY NAME, and a named endpoint port
# against an unnamed service port resolves to nothing — reported as "no
# endpoints available for service", which reads like the backend being down.
NODE_IP=$(kubectl get node -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
kubectl -n $CNPG_NS patch svc cnpg-webhook-service --type=json \
  -p '[{"op":"remove","path":"/spec/selector"}]' >/dev/null 2>&1
kubectl -n $CNPG_NS delete endpointslice -l kubernetes.io/service-name=cnpg-webhook-service >/dev/null 2>&1
kubectl apply -f - >/dev/null 2>&1 <<EOF
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: cnpg-webhook-service-host
  namespace: $CNPG_NS
  labels:
    kubernetes.io/service-name: cnpg-webhook-service
addressType: IPv4
ports:
  - port: 9443
    protocol: TCP
endpoints:
  - addresses: ["$NODE_IP"]
    conditions: {ready: true}
EOF
ok "the webhook Service now resolves to this host ($NODE_IP:9443)"

# env -u: gRPC honours HTTPS_PROXY, and with one set the operator dials the
# PROXY instead of the plugin, reporting a TLS reset from an address that is
# neither of them. cd /: the architecture list is found by the RELATIVE glob
# "operator/manager_*", and without it every status update fails with
# "invalid architecture: amd64".
start_operator() {
  (cd / && env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy \
     -u ALL_PROXY -u all_proxy \
     KUBECONFIG=$KUBECONFIG OPERATOR_NAMESPACE=$CNPG_NS WEBHOOK_CERT_DIR=$CERTS \
     /tmp/cnpg-manager controller --webhook-port=9443 --metrics-bind-address=0 \
     > "$1" 2>&1) &
  OPERATOR_PID=$!
}

say "the real operator, reconciling"
# The operator refuses to start until the webhook certificate it issued ITSELF
# appears in WEBHOOK_CERT_DIR — in a pod that is a mounted secret, and it waits
# for the kubelet to refresh it ("secrets mount still not refreshed"). So let it
# run once to mint the secret, then hand that secret back as a directory.
start_operator $WORK/operator-mint.log
for _ in $(seq 40); do
  kubectl -n $CNPG_NS get secret cnpg-webhook-cert >/dev/null 2>&1 && break; sleep 2
done
kill "$OPERATOR_PID" 2>/dev/null; wait "$OPERATOR_PID" 2>/dev/null
kubectl -n $CNPG_NS get secret cnpg-webhook-cert >/dev/null 2>&1 \
  || { tail -3 $WORK/operator-mint.log; skip "the operator never minted its webhook certificate"; }
kubectl -n $CNPG_NS get secret cnpg-webhook-cert -o jsonpath='{.data.tls\.crt}' | base64 -d > $CERTS/tls.crt
kubectl -n $CNPG_NS get secret cnpg-webhook-cert -o jsonpath='{.data.tls\.key}' | base64 -d > $CERTS/tls.key
# The webhook SERVER serves apiserver.crt, not tls.crt. Anything else there
# fails with "certificate is not valid for any names".
cp $CERTS/tls.crt $CERTS/apiserver.crt
cp $CERTS/tls.key $CERTS/apiserver.key
start_operator $WORK/operator.log
for _ in $(seq 60); do grep -q "Starting workers" $WORK/operator.log 2>/dev/null && break; sleep 1; done
grep -q "Starting workers" $WORK/operator.log \
  || { tail -3 $WORK/operator.log; skip "operator did not start"; }
ok "CloudNativePG running, controllers started, webhooks served from this host"

say "1. the operator discovers the plugin from the Service label alone"
for _ in $(seq 30); do grep -q "Registered plugin" $WORK/operator.log && break; sleep 1; done
if grep -q "Registered plugin" $WORK/operator.log; then
  ok "\"Registered plugin\" — pluginName=$PLUGIN_NAME"
else
  bad "the operator never registered the plugin"
fi

say "2. a Cluster that names the plugin, through the real admission webhooks"
kubectl apply -f - 2>&1 <<EOF | sed 's/^/  /'
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: shadow
  namespace: $APP_NS
spec:
  instances: 2
  storage:
    size: 1Gi
  plugins:
    - name: $PLUGIN_NAME
      parameters:
        mode: shadow
        tables: "public.events"
        freshnessSLO: "60s"
EOF
kubectl -n $APP_NS get cluster shadow >/dev/null 2>&1 \
  && ok "admitted by the real mutating and validating webhooks" \
  || bad "the Cluster was rejected"

say "3. the handshake, and what the operator recorded about us"
for _ in $(seq 40); do
  kubectl -n $APP_NS get cluster shadow -o jsonpath='{.status.pluginStatus}' 2>/dev/null \
    | grep -q "$PLUGIN_NAME" && break
  sleep 2
done
STATUS=$(kubectl -n $APP_NS get cluster shadow -o jsonpath='{.status.pluginStatus}' 2>/dev/null)
if echo "$STATUS" | grep -q "$PLUGIN_NAME"; then
  ok "the operator wrote our capabilities into .status.pluginStatus:"
  echo "$STATUS" | python3 -c "
import json,sys
for p in json.load(sys.stdin):
    print('       name                :', p.get('name'), ' version:', p.get('version'))
    print('       capabilities        :', ', '.join(p.get('capabilities') or []))
    print('       operatorCapabilities:', ', '.join(p.get('operatorCapabilities') or []))
" 2>/dev/null
else
  bad "the operator never recorded the plugin's capabilities"
  grep -oE '"error":"[^"]{0,160}' $WORK/operator.log | sort -u | tail -2 | sed 's/^/    /'
fi

say "4. standing in for the kubelet, once"
# THE one substitution. The bootstrap Job's pod cannot start here, so the
# operator would wait for it forever and never build an instance Pod — which is
# the thing this script exists to inspect. Marking the Job complete is exactly
# what a kubelet's success would have done, and nothing else here is faked.
for _ in $(seq 30); do kubectl -n $APP_NS get job shadow-1-initdb >/dev/null 2>&1 && break; sleep 2; done
if kubectl -n $APP_NS get job shadow-1-initdb >/dev/null 2>&1; then
  T=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  kubectl -n $APP_NS patch job shadow-1-initdb --subresource=status --type=merge \
    -p "{\"status\":{\"active\":0,\"succeeded\":1,\"completionTime\":\"$T\",\"conditions\":[
         {\"type\":\"SuccessCriteriaMet\",\"status\":\"True\",\"lastProbeTime\":\"$T\",\"lastTransitionTime\":\"$T\",\"reason\":\"CompletionsReached\"},
         {\"type\":\"Complete\",\"status\":\"True\",\"lastProbeTime\":\"$T\",\"lastTransitionTime\":\"$T\",\"reason\":\"CompletionsReached\"}]}}" >/dev/null 2>&1
  note "marked the initdb Job complete — the only thing this script fakes"
else
  bad "the operator never created the bootstrap Job"
fi

say "5. the instance Pod the operator built"
for _ in $(seq 40); do kubectl -n $APP_NS get pod shadow-1 >/dev/null 2>&1 && break; sleep 2; done
if ! kubectl -n $APP_NS get pod shadow-1 >/dev/null 2>&1; then
  bad "no instance Pod was created"
else
  kubectl -n $APP_NS get pod shadow-1 -o json > $WORK/pod.json
  python3 <<'PYPOD'
import json, sys
d = json.load(open("/tmp/qs-operator-e2e/pod.json"))
spec = d["spec"]
print("  containers     :", ", ".join(c["name"] for c in spec.get("containers", [])))
print("  initContainers :", ", ".join(c["name"] for c in spec.get("initContainers", [])))
side = next((c for c in spec.get("initContainers", []) + spec.get("containers", [])
             if c["name"] == "quicksilver-mirror"), None)
if side is None:
    print("  FAIL: the sidecar is not in the Pod — the lifecycle hook did not fire")
    sys.exit(3)
print("  ok: THE SIDECAR IS IN A POD THE REAL OPERATOR BUILT")
print("      image        :", side.get("image"))
# A native sidecar is an initContainer with restartPolicy Always. Anything else
# is an init STEP: it would run once, exit, and never mirror a thing.
rp = side.get("restartPolicy")
print("      restartPolicy:", rp, "(Always = a native sidecar, not an init step)")
if rp != "Always":
    print("  FAIL: injected as an init step rather than a sidecar")
    sys.exit(3)
print("      env rendered from the Cluster spec:")
for e in side.get("env", []):
    v = e.get("value")
    if v is None:
        v = "<from %s>" % ",".join((e.get("valueFrom") or {}).keys())
    print("        %-18s = %s" % (e["name"], v))
PYPOD
  [ $? -eq 0 ] || FAIL=1
fi

say "what this run did NOT cover"
note "the kubelet: no pod ever runs, so no PostgreSQL, no WAL, no mirror"
note "failover, switchover and rollouts, which need running instances"
note "multi-node scheduling, affinity and PodDisruptionBudgets"
note "e2e_mirror.sh covers the PostgreSQL half, with a real primary and standby"

printf '\n========================================\n'
[ $FAIL -eq 0 ] && echo "PASS" || echo "FAIL"
printf 'operator log: %s\nplugin log:   %s\n' "$WORK/operator.log" "$WORK/plugin.log"
exit $FAIL
