#!/usr/bin/env bash
# Validate everything we would apply to a cluster, without a cluster.
#
# Two separate checks, because they catch different mistakes:
#
#   kubeconform   the Helm chart's output against the real Kubernetes API
#                 schemas — a Deployment with a misspelled field, a Service with
#                 the wrong port shape. helm lint does NOT do this; it only
#                 checks that the templates render.
#   CRD schema    examples/cluster-shadow.yaml against CloudNativePG's ACTUAL
#                 published CRD — the same schema the API server enforces on
#                 `kubectl apply`, so a wrong type or a malformed plugin block
#                 fails here rather than on the first real deployment.
#
#                 Note what this does NOT do, because the first version of this
#                 script implied otherwise: a CRD schema does not REJECT unknown
#                 fields, it PRUNES them. `instancesss: 3` validates cleanly and
#                 then silently disappears. So the pruning check below is
#                 separate, and it is the one that catches a typo'd field name.
#
# Neither needs a cluster, a container runtime, or CloudNativePG installed.
set -uo pipefail
cd "$(dirname "$0")/../.."
FAIL=0
CRD_URL=https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.25/config/crd/bases/postgresql.cnpg.io_clusters.yaml
CACHE=/tmp/cnpg-clusters-crd.yaml

say() { printf '\n== %s ==\n' "$*"; }
bad() { printf 'FAIL: %s\n' "$*"; FAIL=1; }

say "1. Helm renders, and renders valid Kubernetes objects"
for args in \
  "" \
  "--set certManager.enabled=false --set existingSecrets.server=s --set existingSecrets.client=c" \
  "--set certManager.issuerRef.name=my-ca --set certManager.issuerRef.kind=ClusterIssuer" \
  "--set operatorNamespace=postgresql-operator-system --set replicaCount=2"
do
  label="${args:-defaults}"
  out=$(helm template qs charts/quicksilver $args 2>&1)
  if [ $? -ne 0 ]; then bad "helm template failed for: $label"; echo "$out" | tail -3; continue; fi
  # cert-manager is a CRD we do not vendor; skipping it keeps the check honest
  # about what it did and did not verify.
  if echo "$out" | kubeconform -strict -summary -ignore-missing-schemas \
       -kubernetes-version 1.31.0 2>&1 | sed 's/^/  /'; then
    echo "  ok: $label"
  else
    bad "kubeconform rejected the output for: $label"
  fi
done

say "2. The example Cluster against CloudNativePG's real CRD"
if [ ! -s "$CACHE" ]; then curl -sSL -o "$CACHE" "$CRD_URL" || { bad "could not fetch the CNPG CRD"; }; fi
if [ -s "$CACHE" ]; then
  python3 - "$CACHE" examples/cluster-shadow.yaml <<'PY' || FAIL=1
import sys, yaml, jsonschema

crd_path, doc_path = sys.argv[1], sys.argv[2]
crd = yaml.safe_load(open(crd_path))
schema = None
for v in crd["spec"]["versions"]:
    if v["name"] == "v1":
        schema = v["schema"]["openAPIV3Schema"]
if schema is None:
    print("FAIL: no v1 schema in the CRD"); sys.exit(1)

doc = yaml.safe_load(open(doc_path))
try:
    jsonschema.validate(doc, schema)
except jsonschema.ValidationError as e:
    print("FAIL: the API server would reject this manifest")
    print("  path:", "/".join(str(p) for p in e.absolute_path) or "(root)")
    print("  ", e.message[:300])
    sys.exit(1)

# Unknown fields are pruned by the API server, not rejected, so validation alone
# would pass a manifest whose key half never takes effect. Walk the document and
# name anything that would vanish.
def prunable(node, sch, path=""):
    if not isinstance(node, dict) or not isinstance(sch, dict):
        return []
    if sch.get("x-kubernetes-preserve-unknown-fields"):
        return []
    props = sch.get("properties")
    if props is None:
        addl = sch.get("additionalProperties")
        if isinstance(addl, dict):
            out = []
            for k, v in node.items():
                out += prunable(v, addl, f"{path}/{k}")
            return out
        return []
    out = []
    for k, v in node.items():
        if k not in props:
            out.append(f"{path}/{k}")
            continue
        sub = props[k]
        if isinstance(v, dict):
            out += prunable(v, sub, f"{path}/{k}")
        elif isinstance(v, list) and isinstance(sub.get("items"), dict):
            for i, item in enumerate(v):
                out += prunable(item, sub["items"], f"{path}/{k}[{i}]")
    return out

pruned = prunable(doc, schema)
if pruned:
    print("FAIL: these fields would be SILENTLY PRUNED by the API server:")
    for f in pruned:
        print("  ", f)
    sys.exit(1)
print("  ok: no field would be silently pruned")

plugins = doc["spec"].get("plugins", [])
if not plugins:
    print("FAIL: the example does not actually enable the plugin"); sys.exit(1)
print(f"  ok: {doc['kind']}/{doc['metadata']['name']} valid against the v1 CRD")
print(f"  plugin: {plugins[0]['name']}")
for k, v in sorted(plugins[0].get("parameters", {}).items()):
    print(f"    {k}: {v}")
PY
fi

say "3. Every parameter in the example is one the plugin knows"
python3 - <<'PY' || FAIL=1
import re, sys, yaml
doc = yaml.safe_load(open("examples/cluster-shadow.yaml"))
params = set(doc["spec"]["plugins"][0].get("parameters", {}))
src = open("go/internal/plugin/config.go").read()
block = re.search(r"known := map\[string\]bool\{(.*?)\n\t\}", src, re.S).group(1)
known = set(re.findall(r'"([^"]+)"', block))
known |= set(re.findall(r'AcknowledgeParam\s*=\s*"([^"]+)"', src))
unknown = params - known
if unknown:
    # An example that the plugin's own validator would reject is worse than no
    # example: it is a documented way to fail admission.
    print("FAIL: the example uses parameters the plugin rejects:", sorted(unknown)); sys.exit(1)
print("  ok: all", len(params), "example parameters are accepted by the validator")
PY

printf '\n========================================\n'
[ $FAIL -eq 0 ] && echo "PASS" || echo "FAIL"
exit $FAIL
