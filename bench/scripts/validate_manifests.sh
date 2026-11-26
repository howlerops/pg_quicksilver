#!/usr/bin/env bash
# Validate everything we would apply to a cluster, without a cluster.
#
# Two separate checks, because they catch different mistakes:
#
#   kubeconform   the Helm chart's output against the real Kubernetes API
#                 schemas — a Deployment with a misspelled field, a Service with
#                 the wrong port shape. helm lint does NOT do this; it only
#                 checks that the templates render.
#   CRD schema    every Cluster in examples/ against CloudNativePG's ACTUAL
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
#   rendering     what the chart renders is what its values say: the mirror
#                 image reaches the plugin, the registration labels are the
#                 ones CloudNativePG looks for, the notes name the operand
#                 image. The chart once shipped a value nothing consumed.
#   agreement     the things that live in different files and have to be the
#                 same: the operand image in the chart and in the routed
#                 example, the directory the Dockerfile fills and the Cluster
#                 names, the order of shared_preload_libraries, one version on
#                 everything a release publishes. Each of these fails quietly
#                 on a cluster and loudly here.
#
# None of it needs a cluster, a container runtime, or CloudNativePG installed.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 2
FAIL=0
CRD_URL=https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.25/config/crd/bases/postgresql.cnpg.io_clusters.yaml
CACHE=/tmp/cnpg-clusters-crd.yaml

say() { printf '\n== %s ==\n' "$*"; }
bad() { printf 'FAIL: %s\n' "$*"; FAIL=1; }

# A MISSING TOOL IS NOT A FAILING CHART. Without this, a runner that has helm
# but not kubeconform reported "FAIL manifests (exit 1)" — which reads as a
# broken chart and is actually a missing binary. exit 2 is the suite's
# INCOMPLETE, which says the honest thing: this told us nothing.
for tool in helm kubeconform; do
  command -v "$tool" >/dev/null 2>&1 || {
    printf '\nINCOMPLETE — skipped, which is NOT a pass: %s is not installed, so\n' "$tool"
    printf 'the chart was not validated. Install it rather than reading this as a pass.\n'
    exit 2
  }
done

say "1. Helm renders, and renders valid Kubernetes objects"
for args in \
  "" \
  "--set certManager.enabled=false --set existingSecrets.server=s --set existingSecrets.client=c" \
  "--set certManager.issuerRef.name=my-ca --set certManager.issuerRef.kind=ClusterIssuer" \
  "--set operatorNamespace=postgresql-operator-system --set replicaCount=2" \
  "--set image.mirror=registry.example/m:1 --set image.postgres=registry.example/p:17-1 --set image.pullSecrets[0].name=regcred"
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

say "2. The example Clusters against CloudNativePG's real CRD"
if [ ! -s "$CACHE" ]; then curl -sSL -o "$CACHE" "$CRD_URL" || { bad "could not fetch the CNPG CRD"; }; fi
if [ -s "$CACHE" ]; then
  for example in examples/*.yaml; do
  echo "  $example"
  python3 - "$CACHE" "$example" <<'PY' || FAIL=1
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
  done
fi

say "3. Every parameter in the examples is one the plugin knows"
python3 - <<'PY' || FAIL=1
import glob, re, sys, yaml
src = open("go/internal/plugin/config.go").read()
block = re.search(r"known := map\[string\]bool\{(.*?)\n\t\}", src, re.S).group(1)
known = set(re.findall(r'"([^"]+)"', block))
known |= set(re.findall(r'AcknowledgeParam\s*=\s*"([^"]+)"', src))
bad = 0
for path in sorted(glob.glob("examples/*.yaml")):
    doc = yaml.safe_load(open(path))
    params = doc["spec"]["plugins"][0].get("parameters", {})
    unknown = set(params) - known
    if unknown:
        # An example that the plugin's own validator would reject is worse than
        # no example: it is a documented way to fail admission.
        print(f"FAIL: {path} uses parameters the plugin rejects:", sorted(unknown)); bad = 1
        continue
    # Plugin parameters are map[string]string. `ducklake: true` without the
    # quotes is a YAML boolean, and the API server refuses the whole Cluster.
    notstr = sorted(k for k, v in params.items() if not isinstance(v, str))
    if notstr:
        print(f"FAIL: {path}: these parameters are not strings (quote them):", notstr); bad = 1
        continue
    print(f"  ok: {path}: all {len(params)} parameters are accepted by the validator")
sys.exit(bad)
PY

say "4. What the chart renders is what its values say"
# The template tests. `helm template` for the objects. The notes are a
# template too, and neither `helm template` nor a dry-run install will show
# them without a cluster to ask for its version, so they are rendered through
# a ConfigMap added to a COPY of the chart: same engine, same values.
RENDER=$(mktemp -d)
helm template qs charts/quicksilver > "$RENDER/default.yaml" 2>"$RENDER/err" \
  || { bad "helm template (defaults)"; cat "$RENDER/err"; }
helm template qs charts/quicksilver --set image.mirror=registry.example/m:1 \
  --set image.plugin=registry.example/p:2 --set service.port=7777 \
  --set operatorNamespace=elsewhere > "$RENDER/override.yaml" 2>"$RENDER/err" \
  || { bad "helm template (overrides)"; cat "$RENDER/err"; }
cp -r charts/quicksilver "$RENDER/chart"
cat > "$RENDER/chart/templates/notes-under-test.yaml" <<'TPL'
apiVersion: v1
kind: ConfigMap
metadata:
  name: notes-under-test
data:
  notes: |
{{ include (print $.Template.BasePath "/NOTES.txt") . | indent 4 }}
TPL
helm template qs "$RENDER/chart" -s templates/notes-under-test.yaml \
  --set image.postgres=registry.example/pg:17-9 > "$RENDER/notes.yaml" 2>"$RENDER/err" \
  || { bad "rendering NOTES.txt"; cat "$RENDER/err"; }
# Without cert-manager and without Secrets the chart must REFUSE to render,
# not render a Deployment that mounts Secrets nobody made.
if helm template qs charts/quicksilver --set certManager.enabled=false >/dev/null 2>"$RENDER/err"; then
  bad "the chart rendered with certManager.enabled=false and no existingSecrets"
elif grep -q "existingSecrets" "$RENDER/err"; then
  echo "  ok: certManager.enabled=false without existingSecrets is refused, and says which value"
else
  bad "the refusal for missing existingSecrets does not name the value"; cat "$RENDER/err"
fi
python3 - "$RENDER" <<'PY' || FAIL=1
import sys, yaml
d = sys.argv[1]
values = yaml.safe_load(open("charts/quicksilver/values.yaml"))
bad = 0
def check(cond, msg):
    global bad
    print(("  ok: " if cond else "FAIL: ") + msg)
    if not cond: bad = 1
def objs(name):
    return {(o["kind"], o["metadata"]["name"]): o for o in yaml.safe_load_all(open(f"{d}/{name}")) if o}
def env(dep):
    c = dep["spec"]["template"]["spec"]["containers"][0]
    return c, {e["name"]: e.get("value") for e in c.get("env", [])}

o = objs("default.yaml")
dep, svc = o[("Deployment", "quicksilver")], o[("Service", "quicksilver")]
c, e = env(dep)
check(c["image"] == values["image"]["plugin"], f"the Deployment runs image.plugin ({c['image']})")
# The value that was once declared and consumed by nothing.
check(e.get("QS_DEFAULT_SIDECAR_IMAGE") == values["image"]["mirror"],
      f"image.mirror reaches the plugin as QS_DEFAULT_SIDECAR_IMAGE ({e.get('QS_DEFAULT_SIDECAR_IMAGE')})")
check(svc["metadata"]["labels"].get("cnpg.io/pluginName") == "quicksilver.howlerops.io",
      "the Service carries cnpg.io/pluginName=quicksilver.howlerops.io, which is the whole registration")
ann = svc["metadata"]["annotations"]
check(ann.get("cnpg.io/pluginPort") == str(values["service"]["port"]), "cnpg.io/pluginPort is the Service port, as a string")
secrets = {v["secret"]["secretName"] for v in dep["spec"]["template"]["spec"]["volumes"]}
check({ann.get("cnpg.io/pluginClientSecret"), ann.get("cnpg.io/pluginServerSecret")} == secrets,
      "the Secrets the Service advertises to the operator are the ones the Deployment mounts")
check(dep["metadata"]["namespace"] == svc["metadata"]["namespace"] == values["operatorNamespace"],
      "Deployment and Service are in operatorNamespace")
sc = c["securityContext"]
check(sc.get("readOnlyRootFilesystem") is True and sc.get("allowPrivilegeEscalation") is False,
      "the plugin container is read-only and cannot escalate")

o = objs("override.yaml")
c, e = env(o[("Deployment", "quicksilver")])
check(c["image"] == "registry.example/p:2" and e.get("QS_DEFAULT_SIDECAR_IMAGE") == "registry.example/m:1",
      "--set image.plugin and image.mirror both take effect")
check(all(x["metadata"]["namespace"] == "elsewhere" for x in o.values() if "namespace" in x["metadata"]),
      "--set operatorNamespace moves every namespaced object")
check(o[("Service", "quicksilver")]["metadata"]["annotations"]["cnpg.io/pluginPort"] == "7777"
      and f"--server-address=:7777" in c["args"], "--set service.port reaches the annotation and the server")

notes = (yaml.safe_load(open(f"{d}/notes.yaml")) or {}).get("data", {}).get("notes", "")
check("registry.example/pg:17-9" in notes, "the notes name image.postgres, which is the only place the chart uses it")
check('["pg_duckdb", "pg_quicksilver"]' in notes, "the notes give shared_preload_libraries in the order that starts")
check("duckdb.extension_directory" in notes and "max_slot_wal_keep_size" in notes,
      "the notes name duckdb.extension_directory and max_slot_wal_keep_size")
sys.exit(bad)
PY
rm -rf "$RENDER"

say "5. The chart, the examples, the Dockerfile and the test agree"
python3 - <<'PY' || FAIL=1
import re, sys, yaml
bad = 0
def check(cond, msg):
    global bad
    print(("  ok: " if cond else "FAIL: ") + msg)
    if not cond: bad = 1

chart = yaml.safe_load(open("charts/quicksilver/Chart.yaml"))
values = yaml.safe_load(open("charts/quicksilver/values.yaml"))
routed = yaml.safe_load(open("examples/cluster-routed.yaml"))
shadow = yaml.safe_load(open("examples/cluster-shadow.yaml"))
dockerfile = open("deploy/Dockerfile").read()
e2e = open("bench/scripts/cluster_e2e.sh").read()
notes = open("charts/quicksilver/templates/NOTES.txt").read()

# One version on everything a release publishes: release.yml refuses to
# publish otherwise, and this says so on the pull request instead of on the tag.
v = str(chart["version"])
tag = lambda image: image.rsplit(":", 1)[1]
check(str(chart["appVersion"]) == v, f"Chart.yaml version and appVersion are both {v}")
check(tag(values["image"]["plugin"]) == v and tag(values["image"]["mirror"]) == v,
      f"image.plugin and image.mirror are pinned at {v}")

# The operand image's tag is read by CloudNativePG as a PostgreSQL version:
# machinery/pkg/postgres/version.FromTag takes ^(\d\.?)+ and its first number
# is the major. "0.0.4" is PostgreSQL 0; "dev" is refused.
pg = values["image"]["postgres"]
major = re.search(r"^ARG PG_MAJOR=(\d+)$", dockerfile, re.M).group(1)
m = re.match(r"^(\d\.?)+", tag(pg))
check(bool(m) and m.group(0).split(".")[0] == major,
      f"image.postgres's tag ({tag(pg)}) starts with the PostgreSQL major, {major}, as CloudNativePG reads it")
check(tag(pg) == f"{major}-{v}", f"image.postgres is pinned at {major}-{v}")
base = re.search(r"^ARG CNPG_BASE=(\S+)$", dockerfile, re.M).group(1)
check(tag(base).split(".")[0] == major and re.match(r"^\d+\.\d+-", tag(base)) is not None,
      f"the operand image is built on a pinned PostgreSQL {major} minor ({base})")

check(routed["spec"]["imageName"] == pg, "examples/cluster-routed.yaml uses the chart's image.postgres")
check("cloudnative-pg/postgresql:" in shadow["spec"]["imageName"]
      and "shared_preload_libraries" not in shadow["spec"].get("postgresql", {}),
      "examples/cluster-shadow.yaml keeps the stock image and preloads nothing: mirroring alone needs neither")

# In this order, or PostgreSQL does not start.
spl = routed["spec"]["postgresql"].get("shared_preload_libraries", [])
check("pg_duckdb" in spl and "pg_quicksilver" in spl and spl.index("pg_duckdb") < spl.index("pg_quicksilver"),
      f"the routed example preloads pg_duckdb before pg_quicksilver ({spl})")

# The directory the image fills is the directory the Cluster names. Four
# places spell it; a mismatch means DuckDB quietly downloads into PGDATA, or
# cannot and never routes.
label = re.search(r'duckdb-extension-directory="([^"]+)"', dockerfile).group(1)
places = {
    "the image label": label,
    "the Dockerfile's install stage": re.search(r"qs-duckdb-extensions install (\S+)", dockerfile).group(1),
    "the Dockerfile's offline check": re.search(r"qs-duckdb-extensions verify (\S+)", dockerfile).group(1),
    "examples/cluster-routed.yaml": routed["spec"]["postgresql"]["parameters"].get("duckdb.extension_directory"),
    "cluster_e2e.sh": re.search(r"DUCKDB_EXT_DIR=\$\{DUCKDB_EXT_DIR:-([^}]+)\}", e2e).group(1),
    "the chart's notes": re.search(r"duckdb.extension_directory: (\S+)", notes).group(1),
}
check(len(set(places.values())) == 1, f"DuckDB's extension directory is {label} everywhere it is named ({len(places)} places)")
if len(set(places.values())) != 1:
    for k, val in places.items(): print(f"      {k}: {val}")

params = routed["spec"]["plugins"][0]["parameters"]
check(params.get("ducklake") == "true", 'the routed example sets ducklake: "true"; there is nothing to route to without it')
check("max_slot_wal_keep_size" in routed["spec"]["postgresql"]["parameters"]
      and "max_slot_wal_keep_size" in shadow["spec"]["postgresql"]["parameters"],
      "both examples set max_slot_wal_keep_size")
check(routed["spec"]["instances"] >= 3, "the routed example has at least two replicas")
sql = routed["spec"]["bootstrap"]["initdb"].get("postInitApplicationSQL", [])
ext = [s for s in sql if s.upper().startswith("CREATE EXTENSION")]
check(len(ext) == 2 and "pg_duckdb" in ext[0] and "pg_quicksilver" in ext[1],
      "the routed example creates pg_duckdb, then pg_quicksilver, at bootstrap")
# The two images CI builds and the one the cluster test asks for are named the
# same way.
check(f"pg_quicksilver-postgres:{major}-$VERSION" in e2e, f"cluster_e2e.sh asks for the operand image as {major}-<version>")
sys.exit(bad)
PY

printf '\n========================================\n'
[ $FAIL -eq 0 ] && echo "PASS" || echo "FAIL"
exit $FAIL
