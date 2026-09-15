# 17 — Testing the CNPG-I plugin without a Kubernetes cluster

[docs/16](16-deploying.md) originally listed "the CNPG-I handshake" as the single
largest unverified thing, on the grounds that this environment has no container
runtime. That framing was wrong, and correcting it is the point of this document.

**The CNPG-I handshake is not a Kubernetes thing.** It is CloudNativePG's plugin
client dialling a gRPC server over mutual TLS and asking a fixed sequence of
questions. Exactly one step of it needs a cluster — finding the Service by its
`cnpg.io/pluginName` label — and that step is a label lookup. Everything else
runs here.

Three layers, none of which needs Docker, kind, or CloudNativePG running:

| What | How | Where |
|---|---|---|
| The handshake and mTLS | the real cnpg-i-machinery server + CNPG's own dial code | [`go/fidelity/handshake_test.go`](../go/fidelity/handshake_test.go) |
| The lifecycle hook | against Pods from CNPG's own `specs.NewInstance` | [`go/fidelity/pod_test.go`](../go/fidelity/pod_test.go) |
| The chart and the example | real Kubernetes + CNPG CRD schemas | [`bench/scripts/validate_manifests.sh`](../bench/scripts/validate_manifests.sh) |

```
go test ./fidelity/...                       # handshake, mTLS, real Pods
bash bench/scripts/validate_manifests.sh     # chart + example against real schemas
```

---

## 1. The handshake, for real

`go/fidelity` is a **separate Go module** on purpose: testing this means importing
the CloudNativePG operator, which drags in most of controller-runtime, and that
belongs nowhere near the dependency tree of a plugin binary shipped in a
distroless image.

The test starts the actual `pluginhelper/http.Server` — the same `Start()` the
`quicksilver-plugin` binary reaches through `CreateMainCmd` — with real
certificates issued by a real self-signed CA, in the shape the Helm chart asks
cert-manager for. It then dials it exactly as CloudNativePG does
(`grpc.NewClient` with TLS transport credentials, from
`internal/cnpi/plugin/connection/remote.go`) and runs `LoadPlugin`'s sequence
step for step:

```
plugin quicksilver.howlerops.io version dev, licence Apache-2.0
declared services: [TYPE_OPERATOR_SERVICE TYPE_LIFECYCLE_SERVICE TYPE_POSTGRES]
operator RPCs: [VALIDATE_CLUSTER_CREATE VALIDATE_CLUSTER_CHANGE MUTATE_CLUSTER SET_STATUS_IN_CLUSTER]
lifecycle hook: group="" kind="Pod" ops=[TYPE_CREATE TYPE_PATCH TYPE_UPDATE TYPE_EVALUATE]
postgres RPCs: 1
undeclared WAL correctly unavailable: Unimplemented: unknown service cnpgi.wal.v1.WAL
undeclared ReconcilerHooks correctly unavailable: Unimplemented
```

The last two lines matter as much as the first five. CloudNativePG asks a
service's `GetCapabilities` **only** if the Identity response declared it, so the
declared list and the registered list must agree in both directions: a service
declared but not registered fails the entire plugin load with `Unimplemented`
during discovery; a service registered but not declared is simply never used.

### mTLS is enforced, and the SANs matter

A gRPC server in the operator's namespace that accepts anonymous calls would let
anything in that namespace mutate Cluster specs. Four cases, all run:

| Client | Result |
|---|---|
| no client certificate | **refused** |
| certificate from an unrelated CA | **refused** |
| correct certificate, server name outside the cert's SANs | **refused** |
| the operator's own certificate and a listed name | accepted |

The third is the chart's most likely misconfiguration — the operator dials
`quicksilver.cnpg-system.svc` and the certificate carries only the short name —
and it presents as a plugin outage with a TLS error in the operator's log and
nothing at all in the plugin's. It is why
[`templates/certificates.yaml`](../charts/quicksilver/templates/certificates.yaml)
lists all four forms of the Service name.

---

## 2. The bug this found

`pkg/specs` is public, so the lifecycle hook can be run against the Pod
CloudNativePG *actually* builds rather than a hand-written fixture. Doing that
immediately surfaced something no amount of unit testing would have:

> **`TYPE_EVALUATE` was missing from the declared operation types.**

CloudNativePG's `innerLifecycleHook` skips any plugin whose declared
`OperationTypes` do not contain the verb being asked
(`internal/cnpi/plugin/client/lifecycle.go`). It asks with four verbs from three
places — `CREATE`/`UPDATE`/`PATCH`/`DELETE` when the operator's wrapped
Kubernetes client writes an object, and **`EVALUATE`** from `specs.NewInstance`,
which computes what a running Pod *should* look like.

That evaluated spec is what `checkPodSpecIsOutdated` compares a running instance
against, via `ComparePodSpecs`, to decide whether to roll it — and it is what
lands in the Pod's own pod-spec annotation. Without `EVALUATE` the sidecar is
absent from **both sides** of that comparison. Nothing errors. Nothing logs. The
consequences are simply that:

- changing `sidecarImage` would never roll the instances, and
- the pod-spec annotation would describe a Pod that does not exist.

This is the same shape as every other bug this project has found — a silent
disagreement between two things that each look fine on their own — and it is the
third time the fix has been "make the test use the real thing instead of a
plausible stand-in". The first was running the sidecar as a process instead of
from a harness ([docs/16](16-deploying.md#schema-change)); this is the second and
third.

The regression test asserts `EVALUATE` specifically **and** loops over every
declared verb, injecting into a freshly-built real Pod each time, so a verb that
is declared but does nothing fails too.

### What the real Pod also confirmed

```
CNPG built Pod "app-1": containers=[postgres] initContainers=[bootstrap-controller] volumes=3
sidecar mounts: [pgdata:/var/lib/postgresql/data scratch-data:/run
                 scratch-data:/controller shm:/dev/shm plugins:/plugins]
mirrorPath /var/lib/postgresql/data/quicksilver is inside volume pgdata
```

Two guesses turned into facts. The mirror path really does land inside the data
volume rather than on ephemeral container storage — a mistake that would work
perfectly until the first restart. And `/controller` really is mounted, which is
where the sidecar's default `QS_LOCAL_SOCKET_DIR=/controller/run` expects to find
the instance's unix socket.

---

## 3. The manifests, against real schemas

`helm lint` only checks that templates render. `kubeconform` checks the rendered
objects against the actual Kubernetes API schemas, and the example Cluster is
validated against CloudNativePG's **published CRD** — the same schema the API
server enforces on `kubectl apply`. Four value combinations are rendered, because
the interesting failures are in the paths nobody renders by hand.

One correction is worth recording, because the first version of this check was
vacuous in the way this project keeps catching:

> A CRD schema does **not reject** unknown fields. It **prunes** them.

`instancesss: 3` validates cleanly against the CRD and then silently disappears
on apply. The first version of the script announced "the API server would reject
this manifest" and would have passed a manifest whose key half never took effect.
Detecting a pruned field is a separate walk over the document, and it is now its
own check — which is the one that actually catches a typo'd field name.

A third check confirms every parameter in the example is one the plugin's own
validator accepts, so the documented example cannot be a documented way to fail
admission.

---

## What is still left

Short, and honestly short rather than rhetorically short:

- **Service discovery by label.** The one step of the handshake that genuinely
  needs an API server.
- **The operator process itself** — reconcile loops, rollouts, switchovers, and
  how the plugin behaves across them. The `EVALUATE` bug above is exactly the
  class of thing that reading the operator's source catches and only a running
  cluster confirms.
- **The images**, which are written and never built.

Everything else that [docs/16](16-deploying.md) listed as unverified — the mTLS
dial, the metadata and capability negotiation, the patches against real Pods, the
chart's structure — is now run on every `go test ./...`.
