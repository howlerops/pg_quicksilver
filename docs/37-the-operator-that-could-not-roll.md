# 37 — The operator that could not roll

`mode: takeover` did nothing. So did `freshnessSLO`. So did `sidecarImage`, and
`tables`. You could patch the Cluster, watch the spec change, watch the operator
report `Cluster is Ready`, and the running Pods would go on running exactly what
they were running before — indefinitely.

The cause is one function in CloudNativePG, and the version it changed in is the
floor this plugin now requires.

---

## What the cluster said

```
== 4. changing sidecarImage ROLLS the instances ==
  before: app-1=f57e4941 app-2=99e42475
  patched sidecarImage to localhost/qs/pg_quicksilver-mirror:dev-rolled
  app-1 Running  init/quicksilver-mirror  localhost/qs/pg_quicksilver-mirror:dev
  app-2 Running  init/quicksilver-mirror  localhost/qs/pg_quicksilver-mirror:dev
  FAIL: no Pod was replaced within 10 minutes
```

And earlier, in the run before it, the same thing in its more dangerous form:
the Cluster read `mode: takeover, freshnessSLO: 1s` at generation 3 while both
Pods still ran

```
QS_MODE: shadow    QS_FRESHNESS_SLO: 30s    (and no readinessProbe at all)
```

A container with no readiness probe reports `ready=true` unconditionally. So the
gate that `mode: takeover` exists to install was not merely late — it was absent,
and everything downstream reported healthy.

---

## The function

CloudNativePG decides whether to replace an instance in `checkPodSpecIsOutdated`.
It unmarshals the `cnpg.io/podSpec` annotation off the running Pod, builds what
that Pod *should* look like now, and compares:

```go
match, diff := specs.ComparePodSpecs(storedPodSpec, targetPod.Spec)
```

Everything turns on whether those two specs contain the sidecar.

**1.25** builds the target with `specs.PodWithExistingStorage`:

```go
func PodWithExistingStorage(cluster apiv1.Cluster, nodeSerial int) *corev1.Pod {
    podSpec := CreateClusterPodSpec(podName, cluster, envConfig, gracePeriod, tlsEnabled)
    ...
    if podSpecMarshaled, err := json.Marshal(podSpec); err == nil {
        pod.Annotations[utils.PodSpecAnnotationName] = string(podSpecMarshaled)
    }
```

No plugin client, no lifecycle hook. The annotation records the **unpatched**
spec, and the target is built unpatched too. The sidecar is missing from *both*
sides of the comparison, so it is not that a `sidecarImage` change compares
equal — it is that the sidecar is not in the comparison at all. No plugin
parameter can ever produce a diff.

**1.26** replaced it with `specs.NewInstance`:

```go
pluginClient := cnpgiClient.GetPluginClientFromContext(ctx)
...
podClientObject, err := pluginClient.LifecycleHook(ctx, plugin.OperationVerbEvaluate, &cluster, pod)
```

with the annotation written from a `defer` that captures `pod` *after* the hook
reassigns it. Now the sidecar is on both sides, and a change rolls the instances.

---

## Why the tests did not catch it

They did test this. `go/fidelity/rollout_test.go` drives CNPG's own
`specs.ComparePodSpecs` over Pods built by CNPG's own builder and patched by the
real hook, and asserts the diff names `init-containers`. It passes.

It passes against **the version in `go/fidelity/go.mod`**, which is 1.30.
`bench/scripts/cluster_e2e.sh` installed **1.25.1**.

So the unit test proved a property of an operator the cluster test did not
install. Both were honest; the gap between them was invisible because nothing
compared the two numbers. That is the whole failure, and it is a shape worth
naming: *a test can be correct about a version nobody runs.*

The comment in `lifecycle.go` that argues EVALUATE is not optional is also
correct — and was correct all along. Declaring EVALUATE is necessary. It is not
sufficient, because an operator has to call it.

---

## What changed

- **The floor is CloudNativePG 1.26.** Below it, plugin parameters never reach
  running instances.
- `cluster_e2e.sh` installs 1.26.0 by default and, before anything else, reads
  the *running* operator's image tag and refuses to continue below the floor —
  checked against what is deployed rather than what was requested, because a
  standing cluster may already have an older operator and the install step is
  skipped when the CRDs already exist.
- `go/fidelity/operator_floor_test.go` ties the two numbers together: the
  harness's `CNPG_VERSION`, the `go.mod` requirement, and the documented
  requirement must all be at or above the floor. This is the test that would
  have caught it, and it needs no cluster.

---

## If you are on 1.25

The mirror still works. What does not work is *changing your mind*: the sidecar
is injected correctly when a Pod is created, so a fresh Cluster gets whatever
parameters it was created with. Editing them afterwards has no effect until
something else causes the Pod to be recreated — an operator upgrade, a node
drain, a manual `kubectl delete pod`.

That is a bad failure mode precisely because it is silent and intermittent: the
setting appears to work whenever a rollout happened to occur for an unrelated
reason. Upgrade the operator.
