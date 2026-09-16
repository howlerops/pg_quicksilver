# 21 — Against the real operator

Everything the plugin knew about CNPG-I until now was checked against the
**contract**: generated stubs, envtest, the machinery's own helpers
([docs/17](17-testing-without-a-cluster.md)). That is a good way to find most
problems and a poor way to find the one that matters, because the gap between
*"matches the interface"* and *"the operator actually calls it"* is exactly
where the `TYPE_EVALUATE` bug lived — capabilities that satisfied every type
check and made CloudNativePG skip the hook, silently.

This closes that gap. The real CloudNativePG 1.30 operator, a real API server,
real CRDs, real admission webhooks, and our plugin.

```
bash bench/scripts/operator_setup.sh    # k3s + CNPG CRDs + the operator binary
bash bench/scripts/operator_e2e.sh
```

---

## What it proves

```
=== 1. the operator discovers the plugin from the Service label alone ===
  ok: "Registered plugin" — pluginName=quicksilver.howlerops.io

=== 2. a Cluster that names the plugin, through the real admission webhooks ===
  ok: admitted by the real mutating and validating webhooks

=== 3. the handshake, and what the operator recorded about us ===
  ok: the operator wrote our capabilities into .status.pluginStatus:
       name                : quicksilver.howlerops.io  version: dev
       capabilities        : TYPE_OPERATOR_SERVICE, TYPE_LIFECYCLE_SERVICE, TYPE_POSTGRES
       operatorCapabilities: TYPE_VALIDATE_CLUSTER_CREATE, TYPE_VALIDATE_CLUSTER_CHANGE,
                             TYPE_MUTATE_CLUSTER, TYPE_SET_STATUS_IN_CLUSTER

=== 5. the instance Pod the operator built ===
  containers     : postgres
  initContainers : bootstrap-controller, quicksilver-mirror
  ok: THE SIDECAR IS IN A POD THE REAL OPERATOR BUILT
      restartPolicy: Always (Always = a native sidecar, not an init step)
      env rendered from the Cluster spec:
        QS_TABLES          = public.events        <- from parameters.tables
        QS_FRESHNESS_SLO   = 1m0s                 <- from parameters.freshnessSLO: "60s"
        QS_PRIMARY_HOST    = shadow-rw            <- CNPG's read-write Service
        QS_MIRROR_PATH     = /var/lib/postgresql/data/quicksilver
        QS_PGUSER          = <from secretKeyRef>
        …
```

The last block is the one that could not be faked by a contract test. The
operator built that Pod, our lifecycle hook was called while it did, and the
sidecar came out of it as a **native sidecar** — an init container with
`restartPolicy: Always` — rather than as an init step that would run once, exit,
and never mirror anything.

---

## The substitution, stated plainly

This sandbox cannot run Kubernetes **pods**. The reason is specific and was
worth chasing to the bottom, because the error it produces names nothing:

```
runc create failed: unable to start container process:
  can't get final child's PID from pipe: EOF
  runc init error(s): nsexec[8313]: failed to update /proc/self/oom_score_adj:
                                    Permission denied
```

runc sets a pod sandbox's `oom_score_adj` to −998. A process may only **raise**
`oom_score_adj` unless it holds `CAP_SYS_RESOURCE`, and this sandbox does not
have that capability in its **bounding** set, so it cannot be acquired at all:

```
CapBnd: 000001fffeffffff        <- bit 24, CAP_SYS_RESOURCE, is clear
$ echo -998 > /proc/self/oom_score_adj
bash: echo: write error: Permission denied
```

It fails identically under **kind** and under **k3s**, because it is neither's
fault. containerd's `restrict_oom_score_adj` is exactly the setting for this
case, and containerd 2.2.2 does not honour it for pod sandboxes: the key is
present in the binary, accepted in the config at the path `containerd config
default` prints it, and `crictl info` still reports `restrictOOMScoreAdj:
false`.

So the control plane runs and the pods do not. What that leaves is enough,
because **a plugin's whole job happens while the operator builds a pod spec**:

| | Real | Substituted |
|---|---|---|
| API server | k3s, v1.36 | — |
| CRDs | CloudNativePG 1.30, as released | — |
| Operator | the binary from `ghcr.io/cloudnative-pg/cloudnative-pg:1.30.0` | runs as a host process, not a pod |
| Admission webhooks | the operator's own, mTLS, API-server-verified | Service points at the host |
| Plugin | ours, mTLS, discovered by Service label | runs as a host process |
| Pod specs | built by the operator | never scheduled — no kubelet |
| initdb Job | created by the operator | **marked complete by the script** |

That last row is the only thing the script fakes, it fakes it in one place, and
it says so where it does it. Without it the operator waits forever for a
bootstrap pod that cannot start, and never builds the instance Pod this test
exists to inspect.

**Not covered**, and named in the script's own output so nobody reads a pass as
more than it is: no PostgreSQL runs, so no WAL, no mirror, no failover, no
switchover, no rollout, no scheduling. `bench/scripts/e2e_mirror.sh` covers the
PostgreSQL half, with a real primary and standby.

---

## Six traps between "it compiles" and "it works"

None of these are bugs in the plugin. All six are things a contract test cannot
tell you, and each cost real time, so they are written down.

**1. The plugin's name is matched by string, and nothing checks it.**
The Service label `cnpg.io/pluginName` and `.spec.plugins[].name` must agree
exactly. Registering `quicksilver.cnpg.io` when the plugin identifies as
`quicksilver.howlerops.io` produces no error anywhere — the Cluster is admitted,
the operator runs, and the plugin is simply never called. The script now reads
the name out of the chart rather than retyping it.

**2. gRPC honours `HTTPS_PROXY`.**
With a proxy in the environment the operator dialled the *proxy* instead of the
plugin and reported:

```
transport: authentication handshake failed:
  read tcp 127.0.0.1:38884->127.0.0.1:44517: read: connection reset by peer
```

Port 44517 belonged to something else entirely. The diagnosis came from
`/proc/net/tcp` and an inode lookup, not from the message. Anyone running the
operator behind a corporate proxy will meet this; `no_proxy` needs the plugin's
service name in it.

**3. The API server matches endpoint ports to service ports by NAME.**
A named endpoint port against an unnamed service port resolves to nothing, and
is reported as `no endpoints available for service` — which reads like the
backend being down rather than like a name mismatch.

**4. The operator will not start without its own webhook certificate on disk.**
It waits for `WEBHOOK_CERT_DIR` to contain the certificate it just minted,
because in a pod that is a mounted secret and the kubelet refreshes it. Outside
a pod it retries silently and then reports `secrets mount still not refreshed`.

**5. The webhook server serves `apiserver.crt`, not `tls.crt`.**
Both are required in the directory; giving the wrong one the right contents
fails with `certificate is not valid for any names`.

**6. The operator finds its architectures with a RELATIVE glob.**
`operator/manager_*`, resolved against the working directory. Miss it and every
status update fails with `invalid architecture: amd64`, which says nothing about
a missing file.

---

## What this changes

| | Before | After |
|---|---|---|
| CNPG-I conformance | the contract, via stubs and envtest | the operator itself |
| Sidecar injection | asserted against a `specs.NewInstance` Pod | read out of a Pod the operator built |
| Capability negotiation | unit-tested | recorded by the operator in `.status.pluginStatus` |
| Admission | untested | the Cluster goes through the real webhooks |
| Known gap | "we have never run against the operator" | "we have never run against a kubelet" |

## What to do next

1. **A cluster that can run pods.** Everything below needs one: failover,
   switchover, rollouts, the `TYPE_EVALUATE` path that actually rolls Pods when
   the sidecar changes, and the mirror itself under the operator. A machine with
   `CAP_SYS_RESOURCE` — any ordinary VM — is all it takes.
2. **Assert the rollout path.** The lifecycle hook declares `TYPE_EVALUATE` so
   that a sidecar change rolls the Pods; that it is *declared* is unit-tested,
   that it *works* needs a running instance.
3. **Put the plugin and the sidecar in the same test as a real PostgreSQL.**
   `e2e_mirror.sh` and `operator_e2e.sh` each cover a half, and nothing yet
   covers the join.
