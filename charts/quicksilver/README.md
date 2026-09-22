# quicksilver

Installs the Quicksilver CNPG-I plugin: the gRPC server CloudNativePG dials to
validate Clusters, inject the mirror sidecar into instance Pods, and set the
PostgreSQL parameters the mirror needs.

```
helm install quicksilver oci://ghcr.io/howlerops/charts/quicksilver \
  --version 0.0.2 --namespace cnpg-system
```

or from a checkout, which is what the tests and the e2e scripts use:

```
helm install quicksilver charts/quicksilver --namespace cnpg-system
```

### The published packages are private, and that breaks the pull in two places

GHCR packages inherit the repository's visibility, and this repository is
private. An anonymous pull is refused before it reaches a manifest:

```
$ helm pull oci://ghcr.io/howlerops/charts/quicksilver --version 0.0.2
Error: failed to authorize: failed to fetch anonymous token:
unexpected status ... 401 Unauthorized
```

Two images are pulled, **in two different namespaces**, and fixing one does not
fix the other. This is the part that will cost an afternoon if it is not said
out loud: the sidecar failure appears as `ImagePullBackOff` on an instance Pod
long after the plugin installed cleanly, so it does not look related to the
chart at all.

| image | pulled by | in namespace | fixed by |
|---|---|---|---|
| `-plugin` | this chart's Deployment | `operatorNamespace` | `image.pullSecrets` below |
| `-mirror` | each instance Pod | the **Cluster's** namespace | `spec.imagePullSecrets` on the Cluster |

Either make the packages public, or create the secret in both namespaces.

**Public** — in GitHub, for each of the three packages (`…-plugin`, `…-mirror`,
`charts/quicksilver`): Package settings → Danger Zone → Change visibility →
Public. Then nothing below is needed. Note that this publishes the images to
anyone, while the source stays private.

**Or, a pull secret in each namespace:**

```
kubectl create secret docker-registry ghcr \
  --docker-server=ghcr.io --docker-username=<user> --docker-password=<PAT> \
  --namespace cnpg-system        # for the plugin

kubectl create secret docker-registry ghcr \
  --docker-server=ghcr.io --docker-username=<user> --docker-password=<PAT> \
  --namespace <cluster-namespace>   # for the mirror sidecar
```

```yaml
# values.yaml — the plugin half
image:
  pullSecrets:
    - name: ghcr
```

```yaml
# the Cluster — the sidecar half. CloudNativePG puts these on instance Pods,
# and the injected sidecar shares that Pod, so this is what the mirror uses.
spec:
  imagePullSecrets:
    - name: ghcr
  plugins:
    - name: quicksilver.howlerops.io
```

`helm pull` and `helm install` from the OCI URL need the same credentials:

```
helm registry login ghcr.io -u <user> --password-stdin <<< "$PAT"
```

The plugin does **not** inject `imagePullSecrets` into the Pod it builds, on
purpose. It cannot know whether a Secret of that name exists in the Cluster's
namespace, and naming one that does not produces the same `ImagePullBackOff`
with a more confusing cause. The Cluster declares its own credentials.

The chart installs into the **operator's** namespace, not the Cluster's. That is
not a preference: CloudNativePG discovers plugins by listing Services in its own
namespace, so a plugin installed elsewhere is never found and nothing in either
log says why.

## Requirements

- **CloudNativePG 1.26 or newer.** This is a hard floor, and below it the
  failure is silent: 1.25 builds an instance Pod's expected spec without
  consulting plugins, so the injected sidecar is absent from both sides of the
  comparison that decides whether to replace a Pod. Changing `mode`,
  `freshnessSLO`, `sidecarImage` or `tables` on a running Cluster then has **no
  effect at all** until a Pod is recreated for some unrelated reason. A fresh
  Cluster still gets the parameters it was created with, which is what makes
  this easy to miss. [docs/37](../../docs/37-the-operator-that-could-not-roll.md)
  has the mechanism.
- cert-manager, unless you set `certManager.enabled=false` and supply the mTLS
  Secrets yourself

## Values

| Key | Default | Notes |
|---|---|---|
| `operatorNamespace` | `cnpg-system` | must match where CNPG runs |
| `image.plugin` | `ghcr.io/howlerops/pg_quicksilver-plugin:0.0.2` | |
| `image.mirror` | `ghcr.io/howlerops/pg_quicksilver-mirror:0.0.2` | the sidecar the plugin injects |
| `service.port` | `9090` | also published as the `cnpg.io/pluginPort` annotation |
| `certManager.enabled` | `true` | issues both halves from a self-signed CA |
| `certManager.issuerRef` | `{}` | set to reuse an existing Issuer or ClusterIssuer |
| `existingSecrets.server` / `.client` | `""` | required when cert-manager is off |

## Using it

Installing the chart does nothing on its own. A Cluster opts in:

```yaml
spec:
  plugins:
    - name: quicksilver.howlerops.io
      parameters:
        tables: public.events
```

### Set `max_slot_wal_keep_size`. This is not optional.

```yaml
spec:
  postgresql:
    parameters:
      max_slot_wal_keep_size: "4GB"   # or whatever your pg_wal volume affords
  plugins:
    - name: quicksilver.howlerops.io
      parameters:
        tables: public.events
```

A logical replication slot pins every WAL segment its consumer has not
confirmed. That is what lets a restarted mirror resume instead of
re-snapshotting, and it is a loaded gun pointed at the database being mirrored:
**a mirror that cannot keep up makes the PRIMARY run out of disk.**

This is not theoretical. It was registered as risk R7 in
[docs/09](../../docs/09-risks-and-open-questions.md) and then reproduced exactly
at scale — a 60-second workload of 11.7 million row-changes against a mirror
draining at roughly half that rate took 6.6 GB of free space to 272 KB and
stopped PostgreSQL. See [docs/28](../../docs/28-the-slot-is-a-loaded-gun.md).

`max_slot_wal_keep_size` makes PostgreSQL invalidate the slot instead. The
mirror then rebuilds from a fresh snapshot, which is the documented lifecycle
for an invalidated slot and is enormously cheaper than a primary that has run
out of disk.

The sidecar also bounds its own slot, at `QS_MAX_SLOT_WAL_BYTES` (4 GB by
default), and drops it rather than let it grow further. **That is a second line
of defence, not a substitute**: a guard running inside the process it guards
cannot be trusted alone, and a wedged sidecar is exactly the case where the slot
grows fastest. Set the PostgreSQL parameter.

### Size `sidecarMemory` from your ROW COUNT, not your disk

```yaml
  plugins:
    - name: quicksilver.howlerops.io
      parameters:
        tables: public.events
        sidecarMemory: 2Gi      # ~90 bytes of RSS per live row
```

The sidecar's heap is dominated by one structure — the key index, one entry per
live row. A heap profile against a 20.5M-row mirror:

```
862.64MB 95.76%  mirror.newKeyIndex
 18.80MB  2.09%  mirror.forEachRowGroup
  8.50MB  0.94%  changestream.decodeTuple
```

That is 42 bytes per live row of live heap, and roughly twice that resident.
The mirror weighed 120 MB on disk and the sidecar peaked at 1,854 MB, so
**sizing from the mirror's size on disk is wrong by an order of magnitude.**

| rows | `sidecarMemory` |
|---|---|
| 1M | `256Mi` (the default) |
| 5M | `512Mi` |
| 20M | `2Gi` |

**`kubectl top` after a restart is now a usable second opinion**, because the
key index is built *before* `/readyz` passes. RSS at readiness is the RSS the
Pod will hold. Measured at 4M rows:

```
readiness said yes at   2.24s and  505 MB
the first write took     0.6s and  507 MB
a second, identical one  0.5s
```

This used to be the opposite advice, and the reason is worth keeping. The index
was built lazily, inside the first change:

```
readiness said yes at   0.04s and   25 MB
the first write took     2.3s and  507 MB     <- 20x
```

So the probe put the Pod into service at a twentieth of its real footprint, and
then stalled ~1.8s on the first write behind a signal that had already told
Kubernetes to send traffic. An operator sizing from `kubectl top` at that moment
was reading a number that was wrong by 20×.

Readiness now costs seconds rather than milliseconds, which is the trade: budget
restart time accordingly. A large mirror cannot restart-loop on it — readiness
failing only holds the Pod out of the endpoint, and `/healthz` keeps answering
throughout the build. `bench/scripts/restart_cost.sh` measures all of it.

It is applied as a request *and* a limit, which puts the sidecar in Guaranteed
QoS. That matters more than the exact number: a container requesting far less
than it uses is Burstable, and the Pod it makes an eviction candidate is the one
running PostgreSQL — the same principle as the slot guard above. The mirror is
an optimisation; the database is the database.

Setting it too low makes the mirror **slower, not dead**. The sidecar runs with
`GOMEMLIMIT` at 80% of the limit, so Go collects harder rather than growing past
it. Leave the headroom, though — measured at 20.5M rows, against a 901 MB live
heap:

```
setting                peak RSS   drain
default (GOGC=100)      1854 MB     12s
GOMEMLIMIT 1600MiB      1536 MB     12s   binding, and free
GOMEMLIMIT 1200MiB      1174 MB    125s   thrashing, ten times slower
```

Do not reach for `GOGC` instead. It is a ratio, so it cannot tell a heap that is
one big long-lived index from a heap that is garbage — and here it is almost
entirely the former. `GOGC=25` finished with *more* resident memory than the
default (2,013 MB) and took 183s, because collecting harder slows the apply loop
and the changes left in flight are themselves heap.

### Never `DROP DATABASE` on a database a Quicksilver slot follows

It takes the **standby** down — not the primary, the standby.

Quicksilver's slot is a failover slot and the standby synchronises it, because
that is how a logical slot survives a promotion on PostgreSQL 17 and is the
whole reason the plugin requires 17. A standby cannot replay a `DROP DATABASE`
while its synced copy of that database's slot is in use, so its startup process
dies and the server shuts down:

```
FATAL:  replication slot "..." is active for PID ...
CONTEXT:  WAL redo at 0/... for Database/DROP
LOG:  shutting down due to startup process failure
```

Drop the slot first and confirm the standby has forgotten it:

```sql
-- on the standby; only when this returns nothing is the drop safe
SELECT slot_name, synced FROM pg_replication_slots WHERE database = 'app';
```

Measured at two seconds. If the standby is already down it restarts cleanly once
the conflicting slot is gone — the data is intact, it was the replay that was
stuck. See [docs/30](../../docs/30-dropping-a-database-kills-the-standby.md).

See [`examples/cluster-shadow.yaml`](../../examples/cluster-shadow.yaml) and
[docs/16](../../docs/16-deploying.md).
