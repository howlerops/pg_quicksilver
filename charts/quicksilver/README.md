# quicksilver

Installs the Quicksilver CNPG-I plugin: the gRPC server CloudNativePG dials to
validate Clusters, inject the mirror sidecar into instance Pods, and set the
PostgreSQL parameters the mirror needs.

```
helm install quicksilver charts/quicksilver --namespace cnpg-system
```

The chart installs into the **operator's** namespace, not the Cluster's. That is
not a preference: CloudNativePG discovers plugins by listing Services in its own
namespace, so a plugin installed elsewhere is never found and nothing in either
log says why.

## Requirements

- CloudNativePG with CNPG-I support
- cert-manager, unless you set `certManager.enabled=false` and supply the mTLS
  Secrets yourself

## Values

| Key | Default | Notes |
|---|---|---|
| `operatorNamespace` | `cnpg-system` | must match where CNPG runs |
| `image.plugin` | `ghcr.io/howlerops/pg_quicksilver-plugin:0.1.0` | |
| `image.mirror` | `ghcr.io/howlerops/pg_quicksilver-mirror:0.1.0` | the sidecar the plugin injects |
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

See [`examples/cluster-shadow.yaml`](../../examples/cluster-shadow.yaml) and
[docs/16](../../docs/16-deploying.md).
