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
