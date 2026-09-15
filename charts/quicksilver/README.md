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

See [`examples/cluster-shadow.yaml`](../../examples/cluster-shadow.yaml) and
[docs/16](../../docs/16-deploying.md).
