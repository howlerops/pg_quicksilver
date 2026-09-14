# 05 — CloudNativePG integration

How Quicksilver actually plugs into CNPG, which CNPG-I hooks do what, and — importantly —
what CNPG-I **cannot** do, because that determines how much we have to build ourselves.

---

## CNPG-I in one page

The CloudNativePG Interface is a **gRPC** contract. A plugin is a server implementing one or
more services; the operator (and the instance manager) are clients. Plugins run either as a
**sidecar in the operator Deployment** (Unix socket in `PLUGIN_SOCKET_DIR`, default
`/plugin`) or as a **standalone Deployment in the same namespace** (TCP + mTLS, with certs
referenced via the `cnpg.io/pluginClientSecret` and `cnpg.io/pluginServerSecret` Service
annotations).

Users opt in per cluster:

```yaml
spec:
  plugins:
    - name: quicksilver.howlerops.io
      enabled: true
      parameters:
        replicas: "2"
```

### The services, and what each buys us

| Service | Key RPCs | What Quicksilver uses it for |
|---|---|---|
| **Identity** | `GetPluginMetadata`, `GetPluginCapabilities`, `Probe` | Boilerplate. Declare which services we implement. |
| **Operator** | `ValidateClusterCreate`, `ValidateClusterChange`, `MutateCluster`, `SetStatusInCluster`, `Deregister` | Validate our `parameters` block and **reject bad config at admission** rather than at runtime. `SetStatusInCluster` publishes mirror freshness/health into `Cluster.status` — the single best UX win available, since `kubectl get cluster` then shows mirror lag. |
| **OperatorLifecycle** | `LifecycleHook` (Create/Patch/Update/Delete/Evaluate on k8s objects) | **The workhorse.** Intercept the instance `Pod` to inject our sidecar, volumes and env; intercept the `-ro` `Service` to retarget it. |
| **Postgres** | `EnrichConfiguration` | Set `wal_level`, `max_replication_slots`, `shared_preload_libraries`, `archive_timeout` etc. at init/restore/reconcile/upgrade — declaratively, without the user hand-editing `postgresql.parameters`. |
| **WAL** | `Archive`, `Restore`, `Status`, `SetFirstRequired` | The archive tee (Path 1 in [03](03-wal-ingestion.md)). Note the design tension below. |
| **ReconcilerHooks** | `Pre`, `Post` (for Cluster and Backup reconcilers) | Create and reconcile resources CNPG doesn't know about: our mirror `StatefulSet`, `Service`, `PVC`s, `ConfigMap`s. This is how we manage pods that are not CNPG instances. |
| **Backup** / **RestoreJobHooks** | `Backup`, `Restore` | Later. Mirror state is derivable, so backing it up is an optimisation (fast rebuild), not a requirement. |
| **Metrics** | `Define`, `Collect` | Export mirror lag, compaction backlog, verification status, routing hit-rate through CNPG's existing Prometheus surface. |

---

## What CNPG-I cannot do

Worth stating plainly, because it shapes the build:

1. **There is no "add an instance of a different kind" hook.** CNPG owns the instance
   `StatefulSet` and its pods are Postgres instances managed by the instance manager. We
   cannot ask CNPG to create a fourth pod that is "a Quicksilver node". We get two options:
   **(a)** inject a sidecar into existing instance pods via `OperatorLifecycle`, or **(b)**
   create and reconcile our own workload via `ReconcilerHooks.Post` using the kube API
   directly. Architecture A uses (a); a standalone mirror tier uses (b).

2. **`WAL.Archive` is not additive by default.** The hook exists so a plugin can *be* the
   archiver (that's how `cnpg-i-barman-cloud` works). If the user already has a backup plugin
   or `barmanObjectStore` configured, two plugins both claiming the WAL service is a
   conflict, not a chain. **Open question OQ-3** in [09](09-risks-and-open-questions.md):
   confirm whether CNPG supports multiple WAL-service plugins, and if not, the archive tee
   must be implemented by other means (e.g. a sidecar watching `pg_wal`, or consuming from
   the object store the existing archiver writes to). *This is a real constraint on Path 1
   and must be verified in spike S1 before we plan around it.*

3. **Plugins cannot change CNPG's core reconciliation logic** — only observe it (`Pre`/`Post`)
   and mutate the objects it is about to apply (`LifecycleHook`). So anything that fights
   CNPG's model will be reverted on the next reconcile loop. In particular, hand-editing the
   `-ro` Service out-of-band will not stick; it must be done through `LifecycleHook` so it is
   reapplied every time CNPG rewrites the object.

---

## Architecture A wiring (phase 1)

```
┌──────────────────────── namespace ─────────────────────────┐
│                                                            │
│  Cluster/app (CNPG-managed)                                │
│    app-1 primary ──────┐                                   │
│    app-2 standby (HA)  │ logical replication slot          │
│                        ▼                                   │
│  StatefulSet/app-quicksilver  (ours, via ReconcilerHooks)  │
│    ┌──────────────────────────────────────┐                │
│    │ qs-0                                  │               │
│    │  ├── postgres  (our image, pg_duckdb) │               │
│    │  └── ingest    (change stream → Parquet)│             │
│    │  PVC: mirror data                     │               │
│    └──────────────────────────────────────┘                │
│    qs-1 …                                                  │
│                                                            │
│  Service/app-ro      ── selector retargeted to qs pods ──┐ │
│  Service/app-ro-row  ── original standby selector ───────┤ │
│  Service/app-rw      ── untouched (primary) ─────────────┘ │
└────────────────────────────────────────────────────────────┘
```

### The `-ro` takeover, and why it should be opt-in

The most direct route to G1 is `LifecycleHook` on the `-ro` `Service`, rewriting its selector
to match Quicksilver pods. Then applications connecting to `app-ro` reach the mirror with
zero application change. Mechanically this works.

**But it should not be the default**, for the reason in
[01](01-problem-and-goals.md) / G3: `-ro` today carries a *mix* of analytical and OLTP-shaped
reads, and under Architecture A the OLTP-shaped ones regress. Silently retargeting the
endpoint that production already depends on, to an engine with different performance *and*
different semantics, is how you generate an incident rather than a win.

Proposed policy, escalating with confidence:

| Mode | Behaviour | When |
|---|---|---|
| `off` (default) | Mirror served on a **new** `app-olap` service. `-ro` untouched. | Always, initially. Lets teams point one dashboard at it and measure. |
| `shadow` | `-ro` untouched, but a sampled copy of `-ro` traffic is replayed against the mirror and results/latency compared. | The evidence-gathering mode. This is what answers spike S0. |
| `takeover` | `-ro` retargeted to mirror pods; original standbys remain on `app-ro-row`. | Only once `shadow` shows the workload is favourable. |

`shadow` mode is worth building early and is cheap: it is the mechanism that turns "we think
this is faster" into a per-customer number, and it doubles as the correctness oracle for G7.

Under Architecture C the whole question softens considerably — the node serves both engines,
so `takeover` is much closer to safe by default.

### Readiness gating (goal G4)

Mirror pods must fail their readiness probe when lag exceeds the configured
`freshnessSLO`. Kubernetes then removes them from the Service endpoints automatically, and
queries fall back to whatever else is behind the service. This is the mechanism that makes
"bounded staleness" an enforced property rather than a promise on a slide. It must be in
phase 1.

---

## Cluster spec sketch

Target user-facing surface. Nothing here is implemented yet.

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: app
spec:
  instances: 3
  imageName: ghcr.io/howlerops/pg_quicksilver:17-v0.1   # see docs/07
  plugins:
    - name: quicksilver.howlerops.io
      enabled: true
      parameters:
        # --- topology ---
        replicas: "2"
        storageSize: "200Gi"
        storageClass: "fast-ssd"

        # --- what to mirror ---
        tables: "public.events,public.orders,analytics.*"
        exclude: "public.sessions"

        # --- freshness contract ---
        freshnessSLO: "5s"          # readiness fails above this
        compactionTarget: "60s"

        # --- routing ---
        serviceMode: "off"          # off | shadow | takeover

        # --- ingest ---
        ingest: "logical"           # logical | physical (phase 3)
        verifyInterval: "15m"       # continuous checksum verification (G7)
```

`ValidateClusterCreate` should reject, at admission: `ingest: physical` on an unsupported
major version; `serviceMode: takeover` without at least one non-Quicksilver standby left for
the row-store fallback; table patterns matching nothing; a `freshnessSLO` below what the
chosen ingest path can physically deliver.

---

## Helm chart

Two charts, deliberately separate:

1. **`quicksilver-operator-plugin`** — the plugin Deployment, RBAC, Services, cert-manager
   `Certificate`s, and the CNPG-I socket/mTLS wiring. Installed once per cluster.
2. **`quicksilver-cluster`** (optional convenience) — an opinionated CNPG `Cluster` with the
   plugin block pre-filled, for greenfield users.

Existing users should only need chart 1 plus a `spec.plugins` entry. Requiring them to
re-create their `Cluster` would be a serious adoption tax and is worth avoiding even at
implementation cost.

---

## Sequencing against CNPG lifecycle events (goal G6)

| Event | Required Quicksilver behaviour |
|---|---|
| **Switchover / failover** | Logical ingest: slot must follow the new primary (PG ≥ 17 slot sync, else re-snapshot). Physical ingest: follow the timeline switch. Either way, lag spikes → readiness fails → traffic drains. Must not silently diverge. |
| **Rolling minor upgrade** | Mirror pods upgrade too; mirror format must be forward-compatible within a major version, or the upgrade triggers a rebuild. |
| **Major version upgrade** | Physical decoder is version-pinned. Expect a full rebuild. Document it as such. |
| **Instance re-bootstrap** (`pg_basebackup`) | Relfilenodes change. A physical-WAL mirror keyed on relfilenode must handle remapping or rebuild. |
| **Scale up/down** | New mirror pods bootstrap from the archive tee (Path 1) + an initial snapshot; must not require a slot per pod on the primary. |
| **Node drain / eviction** | PDBs for mirror pods; `applied_lsn` must be durable so a restarted pod resumes rather than re-syncs. |

Every row here is a test case, not just documentation. They are the ones that will actually
break.
