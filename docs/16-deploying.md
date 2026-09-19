# 16 — Deploying Quicksilver

Everything in docs 01–15 is analysis or measurement. This is the part you can
install. It covers the two images, the CNPG-I registration, the Cluster spec,
and — because it is the question that decides whether any of this was worth it —
how to check that the mirror is telling the truth.

**Status.** The plugin and the sidecar are built, unit-tested, exercised
end-to-end against live PostgreSQL 17 by
[`bench/scripts/e2e_mirror.sh`](../bench/scripts/e2e_mirror.sh), and — since
[docs/17](17-testing-without-a-cluster.md) — run against the real CNPG-I
handshake over real mutual TLS, and against instance Pods built by
CloudNativePG's own `specs.NewInstance`. What remains unverified is listed at
the bottom, and it is now a short list.

---

## The two images

| Image | What it is | Where it runs |
|---|---|---|
| `pg_quicksilver-plugin` | the CNPG-I gRPC server | a Deployment in the **operator's** namespace |
| `pg_quicksilver-mirror` | the sidecar that builds and serves the mirror | injected into every instance Pod |

Both come from one [`deploy/Dockerfile`](../deploy/Dockerfile) via `--target`.

The mirror image is **not** a PostgreSQL image. It ships two static binaries and
no server, because it reaches PostgreSQL over the network and the local socket.
That is what lets [docs/07](07-image-strategy.md) hold: the CNPG PostgreSQL image
stays stock upstream and nothing here forks it.

```
docker build --target plugin -t ghcr.io/howlerops/pg_quicksilver-plugin:0.0.1 .
docker build --target mirror -t ghcr.io/howlerops/pg_quicksilver-mirror:0.0.1 .
```

The sidecar image runs as UID 26, the UID the CNPG images use for `postgres`.
Running it as anyone else produces mirror files the instance cannot read.

---

## Installing the plugin

```
helm install quicksilver oci://ghcr.io/howlerops/charts/quicksilver \
  --version 0.0.1 --namespace cnpg-system
```

The chart is pushed as an OCI artifact, so there is no chart repository to add.
From a checkout, `helm install quicksilver charts/quicksilver` does the same
thing against the working tree.

The whole of the plugin's registration is a **Service**. There is no CRD and no
operator configuration to edit: CloudNativePG lists Services in its own
namespace and treats any carrying the `cnpg.io/pluginName` label as a plugin
a Cluster may name.

Three consequences, all of which present as "the plugin isn't loading" with
nothing in either log naming the cause:

- the Service must be in the **operator's** namespace, not the Cluster's;
- the label value must match `.spec.plugins[].name` exactly;
- the `cnpg.io/pluginClientSecret` / `cnpg.io/pluginServerSecret` annotations are
  how the operator finds the mTLS material, and a mismatch surfaces as a
  connection error rather than a missing plugin.

The chart issues both halves of the mTLS pair from a self-signed CA via
cert-manager. Set `certManager.issuerRef` to use your own, or
`certManager.enabled=false` with `existingSecrets` to manage them yourself —
in which case the template fails loudly rather than rendering a Deployment that
mounts Secrets that do not exist.

---

## Enabling it on a Cluster

See [`examples/cluster-shadow.yaml`](../examples/cluster-shadow.yaml). The
minimum is:

```yaml
spec:
  instances: 3
  imageName: ghcr.io/cloudnative-pg/postgresql:17.2-standard-bookworm
  plugins:
    - name: quicksilver.howlerops.io
      parameters:
        tables: public.events,public.orders
```

| Parameter | Default | Notes |
|---|---|---|
| `mode` | `shadow` | `off`, `shadow`, `takeover` |
| `ingest` | `logical` | `physical` is Phase 3 and refused today |
| `tables` | — | required; `schema.table`, unquoted lowercase |
| `freshnessSLO` | `30s` | a mirror behind this leaves the endpoint |
| `database` | `app` | one mirror follows one database |
| `credentialsSecret` | `<cluster>-superuser` | needs REPLICATION, SELECT, CREATE |
| `slotName`, `publication` | `quicksilver_<cluster>` | |
| `mirrorPath` | `/var/lib/postgresql/data/quicksilver` | beside PGDATA, same PVC |
| `sidecarImage` | `…-mirror:latest` | |
| `acknowledgeOLTPRegression` | `false` | required for `mode: takeover` |

Defaults are written back into the Cluster by `MutateCluster`, so
`kubectl get cluster -o yaml` shows what the mirror is actually doing rather
than what this binary's defaults happened to be.

### What the plugin refuses, and why

Validation is where two measured findings are enforced rather than documented:

- **`mode: takeover` without `acknowledgeOLTPRegression: "true"`.** Takeover
  points `-ro` at the columnar mirror, which measured a **median 935× slowdown**
  (up to 3,445×) on OLTP-shaped queries ([docs/11](11-measured-results.md)). One
  word in a YAML file should not be able to do that.
- **`ingest: logical` on PostgreSQL < 17.** Before 17 a logical slot does not
  survive a failover and recovery is a full re-snapshot of every mirrored table
  ([docs/15](15-pg17-slot-failover.md)).

Also refused: unknown parameters (a misspelled `tabels:` otherwise yields a
mirror that mirrors nothing, silently), single-instance clusters, a pinned
`wal_level` that conflicts with `logical`, changes to `ingest`, `mirrorPath` or
`database` on a running cluster, and tables without a single-column primary key.

---

## What the sidecar does on each node

It runs on **every** instance Pod and decides its own role, because promotion is
not an event anyone tells it about:

- **on a replica** — bootstrap by snapshot at the slot's consistent point, stream
  `pgoutput`, apply with DDL barriers, gate `/readyz` on freshness;
- **on the primary** — stand down, and clear any inherited
  `synchronized_standby_slots` entry naming a slot that does not exist here.
  That last one is not tidiness: without it logical decoding on the new primary
  waits indefinitely, with a warning in the log and **no error to any client**
  ([docs/15](15-pg17-slot-failover.md)).

Endpoints on port 9187: `/readyz`, `/healthz`, `/metrics`.

`/readyz` is what actually keeps a stale mirror out of service, so it is a
correctness dependency rather than a nicety — and it distinguishes *idle* from
*behind*: an unchanging source is perfectly fresh. Liveness deliberately does
**not** check freshness; restarting a mirror that is behind discards its progress
and makes it further behind.

**What readiness costs, and why a restart is not instant.** The sidecar builds
its key index before `/readyz` passes, so a restarted Pod takes seconds rather
than milliseconds to return to service, and the RSS you see at that moment is
the RSS it will hold. Measured at 4M rows: ready at 2.24s holding 505 MB, and
the first write after that is 0.6s against a steady-state 0.5s.

It used to report ready in 0.04s at 25 MB and then spend ~1.8s building the
index inside its first write — behind a probe that had already told Kubernetes
to send traffic, and showing an operator a twentieth of the memory the Pod
actually needed ([docs/29](29-what-a-delete-costs-to-read.md)). Budget restart
time accordingly, and size the memory limit from what `/readyz`-time RSS shows,
because it is now the honest number.

A large mirror cannot restart-loop on this: readiness failing only keeps the Pod
out of the endpoint, and liveness is a separate `/healthz` that keeps answering
throughout the build.

---

## Verifying the mirror

```
kubectl exec app-2 -c quicksilver-mirror -- \
  qs-verify -dsn "$DSN" -mirror /var/lib/postgresql/data/quicksilver \
            -table public.events
```

`MATCH` or `DIVERGED`, with row counts and order-independent checksums on both
sides. Silent divergence is the failure this whole design is most exposed to —
counts stay plausible, queries keep answering, nothing errors — so being able to
ask on demand is what makes the mirror safe to put in front of anyone.

---

## Schema change

DDL arrives inside the change stream as a barrier and the mirror evolves at it.
Three outcomes:

| Change | Result |
|---|---|
| `ADD COLUMN` (no default), `DROP COLUMN` | applied; old files fill with NULL |
| `ADD COLUMN … DEFAULT <immutable>` | applied; old files fill with the catalog's `attmissingval` |
| `ADD COLUMN … DEFAULT <volatile>`, stored generated columns, retypes | **halts that table, durably** |

The middle row is the one that bit us. `ADD COLUMN channel text DEFAULT 'web'`
writes **no row-level WAL at all** — PostgreSQL stores one value in
`attmissingval` and every pre-existing row reads it back untouched. A logical
consumer sees the DDL and nothing else, leaves its own rows NULL, and diverges on
a column nobody will think to check, with identical row counts. The mirror now
reads that value out of the catalog at the barrier and substitutes it for files
written before the column existed, which is exactly what PostgreSQL does.

The third row cannot be recovered from the stream at all: a volatile default
rewrites the heap, every row gets its own value, and none of it is replicated.
There the mirror stops and says so. The halt is written to `state.json` on
purpose — a halt that lives only in a running process is undone by the next
restart, which re-reads the catalog, sees the new schema as though it had always
been there, and serves the corruption it stopped for. Recovery is deliberately
manual: delete the mirror directory and let it re-snapshot.

---

## What has not been verified

Stated plainly, because a deployment guide that implies more than was tested is
worse than no guide.

Four of these have since been closed and are listed below as what they now are,
because a list that keeps saying "untested" about things that have been tested
stops being read — which is the same failure as overstating, arriving from the
other direction.

- **Service discovery — now verified, with a stated substitution.** The real
  CloudNativePG operator (1.30, the released binary) discovers the plugin from
  the Service label alone, admits a Cluster naming it through the real admission
  webhooks, completes the CNPG-I handshake over mTLS, records our capabilities
  in `.status.pluginStatus`, and calls the lifecycle hook while building the
  instance Pod — with the sidecar in that Pod as a native sidecar
  ([`bench/scripts/operator_e2e.sh`](../bench/scripts/operator_e2e.sh)).
  The substitution: this sandbox does not grant `CAP_SYS_RESOURCE`, so there is
  no kubelet and Pods stay Pending. The API server, CRDs, operator and webhooks
  are real; everything up to and including the Pod **spec** the operator builds
  is real, which is where a plugin lives. What runs inside the Pod is not.
- **Live reconcile behaviour — still not observed.** Rollouts, switchovers and
  CNPG's own Pod comparison are reasoned about from the operator's source. The
  Pod spec is now real (above), but nothing has watched the operator *re*-build
  one. The `EVALUATE` bug in docs/17 is exactly the class of thing that reading
  catches and only a cluster confirms.
- **The images — now published, but never pulled.** The `images` job in
  [`ci.yml`](../.github/workflows/ci.yml) builds both targets from
  `deploy/Dockerfile` on every commit, so a Dockerfile that stops building is
  caught on the commit that broke it. `release.yml` has now run to completion
  once and pushed `pg_quicksilver-plugin:0.0.1`, `pg_quicksilver-mirror:0.0.1`
  (both also `:latest`) and `oci://ghcr.io/howlerops/charts/quicksilver:0.0.1`.
  Two things that publish does **not** establish: it was a `workflow_dispatch`
  on `main`, so the `tags: ["v*"]` path the release notes assume still has zero
  runs; and nothing has pulled either image, so "builds" and "runs from a
  registry" remain different claims.
- **`mode: takeover` — still not implemented.** The validation gate is tested;
  the service retarget it gates does not exist.
- **Serving — verified through DuckDB, not yet over the wire.** The mirror
  publishes a SELECT that reconstructs itself from the manifest, deletion
  vectors and column-partial deltas, and that view is checked for both speed and
  agreement with the source on every matrix run
  ([docs/20](20-serving-the-mirror.md)). What is still missing is answering a
  `SELECT` through *PostgreSQL* — the `pg_duckdb` path in
  [docs/12](12-s5-serving-path.md), which needs the 58-line patch landed.
- **Scale — verified at 23M rows.** The claim here used to be that everything
  ran against tens of thousands of rows and that "the key→position index is
  still a JSON map; it will not survive production volumes". Both halves are
  now out of date: a full run verified at **23,005,206 rows, MATCH**, and the
  index is a pointer-free `map[int64]loc` in memory with no persisted JSON at
  all — the file that used to hold it measured 2.7x the size of the Parquet it
  indexed and slower to load than rebuilding from it ([docs/32](32-the-knob-that-barely-fires.md)).

First real deployment should still be `mode: off` on a throwaway cluster, to
confirm the plugin loads and validates at all, then `mode: shadow` on one table.
What that sequence is now for is the two things above that a cluster is the only
way to reach: live reconcile behaviour, and a Pod that actually runs.
