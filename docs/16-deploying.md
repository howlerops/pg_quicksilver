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

> **CloudNativePG 1.26 or newer is required**, and below it the failure is
> silent rather than loud. 1.25 computes an instance Pod's expected spec without
> consulting plugins, so the sidecar is missing from both sides of the
> comparison that decides whether to roll a Pod — and `mode`, `freshnessSLO`,
> `sidecarImage` and `tables` therefore never reach a running instance. A Pod
> created fresh still gets the parameters it was created with, so this looks
> like it works until the first time you change your mind.
> [docs/37](37-the-operator-that-could-not-roll.md) has the function and the
> diff.

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
docker build --target plugin -t ghcr.io/howlerops/pg_quicksilver-plugin:0.0.2 .
docker build --target mirror -t ghcr.io/howlerops/pg_quicksilver-mirror:0.0.2 .
```

The sidecar image runs as UID 26, the UID the CNPG images use for `postgres`.
Running it as anyone else produces mirror files the instance cannot read.

---

## Installing the plugin

```
helm install quicksilver oci://ghcr.io/howlerops/charts/quicksilver \
  --version 0.0.3 --namespace cnpg-system
```

The chart is pushed as an OCI artifact, so there is no chart repository to add.
From a checkout, `helm install quicksilver charts/quicksilver` does the same
thing against the working tree.

**Do not install `0.0.2`**: its mirror sidecar binds `:9187` and stops
PostgreSQL from starting at all ([docs/35](35-the-port-that-stopped-postgresql.md)).
`0.0.3` is the first version that runs.

**That command needs no credential.** Checked against the registry rather than
assumed — an anonymous `helm pull` of the chart returns a digest, and both
image repositories answer an anonymous tag listing. An earlier version of this
paragraph said it fails with a 401, which was true while the repository was
private: GHCR packages inherit a repository's visibility when they are created
and do not follow it afterwards, so the claim outlived the fact.

If you fork this into a private repository, the credential problem has **two
halves in two namespaces**: the plugin image is pulled by this chart's
Deployment in the operator's namespace, while the mirror image is pulled by
instance Pods in the *Cluster's* namespace. The second half is
`spec.imagePullSecrets` on the Cluster and no amount of chart configuration
reaches it. Both are spelled out in
[the chart README](../charts/quicksilver/README.md#if-your-registry-is-private).

It is worth knowing the shape of that failure: the plugin installs and runs
perfectly, and the mirror turns up as `ImagePullBackOff` on an instance Pod much
later, looking like a Cluster problem rather than a registry one.

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
| `freshnessSLO` | `30s` | in `takeover`, a mirror behind this leaves the endpoint |
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
  serves `-ro` reads from the columnar mirror, which measured a **median 935×
  slowdown** (up to 3,445×) on OLTP-shaped queries
  ([docs/11](11-measured-results.md)), and it makes mirror freshness a condition
  of serving at all — a write burst can empty the read endpoint
  ([docs/33](33-the-probe-that-gated-the-wrong-thing.md)). One word in a YAML
  file should not be able to do either.
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
  `pgoutput`, apply with DDL barriers, report freshness on `/readyz`;
- **on the primary** — stand down, and clear any inherited
  `synchronized_standby_slots` entry naming a slot that does not exist here.
  That last one is not tidiness: without it logical decoding on the new primary
  waits indefinitely, with a warning in the log and **no error to any client**
  ([docs/15](15-pg17-slot-failover.md)).

Endpoints on port **9188**: `/readyz`, `/healthz`, `/metrics`.

Not 9187, which is where CloudNativePG's instance manager serves its own
metrics. Containers in a Pod share one network namespace, and the mirror is a
native sidecar, so it starts first and binds the port — then the instance
manager cannot, calls it `unretryable`, and exits. PostgreSQL never starts and
the instance Pod crash-loops, in `shadow` mode, on every instance
([docs/35](35-the-port-that-stopped-postgresql.md)).

`/readyz` distinguishes *idle* from *behind*: an unchanging source is perfectly
fresh. Liveness deliberately does **not** check freshness; restarting a mirror
that is behind discards its progress and makes it further behind.

**`/readyz` is wired as the Pod's readiness probe only in `mode: takeover`.**
This is a native sidecar, and Kubernetes uses a restartable init container's
readiness probe to decide the *Pod's* readiness — so the probe does not gate the
mirror, it gates PostgreSQL. In `shadow` that is backwards: a mirror nobody is
querying would pull a healthy replica out of `-ro`, and out of every replica at
once, since they all fall behind the same writer. It used to be wired in every
mode; [docs/33](33-the-probe-that-gated-the-wrong-thing.md) is what that was and
why takeover is the only place it belongs.

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
- **Live reconcile behaviour — now verified, on a real cluster.**
  `lifecycle.go` argues that declaring `EVALUATE` is not optional, and
  `go/fidelity/rollout_test.go` drives CloudNativePG's own
  `specs.ComparePodSpecs` over Pods built by its own `specs.NewInstance`, which
  answers `init-containers: container quicksilver-mirror differs in image` and
  compares an unchanged Cluster as equal. The operator *acting* on that was the
  last thing outstanding, and [`cluster_e2e.sh`](../bench/scripts/cluster_e2e.sh)
  sections 4 and 5 now show both:

  ```
  app-2 replaced: 69d4df4b -> 44e32435
  ok: the operator rolled the instances after sidecarImage changed

  ok: every instance Pod now runs QS_MODE=takeover
  app-1: postgres ready, mirror NOT ready, and absent from app-ro
  ok: mirror freshness gated the endpoint, with PostgreSQL itself healthy
  ```

  The second is the property [docs/33](33-the-probe-that-gated-the-wrong-thing.md)
  reasons about from the Kubernetes contract: PostgreSQL healthy, the mirror
  behind, the node out of service because of the mirror. Two cautions that the
  history earns. Section 4 once reported a pass while watching a second instance
  be created for the first time, so it now compares uid **per instance name** —
  `app-2 replaced: … -> …` is a named Pod that was replaced, which a new Pod
  appearing cannot fake ([docs/37](37-the-operator-that-could-not-roll.md)). And
  none of it works below **CloudNativePG 1.26**, which the harness now asserts
  against the running operator rather than the requested one.
- **The images — built, published, run locally; pulled by a Pod only when
  [`cluster-e2e`](../.github/workflows/cluster-e2e.yml) runs.** CI builds both
  targets on every commit and `release.yml` has published twice.
  [`image_e2e.sh`](../bench/scripts/image_e2e.sh) starts the shipped images and
  `qs-verify` from the same image reports MATCH. What none of that touches is a
  registry: a pull into a Pod needs a kubelet this machine cannot provide
  ([docs/21](21-against-the-real-operator.md)). The packages themselves are
  public — an anonymous `helm pull` of the chart returns a digest and both image
  repositories answer an anonymous tag listing, checked against ghcr rather than
  assumed.
  `cluster-e2e` is that check — it pulls the chart and both images, then makes a
  Cluster pull them, which is also the first test of the install instructions in
  the chart README. **Still open, and precisely this much:** cluster-e2e now
  passes, but the runs that pass it use `images: source`, which side-loads into
  the node and says so in its own output (`LOCAL_IMAGES=1 — side-loading, which
  does NOT exercise the registry`). The registry half runs in `published` mode,
  which the release workflow triggers automatically on a tag. So a Pod pulling
  *these* images works; a Pod pulling them *from ghcr* is verified at the next
  release and not before.
- **`mode: takeover` — now observed working.** It is one property: mirror
  freshness gates the `-ro` endpoint. The service retarget the original design
  called for turned out not to be reachable under a sidecar architecture, and
  `app-ro-row` with it — one PostgreSQL serves both engines on the same Pod, so
  there is nothing to point elsewhere
  ([docs/33](33-the-probe-that-gated-the-wrong-thing.md)). The probe's presence
  per mode is unit-tested in both directions, and a probe actually *failing* and
  removing a Pod from a Service has now happened on a real cluster:

  ```
  app-1: postgres ready, mirror NOT ready, and absent from app-ro
  ```

  PostgreSQL healthy, the mirror behind, the node out of service because of the
  mirror — which is the whole of what the mode claims.
- **Serving — now answered through PostgreSQL 17.** The mirror publishes a
  SELECT that reconstructs itself from the manifest, deletion vectors and
  column-partial deltas, and that view is checked for both speed and agreement
  with the source on every matrix run ([docs/20](20-serving-the-mirror.md)). It
  is now also answered *through PostgreSQL*, by an unprivileged role holding
  neither `pg_read_server_files` nor `pg_write_server_files`, against a mirror
  built by the production sidecar on a standby
  ([`bench/scripts/serving_pg17.sh`](../bench/scripts/serving_pg17.sh)) — 196,000
  rows and an identical checksum, with everything outside the mirror directory
  denied. The caveat is the dependency: this needs `pg_duckdb` plus the
  `allowed_directories` patch in [`bench/patches/`](../bench/patches/), which is
  not upstream and not packaged. Without it the section reports INCOMPLETE.
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
