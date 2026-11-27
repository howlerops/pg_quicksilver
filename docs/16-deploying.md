# 16 — Deploying Quicksilver

Everything in docs 01–15 is analysis or measurement. This is the part you can
install. It covers the three images, the CNPG-I registration, the Cluster spec,
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

## Engine versions

- The required DuckDB major is 2.0.
- The pinned pg_duckdb is 1.2.2, which embeds DuckDB 2.0.1. `deploy/Dockerfile` and the `extension` job in `ci.yml` fetch that package. `deploy/duckdb-extensions.sh` takes its DuckDB version from the installed `libduckdb.so`, not from a pin.
- There are two ways to get that pg_duckdb into the `postgres` image, chosen by the build argument `PG_DUCKDB_BUILD` in `deploy/Dockerfile`:
  - `package` (the default): the stock pg_duckdb 1.2.2 Debian package, nothing compiled. The mirror is readable by superusers only. A role that is not a superuser can read local files only if it holds both `pg_read_server_files` and `pg_write_server_files`, and holding both lets it read and write any file the server can.
  - `source`: pg_duckdb v1.2.2 built in the image at `PG_DUCKDB_COMMIT`, with `bench/patches/pg_duckdb-1.2.2-allowed-directories.patch`. The mirror is readable by ordinary roles. A role that is a member of `duckdb.postgres_role` and of neither server-file role can read local files only under `duckdb.allowed_directories`. It cannot write anything, and it cannot read anything else, including through `..`, a symlink, HTTP or `ATTACH`. The settings it needs, in `postgresql.conf`:
    - `shared_preload_libraries = 'pg_duckdb'`
    - `duckdb.postgres_role = '<role>'`: a restart is needed to change it.
    - `duckdb.allowed_directories = '<dir>/'`: superuser only, comma-separated for several directories, trailing slash on each.
    - Nothing else. The patch turns `enable_external_access` off and locks the DuckDB configuration for such a role itself, so the role cannot widen the allowlist from SQL.

    It takes about an hour, because DuckDB is compiled too. Build it with `docker build --build-arg PG_DUCKDB_BUILD=source --target postgres ...`. The confinement is checked by `bench/scripts/pg_duckdb_confinement_check.sh`, and the `serving` job in `ci.yml` builds the same patch and runs that check on every push.
- The python `duckdb` used by the tests and benchmarks is 2.0.1 (`duckdb==2.0.1` in `ci.yml`).
- The measurements in docs 38 to 40 were taken on pg_duckdb 1.2.0 (DuckDB 2.0.0). They are records, not pins. On 1.2.2, only the extension's regression tests and the equality harness were re-run; see docs/40's "pg_duckdb 1.2.2" section.
- The package source drops old builds: the repository serves only the newest one, and 1.2.0 now returns HTTP 404. When the pin 404s, move it: take `Filename` and `SHA256` from the repository's `Packages` index, change the URL and checksum in `deploy/Dockerfile` and in the `extension` job's `env` in `ci.yml`, and re-run the `extension` job. That job proves the extension, the DuckLake extensions and the regression suite against the new build. Re-run `bench/scripts/serving_pg17.sh` too if the DuckDB major changes.

## The images

| Image | What it is | Where it runs |
|---|---|---|
| `pg_quicksilver-plugin` | the CNPG-I gRPC server | a Deployment in the **operator's** namespace |
| `pg_quicksilver-mirror` | the sidecar that builds and serves the mirror, with `qs-verify` and `qs-query` beside it | injected into every instance Pod |
| `pg_quicksilver-postgres` | CloudNativePG's PostgreSQL 17 with pg_duckdb, `pg_quicksilver` and DuckDB's `ducklake` and `sqlite` extensions | the Cluster's `imageName`, **only** for Clusters that route reads |

All three come from one [`deploy/Dockerfile`](../deploy/Dockerfile) via `--target`.

The mirror image is **not** a PostgreSQL image. It ships three static binaries
(built with cgo off, into distroless) and no server, because it reaches
PostgreSQL over the network and the local socket. That is what lets
[docs/07](07-image-strategy.md) hold for a Cluster that only mirrors: its
PostgreSQL image stays stock upstream. The third image is for the one feature
that runs inside the server; [Routing reads](#routing-reads-with-pg_quicksilver)
has what is in it and how it is checked.

```
docker build --target plugin   -t ghcr.io/howlerops/pg_quicksilver-plugin:0.0.4      -f deploy/Dockerfile .
docker build --target mirror   -t ghcr.io/howlerops/pg_quicksilver-mirror:0.0.4      -f deploy/Dockerfile .
docker build --target postgres -t ghcr.io/howlerops/pg_quicksilver-postgres:17-0.0.4 -f deploy/Dockerfile .
```

**The operand image's tag starts with the PostgreSQL major, and has to.**
CloudNativePG reads a Cluster's PostgreSQL version from the front of the image
tag (`version.FromTag`, the regular expression `^(\d\.?)+`): `0.0.4` is read as
PostgreSQL 0, and `dev` or `latest` is refused at admission with `invalid
version tag`. So it is `17-<version>` everywhere, and
`bench/scripts/validate_manifests.sh` fails a chart whose `image.postgres` does
not parse as 17.

The sidecar and operand images run as UID 26, the UID the CNPG images use for
`postgres`. Running the sidecar as anyone else produces mirror files the
instance cannot read.

`0.0.3` is the newest **published** version and has only the first two images.
The chart in this tree is `0.0.4`, which is not published until it is tagged
(HANDOFF.md, *Cutting a release*).

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
| `slotName`, `publication` | `quicksilver_<cluster>` | `slotName` is a prefix: each Pod's slot is `<slotName>_<pod>` |
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

- **on a replica** — take a replication slot of its **own**, named
  `<slotName>_<pod name>` (lower-cased, anything outside `[a-z0-9_]` becomes
  `_`, cut to 63 bytes with a hash if it has to be). A slot is one cursor, and
  two sidecars sharing one meant the second silently missed every row written
  while the first held it. On a restart the slot's `confirmed_flush_lsn` is
  compared with each table's `applied_lsn`; a table the slot has got ahead of is
  re-snapshotted, with a `WARN` saying why, instead of resumed with a hole in it;
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

## Publishing the mirror as a DuckLake

Off by default. With `QS_DUCKLAKE=on` the sidecar also publishes every mirrored
table as a [DuckLake](https://ducklake.select) table: the mirror's own Parquet
files, hard-linked under `<mirror>/_ducklake/data`, behind a catalog in SQLite
that the sidecar writes itself (`go/internal/ducklake`, pure Go, no DuckDB in
the sidecar). Nothing is rewritten. A reader attaches the catalog and reads a
table:

```
qs-query -mirror /var/lib/postgresql/data/quicksilver -table public.orders -ducklake

ATTACH 'ducklake:sqlite:/var/lib/postgresql/data/quicksilver/_ducklake/catalog.sqlite' AS "lake" (READ_ONLY);
-- the table: lake."public"."orders"
SELECT applied_lsn, manifest_seq, snapshot_id, refusal
  FROM "__ducklake_metadata_lake".qs_mirror_table WHERE table_name = 'public.orders';
```

The reason to want it: the generated view of [docs/20](20-serving-the-mirror.md)
names files, so it describes one manifest and has to be generated again after
every apply; a view over a DuckLake table names the table and reads the
current snapshot, always. It is also faster to read once deletes
exist. Two million rows after 150 applies of 2,000 updates + 400 deletes + 400
inserts (14 data files, 12 with deletes), python duckdb 2.0.1, `taskset -c 1`,
one thread, warm, best / median of 5, answers equal:

| | published view | lake table |
|---|---|---|
| `count(*)` | 65.5 / 68.2 ms | 2.8 / 2.9 ms |
| `GROUP BY status` | 83.1 / 84.5 ms | 32.5 / 36.4 ms |
| `sum(amount)` | 109.5 / 114.6 ms | 63.0 / 63.2 ms |
| one-month range, count + sum | 114.2 / 119.1 ms | 104.5 / 105.2 ms |

| setting | default | |
|---|---|---|
| `QS_DUCKLAKE` | off | `on` publishes every table in `QS_TABLES` |
| `QS_DUCKLAKE_RETENTION` | `5m` | how long a snapshot stays readable after a newer one replaced it. This, not `QS_RETIRE_GRACE_SECONDS`, is the grace of a lake reader: the lake holds its own link to every file |
| `QS_DUCKLAKE_PATH` | `<mirror>/_ducklake` | must be on the mirror's filesystem |

**One manifest, one snapshot.** Each manifest the mirror commits is handed to
the publisher, which brings the lake table to it in one SQLite transaction that
is one DuckLake snapshot: new files linked and registered with statistics read
from their Parquet footers, a delete file written for each file whose dead set
changed, dropped files ended, columns added (with `attmissingval` as the
DuckLake `initial_default`) or dropped. The snapshot records the manifest's
`applied_lsn`, and `qs_mirror_table` holds the current one for a freshness
check. A reader sees a state the mirror was in or the one before it, never
anything between.

**Not on the apply path.** The hand-off stores a pointer. Publishing happens on
another goroutine; if it falls behind it publishes the newest manifest and
skips the ones in between. Measured with the real sidecar on that table, 150
transactions 0.3 s apart, one run each on a shared 4-core machine:

| | apply tick, median / p95 | apply phase, median / p95 |
|---|---|---|
| `QS_DUCKLAKE=on` | 64 / 1380 ms | 59 / 223 ms |
| off, `QS_PARTIAL_DELTAS=0` (the like-for-like baseline) | 68 / 1168 ms | 66 / 159 ms |
| off, defaults | 85 / 1339 ms | 82 / 275 ms |

The publish itself, off to the side: median 40 ms, p95 102 ms over 134
publishes, none coalesced. It follows the number of files whose deletes changed
in the apply, as the prototype's did (129 ms there).

**What a reader gets when a table must not be served.** A halted table, or one
being rebuilt with nothing kept to read, is ended in the lake and a view of the
same name is put in its place whose only effect is to fail with the reason:

```
Catalog Error: Table with name quicksilver mirror public.orders is halted and
must not be served: <the halt reason> does not exist!
```

The same is done for a table that cannot be a DuckLake table as it stands, and
the sidecar then fixes it by itself:

- **Column-partial patch files.** A DuckLake data file is whole rows, so
  `QS_DUCKLAKE=on` turns partial deltas off (as `QS_PARTIAL_DELTAS=0`), and what
  they save on wide rows is given up. A mirror that already holds patch files is
  refused in the lake, a base rewrite is started, and the table is published
  when it lands (13 s at two million rows).
- **Files that hold a column another way than the mirror writes it now**, which
  a sidecar upgrade that changes how a type is stored leaves behind, whether
  next to newer files or alone. The lake is the files, so it can publish such a
  table only where one source value reads as the same value from either form,
  through `=`, a range, `DISTINCT`, a join and `ORDER BY`. Two cases do, each
  proved against DuckDB 2.0.1 (`TestWidenedColumnsReadAsOneValue`): integers
  of different widths are published as the wider type, and the column narrows
  again when the last wide file is gone; and a uuid stored as text in older
  files is published as `UUID`, which DuckDB's DuckLake reads correctly
  although its own `ducklake_add_data_files` refuses to register one.
  Everything else is refused until a base rewrite, started the same way,
  leaves one form:
  - `real` held as `DOUBLE` (it reads as `0.1` where the newer files, and the
    source, hold the float4 `0.1`, which is `0.10000000149011612` as a double:
    two values for one);
  - an unconstrained `numeric` held as `DECIMAL(38,9)`, and `bytea` held as text;
  - a `date`, `timestamp` or `timestamptz` file whose statistics say it may
    hold `-infinity` as the previous release stored it (the type's minimum),
    which DuckDB reads as a finite date in 290309 BC. The mirror's own view
    corrects that per file; a lake cannot.

A rewrite is asked for once per reason. While the publish of the rewrite's own
manifest is failing (a full disk, say) none is asked for: the publish is
retried instead. A rewrite that was not started, or that left the same reason
behind, is asked for again after 15 seconds, then 30, doubling to 15 minutes.
`quicksilver_ducklake_refused` is 1 for as long as the table waits.

**Expiry.** A snapshot is expired once it has been superseded for longer than
the retention window: its catalog rows go in one transaction, and only after
that has committed are the files nothing retained can see unlinked. Catalog
size does not grow with the number of publishes: 483 KB after 1,000 publishes
and 492 KB after 5,000, at 209 snapshots retained throughout.

**Restart and crash.** Nothing about the lake is kept outside the catalog. A
publish that dies leaves the previous snapshot as it was, because nothing in
the catalog changed; the files it had made are unnamed and are removed when the
lake is next opened. At start the sidecar submits each table's current manifest
and the lake is brought to it.

**PostgreSQL, through pg_duckdb.** The view is created once:

```
qs-query -mirror ... -table public.orders -ducklake -engine postgres \
         -view qs_lake.orders -dsn "$DSN" [-attach-on-login]
```

pg_duckdb's DuckDB lives in the backend, so the `ATTACH` has to run once in
every session before the view is used. `qs-query` prints it
(`SELECT duckdb.raw_query($qs$ ATTACH IF NOT EXISTS ... $qs$)`), and with
`-attach-on-login` a PostgreSQL 17 login event trigger that runs it for every
session, on standbys too. The trigger has no `EXCEPTION` block, because
pg_duckdb refuses subtransactions; it attaches only if the catalog file is
there, so that a node without a mirror logs in normally. If it ever locks
sessions out: `event_triggers = off`.

**Metrics**, on the sidecar's `/metrics`, per table:
`quicksilver_ducklake_published_lsn`, `..._published_manifest_seq` against
`..._submitted_manifest_seq`, `..._lag_seconds` (how long the oldest
unpublished manifest has waited), `..._last_publish_seconds`,
`..._coalesced_total`, `..._publish_errors_total`, `..._refused`; and for the
lake `quicksilver_ducklake_publish_seconds{quantile}`, the expiry counters and
`quicksilver_ducklake_catalog_bytes`.

**What it depends on, and what breaks it.**

- *One filesystem.* The lake is hard links. `QS_DUCKLAKE_PATH` on another
  filesystem is refused at start, with a message that says so; object storage
  is not supported.
- *The catalog format.* The sidecar writes DuckLake catalog format **1.1** and
  refuses, at start and again inside every publish, a catalog that says
  anything else. A DuckDB new enough to migrate the catalog, attached
  read-write, would do exactly that: readers must attach `READ_ONLY` (the
  statement `qs-query` prints does). A format bump is a change to one file,
  `go/internal/ducklake/catalog.go`, and the conformance test is what says
  whether it is right.
- *Readers write `-shm`.* SQLite in WAL mode needs a reader to be able to
  create `catalog.sqlite-shm` beside the catalog, so a reader needs write
  permission on `_ducklake/`. In a CNPG Pod the sidecar and PostgreSQL are the
  same user.
- *Nothing else may write the lake.* DuckLake's own compaction or expiry would
  produce a table the next manifest does not describe. A lock file stops a
  second sidecar; it does not stop DuckDB.
- A table removed from `QS_TABLES` stays in the lake as it last was.

How it was checked ([`go/ducklake/`](../go/ducklake/README.md) has the commands):

- **Conformance against DuckDB**, in `go test`: a table driven through 1,500
  random steps (applies, delete-only applies, delta merges, base rewrites with
  and without an apply in the middle, TRUNCATE, ADD COLUMN with and without a
  default, DROP COLUMN, halt, rebuild under reused file names), publishing after
  each. After every publish, python duckdb 2.0.1 confirmed that the lake table
  equals the mirror's own view (count and a hash of every row) and the test's
  model; that every retained snapshot still reads as it did when published
  (10,854 time-travel reads); that every expired one is gone (1,385); that 9,536
  filtered queries agree with the view; and that the lake directory holds
  exactly the files the catalog names. `ducklake_list_files` and
  `ducklake_table_info` agree with the manifest, and the per-file statistics
  equal the ones DuckDB's own `ducklake_add_data_files` writes for the same
  files.
- **Concurrency**: three reader processes querying in a loop for 240 s while
  the publisher wrote 5,689 snapshots, expired 5,630 and unlinked 17,842 files:
  23,016 queries, no error, and each of the 12,335 distinct answers was a state
  the mirror had committed.
- **pg_duckdb 1.2.0** (DuckDB 2.0.0) attaches the SQLite-catalog lake. A view
  over `lake.public.orders`, created once, agreed with the heap after each of
  three rounds of churn, on the primary and on the hot standby, in a session
  opened before the publishes and in a new one.
- **The real sidecar**: `qs-verify` MATCH, and the lake against the heap
  (count, sum, a hash of every row) equal, after churn and after each of six
  `kill -9` under load. Two of the six were aimed at the moment a publish had
  written its files and not committed: the lake read as its previous snapshot
  before the restart, and the two unnamed files were removed at the next open.

Not verified: a lake on anything but a local ext4 volume; a catalog format other
than 1.1; a reader that outlives the retention window (it is told a file is
missing); more than one table under churn (three tables in one lake are tested, at rest;
the conformance test is one table);
`-attach-on-login` under a connection pooler.

---

## Routing reads with pg_quicksilver

Optional, and the only part of this that needs anything in the PostgreSQL
image. [`ext/pg_quicksilver`](../ext/pg_quicksilver) is an extension that
answers a read from the mirror when every construct in the statement is on a
verified list, the plan is expensive enough and the mirror has caught up, and
from the heap otherwise ([docs/40](40-the-router-moves-inside.md)). The
sidecar does its half by itself when it finds the extension installed.

**The image.** The image the Cluster's `imageName` points at must contain

| | |
|---|---|
| pg_duckdb | 1.2.2 (DuckDB 2.0.1), the pin. It reads the mirror. See Engine versions above. |
| `pg_quicksilver.so`, `pg_quicksilver.control`, `pg_quicksilver--0.1.sql` | built from `ext/pg_quicksilver` against the same PostgreSQL major (PGXS, `postgresql-server-dev-17`, nothing else) |

`deploy/Dockerfile` has a target for it, published as
`ghcr.io/howlerops/pg_quicksilver-postgres:17-<version>`:

```
docker build --target postgres -t registry.example/pg_quicksilver-postgres:17-0.0.4 -f deploy/Dockerfile .
```

What the target does, and what is pinned (the `ARG`s at the top of the file):

| | pinned to | |
|---|---|---|
| base | `ghcr.io/cloudnative-pg/postgresql:17.12-standard-bookworm` | by PostgreSQL **minor**: the extension is compiled against that minor's headers, installed at exactly the server's package version (PGDG, falling back to its archive suite) |
| pg_duckdb | 1.2.2, one `.deb` by URL and SHA-256 | pg_duckdb publishes container images, not packages, and PGDG does not carry it. The image takes the Debian 12 (bookworm) build; the `extension` job in CI takes the Ubuntu 24.04 (noble) build of the same version, with its own URL and checksum. Each is fetched as one checksummed file rather than by adding the repository. Both checksums match the `Packages` index of the repository for 1.2.2; the downloads themselves are unverified until CI runs |
| `pg_quicksilver` | this checkout | PGXS in a build stage (`with_llvm=no`), three files copied out |
| DuckDB's `ducklake` and `sqlite` | whatever DuckDB 2.0.1's repository serves on the day of the build | installed by pg_duckdb's own DuckDB into `/usr/lib/quicksilver/duckdb-extensions`. DuckDB writes them 0600, so `install` makes them readable by everyone, and the directory is then copied root-owned. DuckDB decides the `v2.0.0/linux_amd64/` layout, so this stays right when the package moves |

Nothing about CloudNativePG's contract changes: same base, UID 26, same `PATH`,
no `ENTRYPOINT`, and nothing added outside `/usr`.

**The build is also the test.** Its last stage runs
[`deploy/duckdb-extensions.sh verify`](../deploy/duckdb-extensions.sh) under
`RUN --network=none`, as UID 26: a scratch PostgreSQL with
`shared_preload_libraries = 'pg_duckdb,pg_quicksilver'`, both extensions
created, the statement the sidecar itself runs on a new cluster
(`duckdb.install_extension`), and a DuckLake on a SQLite catalog written and
read back. If any of that needs the network, or the `.so` does not load in
that server, the image does not build. The transcript is in the image at
`/usr/share/quicksilver/verified.txt`.

By hand it is `make -C ext/pg_quicksilver && make -C ext/pg_quicksilver install`
on a machine with the server headers, the three files above in `pg_config
--pkglibdir` and `--sharedir`'s `extension/`, and `deploy/duckdb-extensions.sh
install <dir>` as the `postgres` user.

*Checked on the development host, without Docker* (Ubuntu 24.04, PostgreSQL 17.12, pg_duckdb 1.2.0): the Go commands of the build stage, as written, with `CGO_ENABLED=0`; the extension with PGXS and `DESTDIR` into a scratch directory, giving the layout the `COPY`s expect; `deploy/duckdb-extensions.sh install` as the `postgres` user, then `verify` under `unshare -n`, as `postgres`, against a root-owned directory, and the control that `install` **fails** under `unshare -n`. That check found a real defect: DuckDB's files were 0600, so the server could not open them after the copy to root. `install` now makes them readable (commit `48cc589`). `hadolint` passes. *Not checked until CI builds it:* the image itself, the `RUN --network=none` stages under BuildKit, the pinned CloudNativePG base (`17.12-standard-bookworm`, which needs the registry), and the pinned pg_duckdb packages.

**The Cluster.**

[`examples/cluster-routed.yaml`](../examples/cluster-routed.yaml) is the whole
thing, with the reason for each line. The part that differs from a Cluster that
only mirrors:

```yaml
spec:
  imageName: ghcr.io/howlerops/pg_quicksilver-postgres:17-0.0.4
  postgresql:
    shared_preload_libraries: ["pg_duckdb", "pg_quicksilver"]   # in this order
    parameters:
      duckdb.extension_directory: /usr/lib/quicksilver/duckdb-extensions
  plugins:
    - name: quicksilver.howlerops.io
      parameters:
        tables: public.orders,public.customers
        ducklake: "true"                 # the mirror relation is a view over the lake table
        serveWithHaltedTables: "true"    # a halted table is answered from the heap; see below
        sidecarMemory: 2Gi
```

and once, on the primary: `CREATE EXTENSION pg_duckdb; CREATE EXTENSION
pg_quicksilver;` in the mirrored database, and `quicksilver.route = auto` for
the roles or the database that should be routed (`shadow` first:
`quicksilver.reason_stats` then says what would have moved). On a new Cluster
`bootstrap.initdb.postInitApplicationSQL` does all three, which is what the
example does.

**The volume layout is one volume.** The mirror is at
`/var/lib/postgresql/data/quicksilver` (`mirrorPath`'s default, beside PGDATA
on the instance's PVC) and the lake at `quicksilver/_ducklake` under it. The
lake is hard links, so it has to be on the mirror's filesystem
(`go/internal/ducklake/publisher.go` refuses to start otherwise), and
PostgreSQL has to read both, which it can because the sidecar shares the volume
and the UID. `walStorage` or a tablespace on another volume changes none of
this; moving `mirrorPath` to one would.

| plugin parameter | default | sidecar setting | |
|---|---|---|---|
| `ducklake` | `false` | `QS_DUCKLAKE` | publish every table as a DuckLake table. Required for routing: it is the mirror relation that survives a compaction |
| `ducklakeRetention` | unset (the sidecar's 5m) | `QS_DUCKLAKE_RETENTION` | how long a lake snapshot outlives being replaced; at least 10s, and only with `ducklake` |
| `serveWithHaltedTables` | `false` | `QS_SERVE_WITH_HALTED_TABLES` | keep the Pod ready with a halted table. With the extension that table is answered from the heap on that Pod, so there this is safe |
| `extension` | `auto` | `QS_EXTENSION` | `auto`: register mirror relations and report applied LSNs if `pg_quicksilver` is installed, do nothing and say so once if it is not. `off`: never touch it |
| `sidecarMemory` | `1Gi` | request, limit, `GOMEMLIMIT` | see below |
| `slotJanitor` | on | `QS_SLOT_JANITOR` | `off` stops the sidecar dropping the replication slots of Pods that are gone; their WAL is then kept until someone drops them |
| `slotJanitorAfter` | `6h` | `QS_SLOT_JANITOR_AFTER` | inactivity before such a slot is dropped; at least `1m` |
| `autoRebuildLegacy` | `"false"` | `QS_AUTO_REBUILD_LEGACY` | rebuild a table holding data from an older writer at start (see *Files from an older sidecar*) |
| `bloomColumns` | the sidecar's default | `QS_BLOOM_COLUMNS` | the bloom filter columns, in the sidecar's spelling (see *Bloom filters*) |
| `clusterBy` | the sidecar's default | `QS_CLUSTER_BY` | the clustering column per table, `schema.table=column,…`, or `none` for a table that should not be clustered |
| `numericScale` | unset | `QS_NUMERIC_SCALE` | the opt-in decimal scale for `numeric` columns (see *`numeric` without a precision*) |

A value that is not a boolean, a duration or a quantity is refused at
admission; the sidecar itself would have read it as off.

**What the sidecar then does**, with no further configuration: creates the
schema `quicksilver_mirror` and one view per table over `lake."schema"."table"`,
registers it, records the lake's catalog path in `quicksilver.lakes`, installs
DuckDB's `ducklake` and `sqlite` extensions through pg_duckdb, and reports each
table's applied LSN to the PostgreSQL in its own Pod every 25 ms. The view
names no file and no path, so it is the same on every instance; the extension
attaches the lake in a session itself, only where the catalog file exists, so
a primary (which has no mirror) is untouched and answers from the heap.

- *DuckDB's extensions.* Every instance loads `ducklake` and `sqlite` when a
  session first uses DuckDB. pg_duckdb's setting for where they are is
  `duckdb.extension_directory`, and its default is
  `$PGDATA/pg_duckdb/extensions`: on the volume, so empty on a new instance,
  and filled by a download from `extensions.duckdb.org` the first time. An
  instance that cannot reach it stays on the heap and does not say why. The
  operand image carries both under `/usr/lib/quicksilver/duckdb-extensions`,
  read-only, and **the Cluster has to name that directory** (above); the
  sidecar's `duckdb.install_extension` then finds the files and fetches
  nothing. Tested with no network: by the image build, and by CI's `extension`
  job with `unshare -n`. The cluster test asserts that nothing was downloaded
  into PGDATA. Leave the parameter out and it still works wherever the
  instances can reach the repository, which is the failure worth avoiding: it
  works in the test cluster.
- *Who is routed.* Stock pg_duckdb lets only a superuser read local files, so
  only superusers are routed and everybody else is answered from the heap,
  correctly.
- *Freshness is strict by default*: a statement is routed only if the mirror
  has reached what the statement can see on its instance. With steady writes
  to a mirrored table that is rarely true (docs/40 has the numbers); such a
  workload sets `quicksilver.max_lag` or stays on the heap.

### Sizing `sidecarMemory`

The default is **1Gi**. It was 256Mi, sized from the key index at readiness
(90 to 130 bytes of RSS per live row, METRICS.md). A limit has to hold the
peak, and the peak is not at readiness: it is during a catch-up and a base
rewrite together. A sidecar killed in the middle of a rewrite restarts,
rewrites again and is killed again.

**The rule, measured at 3M rows only:** budget **110 bytes per live row**.

The measurement: a table of 3,000,000 narrow rows with a UUID primary key, run
under a cgroup v1 memory limit with `GOMEMLIMIT` at 80% of it, as the plugin
sets it. Each run was an initial snapshot and key index, then a 200,000-change
backlog applied after the sidecar had been paused, then one base rewrite
(`why=idle`), with `qs-verify` MATCH after each phase. The build is this one,
with the tick sizing above.

| limit | every phase completed | peak resident (VmHWM) | per row |
|---|---|---|---|
| 256Mi | yes, no OOM kill (two runs) | 251 MiB | 88 bytes |
| 384Mi | yes | 280 MiB | 98 bytes |
| 512Mi | yes | 297 MiB | 104 bytes |
| 1Gi | yes | 342 MiB | 120 bytes |

The peak rises with the limit because the collector runs less often when it
has more room, so the 256Mi row is the one for the rule: 1.25 times 88 bytes
is 110 bytes, and 3M rows at 110 bytes is 314 MiB, which rounds to `320Mi`.

| rows (measured) | `sidecarMemory` |
|---|---|
| 3M | `320Mi` |

**What this does not show.** Nothing above 3M rows, and nothing with wide rows.
An older measurement at 10.5M rows (`bench/engine_compare/merged_build/out/p2_summary.txt`:
1,395 MB at the snapshot, 2,972 and 3,040 MB during a catch-up of 3.5 GB of WAL
and an idle base rewrite, 290 bytes per row) is not reproduced by the 3M
measurement above. The 3M test did not catch up 3.5 GB of WAL. For a larger
mirror, or wider rows, run the same phases at your own row count before you
choose a limit. The default `1Gi` is above the 3M rule and is what a plugin
gets when no `sidecarMemory` is set. The request equals the limit, so the sidecar is
Guaranteed QoS and cannot get the PostgreSQL Pod evicted, and `GOMEMLIMIT` is
80% of it, so an undersized sidecar gets slow before it gets killed. A
quantity under 128Mi is refused. `kubectl top` at readiness shows the steady
figure, not the peak.

### Sizing `sidecarMemory` for several tables

The rule above is for one table. With several tables in one sidecar, the peak
is not their sum. A base rewrite is the largest single cost, and it is per
table, so the sidecar runs **one base rewrite at a time** for the whole
process (`QS_MAX_CONCURRENT_REWRITES`, default 1). Another table waits for its
next tick and merges its deltas meanwhile, which keeps its file count bounded.
Row groups are also capped at 32 MiB of values (`QS_ROW_GROUP_BYTES`), because
a reader decodes a row group whole.

Measured, four tables in one sidecar, DuckLake on, four concurrent writers for
10 minutes with a TRUNCATE and an `ADD COLUMN` on the way, and `qs-verify`
MATCH plus lake-equals-heap at every minute:

| table | rows at start | rows at 10 min (final build) | row width |
|---|---|---|---|
| `ev_uuid` (uuid key, text) | 2,000 | 90,347 | about 50 bytes |
| `ledger` (numeric, timestamptz) | 2,000 | 118,929 | about 50 bytes |
| `wide_text` (12 text columns) | 2,000 | 77,010 | about 5 KB |
| `lookup` (small, truncated once) | 50 | 279 | about 50 bytes |

The row counts vary from run to run with the writers' timing: the baseline run
ended with 124,073, 177,162, 113,591 and 297.

| build | peak VmHWM | RSS p50 / p95 |
|---|---|---|
| before the fix (three rewrites overlapped) | 1,369 MB | 299 / 1,369 MB |
| rewrites one at a time, row groups by rows only | 2,146 MB | 498 / 981 MB |
| rewrites one at a time, row groups capped at 32 MiB | 490 MB | 207 / 357 MB |
| the same, under a 1 GiB cgroup limit (`GOMEMLIMIT` 858 MB) | 861 MB | 264 / 458 MB |

The 1 GiB run completed with no OOM kill, and every check was MATCH in each
build. Without the 32 MiB cap, the `wide_text` row group alone was 370 MB of
decoded text, and the rewrite that read it was the peak, whatever the number
of tables.

**What to use.** For this mix, the default `1Gi` is enough, with the sidecar
at 861 MB at its highest under the limit. Size for the largest single table
with the one-table rule above, and do not add the tables together. The one-table
rule's bytes per row was measured on narrow rows and does not carry over to rows
of several KB: a row group is capped at 32 MiB, but the rewrite's decoding
overhead on top of it was not measured separately. The measurements here do not
cover more than four tables, rows wider than 5 KB, or tables above about 180,000
rows each. Run your
own shape before you lower the limit.

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
manual; see the next section.

---

## How types are stored

A column is stored in the query engine's native type when that type holds every
value of the PostgreSQL type exactly, and otherwise as the text PostgreSQL
prints. The full table, and the reason for each row, is the comment at the top
of `go/internal/mirror/types.go`. What a reader sees through DuckDB:

| PostgreSQL | DuckDB reads |
|---|---|
| `smallint` / `integer` / `bigint` | `SMALLINT` / `INTEGER` / `BIGINT` |
| `real` / `double precision` | `FLOAT` / `DOUBLE` |
| `boolean`, `uuid`, `bytea` | `BOOLEAN`, `UUID`, `BLOB` |
| `numeric(p,s)` with p ≤ 38 | `DECIMAL(p,s)` |
| `numeric` with no precision, or p > 38 | `VARCHAR`: every digit PostgreSQL printed (below) |
| `date`, `time`, `timestamp`, `timestamptz` | the same; `infinity` and `-infinity` are DuckDB's own |
| everything else (`text`, `char(n)`, `jsonb`, enums, arrays, `interval`, `inet`, `money`, …) | `VARCHAR`, the text PostgreSQL prints |

### `numeric` without a precision

An unconstrained `numeric` column holds whatever it is given: `0.1234567890123`,
a 40-digit integer, `NaN`. No fixed decimal holds all of those, and the mirror
used to store such a column as `DECIMAL(38,9)` and **halt the table** on the
first value that did not fit. It now stores the column as text — exactly what
PostgreSQL printed, nothing rounded, nothing refused — and a reader is given
exactly that, as `VARCHAR`, through the view and through the DuckLake alike.

The rule is one per column, decided by the declared type and by
`QS_NUMERIC_SCALE` and by nothing in the data, so it never changes from one
file or one day to the next:

| the column is | a reader gets |
|---|---|
| `numeric(p,s)`, p ≤ 38 | `DECIMAL(p,s)` |
| `numeric`, or `numeric(p,s)` with p > 38 | `VARCHAR`, exact |
| `numeric`, named in `QS_NUMERIC_SCALE` | `DECIMAL(38,s)`, and a halt on a value that does not fit |

The view used to cast the text to `DOUBLE`, as DuckDB's PostgreSQL scanner
does. That made `1e400` read as `inf` and a 29-digit value lose twelve digits,
with no error, while the DuckLake of the same mirror said `VARCHAR`. A mirror
must not differ from its source without saying so, so there is no setting that
brings the cast back. The consequences, so that nobody finds them by surprise:

- the column no longer halts on anything, and no digit is lost;
- it is text: `sum(n)` is an error, and `ORDER BY n` and `n > 5` compare
  strings. A query that wants arithmetic says which arithmetic, and so chooses
  its own loss: `sum(CAST(n AS DECIMAL(38,6)))` (an error on a value that does
  not fit, `TRY_CAST` for NULL instead) or `CAST(n AS DOUBLE)` (15–17
  significant digits);
- `n = '1.5'` is a comparison of text, and PostgreSQL prints a numeric with the
  scale it was given: `1.50` is not `1.5`. Cast before comparing values.

When you know a column's scale and want exact decimal arithmetic, opt it in:

```
QS_NUMERIC_SCALE=public.orders.amount=4,public.fx.rate=12
QS_NUMERIC_SCALE=9        # every unconstrained numeric: the behaviour before this change
```

A column named there is stored as `DECIMAL(38,s)`, reads as that, and takes
back the halt: a value with more than `s` fractional digits, more than `38-s`
integer digits, `NaN` or `Infinity` halts the table (next section). Better
still, declare the precision in PostgreSQL — `numeric(18,4)` needs no setting.
Naming a column that **already holds rows** is a transition, not a switch. The
view used to cast the stored text to the decimal at once, and an engine's cast
rounds: `0.123456` at scale 4 read as `0.1235`, silently, until the next write
of such a row halted the table. Now the sidecar checks first. At its first
write after the restart it scans the column once:

- every stored value fits `DECIMAL(38,s)` exactly: the column becomes the
  decimal from that manifest on (`numeric_scales` in `state.json` records it),
  the view casts the older files — exactly, because that is what was checked —
  and a base rewrite converts them (`compaction started ... numeric-opt-in`);
- a value does not fit: the option is **refused for that column**, with one log
  line naming the value and its key (`QS_NUMERIC_SCALE is REFUSED for this
  column`), and the column goes on being served as exact text. Nothing is
  rounded and nothing halts. Correct the row or choose a scale that holds it,
  and restart: a refusal is looked at again on every start.

### Bloom filters

Every data file carries a Parquet bloom filter on the primary-key column and on
every other `uuid` column (the usual shape of a foreign key). A point lookup or
a selective join on such a column then skips the row groups that cannot hold
the value, which min/max statistics never do for a random key. The cost is about
two bytes per row per filtered column (1% false-positive rate). Measured on 2M
uuid-keyed rows with a uuid foreign key (DuckDB 2.0.1, one thread, best of 5;
`TestWriteUUIDBenchMirrors` writes the three mirrors):

| | uuid as text (before) | uuid native, no filters | uuid native + filters |
|---|---:|---:|---:|
| orders file | 94.3 MB | 83.1 MB | 91.2 MB |
| `WHERE id = …` | 126 ms | 39 ms | 9.9 ms (1 of 31 row groups read) |
| `WHERE customer = …` | 306 ms | 71 ms | 20 ms |
| join to 200k customers, grouped | 274 ms | 117 ms | 120 ms |

The two filters cost 8.1 MB, 2.0 bytes per row each. To choose the columns
yourself:

```
QS_BLOOM_COLUMNS=public.orders=customer_id,sku;public.events=-
QS_BLOOM_COLUMNS=off
```

### Upgrading a mirror written by an older sidecar

Nothing is rewritten on upgrade. Older files hold every integer as `BIGINT`,
every float as `DOUBLE`, `uuid` and `bytea` as text and unconstrained `numeric`
as `DECIMAL(38,9)`; they stay as they are until the table's next base rewrite
(compaction), and until then a table has both kinds of file. The view casts
every file to the declared type, so readers see the new types from the first
snapshot the new sidecar publishes, and `qs-verify` compares both kinds.
(A DuckLake cannot cast: it publishes the mixes it reads correctly and refuses
the rest until the rewrite, which it asks for itself. See *Publishing the
mirror as a DuckLake* above.)

An unconstrained `numeric` in an older file lost the scale PostgreSQL printed
it with (`1.50` was stored as `1.500000000`). It reads as `1.5`, the padding
dropped, from the old file and from the file the rewrite makes of it; a row
written after the upgrade reads `1.50`, as the source prints it.

Two things an upgrade does **not** repair, because the old files do not hold
what the source held. A mirror whose `char(n)` or `inet` columns were loaded by
the old *snapshot* has them without their padding, or with a `/32` the source
does not print; `qs-verify` now reports those tables `DIVERGED`, correctly.
Rebuild them (create `REBUILD` in the table's mirror directory and restart).
And a reader that depended on an unconstrained `numeric` being a number
should set `QS_NUMERIC_SCALE` (`=9` is what it was before: `DECIMAL(38,9)`), or
cast in its queries; without either the column is `VARCHAR`.

### Files from an older sidecar that cannot be trusted

`state.json` records a writer format version for each data file
(`file_format`; a file with no entry was written before versions were
recorded, and a file made from older files keeps their version).
`Table.LegacyFormatColumns()` reports, per table, the columns whose data comes
from a version known to be inconsistent for that column's type and which
therefore need the table rebuilt: the `::text` spellings above (`char(n)`,
`inet`, `cidr`, `macaddr`, `bit`, `money`, `interval`, `timetz`) in a legacy
base file, and an unconstrained `numeric` **primary key** that an older sidecar
stored as `DECIMAL(38,9)` — there a key the source prints as `1.50` does not
compare equal to the `1.5` the old file gives back, so changes to that row
leave a duplicate until the table is rebuilt.

At start the sidecar checks every table this way. For each table that reports
any such column it logs one `WARN` naming the table, the columns and the remedy,
and it exports `quicksilver_table_needs_rebuild{table="public.orders"} 1` on
`/metrics` for as long as the files are there. The default is to stop there:
the table keeps serving what it has, and the operator decides when to rebuild
it (create `REBUILD` in its mirror directory and restart, as for a halted
table). With `QS_AUTO_REBUILD_LEGACY=true` the sidecar does it itself: it halts
the table with a reason that says why, writes the `REBUILD` file, and the
rebuild runs through the same start-up path. A table that is already halted for
another reason is never rebuilt automatically, because that halt is there for a
person to read. The rebuild takes the snapshot's time and the table is out of
service for it, so leave the variable off on a large table until a window is
planned.

---

## Disk the mirror holds besides the data

**Large transactions** are spooled under `<mirror>/.spool`. The total is
bounded by `QS_SPOOL_MAX_BYTES` (unset: a quarter of the volume's free space
when the stream starts, at most 64 GiB; `off`: no bound). At the bound the
sidecar stops reading from PostgreSQL until a confirmation frees spooled
transactions that have committed, so the backlog waits in WAL behind the slot.
If the transactions still arriving do not fit by themselves the stream stops
with `spool budget exceeded: ...`, which says how many bytes had been reached;
nothing of them was applied and the slot has not moved.

**Superseded files** are kept for `QS_RETIRE_GRACE_SECONDS` so that a query
already running can finish, up to `QS_RETIRE_MAX_BYTES` per table. Unset, that
cap is the larger of 1 GiB and a quarter of the volume's free space. Whatever
the cap, the base file most recently superseded keeps its grace while the
volume has as much free space again as the file is large. When it has not,
the published `snapshot.json` carries `grace_unprotected` with the reason: a
reader that sees it should re-read the snapshot on "file not found" rather
than report an error.

**`snapshot.json` names files relative to the table directory** (`"paths":
"relative"`, with the writer's directory in `base_dir`), so a reader that
mounts the mirror at another path can use it: `qs-query -published` renders
the paths under the `-mirror` it was given, or under `-root`.

**A table that receives no changes writes nothing.** Its `state.json` then
says where it last changed; how far it is known to be current is in
`<mirror>/watermark.json`, which the sidecar writes before it confirms a
position (`qs-query -lsn` and `-published -lsn` already report that).

---

## A halted table

A table halts when the mirror cannot represent the source faithfully: an
unsupported schema change (above), a value the column type cannot store exactly
(`NaN` in a `numeric(12,2)`, a value with more digits than a column opted into
`QS_NUMERIC_SCALE` holds), a timestamp or date out of range. The halt is durable
and it belongs to **that table only**.

What the sidecar does:

- The halted table is out of service. Its changes are **discarded** while it is
  halted (it comes back by snapshot, never by replay), and it is skipped by
  apply, compaction and index build. `qs-query` and anything else that asks for
  its view is refused: `mirror is halted and must not be served: …`.
- Every other table keeps mirroring from the same slot. The stream is still
  confirmed, so the slot advances past the halted table's changes and pins no
  WAL on its account.
- Errors that are not about the table — I/O failure, a full disk, a cancelled
  context, a lost connection — still stop the process and retry with backoff, as
  before. Only an error the mirror itself recorded as a halt takes a table out.
- If **every** table is halted there is nothing to stream: the sidecar stops
  mirroring and drops its slot, as a halt always used to.

What you see (`/readyz` on the sidecar's health port, 9188):

```
HTTP 503
1 table is halted and not mirrored: public.orders (halting public.orders: column
"amount" (numeric(12,2)) holds "NaN", which the mirror's column type decimal(12, 2)
cannot store exactly. Storing NULL or a rounded va…). The other tables are still
mirrored; this Pod is unready because QS_SERVE_WITH_HALTED_TABLES is not set. To
rebuild a halted table, create a file named REBUILD in its mirror directory and
restart the sidecar
```

The reason is read from the manifests when the sidecar starts, so it reads the
same after a restart; it is not "diverged" and not "still bootstrapping". Each
table's reason is cut at 160 characters in the probe and the metric; the full
text is in the log line `table halted` and in `halted_tables` in the `/readyz`
JSON. `/metrics` carries:

```
quicksilver_halted_tables 1
quicksilver_table_halted{table="public.orders",reason="halting public.orders: column \"amount\" …"} 1
```

Alert on `quicksilver_halted_tables > 0`.

**Should the Pod be unready when one table of several is halted?** By default,
yes. A reader behind the Service cannot tell a Pod mirroring three tables from
one mirroring two, so by default it never reaches the second kind. Set
`QS_SERVE_WITH_HALTED_TABLES=true` on the sidecar to keep the Pod ready on the
strength of its healthy tables; the probe then answers 200 with
`…; DEGRADED, serving the other tables (QS_SERVE_WITH_HALTED_TABLES): 1 table is
halted and not mirrored: …`, the metrics above are unchanged, and the halted
table's view is still refused. Whatever the switch says, a Pod whose tables are
all halted is unready. (Like `QS_MAX_SLOT_WAL_BYTES`, this is read from the
sidecar's environment; the plugin does not have a parameter for it yet.)

**Rebuilding one table.** Fix the cause at the source first (update or delete
the row, or finish the schema change), then ask for the rebuild with a file in
that table's mirror directory, on the Pod concerned, and restart the sidecar:

```sh
kubectl exec <pod> -c postgres -- \
  touch /var/lib/postgresql/data/quicksilver/public.orders/REBUILD
kubectl delete pod <pod>        # or any restart of the sidecar
```

At start, a table that is halted **and** carries that file is re-snapshotted
through the same path as a table that joined an existing slot (an exported
snapshot with an exact LSN floor); its halt is cleared only after the snapshot
is durable, and the file is then removed. The other tables resume from the slot
and are not re-snapshotted — the log shows `snapshot complete` for the rebuilt
table alone. Without the file a halted table stays halted across any number of
restarts. If the snapshot meets the same problem again the table stays halted,
the file is kept, and the other tables carry on. A `REBUILD` file on a table
that is not halted is discarded with a warning: it never re-snapshots a healthy
table and never waits around to rebuild a future halt unattended.

It is a file and not an environment variable because it is a request, which is
spent once honoured; an env var stays in the Pod spec and would rebuild away
every later halt without anyone looking. It is also per Pod, like the halt.

When every table was halted the sidecar keeps retrying with backoff and
re-reads the manifests each time, so there the `REBUILD` files are picked up
without a restart; the slot is new, so each requested table is snapshotted.

Deleting the whole mirror directory still works and still costs every table a
snapshot.

---

## Join domains (EXPERIMENTAL, off by default)

> EXPERIMENTAL. Off unless `QS_JOIN_DOMAINS` is set. With it unset the sidecar
> writes the same files, manifest and behaviour as before.

A join on two `uuid` columns spends most of its time hashing 16-byte keys. A
join domain names the columns whose values are compared for equality (a key
and the foreign keys that reference it), and the sidecar gives each one a
hidden companion column holding a dense integer id for the value. A join on
the companions compares integers.

```sh
QS_JOIN_DOMAINS='cust=public.customers.id,public.orders.customer_id'
QS_JOIN_DOMAIN_MAX_KEYS=5000000   # default; per domain
```

Each entry is `name=schema.table.column,...`, separated by `;`. Only `uuid`
columns are accepted, and a column belongs to one domain. A bad value, or a
member that is not `uuid`, stops the sidecar at start.

What it writes:

- `<mirror>/_domains/<name>/dict.log`: one 16-byte record per value, in the
  order first seen. The id of a value is its record number, and never changes.
  The log is synced before any manifest that names a file holding one of its ids.
- `__qs_d_<column>` (INT64, nullable) in every new data file that carries the
  member. It is NULL where the member is NULL. The mirror's own reads, `Live`,
  verification and the published view never see it.
- `<mirror>/_domains/<name>/disabled`, if the domain passes its cap. The mirror
  keeps running, and its tables stay incomplete. Nothing re-enables a disabled
  domain, a restart included: the marker is durable.

What happens at the cap is the part that must be right, because a routed join
compares companions, and a companion that is NULL for a non-NULL value makes
that row silently fail to match. So the writer never does that:

- A file is never written with a NULL companion for a non-NULL member. A
  non-NULL member whose id cannot be assigned, because the domain is disabled
  or it trips the cap part-way through the write, is not written with NULL.
  The batch is converted again without the companion column and written
  without it (a batch write), or the streaming write fails and its file is
  abandoned, to be written again on the next pass, without the column.
- Any file written once a domain is disabled carries no companion column for
  it at all. The file's schema says so, and a reader cannot mistake it for a
  NULL companion.
- The lake declares the companion of every configured member column, from the
  moment the domain is configured, and not only once every file carries it. A
  file without the companion reads it as NULL (the column's initial default),
  so the mirror relation that exposes it is the same on every node. Whether a
  node may compare companions is not the lake's to say: each manifest lists
  the complete member columns (`JoinComplete`), and the sidecar reports those
  to its own node (docs/40, "Join keys"). A NULL companion is therefore never
  compared unless that node has said its column is complete.

Tests: `TestJoinDomainCapMidBatch` and `TestJoinDomainCapMidRewrite` (go/internal/mirror)
trip the cap in the middle of a batch and in the middle of a background rewrite
and read every file with its hidden column: no row has a non-NULL member and a
NULL companion. `TestJoinDomainCapTripKeepsCompanionDeclared`
(go/internal/ducklake) checks that the companion stays declared after the cap
trips, that the companion-less file reads NULL, and that the manifest no longer
lists the column as complete.

A table counts as complete for a domain when every file it names that carries
the member also carries the companion. A table written before the domain was
switched on is incomplete until its next base rewrite, which writes the
companion, and no older delta files remain. The manifest lists its complete
member columns (`JoinComplete`); the sidecar reports completeness per node,
before an incomplete manifest is published and after a complete one is
(extension.go, `beforePublish` and `afterPublish`). Metrics: `quicksilver_join_domain_keys` and
`quicksilver_join_domain_disabled`, one series per domain.

Measured on this tree (single thread, best of 5, DuckDB 2.0.1, 50k customers
and 500k orders, both tables complete): the uuid join takes 30.1 ms and the
companion join 8.4 ms, the same 500,000 rows and the same sum. The companions
add 1.09 MB to the orders base file (+12.6%) and about 2 KB to customers.

With the extension (pg_quicksilver, docs/40 "Join keys") the sidecar also
keeps `quicksilver.join_domains` on the primary: one row per configured member
column, written once when the domain is configured and never removed because a
column is incomplete. The mirror relation exposes its companion from then on.
Whether a join is compared on companions is decided by each node's report
(`quicksilver.report_join_complete`), and a column that stops being complete on
a node has its joins compared on the uuid columns on that node only.
All sidecars of one primary must have the same `QS_JOIN_DOMAINS`: the registry
row and the view are shared, and a sidecar that does not mirror a table removes
them.

Not verified: a crash of the whole host, as opposed to the fsync failures the
tests inject; joins from engines other than DuckDB; domains across more than
two tables in production volume.

## Running the cluster test

- **GitHub Actions**: the `cluster-e2e` workflow, by hand. `images: source`
  builds this checkout and side-loads it into kind; `images: published`, the
  default, and what a release triggers, pulls `version` from ghcr with the
  workflow's own token.
- **A host with Docker and kind**: `bash bench/docker/cluster_kind.sh`, about
  45 minutes; `KEEP=1` leaves the cluster up. It checks for Docker, cgroups and
  `CAP_SYS_RESOURCE` first, and exits INCOMPLETE rather than failing halfway.
- **Any cluster**: `KUBECONFIG=… VERSION=<a published version> GHCR_USER=… GHCR_TOKEN=… bash
  bench/scripts/cluster_e2e.sh` pulls a published version. With images already
  on the nodes, `LOCAL_IMAGES=1 ROLL_IMAGE=<a second tag>` says so in the output.
  `SCOPE=legacy` is for 0.0.3 and earlier; the default follows the version.
- **No cluster**: `sudo bash bench/scripts/cluster_e2e_selftest.sh` runs the
  slot, verify, routing, halt and REBUILD sections against a primary, a standby
  and the sidecar on this host. It needs PostgreSQL 17 with pg_duckdb and
  pg_quicksilver installed. Set `DUCKDB_EXT_DIR` to also check that DuckDB's
  extensions are carried rather than fetched.

A failed run leaves its logs in `$ARTIFACTS` (default `/tmp/qs-cluster-e2e`),
and the workflow uploads them.

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
- **The images — built, published, run locally, and pulled from ghcr by a Pod
  in [`cluster-e2e`](../.github/workflows/cluster-e2e.yml) run 32.** CI builds
  both targets on every commit and `release.yml` has published three times.
  [`image_e2e.sh`](../bench/scripts/image_e2e.sh) starts the shipped images and
  `qs-verify` from the same image reports MATCH. What none of that touches is a
  registry: a pull into a Pod needs a kubelet this machine cannot provide
  ([docs/21](21-against-the-real-operator.md)). The packages themselves are
  public — an anonymous `helm pull` of the chart returns a digest and both image
  repositories answer an anonymous tag listing, checked against ghcr rather than
  assumed.
  `cluster-e2e` is that check — it pulls the chart and both images, then makes a
  Cluster pull them, which is also the first test of the install instructions in
  the chart README — in `published` mode it now installs the chart from
  `oci://`, not from the checkout, so the packaged chart is under test rather
  than standing in for itself. **Closed at 0.0.3.** Earlier
  passing runs used `images: source`, which side-loads into the node and says so
  in its own output (`LOCAL_IMAGES=1 — side-loading, which does NOT exercise the
  registry`). Run 32 was the first in `published` mode, fired by the `v0.0.3`
  release: `chart: oci://ghcr.io/howlerops/charts/quicksilver --version 0.0.3`,
  `PULLED: imageID=…-mirror@sha256:9b5a206d…` (the digest ghcr serves for
  0.0.3), a named instance replaced, PASS. A Pod pulling these images *from
  ghcr* works.
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

- **Three instances, two mirrors, DuckLake and the router on a cluster — the
  test exists and has not run.** Everything in the DuckLake and routing
  sections above was validated on one host with one standby.
  [`cluster_e2e.sh`](../bench/scripts/cluster_e2e.sh) was extended to assert it
  on a three-instance Cluster built from the operand image (per-Pod slots,
  `qs-verify` MATCH in both replica Pods, an aggregate through `-ro` routed and
  equal to the primary's, a key lookup on the heap, a halt and its `REBUILD`, a
  switchover, a scale-down, the roll and the takeover gate), and
  [`cluster-e2e`](../.github/workflows/cluster-e2e.yml) now runs it on pull
  requests. It was written where no Pod can start. What stands in for a run:
  [`cluster_e2e_selftest.sh`](../bench/scripts/cluster_e2e_selftest.sh) runs
  the slot, verify, routing, halt and REBUILD sections of that same script
  against a real primary, standby and sidecar, and passes (run by hand on the
  development host, with `DUCKDB_EXT_DIR` set; `make installcheck` passes 10 of
  10 against the same cluster); the Kubernetes half
  (admission, Pods, Services, the second replica, switchover, scale-down) has
  been linted and read, not run. Treat the first cluster-e2e run of this as
  the first run.

First real deployment should still be `mode: off` on a throwaway cluster, to
confirm the plugin loads and validates at all, then `mode: shadow` on one table.
What that sequence is now for is the two things above that a cluster is the only
way to reach: live reconcile behaviour, and a Pod that actually runs.
- **Orphaned per-Pod slots — NOT handled, and this one can take the primary
  down.** Each mirroring Pod owns a slot named after itself. Nothing removes
  that slot when the Pod goes away for good: a scale-down, a replica that is
  deleted and comes back under a new name, a Cluster that drops the plugin. The
  slot stays on the primary, inactive, pinning every WAL segment since its last
  confirmation, and the sidecar's own slot guard cannot help because the sidecar
  that would run it no longer exists. There is no reconciler. Until there is:

  Two cases the sidecar does handle itself, because it is still running when
  they happen: a Pod that is **promoted** drops its own slot when it stands
  down (it would otherwise sit on the new primary after every failover or
  switchover), and a mirror whose tables are **all halted** drops its slot
  rather than pin WAL until an operator arrives (one halted table among
  several does not: the stream carries on and keeps confirming). Both mean a snapshot the next time that Pod
  mirrors. Neither has been exercised on a real cluster **yet**:
  [`cluster_e2e.sh`](../bench/scripts/cluster_e2e.sh) section 8 now does a
  switchover and requires the primary to hold exactly one active slot per
  replica afterwards, and section 9 scales 3 to 2 and reports the removed
  Pod's slot. With no janitor in the build, section 9 marks the janitor check
  NOT RUN (which is not a pass), drops the slot the documented way and
  requires that to work. Neither section had run on a cluster when this was
  written.

  - set `max_slot_wal_keep_size`, so PostgreSQL bounds what any slot can pin;
  - after removing an instance, drop its slot by hand —

    ```sql
    SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
      FROM pg_replication_slots WHERE slot_name LIKE 'quicksilver\_%';
    SELECT pg_drop_replication_slot('quicksilver_<cluster>_<pod>');
    ```

  The same applies **once, on upgrade**: a release before per-Pod slots used the
  bare `slotName` as the slot. The new sidecars never touch it, so it is orphaned
  the moment they start, and each of them re-snapshots into its own new slot.
  Drop `quicksilver_<cluster>` by hand after upgrading. The re-snapshot path
  also needs one spare replication slot for a moment (a `TEMPORARY` one, to get
  a snapshot consistent with an LSN); the plugin's headroom covers it.
