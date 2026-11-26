# Handoff — what is proven, what is not, and how to finish it

Written for a session that has a working Docker and is picking this up cold.

Everything is on `main`. There is exactly **one open question**, it needs a real
kubelet, and there is a script that answers it.

---

## TL;DR

```sh
# The cluster path. The two-instance version was green on 29 Sep 2026; the
# three-instance version with DuckLake and the router has NOT RUN YET. ~45 min.
bash bench/docker/cluster_kind.sh

# Everything else, reproducible in a container. ~20 min.
bash bench/docker/testbed.sh

# …including the serving path, which compiles pg_duckdb the first time. ~1h once.
bash bench/docker/testbed.sh --serving
```

`cluster-e2e` also runs in GitHub Actions again, now on pull requests, and
when it does, prefer it — `bench/docker/` exists to survive CI being
unavailable, not to replace it.

**Read [What CI proves now](#what-ci-proves-now) before trusting a green
tick.** The cluster test was rewritten in a sandbox that cannot start a Pod,
and that section lists exactly what was run and what was only written.

---

## Why this exists

> **Resolved 29 Sep 2026.** Actions allocates runners again. The rest of this
> section is kept because the diagnostic below is the part worth reusing, not
> because the outage is still live — and if Actions works for you, prefer it
> over `bench/docker/`, which exists only to survive it not working.

GitHub Actions stopped allocating runners for this account on **22 Sep 2026**.
Every job of every workflow failed in 3–4 seconds with no steps, no logs and no
runner assigned — including the Sunday cron, which fired on its own on 27 Sep
against an untouched commit and failed identically.

GitHub says why, and it is worth knowing **where** it says it, because the
failure carries no job log at all:

```
The job was not started because recent account payments have failed or your
spending limit needs to be increased. Please check the 'Billing & plans'
section in your settings
```

That is a **check-run annotation**, not a job log. The jobs never start, so
there is nothing in the logs API — `GET /actions/runs/<id>/jobs` returns jobs
with an empty `runner_name` and no steps, and the log endpoint 404s. The reason
lives here instead, and on a public repository it needs no credential:

```sh
curl -s https://api.github.com/repos/OWNER/REPO/commits/<sha>/check-runs \
  | jq -r '.check_runs[].output.annotations_url' | head -1 | xargs curl -s \
  | jq -r '.[].message'
```

Worth doing FIRST next time. Six days were spent inferring the cause from
timing and trigger types when one request would have quoted it.

Note the repository is **public**, where standard GitHub-hosted runners are
free and unmetered — so this is not minutes exhaustion. A failed payment or a
breached spending limit on the *account* suspends Actions regardless of a
repository's visibility.

So the verification needed somewhere else to run. `bench/docker/` is that: the
same commands CI ran, in Docker.

---

## What a session needs to be able to do

Check these first; each one costs an hour if you find it later.

| Need | Check | Needed by |
|---|---|---|
| A reachable Docker daemon | `docker info` | both scripts |
| `CAP_SYS_RESOURCE` | `awk '/CapBnd/{print $2}' /proc/self/status` → bit 24 set | `cluster_kind.sh` only |
| cgroup v2 | `stat -fc %T /sys/fs/cgroup` → `cgroup2fs` | `cluster_kind.sh` only |
| kind, kubectl, helm | on `PATH` | `cluster_kind.sh` only |
| ≥12 G free on `/` | `df -h /` | `cluster_kind.sh` |
| ~25 G free | | `testbed.sh --serving` (DuckDB build tree) |

`cluster_kind.sh` checks all of its own and refuses with a reason rather than
failing halfway. `testbed.sh` checks the daemon.

A rootless or sandboxed host typically fails the first two. That is not a
misconfiguration to work around — it is the reason this work could not be
finished where it was written.

---

## What is already proven

| Claim | How | Where |
|---|---|---|
| Mirror is correct vs the source | `e2e_mirror.sh`, value-by-value | suite `e2e` |
| Value rendering matches PostgreSQL | `rendering_fidelity.sh` | suite `rendering` |
| An unprivileged role can read the mirror **through PostgreSQL 17**, and nothing else | 9 sections, 8 denials | suite `serving` |
| The shipped images run and self-verify | `image_e2e.sh` | suite `images` |
| CNPG-I handshake over real mTLS | `go/fidelity` | suite `cnpgi` |
| The projection pays 2×–12.7×, falling with columns touched | 27 measurements, all agreeing | [docs/36](docs/36-does-the-projection-pay.md) |
| Sidecar is injected into a real instance Pod, image pulled with an imageID | cluster run 6 | [docs/35](docs/35-the-port-that-stopped-postgresql.md) |
| PostgreSQL and the mirror coexist (the 9187 fix) | cluster run 6 | docs/35 |
| The mirror bootstraps on a replica in a real cluster | cluster run 6 | docs/35 |

The last CI run before the outage was **green on all six jobs**.

---

## What is NOT proven

**Nothing, as of cluster-e2e run 24 (29 Sep 2026, `d23905e`), which repeated
run 18's result on a later tree** — `app-2 replaced: c4c75a9a -> 0426e9df`,
`app-1: postgres ready, mirror NOT ready, and absent from app-ro`, operator
1.26.0, PASS. Run 18's output is kept below because it is the one that first
closed these. The two claims
that stood open for the life of this project — a Pod being ROLLED by a plugin
parameter change, and a Pod LEAVING the `-ro` Service because the mirror's
readiness probe failed — both passed on a real cluster:

```
== 4. changing sidecarImage ROLLS the instances ==
  before: app-1=9b1d55b0 app-2=69d4df4b
  app-2 replaced: 69d4df4b -> 44e32435
  ok: the operator rolled the instances after sidecarImage changed
== 5. in takeover, a stale mirror LEAVES the -ro endpoints ==
  ok: every instance Pod now runs QS_MODE=takeover
  app-1: postgres ready, mirror NOT ready, and absent from app-ro
  ok: mirror freshness gated the endpoint, with PostgreSQL itself healthy
```

Section 5 is the property [docs/33](docs/33-the-probe-that-gated-the-wrong-thing.md)
reasoned about from the Kubernetes contract and nothing had ever observed:
PostgreSQL healthy, the mirror behind, and the node out of service *because of
the mirror*.

Section 4 is worth reading closely, because it once reported a pass while
observing a second instance being created for the first time — see
[docs/37](docs/37-the-operator-that-could-not-roll.md). It now tracks Pod uid
per instance name, and `app-2 replaced: 69d4df4b -> 44e32435` is a named
instance whose uid changed, which a new Pod appearing cannot fake.

The run also exercised the CloudNativePG floor in situ:
`CloudNativePG ready in cnpg-system (operator 1.26.0, floor is 1.26)`.

**The registry PULL path, closed by cluster-e2e run 32** (30 Sep 2026, fired
by `workflow_run` off release run 4 for `v0.0.3`, tree `47dc53e`). Runs 18 and
24 side-loaded their images; run 32 pulled the published ones, and a kubelet
did it:

```
event=workflow_run -> images=published
pulling 0.0.3
  chart: oci://ghcr.io/howlerops/charts/quicksilver --version 0.0.3
  ok: plugin running — the -plugin image was PULLED and STARTED
  ok: PULLED: imageID=ghcr.io/howlerops/pg_quicksilver-mirror@sha256:9b5a206d…
  app-2 replaced: 0675140f -> 6e7884a7
  app-1: postgres ready, mirror NOT ready, and absent from app-ro
PASS
```

That digest is the one ghcr serves for both `-mirror:0.0.3` and `:latest`. The
run pulled with an `imagePullSecret`; that no credential is *needed* is
established separately — all three packages answer an **anonymous** pull.

**Still outside what any of this shows:** cold-cache query performance is not
measured anywhere (see [docs/36](docs/36-does-the-projection-pay.md)'s own
caveats).

---

## Two traps that will cost you an hour each

**1. CloudNativePG 1.26 is a floor.** On 1.25 the operator builds an instance
Pod's expected spec *without consulting plugins*, so the sidecar is absent from
both sides of the comparison that decides whether to replace a Pod — and `mode`,
`freshnessSLO`, `sidecarImage` and `tables` never reach a running instance. A
Pod created fresh still gets the parameters it was created with, so it looks
like it works until the first time you change your mind. `cluster_e2e.sh`
installs 1.26.0 and refuses to run below it. Full account: docs/37.

**2. `CAP_SYS_RESOURCE`.** Without it runc cannot set a pod sandbox's
`oom_score_adj` to −998, no pod sandbox can be created at all, and the failure
surfaces as Pods stuck `Pending` with an error naming neither runc nor the
capability. The sandbox this was developed in lacks it, which is the entire
reason the cluster test was never run locally.
[docs/21](docs/21-against-the-real-operator.md) has the diagnosis;
`cluster_kind.sh` checks it before doing any work and tells you plainly.

---

## The scripts

### `bench/docker/cluster_kind.sh` — the open question

Creates a kind cluster on the host's Docker, builds this checkout's three
images (plugin, mirror, and the PostgreSQL operand image, tagged `17-dev`),
side-loads them plus a second `:dev-rolled` tag for the roll, and runs
`cluster_e2e.sh` against it in full scope. It is the same run `cluster-e2e`
makes on a pull request.

```sh
bash bench/docker/cluster_kind.sh          # ~45 min, deletes the cluster after
KEEP=1 bash bench/docker/cluster_kind.sh   # leave it up to poke at
```

It preflights before spending the 45 minutes: Docker reachable, cgroup version,
`CAP_SYS_RESOURCE`, kind/kubectl/helm present, ≥14 G free. Each prints a reason
and exits 2 (INCOMPLETE) rather than failing halfway through.

Whatever happens, `cluster_e2e.sh` leaves `/tmp/qs-cluster-e2e/` behind: both
containers' logs per Pod, each sidecar's `/metrics` and `/readyz`,
`quicksilver.mirror_status()`, every replication slot, the Cluster as the API
server holds it, and the operator's and plugin's logs.

`:dev-rolled` is deliberately not `:latest` — Kubernetes defaults that tag's
`imagePullPolicy` to `Always`, which ignores the node's copy and goes to a
registry that is not there. That cost a run already.

**This side-loads images, so it does NOT exercise the registry pull path.** The
script says so in its own output. Pulling published artifacts is the release
workflow's job and needs a registry credential.

### `bench/docker/testbed.sh` — everything else

```sh
bash bench/docker/testbed.sh             # correctness; skips serving
bash bench/docker/testbed.sh --serving   # + build pg_duckdb, run serving
bash bench/docker/testbed.sh --full      # + performance sections (incl. docs/36)
bash bench/docker/testbed.sh --shell     # a prompt inside the image
```

`Dockerfile.testbed` carries PostgreSQL 17, Go, helm, kubeconform, and — in its
own cached stage — **pg_duckdb v1.2.2 plus this repo's confinement patch**,
which is not packaged anywhere and compiles DuckDB with it. That is about an
hour once, then nothing. It is opt-in for that reason; without `--serving` the
suite is told `SKIP=serving`, which it prints and counts rather than hiding.

The container runs `--privileged` and mounts the Docker socket when present: the
suite starts real PostgreSQL clusters and `image_e2e.sh` runs the shipped
container images, which needs a usable engine inside.

---

## How to read the result

The suite has **three** outcomes and the middle one is the point:

- `PASS` — ran and agreed.
- `FAIL` (exit 1) — ran and disagreed.
- `INCOMPLETE` (exit 2) — **could not run**. Not a pass. A section that could not
  run has told you nothing, and reporting that as success is the failure mode the
  whole harness is arranged against.

`SKIP` is a fourth thing and is not an outcome: it is the caller saying "this
runs somewhere else". It is printed where the section would have run, counted
separately, and the final line reads `PASS, with N section(s) skipped by SKIP and
NOT covered here`.

---

## If section 4 or 5 fails

Do not assume the harness is right. Five of the bugs found in this project were
measurement bugs — checks that could pass without the property holding
(docs/32, docs/33, docs/37 and two more in the commit log). The current
section 4 and 5 both print diagnostics designed to separate the two candidate
explanations:

- Section 4 prints each Pod's phase and each init container's image and waiting
  reason. `ImagePullBackOff` means the operator acted and the replacement cannot
  start — the opposite diagnosis from "the operator did not act".
- Section 5 first asserts every Pod actually carries `QS_MODE=takeover`, and on
  failure prints each Pod's mode, SLO and probe path beside what the Cluster spec
  says. A container with **no** readiness probe reports `ready=true`
  unconditionally, so the gated state can never appear however long you wait.

---

## Repository orientation

```
go/internal/plugin/     the CNPG-I plugin: lifecycle hook, config, operator svc
go/cmd/qs-mirror/       the sidecar that builds the Parquet mirror
go/cmd/qs-query/        generates the view that serves the mirror
go/fidelity/            separate module — imports the CNPG operator, tests the
                        handshake and the rollout comparison without a cluster
bench/scripts/          every measurement and end-to-end check; suite.sh runs them
bench/docker/           this handoff's runners
charts/quicksilver/     the Helm chart
docs/                   numbered, roughly chronological; 30+ are post-mortems
```

Start with [docs/36](docs/36-does-the-projection-pay.md) for what the project is
worth, and [docs/37](docs/37-the-operator-that-could-not-roll.md) for the most
recent finding.

---

## Cutting a release

**`0.0.3` is published** (30 Sep 2026): `v0.0.3` on `47dc53e`, release run 4
green, cluster-e2e run 32 PASS in published mode. `:latest` on both images and
an unpinned `helm install` now resolve to 0.0.3.

**`0.0.2` is left published, deliberately.** Deleting a ghcr version is
irreversible and breaks anyone who pinned it; nothing resolves to it by default
any more, and every README warns about it by name. That is the owner's call and
it was made.

The sandbox that prepared this release has an egress proxy that allows branch
pushes and refuses `refs/tags/*` with a bare HTTP 403 on the `git-receive-pack`
POST (the ref negotiation succeeds, so `git push --dry-run` reports
`* [new tag]` and tells you nothing). Tags go from a checkout that can reach
github. For the record, 0.0.3 was:

```sh
git fetch origin main
git tag -a v0.0.3 origin/main -m "0.0.3: the mirror no longer takes 9187 from the instance manager"
git push origin v0.0.3
```

What that sets off, in order, and what each step is worth watching for:

1. **`release`** — refuses unless `Chart.yaml` version, `appVersion` and the
   image pins in `values.yaml` all equal the tag. (For 0.0.3 that guard was
   replayed verbatim and both image targets were built locally at
   `VERSION=0.0.3`.) It then pushes `-plugin`, `-mirror` and the chart to
   ghcr, and from the next release `-postgres` as well: see below.
2. **`cluster-e2e`**, automatically, in **`published`** mode — the run that
   closed the registry gap above (run 32 for 0.0.3). It resolves the version with `helm show chart`
   against the registry, which returns the newest chart there; verified live.
3. Nothing else. `helm install --version 0.0.3` then works anonymously, because
   the packages are public.

`release` also takes a `workflow_dispatch` with a `version` input if the tag is
inconvenient — it publishes the same three artifacts, and `cluster-e2e` still
follows it. It does not create the git tag.

---

### The next release is 0.0.4, and it is prepared, not published

`Chart.yaml`, `appVersion`, `image.plugin` and `image.mirror` say `0.0.4`, and
`image.postgres` says `17-0.0.4`. Nothing with those tags exists on ghcr until
`v0.0.4` is tagged, so an unpinned `helm install charts/quicksilver` from a
checkout names images that are not there yet; every README still tells people
to install `0.0.3`, which is.

`release` gained one thing: a third image, `-postgres`, pushed as
`17-<version>` and `17`, after the other two. Its build ends in an offline
self-check, so a release on a day when pg_duckdb's package or DuckDB's
extension repository is wrong stops before the chart is pushed. The guard now
also requires `image.postgres` to be `17-<tag>` and
`examples/cluster-routed.yaml` to name it; replayed against this tree it
passes for 0.0.4 and fails, on six counts, for 0.0.3. What triggers a release
is unchanged.

The operand tag is `17-<version>` and not `<version>` because CloudNativePG
reads the PostgreSQL major from the front of an image tag: `0.0.4` is
PostgreSQL 0 to the operator, and `dev` is refused at admission.

After the release, `cluster-e2e` runs in `published` mode as before. It picks
its scope from the version: `0.0.3` and earlier are **legacy** (two instances,
stock image, the original pull, roll and gate), anything later is **full** and
pulls `-postgres:17-<version>` too.

---

## What CI proves now

Two workflows on a pull request, and what each job is evidence of.

**`ci`**, every push and pull request:

| Job | Proves |
|---|---|
| `test` | every Go package builds, vets and passes with `-short`, with explicit timeouts |
| `ducklake` | the DuckLake publisher against DuckDB 2.0.1: four named tests must PASS, not skip |
| `full` | every Go test without `-short`, the heavy ones included (`-timeout 60m`) |
| `flags-off` | `internal/mirror` with every escape-hatch flag set. **Red on this tree**, see below |
| `race` | the `-short` tests of `internal/...` and `cmd/...` under `-race` |
| `race-mirror (0..2/3)` | the full `internal/mirror` suite under `-race`, split across three jobs by [`go_test_shard.sh`](bench/scripts/go_test_shard.sh), which takes the test list from the package at run time and fails if the shards do not partition it |
| `cnpgi` | the CNPG-I handshake and Pod facts against the operator's own code |
| `manifests` | `helm lint`; kubeconform; both examples against CloudNativePG's CRD; the chart's rendering; that the chart, the examples, the Dockerfile and the cluster test agree on the operand image, its extension directory and the preload order |
| `lint` | `actionlint` on the workflows, `hadolint` on the Dockerfile, `shellcheck` on the scripts CI runs and any a pull request changes |
| `images` | all three images build; the operand image passes its offline self-check during the build and again as a container with no network and a read-only root |
| `integration` | the suite against PostgreSQL 17; `bench/exactness --selfcheck` |
| `extension` | `make installcheck` for `pg_quicksilver` against PostgreSQL 17 with pg_duckdb 1.2.2; DuckDB's extensions load under `unshare -n`; the cluster test's slot, verify, routing, halt and REBUILD sections against a real primary, standby and sidecar |
| `serving` | the patched pg_duckdb, built and served through (unchanged) |

**`cluster-e2e`**, on a pull request to `main` that touches something that ends
up in a Pod (not docs, not tests), on a push to `main`, after a release,
weekly, and on demand. Its eleven sections are listed at the top of
[`cluster_e2e.sh`](bench/scripts/cluster_e2e.sh).

**What was run here, and what was not.** This was written in a sandbox with no
Docker daemon and no Kubernetes.

*Run, and passing:* `actionlint`, `hadolint`, `shellcheck` (warning level, as
`lint` runs it), `helm lint`, `helm template`, `validate_manifests.sh`;
`go build`, `go vet`, `go test -short ./...` and `go test -short -race ./internal/... ./cmd/...`
(every package `ok`, including the DuckLake tests against DuckDB 2.0.1); `go/fidelity`; the Dockerfile's Go build
commands and its extension build stage, run as written outside Docker
(`CGO_ENABLED=0`, PGXS with `DESTDIR` into a scratch directory); the extension's
DuckDB install and offline verify, run as the `postgres` user with `unshare -n`,
and its control; `make installcheck` (10 of 10) against the
`validate/real` cluster; `cluster_e2e_selftest.sh` (PASS, with
`DUCKDB_EXT_DIR`).
The verify run found a real defect (DuckDB's 0600 files were unreadable after
the copy to root), fixed in `deploy/duckdb-extensions.sh`.

*Not run, and so unverified until CI does:*

- any `docker build`: the three targets as images, the PGDG header pin inside
  the CloudNativePG base, the Debian 12 build of the pg_duckdb package,
  `ADD --checksum` and `RUN --network=none` under the runner's BuildKit;
- anything on Kubernetes: admission refusals by the plugin, three instances on
  the operand image, two sidecars against one primary, routing through the
  `-ro` Service, the halt and REBUILD by Pod deletion, the switchover, the
  scale-down, and whether sections 10 and 11 still pass on three-then-two
  instances with the DuckLake parameters set;
- the workflows as workflows: triggers, path filters, the concurrency
  expression, artifact upload, the `images` job's cache scopes, and every
  step's behaviour on a GitHub-hosted runner (`sudo unshare -n`, system `pip`);
- `release.yml`'s new image push, and the pinned pg_duckdb packages: their URLs
  returned HTTP 404 from the development host, so neither the Debian 12 nor the
  Ubuntu 24.04 checksum has been checked against its file.
- the `full` job (`go test ./...` without `-short`, 60 minutes) and the sharded
  `race-mirror` jobs, which were not run here; `ducklake`'s exact CI step was
  run as part of the `-short` suite, not as its own job.

*Known before the first run:*

- `ci`'s **`flags-off`** job fails on this tree, and the cause is in
  `go/internal/mirror`, not in CI. Four tests there assert native temporal types
  and do not skip under `QS_TEMPORAL_TYPES=0`, as their neighbours do for their
  own flags: `TestEveryTypeIsStoredAsDeclared`,
  `TestOldFilesAndNewCoexist`, `TestNegativeInfinityIsStoredAsTheEngineReadsIt`
  and `TestClusteredRewriteKeepsRanges`. Run here with the flags set, the first
  three report `is stored as VARCHAR, expected DATE` and the fourth reports a
  clustering mismatch. The fix is in those tests (a `t.Skip` on the flag, or the
  feature honouring it), not in the workflow. The workflow does **not** skip them:
  an earlier version of this note said it did, and that was never implemented.
- **There is no slot janitor in this build.** Sections 8 and 9 assert slot
  state and mark the janitor check NOT RUN. The script detects a janitor by
  the plugin reporting a parameter with "janitor" in its name
  (`JANITOR_PARAM` overrides) and then sets it to `JANITOR_PERIOD` (20s) and
  requires orphans to be gone.
- **Section 9 will report an orphaned slot** after the scale-down, drop it by
  hand as docs/16 says, and pass. That is the documented gap, shown.
- If `cluster-e2e` is made a required check, a docs-only pull request never
  starts it and waits forever: path-filtered workflows report nothing when
  they do not run.

---

## When Actions comes back

`cluster-e2e` used to run on a push, to any branch, touching the harness, the
plugin, the chart or the Dockerfile. It now runs on a **pull request to
`main`** and on a push to `main`, when the change touches the sidecar, the
plugin, the extension, the chart, `deploy/` or the harness, in `source` mode.
Pull requests get a concurrency group each; everything else still shares one.
A branch with no pull request no longer gets a cluster run by pushing: use
`workflow_dispatch` with `images: source`. The reasons are at the top of the
workflow file. `ci` runs on every push.
