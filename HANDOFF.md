# Handoff — what is proven, what is not, and how to finish it

Written for a session that has a working Docker and is picking this up cold.

Everything is on `main`. There is exactly **one open question**, it needs a real
kubelet, and there is a script that answers it.

---

## TL;DR

```sh
# The one thing that still needs doing. ~25 min. Needs Docker + full capabilities.
bash bench/docker/cluster_kind.sh

# Everything else, reproducible in a container. ~20 min.
bash bench/docker/testbed.sh

# …including the serving path, which compiles pg_duckdb the first time. ~1h once.
bash bench/docker/testbed.sh --serving
```

If `cluster_kind.sh` prints `PASS`, the last two unverified claims in this
project are verified and nothing is outstanding.

---

## Why this exists

GitHub Actions stopped allocating runners for this account on **22 Sep 2026**.
Every job of every workflow fails in 3–4 seconds with no steps, no logs and no
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

Two claims, both in `bench/scripts/cluster_e2e.sh`, both needing a kubelet:

- **Section 4 — a Pod is ROLLED when a plugin parameter changes.**
- **Section 5 — a Pod LEAVES the `-ro` Service when the mirror's readiness
  probe fails** (the property [docs/33](docs/33-the-probe-that-gated-the-wrong-thing.md)
  reasons about and nothing has ever observed).

Section 4 has never passed honestly. It once *reported* a pass while observing a
second instance being created for the first time — see
[docs/37](docs/37-the-operator-that-could-not-roll.md) and the commit
`fix(ci): section 4 passed without seeing a rollout`. The check now tracks Pod
uid per instance name, which is the property it always meant.

Section 5 has never been reached.

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

Creates a kind cluster on the host's Docker, builds this checkout, side-loads
both images plus a second `:dev-rolled` tag for the roll, and runs
`cluster_e2e.sh` against it.

```sh
bash bench/docker/cluster_kind.sh          # ~25 min, deletes the cluster after
KEEP=1 bash bench/docker/cluster_kind.sh   # leave it up to poke at
```

It preflights before spending the 25 minutes: Docker reachable, cgroup version,
`CAP_SYS_RESOURCE`, kind/kubectl/helm present, ≥12 G free. Each prints a reason
and exits 2 (INCOMPLETE) rather than failing halfway through.

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
own cached stage — **pg_duckdb v1.1.1 plus this repo's confinement patch**,
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

## When Actions comes back

Nothing needs changing. `cluster-e2e` runs on a push touching
`bench/scripts/cluster_e2e.sh`, `.github/workflows/cluster-e2e.yml`,
`go/internal/plugin/**`, `charts/quicksilver/**` or `deploy/Dockerfile`, in
`source` mode, bounded to one run at a time. `ci` runs on every push and was
green on all six jobs. Both are current on `main`.
