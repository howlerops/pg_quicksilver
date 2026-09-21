# 35 — The port that stopped PostgreSQL

The first time this plugin ran inside a real CloudNativePG instance Pod, it
stopped PostgreSQL from starting.

```
qs-e2e  app-1  0/2  CrashLoopBackOff  6 (4m23s ago)  10m
```

Not the mirror. PostgreSQL.

---

## The line

```json
{"level":"error","msg":"Error while running the web server",
 "logger":"instance-manager","address":":9187",
 "error":"listen tcp :9187: bind: address already in use"}
...
Error: unretryable: listen tcp :9187: bind: address already in use
```

CloudNativePG's instance manager serves its metrics on **:9187**. So did the
mirror sidecar — `HealthPort = 9187`, `EXPOSE 9187`, `QS_HEALTH_ADDR=:9187`,
and a line in docs/16 advertising it.

Every container in a Pod shares one network namespace. Two containers binding
one port are not two listeners; they are one listener and one crash. And the
mirror is a **native sidecar**, so it starts *first*: it wins the port, the
instance manager loses it, calls the failure `unretryable`, and exits.

The Pod then crash-loops. In `shadow` mode. On every instance. A plugin whose
entire premise is that it does not disturb the database was preventing the
database from booting.

---

## Why nothing here saw it

Everything in this repository runs the sidecar *beside* PostgreSQL, never
*inside* its Pod:

- `e2e_mirror.sh` starts a PostgreSQL on 5443 and a sidecar as an ordinary
  process. Different network namespaces; no collision possible.
- `image_e2e.sh` runs the shipped image under `--network=host` against the same
  kind of external PostgreSQL. Still no CNPG instance manager anywhere.
- `go/fidelity` builds the real Pod **spec** — and this is the interesting one
  — but only ever asked whether the sidecar was present and correctly shaped.
  It never asked whether the spec it produced could run.

The collision is a property of one Pod with two containers in it, and until
[`bench/scripts/cluster_e2e.sh`](../bench/scripts/cluster_e2e.sh) there was
never one.

---

## The fix, and the test that would have caught it

`HealthPort` is **9188**: adjacent, and free in CNPG's layout (5432, 8000, 8010,
9187).

The test is in `go/fidelity`, because that is where a real Pod spec already
exists. It asks CloudNativePG to build an instance, runs the real lifecycle
hook over it, and refuses any port claimed twice:

```
port  9188  quicksilver-mirror (qs-health)
port  5432  postgres (postgresql)
port  9187  postgres (metrics)
port  8000  postgres (status)
```

Put back to 9187, it says so in the terms that matter:

```
port 9187 is claimed by BOTH "quicksilver-mirror" (qs-health) and
"postgres" (metrics) in one network namespace; whichever starts second
fails to bind
```

Structural, not a hardcoded list: any future port the sidecar claims is checked
against every port CNPG declares, without anyone remembering to update it. The
one hardcoded assertion — *not 9187* — is there because the structural check
only works while CNPG keeps declaring the port in its Pod spec, and a
`containerPort` is documentation, not a binding.

---

## What it cost to find, and what that says

Four runs, each ~11 minutes, and three of the four failures were in the test
rather than the product:

| run | failed on | whose fault |
|---|---|---|
| 1 | `kubectl wait` returning before Pods existed | the test |
| 2 | no `<cluster>-superuser` Secret | the test |
| 3 | `public.events` never created | the test |
| 4 | **port 9187** | **the product** |
| 5 | rolling to a tag nothing had loaded | the test |

That ratio is the honest shape of first-contact testing: most of what breaks is
the harness learning what the environment actually requires. It is also why the
sequence was worth running — three harness bugs are cheap, and the fourth
finding is one that would have met the first person who ever installed this.

Run 5 is where 9188 was confirmed, and it is worth quoting because it is the
first time any of this has been seen rather than argued:

```
Port: 9188/TCP (qs-health)
instance-manager  address=":9187"        ← bound, no collision
postgres:  "database system is ready to accept connections"
mirror:    "this node is the primary — standing down"
```

The same run then failed in section 4, on the harness again. It rolls the
instances by patching `sidecarImage` to `:latest`, which exists on ghcr and was
never loaded onto a kind node — so in `images: source` the replacement Pods went
to `ImagePullBackOff` and the section reported *"no Pod was replaced within 10
minutes"*. True, and misleading: the operator had acted, and the Pod it created
could not start.

Two fixes, and the second is the interesting one. The roll target is now
`ROLL_IMAGE`, a second tag loaded beside the first — deliberately not `:latest`,
because Kubernetes defaults that tag's `imagePullPolicy` to `Always` and would
go to the registry even with the image on the node. And the check for it runs in
the **preconditions**, seventeen minutes before section 4 needs it, because a
misconfiguration that announces itself as a ten-minute timeout is a
misconfiguration that gets diagnosed as something else.

A second, smaller thing the same run exposed: the sidecar logged one identical
WARN every five seconds while waiting for a PostgreSQL that could not start —
about a hundred lines in eight minutes, burying the instance manager's single
useful error. That window only exists inside a Pod, because only there does the
sidecar start before PostgreSQL. It now logs when the message *changes*.

---

## The workflow change this forced

`cluster-e2e` pulled the **published** images, so a fix to the sidecar could not
be tested until it had been released. The log-noise fix landed in `main` and the
next run still showed the old behaviour, because the run was testing `0.0.2`.

The workflow now takes `images: published | source`. `published` stays the
default — verifying the release is the other half of this job — and `source`
builds the checkout and `kind load`s it, so a product bug can be fixed and
confirmed without a release in between. Iterating on a bug through a release
cycle is not iterating.
