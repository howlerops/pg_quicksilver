# pg_quicksilver

A [CloudNativePG](https://cloudnative-pg.io/) plugin that replaces a cluster's **read
replicas** with nodes carrying a **columnar mirror of the primary, built from the WAL** — so
the same `SELECT`, to the same endpoint, over the same Postgres wire protocol, comes back
faster for scans and aggregates.

**Requires PostgreSQL ≥ 17** (before 17 a logical slot does not survive failover) and
**CloudNativePG ≥ 1.26** ([why](docs/37-the-operator-that-could-not-roll.md)).

---

## What it's worth

Measured on 2026-11-26 with the system as it now runs: a primary, a hot standby, and the
sidecar publishing a DuckLake that the routing extension reads from inside PostgreSQL.
10,000,000 orders and 500,000 customers, best of five warm runs, every answer checked equal
to the heap's. Full table, earlier runs and caveats: **[METRICS.md §0](METRICS.md)**.

### Re-measured 2026-11-28, at 1,000,000 orders

The 10M run was not repeated: on the shared disk the cluster would not fit (1M orders are 352 MB, and a
standby doubles that). Same bench layout (500,000 customers, 24 months), join domains bootstrapped on, UTC,
on a shared 4-core host under load 2 to 15: median of five warm runs, every answer checked equal to the heap's.

| query | heap (standby) | routed through the extension | heap ÷ routed |
|---|---:|---:|---:|
| revenue by month, join to customers | 916.6 ms | 162.4 ms | **5.6×** |
| `GROUP BY status` | 133.5 ms | 22.7 ms | **5.9×** |
| `sum(amount)` | 105.8 ms | 18.1 ms | **5.9×** |
| `count(*)` | 67.6 ms | 10.7 ms | **6.3×** |
| one month, count and sum | 12.3 ms | 10.8 ms | 1.1× (the router keeps it on the heap) |

Full table, New York, the runs without join domains and the caveats: **[METRICS.md §0.2](METRICS.md)**.
Also measured there: a `kill -9` of the sidecar in a replay hold (three runs, no loss, no duplicate), and a
promotion with a second standby repointed at the new primary.

**Known issue:** with `quicksilver.max_lag` above 0, a promoted node keeps answering from its frozen mirror
until the lag exceeds that many bytes. Under the default `max_lag = 0` it stops routing at once. Reproduction
and the suggested fix are in METRICS §0.2, part C.

### Fresh after the load, 10M orders

| query | heap (standby) | routed through the extension | DuckDB reading the lake directly | heap ÷ routed |
|---|---:|---:|---:|---:|
| `GROUP BY status` | 590.7 ms | 36.4 ms | 48.2 ms | **16.2×** |
| `sum(amount)` | 498.3 ms | 30.2 ms | 48.4 ms | **16.5×** |
| `count(*)` | 251.9 ms | 8.5 ms | 2.8 ms | **29.6×** |
| filter on a month + aggregate | 66.6 ms | 20.4 ms | 17.5 ms | **3.3×** |
| revenue by month, join to customers | 3,739 ms | 3,845 ms (heap) | 3,211 ms | 1.0× |
| point lookup by key | 0.1 ms | 0.2 ms (heap) | 23.4 ms | stays on the heap |

After the change workload (700k updated, deleted or inserted rows, 3.6 GB of WAL) the heap's
`count(*)` went to 1,539 ms; the routed one stayed at 7.5 ms. The table is in METRICS §0 for that
state and for the state after the mirror's own compaction.

The earlier 5.88M-row table, measured against the view-era mirror, is kept as history in
**[docs/36](docs/36-does-the-projection-pay.md)**.

The case for whoever signs the invoice: **replace 8 read replicas with 3**, with those 3
still counting toward HA. Break-even analytical share is **`f ≈ 1/R`** — 13.3% at 8 replicas,
6.6% at 16 — and is nearly independent of how fast the column store is. Measure your own with
[`bench/s0_workload_profile.sql`](bench/s0_workload_profile.sql);
[docs/10](docs/10-scaling-economics.md) has the consolidation math and the three cases where
it doesn't hold.

### Three things that table says

**1. The routed win is what the extension lets through.** Aggregates the extension routes run
16 to 30 times faster than the heap; the filtered aggregate 3×. The point lookup is cheap on the
heap by design, so it is not routed. The revenue-by-month join is routed in the session time zones
the extension has proven: `date_trunc` reads the session's `TimeZone`, so a zone is admitted only
after DuckDB's answer for it has been checked against PostgreSQL's (240 of the 499 zones checked; 40 of
them for `hour`; the check is in `ext/pg_quicksilver/validate/results/tz_check.txt`).
In any other zone the join stays on the heap. Measured 2026-11-27 on 1,000,000 orders spread over 24
months, on a standby, median of five warm runs: UTC, heap 455 ms against routed 73 ms; America/New_York,
heap 469 ms against routed 89 ms; the answers were identical in both zones. The table above was measured
before this, when the join was not routed; [METRICS.md](METRICS.md) §0.1 has the newer numbers. Routing is per statement,
and the answer is the heap's whichever side runs it.

A routed read is the heap's answer at the same point. A mirror is read only when it has reached the
statement's replay position on the standby and is not past it (both bounds are in
[docs/40](docs/40-the-router-moves-inside.md), "Freshness is strict by default"), and the sidecar
holds a commit back until the standby has replayed it (docs/16, "The replay gate").

**2. The heap degrades with churn; the mirror does not.** After 700k changes the heap's `count(*)`
went from 252 ms to 1.5 s and the routed one held at 7.5 ms. Across the three states measured, every
routed aggregate took between 7.5 and 72 ms (the slowest was measured while the mirror was compacting).
A commit is visible on the standby in a median of 174 ms (p95 396 ms, 30 commits of 5,000 rows).

**3. A columnar read is not a lookup.** Read directly with DuckDB, a point lookup takes 24 ms against
the heap's 0.1 ms. That is why the routed path keeps key lookups on the heap, and why
`mode: takeover` still sits behind an explicit acknowledgement. The earlier "16× worse" figure was the
view-era number and is history.

> Every timing above is a **warm cache on both sides**, the case that flatters a column store
> least. For cold or I/O-bound workloads these are closer to a floor than a ceiling.
>
> One host, 4 vCPU, with a second Quicksilver stack running alongside, so the load stayed near 2 to 4
> during the timed runs. Ratios are the figure to read; the absolute milliseconds are not a quiet-host
> measurement. Part 4 repeated the sync, the drain and the rewrite under a 1 GiB memory limit: all
> completed, with no OOM kill, and the limit was reached in each one.

---

## Why a sidecar on a standby

Two measured findings shaped the whole design ([docs/11](docs/11-measured-results.md)):

- **A columnar mirror is not uniformly faster.** On OLTP-shaped queries it is *far* worse —
  up to 3,445× on the 30 M-row study. A straight `-ro` takeover doesn't regress some
  traffic, it takes production down.
- **Physical WAL cannot reconstruct row-level changes on its own.** Against a row 67× wider,
  a `DELETE` record stays 54 bytes and an `UPDATE` 69 — the contents simply aren't there. Any
  physical-WAL decoder needs its own TID-addressed copy of the heap.

The second looks like bad news and is the key: **on a hot standby that copy already exists —
it's the standby's data directory.** So the architecture that solves the hard technical
problem also solves the hard product problem.

> A Quicksilver node is an ordinary CNPG hot standby **plus** a locally-built columnar
> mirror of the same data.

Point lookups hit the real heap and real indexes at today's speed. Scans hit the column
store. The primary sees one ordinary replication stream.

---

## Getting started

```sh
helm install quicksilver oci://ghcr.io/howlerops/charts/quicksilver \
  --version 0.0.3 -n cnpg-system
```

> **Pin the version, and do not install `0.0.2`.** In `0.0.2` the mirror sidecar binds
> `:9187`, which is the port CloudNativePG's instance manager needs. The sidecar is a
> native sidecar, so it starts first, wins the port, and the instance manager exits
> `unretryable` — **PostgreSQL never starts**, in the default `shadow` mode, on every
> instance. Fixed in `0.0.3` ([docs/35](docs/35-the-port-that-stopped-postgresql.md)).

Then add the plugin to a `Cluster` — [docs/16](docs/16-deploying.md) has the spec, the
images, what the plugin refuses, and what is not yet verified.
[`examples/cluster-shadow.yaml`](examples/cluster-shadow.yaml) mirrors and changes nothing
else. [`examples/cluster-routed.yaml`](examples/cluster-routed.yaml) also publishes the
mirror as a DuckLake and routes reads to it from inside PostgreSQL, which needs the
operand image (`pg_quicksilver-postgres:17-<version>`, first published with `0.0.4`).

To run the verification yourself, in Docker: **[HANDOFF.md](HANDOFF.md)**. The
cluster test runs three instances on a kind cluster, from the `cluster-e2e` workflow or
`bash bench/docker/cluster_kind.sh`; [docs/16](docs/16-deploying.md#running-the-cluster-test)
says how to run it against any cluster.

---

## Evidence

- **[METRICS.md](METRICS.md)** — every measurement, the script that produced it, its raw
  output under [`bench/results/`](bench/results/), and a section on what the numbers do
  **not** say.
- **[docs/](docs/README.md)** — 40 documents. The 20s and 30s are mostly post-mortems:
  something measured that turned out to be wrong, and what it cost to find out.
- Charts are **generated from the measurement files**, never drawn
  ([`plot_projection.py`](bench/scripts/plot_projection.py)). No figure here is typed by hand.

---

## Non-goals

- Replacing the OLTP primary. Quicksilver never accepts writes.
- Being a data lake play. Open table formats are a means, not the product.
- Multi-source ingest. One CNPG cluster in, one mirror out.
- Sub-second freshness in v1.

## Licensing

Intended licence **Apache-2.0**, matching CNPG. Several inspirations (walshadow, pgrust) are
AGPL-3.0 — readable for ideas, not vendorable. See
[docs/02](docs/02-prior-art.md#licence-analysis).
