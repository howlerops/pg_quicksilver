# pg_quicksilver

A [CloudNativePG](https://cloudnative-pg.io/) plugin that replaces a cluster's **read
replicas** with nodes carrying a **columnar mirror of the primary, built from the WAL** — so
the same `SELECT`, to the same endpoint, over the same Postgres wire protocol, comes back
faster for scans and aggregates.

**Requires PostgreSQL ≥ 17** (before 17 a logical slot does not survive failover) and
**CloudNativePG ≥ 1.26** ([why](docs/37-the-operator-that-could-not-roll.md)).

---

## What it's worth

![Speedup against the PostgreSQL heap, by columns touched and table size](docs/img/projection-benefit.svg)

Same node, same page cache, best of 3, every answer checked against the heap before its
timing was believed. Full method and caveats: **[docs/36](docs/36-does-the-projection-pay.md)**.

### At 5.88M rows, 20-column fact table

| query | columns read | heap | mirror | |
|---|---:|---:|---:|---:|
| `GROUP BY` region | 2 | 734 ms | 58 ms | **12.7×** |
| `sum(amount)` | 1 | 538 ms | 48 ms | **11.2×** |
| filter + aggregate | 2 | 480 ms | 54 ms | **8.9×** |
| `count(*)` | 0 | 249 ms | 33 ms | **7.5×** |
| sum of 5 columns | 5 | 717 ms | 109 ms | **6.6×** |
| touch 10 columns | 10 | 1103 ms | 263 ms | **4.2×** |
| touch all 19 | 19 | 2843 ms | 1421 ms | **2.0×** |
| **point lookup by key** | 1 | **2 ms** | **40 ms** | **0.06×** |

**Storage: 3.2× smaller**, at every scale tested (2936 MiB of heap → 916 MiB of Parquet).

The case for whoever signs the invoice: **replace 8 read replicas with 3**, with those 3
still counting toward HA. Break-even analytical share is **`f ≈ 1/R`** — 13.3% at 8 replicas,
6.6% at 16 — and is nearly independent of how fast the column store is. Measure your own with
[`bench/s0_workload_profile.sql`](bench/s0_workload_profile.sql);
[docs/10](docs/10-scaling-economics.md) has the consolidation math and the three cases where
it doesn't hold.

### Three things that table says

**1. The win tracks what you let it skip.** 11.2× at one column, 4.2× at ten, 2.0× at
nineteen. The mechanism is bytes not read, so a query that reads everything gets almost
nothing — the 2× left over is what encoding and vectorised execution are worth alone. There
is no single "how much faster"; there is a function of your query.

**2. It needs the data to outgrow memory.** The same aggregate, as the heap grows against
1 GB of `shared_buffers`:

| live rows | heap size | heap | mirror | |
|---:|---:|---:|---:|---:|
| 490 K | 245 MiB | 46.9 ms | 24.3 ms | 1.9× |
| 1.96 M | 979 MiB | 207.4 ms | 31.3 ms | 6.6× |
| 5.88 M | 2936 MiB | 537.5 ms | 48.0 ms | **11.2×** |

Below the crossover both sides read from RAM and the advantage nearly vanishes. At 490 K
rows four of nine queries fail to clear 2× and `count(*)` is *slower*.

**3. It is ~16× worse at point lookups**, and degrades with scale — an index probe is
O(log n), the mirror's fixed cost is not. That is why `mode: takeover` sits behind an
explicit acknowledgement rather than being the default.

> Every timing above is a **warm cache on both sides**, which is the case that flatters a
> column store *least*: the row store's handicap is bytes, and bytes already in RAM are
> nearly free. For cold or I/O-bound workloads these are closer to a floor than a ceiling.

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

Then add the plugin to a `Cluster` — [docs/16](docs/16-deploying.md) has the spec, the two
images, what the plugin refuses, and what is not yet verified.

To run the verification yourself, in Docker: **[HANDOFF.md](HANDOFF.md)**.

---

## Evidence

- **[METRICS.md](METRICS.md)** — every measurement, the script that produced it, its raw
  output under [`bench/results/`](bench/results/), and a section on what the numbers do
  **not** say.
- **[docs/](docs/README.md)** — 37 documents. The 20s and 30s are mostly post-mortems:
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
