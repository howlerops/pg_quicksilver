# Every number, in one place

Every measurement this project rests on, with what produced it and what it does
not say. Nothing here is estimated or extrapolated: each row was produced by a
script in [`bench/scripts/`](bench/scripts/) and its raw output is checked in
under [`bench/results/`](bench/results/).

**Read the caveats.** Several of these numbers are unflattering, and the ones
that are unflattering are the load-bearing ones — a columnar mirror that is
935× slower at a point lookup is not a footnote, it is the reason
`mode: takeover` is gated behind an explicit acknowledgement.

Where a claim is sensitive to the machine, the machine is stated. Most runs are
a 4 vCPU / 16 GB host; the hardware line in each results file is authoritative.

---

## 1. The headline trade

| | PostgreSQL | mirror | |
| --- | ---: | ---: | --- |
| analytical queries, 30M rows | — | — | **median 30× faster** (5.8×–68.3×) |
| analytical queries, 3.2M rows, production sidecar | — | — | **median 5.8×** (up to 12.6×) |
| OLTP-shaped queries | — | — | **median 935× SLOWER** (182×–3,445×) |

Source: [docs/11](docs/11-measured-results.md), [`perf_mirror_pg17.txt`](bench/results/perf_mirror_pg17.txt).

The two halves are the whole design. A straight `-ro` takeover does not regress
some traffic — at 935× it takes production down, which is why `mode: takeover`
refuses to start without `acknowledgeOLTPRegression: "true"`.

Speedup scales with **column count**, because a column store's win is the
columns it does not read: 4 columns ≈ 7×, 12 columns ≈ 5.8×, 30 columns ≈ 30×.

---

## 2. Ingest and freshness — the production sidecar

From [`perf_mirror_pg17.txt`](bench/results/perf_mirror_pg17.txt), PostgreSQL 17
primary + streaming standby, sidecar configured only by the environment the
CNPG-I plugin sets.

| Metric | Value |
| --- | ---: |
| bootstrap to ready | 35.4s for 3,000,000 rows → **84,643 rows/s** |
| sustained drain through the mirror | **24,600 rows/s** |
| commit-to-visible p50 | **187.5 ms** |
| commit-to-visible p90 / p95 / p99 | 190.0 / 2382.2 / 2555.1 ms |
| storage | 645.9 MB heap+indexes → **116.3 MB parquet, 5.6× smaller** (38.0 B/row) |
| sidecar CPU | 59.5s total, **292.1 µs per row** |

The p50→p95 jump is compaction and merging, not steady state. `p50` includes the
200 ms apply interval, so ~187 ms is the floor by construction, not a latency the
design is fighting.

---

## 3. Per-shape ingest (the workload matrix)

Six table shapes chosen because each breaks something different.
[`workload_matrix.txt`](bench/results/workload_matrix.txt), 400k rows/shape, 20s
of workload, **every shape verified against its source**.

| shape | bootstrap | drain (changes/s) | p50 / p99 | storage | CPU/change | verified |
| --- | ---: | ---: | ---: | --- | ---: | --- |
| narrow | 195,364 rows/s | 129,001 | 207 / 4884 ms | 3.3× smaller | 26 µs | MATCH |
| wide | 97,934 rows/s | 72,993 | 206 / 803 ms | 2.7× smaller | 38 µs | MATCH |
| jsonb | 16,501 rows/s | 84,462 | 206 / 348 ms | **0.6× — LARGER** | 24 µs | MATCH |
| churn | 194,353 rows/s | 58,761 | 206 / 223 ms | 15.6× smaller | 9 µs | MATCH |
| deletes | 195,015 rows/s | 255,969 | 206 / 1919 ms | 4.1× smaller | 11 µs | MATCH |
| purge | 187,994 rows/s | 5,075 | 206 / — | 13.7× smaller | 14 µs | MATCH |

`purge` is newer than that results file; its numbers are in
[`workload_matrix_purge.txt`](bench/results/workload_matrix_purge.txt).

**The jsonb row is the honest one.** On a table of ~6 KB documents the mirror is
*larger* than the source, because PostgreSQL TOASTs and compresses the document
out of line and Parquet stores it inline. That shape is in the matrix precisely
because it used to fail its correctness check silently.

`purge` is rate-limited by design (a retention sweep), so its drain figure is a
property of the workload, not a ceiling.

---

## 4. Verified at scale

[`large_scale_20m.txt`](bench/results/large_scale_20m.txt):

```
23,005,206 rows   MATCH   checksum c5869d0f2beb3a0e
source 2.0G / mirror 133M  ->  14.7x smaller
seeded at 368,127 rows/s (32.7 MB/s)
```

Query comparison at that size — note the last row:

| query | postgres | mirror | |
| --- | ---: | ---: | ---: |
| count(*) | 353.0 ms | 294.4 ms | 1.20× |
| date arithmetic | 563.0 ms | 300.7 ms | 1.87× |
| truncate to the hour | 1293.6 ms | 231.5 ms | 5.59× |
| sum one column | 656.8 ms | 222.9 ms | 2.95× |
| group by sku top 10 | 3594.2 ms | 820.7 ms | 4.38× |
| filter + aggregate | 690.1 ms | 144.3 ms | 4.78× |
| **point lookup by key** | **0.3 ms** | **49.6 ms** | **0.01×** |

All seven agree with the source. The point lookup losing by 165× is the expected
shape of the trade, and it is in the table because leaving it out would have been
the flattering choice.

---

## 5. Cold cache

A warm page cache flatters the row store more than the mirror, so
[`cold_cache.txt`](bench/results/cold_cache.txt) drops caches between runs:

| query | postgres | mirror | |
| --- | ---: | ---: | ---: |
| count(*) | 57.7 ms | 18.5 ms | 3.11× |
| date arithmetic | 112.5 ms | 20.6 ms | 5.45× |
| truncate to the hour | 271.3 ms | 12.0 ms | 22.52× |
| sum one column | 120.5 ms | 10.9 ms | 11.10× |
| group by sku top 10 | 3670.3 ms | 296.8 ms | 12.37× |

---

## 6. Restart cost, and an honest readiness probe

[`restart_cost_4m.txt`](bench/results/restart_cost_4m.txt) (before) against
[`restart_cost_4m_eager.txt`](bench/results/restart_cost_4m_eager.txt) (after),
at 4M rows. The key index used to be built lazily inside the first change:

| | lazy | eager |
| --- | --- | --- |
| ready | 0.04s / **25 MB** | 2.24s / **505 MB** |
| first write after restart | **2.3s** / 507 MB | **0.6s** / 507 MB |
| a second, identical write | 0.5s | 0.5s |
| hidden stall | **~1.8s** | **~0.0s** |

The 25 MB was the number an operator would have sized a memory limit from, and
it was a twentieth of what the Pod actually needed. **Size memory limits from
what RSS shows at readiness**, which is now the honest figure: roughly
90–130 bytes of RSS per live row, dominated by the key index.

---

## 7. What a deletion vector costs to read

[`dv_cost_by_shape.py`](bench/scripts/dv_cost_by_shape.py), on a 20.5M-row
narrow mirror. `count(*)` against the base file:

| dead rows in the base | count(\*) | vs a clean scan |
| ---: | ---: | ---: |
| 0 | 2.6 ms | — answered from the Parquet footer |
| 20,458 | 64.1 ms | 21.2× |
| 204,582 | 112.9 ms | 38.0× |
| 2,045,826 | 220.3 ms | 75.0× |

The step from **no** vector to **any** vector is 24×; a hundred times more dead
rows is only a further 3.5×. So the lever is how long a base is allowed to carry
a vector at all — a statement about compaction frequency, not a dead-row ceiling.

The penalty is a property of **narrow** rows, because the anti-join is per-row
while the scan it competes with is per-byte:

| shape | bytes/row | column scan | penalty at 1% dead |
| --- | ---: | ---: | ---: |
| narrow | 6 | 1.0 ms | 13.9 ms (14.5×) |
| wide | 27 | 1.1 ms | 7.8 ms (6.9×) |
| jsonb | 1692 | 238.3 ms | 2.0 ms (<0.01×) |

---

## 8. Compaction policy, measured

`QS_COMPACT_DEAD_FRACTION` adds a second compaction trigger. It ships **off**,
and [docs/32](docs/32-the-knob-that-barely-fires.md) is the story of finding out
why that is the right default.

On the `purge` shape (the only one that reaches the trigger):

| | off (0) | on (0.01) |
| --- | ---: | ---: |
| compactions | **0** | **1** (`why=dead-fraction`) |
| base rows / dead | 400,000 / 100,800 | 299,258 / 0 |
| count(\*) | 15.1 ms | **5.7 ms** |
| sum | 12.4 ms | **4.8 ms** |
| filtered count | 14.6 ms | **5.6 ms** |

**2.6× on every query.** On narrow, wide, jsonb, inline, churn and deletes it
fires zero extra times — the churn trigger gets there first — so turning it on by
default would be inert almost everywhere and meaningful in one case.

---

## 8b. Serving through PostgreSQL, as an ordinary role

[`serving_pg17.sh`](bench/scripts/serving_pg17.sh), PostgreSQL 17.11, primary +
standby, mirror built by the production sidecar on the standby and queried
there. `pg_duckdb` v1.1.1 + the `allowed_directories` patch, built against 17.

| | |
| --- | --- |
| unprivileged role reads the mirror | 2,940,000 rows, sum **147079460.00** |
| the source heap says | 2,940,000 rows, sum **147079460.00** |
| privileges held | `duckdb_users` only — **not** `pg_read_server_files`, **not** `pg_write_server_files` |
| `/etc/passwd`, `pg_hba.conf`, `PG_VERSION`, `..` traversal, HTTP | **all denied** |
| widening the confinement from SQL | **denied**, three ways |
| `ORDER BY` over a NULLABLE column | agrees with the source |

**And it is faster**, which is the half that was never measured. Both columns are
the same PostgreSQL on the same node — the heap column an ordinary query, the
mirror column through `pg_duckdb` to Parquet — minimum of three runs, every
answer compared:

| query | heap | mirror | |
| --- | ---: | ---: | ---: |
| count(*) | 52.4 ms | 14.5 ms | **3.61×** |
| sum one column | 90.2 ms | 26.1 ms | **3.46×** |
| filter + aggregate | 96.3 ms | 24.4 ms | **3.94×** |
| group by sku | 120.3 ms | 23.8 ms | **5.07×** |
| truncate to the hour | 784.3 ms | 202.3 ms | **3.88×** |
| **point lookup by key** | **1.5 ms** | **19.1 ms** | **0.08×** |

This closes [docs/11](docs/11-measured-results.md) Results 6 and 7, which
together had blocked the serving path: the 1.27× regression was `pg_duckdb` over
the *heap*, which nothing here uses, and the privilege wall on the Parquet path
is gone. The safe path is now the fast path.

The speedups are lower than the 5.8×–30× measured against DuckDB directly,
because this is a 5-column table and the win scales with the columns a query
does *not* read — and because `pg_duckdb` is in the path. The point lookup is the
same trade as everywhere else in this file.

The mirror carried a deletion vector and column-partial deltas, so the view was
reconstructing rather than scanning one file. Raw output:
[`serving_pg17.txt`](bench/results/serving_pg17.txt).

---

## 9. Correctness, and the things that were wrong

Every performance figure above comes from a run that also verified the mirror
against its source. A fast mirror that disagrees is not a result.

| property | how it is checked |
| --- | --- |
| row-for-row agreement | `qs-verify`, order-independent checksums both sides |
| one live copy per key | asserted after **every** apply, swap and merge |
| the index names that copy | same check, same frequency |
| CNPG-I handshake | against the real CloudNativePG 1.30 operator |
| a sidecar change triggers a rollout | CNPG 1.30's own `specs.ComparePodSpecs` |
| a mirror rebuilt from files alone | e2e deletes every index and re-derives it |
| the shipped image, not the local binary | `image_e2e.sh` runs it and verifies with `qs-verify` from the same image |
| an ordinary role reading through PostgreSQL | `serving_pg17.sh`, no `pg_read_server_files` |

Bugs these caught, each of which produced *plausible* output:

- **A key written to one file twice** ([docs/31](docs/31-a-duplicate-row-under-compaction.md)) — a delete-then-reinsert inside one batch. ~1 in 1000 runs. Verified fixed at **10,000 runs, 0 failures**.
- **A table added to an existing slot never snapshotted** — 1,010 rows against a source of 6,000, no error.
- **A persisted index 2.7× the size of the Parquet it indexed**, and slower to load than rebuilding from it.
- **An A/B that compared a configuration with itself** and looked like a free knob.
- **The sidecar bound the port CloudNativePG needs** ([docs/35](docs/35-the-port-that-stopped-postgresql.md)) — `:9187`, the instance manager's metrics port, in a shared network namespace. As a native sidecar it started first, so PostgreSQL could not bind, called it `unretryable` and exited: the instance Pod crash-looped in `shadow` mode, on every instance. Only visible inside a real CNPG Pod.
- **A benchmark setting that outlived its run** ([docs/34](docs/34-a-setting-that-outlived-its-run.md)) — `synchronized_standby_slots` left pointing at a slot that no longer existed, which stops a *failover* logical slot dead with no error to any client. The mirror bootstrapped, passed readiness and never gained a row. An ordinary slot on the same server was unaffected, which is what made it look like a sidecar bug.

---

## 10. What these numbers do not say

- **`mode: takeover` is implemented but never observed.** It is one property — mirror freshness gates the `-ro` endpoint — because the service retarget the design called for is not reachable under a sidecar architecture ([docs/33](docs/33-the-probe-that-gated-the-wrong-thing.md)). A probe actually failing and removing a Pod needs a kubelet.
- **Serving through PostgreSQL needs an unpackaged patch.** A `SELECT` is now answered through PostgreSQL 17 by an unprivileged role ([docs/12](docs/12-s5-serving-path.md)), but only with `pg_duckdb` plus the `allowed_directories` patch in `bench/patches/`, which is not upstream. Stock `pg_duckdb` cannot do it without also granting `COPY FROM '/etc/passwd'`.
- **Nothing has *pulled* the published images.** They build, push, and run — `qs-verify` from the shipped image calls its own mirror MATCH ([`image_e2e.txt`](bench/results/image_e2e.txt)) — but the ghcr packages are private and a Pod pulling them needs a kubelet this machine cannot provide. The check exists ([`cluster-e2e`](.github/workflows/cluster-e2e.yml)) and has not been run.
- **A rollout is decided correctly but never watched.** CNPG 1.30's own `specs.ComparePodSpecs` confirms a `sidecarImage` change reads as `init-containers: container quicksilver-mirror differs in image`, and that an unchanged Cluster compares equal. No Pod has been rolled and none has been removed from a Service by the freshness probe: both need a kubelet that can create pod sandboxes, which needs `CAP_SYS_RESOURCE` ([docs/21](docs/21-against-the-real-operator.md)). Steps 4 and 5 of [`cluster_e2e.sh`](bench/scripts/cluster_e2e.sh) assert them against a real cluster.
- **Most runs are one machine, 4 vCPU.** Ratios travel; absolute milliseconds do not.

The full, current list is in [docs/16 — What has not been verified](docs/16-deploying.md).

---

## Reproducing any of it

```bash
bash bench/scripts/setup_cluster.sh     # PostgreSQL 17 primary + standby
bash bench/scripts/suite.sh             # correctness sections
FULL=1 bash bench/scripts/suite.sh      # + the workload matrix and serving
```

A section that cannot run exits **2 as INCOMPLETE** and never reports success —
a benchmark that silently measures nothing is the failure mode this repository
has hit most often, and the exit code exists to make it loud.

The full suite is green end to end: **14 passed, 0 failed, 0 incomplete**
([`suite_full.txt`](bench/results/suite_full.txt)). That took three harness
fixes to reach, all of one shape — state that outlives a run: a leftover
`synchronized_standby_slots` entry ([docs/34](docs/34-a-setting-that-outlived-its-run.md)),
a standby `e2e` had promoted and `setup_cluster.sh` then accepted as a standby,
and container storage filling the disk until PostgreSQL reported it four levels
down as "primary would not start".
