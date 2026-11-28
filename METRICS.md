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

## 0. Final end-to-end validation, 2026-11-26

Run on branch `claude/busy-wozniak-0k5d5n` at `b4c5fda` (product code unchanged). Scripts:
[`bench/final_validation/scripts/`](bench/final_validation/scripts/); raw output:
[`bench/final_validation/out/`](bench/final_validation/out/). Sections 1 to 10 below are the earlier
measurements and stand as they were; where this section and an older one disagree, this one is the
newer measurement and the older one is history.

**Method.** One host, 4 vCPU, 16 GB RAM, Linux 6.18, PostgreSQL 17.12 with the project's patched
pg_duckdb 1.2.2 (DuckDB 2.0.1), `pg_quicksilver.so` built 2026-11-26 21:17. Primary, hot standby and
the production sidecar (`qs-mirror`, `QS_DUCKLAKE=on`) on one host. Every answer is checked
against the heap before its time counts. Part 1 is functional; Part 2 is the equality harness;
Part 3 is timed on 10,000,000 orders and 500,000 customers (the `load.sql` dataset, not 3M: the
load took 113 s); Part 4 repeats three phases under a 1 GiB cgroup. Timed server processes were
pinned with `taskset -c 0-2` (the three cores of the earlier runs); the benchmark client ran on
CPU 3. **The machine was not quiet:** another Quicksilver stack on the same host (processes under
`/var/lib/postgresql/qsexp`, not started by this run and not touched) kept the 1-minute load at
2 to 4 throughout, so the "load below 2" rule could not be met. Each table below is one run unless
it says best of 5 (best/median of five warm runs).

### 0.1 The replay gate, the routed join, and the final tree (2026-11-27)

Run on branch `worktree-agent-a2364e4f4c4d74a28` from `7bc7565`, with the commits that add the replay
gate. The host, PostgreSQL 17.12 and the pg_duckdb build are those of §0. The primary, a hot standby and
the production sidecar were built here from a private copy of the PostgreSQL 17 tree (the copy of the
current pg_duckdb patch, checked line by line against the patch's added code). Each live cluster is
primary + standby + `qs-mirror` on its own ports; nothing else was started on them.

**Visibility gate, live.** Replay was paused on the standby, 500 rows written on the primary, and the
routed count was compared with the heap count of the same session, ten samples each. Before: the
pre-change extension with the gate off (`QS_GATE_ON_REPLAY=off`).

| state | heap | routed | samples where routed = heap |
|---|---|---|---|
| paused, before (pre-change extension, gate off) | 1,001,000 | **1,001,500** | 0 of 10 (routed ahead of the heap by 500) |
| paused, after (gate on) | 1,006,813 | 1,006,813 | 10 of 10 |
| paused, relaxed freshness (`quicksilver.max_lag = 100MB`), after | 1,006,813 | 1,006,813 | routed never above the heap (4 of 4) |
| after resume, after | 1,007,313 | 1,007,313 | equal; replay caught up in 0.10 s |

The "before" row is the old behaviour reproduced: the mirror had the 500 rows that the standby's heap did
not have yet, and a read sent to the mirror saw newer data than the heap at the same moment. It is also
why the freshness check needed an upper bound, which the extension now has on a standby. The "before" run
was with the gate off and the old extension together; the gate off alone, with the new extension, gives
the heap's answer, because the new upper bound refuses a mirror that is ahead.

**Continuous writes, gate on.** A writer inserted one row per 20 ms for 180 s. A sampler on the standby read
heap, routed, heap in one session, 2,567 times. Violations of `heap1 <= routed <= heap2`: **0**. Writes
8,267, write errors 0. Before (gate off, old extension, 90 s): 1,283 samples, 0 violations, 4,146 writes.
Under continuous writes the mirror normally trails the standby's replay, so the race the gate closes
rarely shows; the paused run above is the case that shows it.

**Visibility lag, commit to routed-visible.** One insert per commit every 50 ms for 60 s; the routed count
was polled every 5 ms.

| run | commits seen | median | p95 | max |
|---|---|---|---|---|
| before (gate off, old extension) | 1,167 | 50.2 ms | 79.4 ms | 283.8 ms |
| after (gate on), run 1 | 1,159 | 55.4 ms | 93.8 ms | 248.3 ms |
| after (gate on), run 2 | 1,167 | 52.7 ms | 87.1 ms | 264.2 ms |

The gate adds about 3 to 5 ms to the median and 8 to 14 ms to the p95 in these runs. That is the standby's
own replay lag, which a commit now waits for before it can be routed. Each figure is one 60-second run,
and the "before" side has one run; the spread between the two "after" runs is the only variance measured.

**Readiness while replay is paused.** `/readyz` said `ready: true`, `lag 3.1s within SLO 30.0s; waiting for
local replay: 1 change(s) held back until the standby replays through 0/F458EB9 (replay is at 0/F458D98)`,
and `slot_retained_bytes` was 243,920 while the hold lasted. After resume: lag 0.2 s.

**Join domain registry (Bug E), live, one sidecar.** A registry row for a column this sidecar does not
mirror was planted on the primary, and the sidecar restarted. The row survived; the sync said
`unchanged=9 dropped=[]`. The old code deleted it. The two-sidecar case on one host was not run: the
lake catalog path is one per cluster, which the join setup already notes as unshown.

**Regression suite, serial (`--max-connections=1`), 20 tests** (the `REGRESS` list has 20 names, not 21):
`All 20 tests passed`, on a fresh cluster with the sidecar running and `primary_report.sh` run first. One
expected file changed: `freshness.out`, for the reason text of a fed, stale mirror on the primary (Item F).
Before that change the same suite gave 19 of 20, with only that file differing.

**Equality (`validate/equality.py`), join domains on, 1,000,000 orders.** Each run compares every statement
with the heap and with the router, on the primary, after `primary_report.sh`.

| run | statements | routed | identical | routed different | heap different |
|---|---|---|---|---|---|
| superuser, UTC | 1,142 | 449 | 449 | **0** | **0** |
| superuser, America/New_York | 1,142 | 449 | 449 | **0** | **0** |
| `qs_eq` (ordinary role), UTC | 1,142 | 449 | 449 | **0** | **0** |
| `qs_eq` (ordinary role), America/New_York | 1,142 | 449 | 449 | **0** | **0** |

**Transaction fuzz (`validate/real/txfuzz.py --scripts 2000 --seed 1`).** 2,000 scripts, 26,866 statements,
9,660 of them errors on both sides and the same, **7,086 answered by the mirror**, 0 scripts differing,
0 connections lost.

**Revenue by month, routed (1,000,000 orders, 24 months).** The seed's `created_at` spans 12 days, so for
this measurement it was respread on the primary to `2024-01-01 + (id mod 730) days + (id*7919 mod 86400)
seconds`: 24 distinct months, 2024-01 to 2025-12. Standby, the join to `customers` grouped by month, median
of five runs (the first included, each after `quicksilver.forget_last()`), `quicksilver.route` heap against
auto, with `SET TimeZone` to the zone and `min_cost = min_rows = 0`. The query:

```sql
SELECT date_trunc('month', o.created_at) AS month, sum(o.amount) AS revenue, count(*) AS orders
  FROM public.orders o JOIN public.customers c ON c.id = o.customer_id
 GROUP BY 1 ORDER BY 1
```

The runner was a short Python script that is not checked in; it compares the md5 of the rows of the two
sides, and the answers were equal in both zones:

| zone | heap | routed | speedup | answers |
|---|---|---|---|---|
| UTC | 455.3 ms | 73.4 ms | 6.21x | identical (24 rows) |
| America/New_York | 469.1 ms | 89.4 ms | 5.25x | identical (25 rows) |

The routed plan in New York was `Custom Scan (QuicksilverRoute)`, `Quicksilver: mirror`, decision `mirror,
routed`. The 10,000,000-order run was not done; `bench/final_validation/scripts/fv_bench.py` was not used,
because its revenue query needs the bench dataset (`customers.created_at`), which the seed does not have.

**Go.** `go build ./...`, `go vet ./...`, and `go test -short -count=1 ./...` pass (10 packages with tests).

**What is not measured.** A `kill -9` of the sidecar while it holds commits, live; a snapshot taken at S
and read before replay reaches S, live (the rule is unit-tested only); `recovery_min_apply_delay`, live; a
promoted node's refusal reason, live; the two-sidecar join case; the 10,000,000-order headline; the
`fv_bench` suite.

### 0.2 The owed live validations, 2026-11-28 (A: headline, B: kill -9 in a hold, C: promotion)

Branch `claude/busy-wozniak-0k5d5n` at `4a99c0f`. pg_quicksilver built from that tree against the patched
PostgreSQL 17.12 build (pg_duckdb 1.2.2), in a private prefix under `/var/lib/postgresql/w2e`; the installed
`.so` predates `quicksilver.sidecar_claims`, so it was not used. **Machine caveat:** one shared host, 4 cores,
15 GB; other agents' clusters ran throughout (1-minute load 2 to 15), and the disk had 0.8 to 4 GB free. Every
number is a single run, or best and median of five warm runs where the table says so. Scripts and raw results:
`bench/owed_live_2026-11-28/`.

**A. The headline at 1,000,000 orders, not 10,000,000.** The 10M run did not fit the disk: 1M orders are
352 MB (271 MB heap) with the bench layout, a standby doubles that, and the disk had under 4 GB free. The
data is the bench layout (`load.sql`: uuid keys, jsonb payload, 730 days, 500,000 customers). Heap is
`quicksilver.route = heap` on the standby, routed is `auto`; one warm-up, then five timed runs; every timed
answer's full result set is an md5 of the sorted rows, compared with the heap's on the same state.

| query | TZ | join domains | heap best / median (ms) | routed best / median (ms) | median ratio | answers |
|---|---|---|---:|---:|---:|---|
| revenue by month, join to customers (25 rows) | UTC | bootstrapped with them | 850.1 / 916.6 | 154.4 / 162.4 | 5.6x | identical |
| revenue by month, join to customers | UTC | off | 696.8 / 719.8 | 178.7 / 213.1 | 3.4x | identical |
| revenue by month, join to customers | America/New_York | bootstrapped with them | 626.9 / 891.2 | 196.3 / 231.8 | 3.9x | identical |
| revenue by month, join to customers | America/New_York | off | 707.0 / 879.7 | 156.0 / 168.3 | 5.2x | identical |
| `count by status` (6 groups) | UTC | bootstrapped with them | 93.7 / 133.5 | 20.8 / 22.7 | 5.9x | identical |
| `sum(amount)` | UTC | bootstrapped with them | 94.0 / 105.8 | 11.8 / 18.1 | 5.9x | identical |
| `count(*)` | UTC | bootstrapped with them | 66.3 / 67.6 | 9.3 / 10.7 | 6.3x | identical |
| one month, `count(*)`, `sum(amount)` (June 2025) | UTC | bootstrapped with them | 11.3 / 12.3 | 10.6 / 10.8 | 1.1x | identical; the router chose the heap (cost 2,737 under 10,000) |
| `count by status` (6 groups) | America/New_York | off | 71.6 / 78.6 | 10.1 / 10.4 | 7.6x | identical |
| `sum(amount)` | America/New_York | off | 64.6 / 79.1 | 9.8 / 10.6 | 7.5x | identical |
| `count(*)` | America/New_York | off | 41.4 / 54.1 | 4.9 / 6.2 | 8.7x | identical |

The `count by status`, `sum` and `count(*)` routed through the mirror (`mirror, routed`) in every run; the
headline did too (`Join keys: 1 rewritten to companions` on the join-domain cluster). The one-month filter
is cheap on the heap, so the router keeps it there: in New York the routed statement took 10.2 ms against the
heap's 5.4 ms, which is the router's own overhead on a statement it did not route. Heap times here are higher
than §0.1's (455 ms then, 620 to 920 ms now) on the same query, because the host was loaded; compare ratios.

Sidecar: bootstrap by snapshot took 7.8 s for the 1M orders and 2.2 s for the 500k customers (join domains on;
4.8 s and 1.5 s with them off). Peak RSS (`VmHWM`) of the join-domain sidecar after its bootstrap and the lag
runs: 303 MB. The mirror on disk: 141 MB for 1M orders and 500k customers.

Commit to routed-visible (METRICS §0.1 method: one insert per commit every 50 ms for 60 s, routed count
polled every 5 ms, replay gate on):

| run | commits | median | p95 | max |
|---|---:|---:|---:|---:|
| join domains, bootstrapped with them | 1,105 | 25.9 ms | 58.3 ms | 118.8 ms |
| join domains, added on restart (run 1) | 1,109 | 30.9 ms | 73.8 ms | 112.3 ms |
| join domains, added on restart (run 2) | 1,117 | 32.4 ms | 80.4 ms | 125.5 ms |

Some commits were seen up to 263 ms before the client's commit returned: the row was on the standby before
the client woke. That is the client's scheduling on a loaded host, not an early read (both sides read the
same monotonic clock); the medians are unaffected by it.

Two findings from A. (1) **Join domains added to an existing mirror are not used.** On the cluster that had
been bootstrapped without them, adding `QS_JOIN_DOMAINS` and restarting the sidecar gave no `Join keys` line
in `EXPLAIN` and no change in the routed time; the mirror was not rewritten to companions. On a cluster
bootstrapped with the registry from the start the rewrite is used. The join-domain columns are reported
complete only once a complete manifest is published (`go/cmd/qs-mirror/extension.go`, the completeness
comment), and an existing mirror's files have no companion column, so it is not complete. Not established
whether it converges later; enable join domains at bootstrap. (2) In the first attempt the second sidecar
of §C wrote its lake path into the registry of the shared cluster; the first standby's lake fell outside its
`duckdb.allowed_directories` and it never routed. Two sidecars on one cluster must see the same mirror path
(per-node mount namespaces, as `joinpods_setup.sh` does).

**B. `kill -9` during a replay hold.** Standby replay paused with `pg_wal_replay_pause()`; the primary
took commits to `orders` and `customers`, including one 100,000-row transaction (run 1 and 2) or a 160,000-row
transaction streamed while the standby was paused (run 3, above the 50,000-change spool threshold). Routed
and heap counts were sampled 8 times at each state; the verify is `qs-verify` on both tables and the md5
of the primary's and the standby's rows.

| run | kill | at the kill | after the restart (still paused) | after resume | verify |
|---|---|---|---|---|---|
| 1, hold | during the hold | `waiting for local replay: 243 change(s) held back`; routed = heap 8 of 8 | restart: `waiting ... 60 change(s)`; routed = heap 8 of 8 | caught up in 1.55 s | orders 1,201,465, customers 500,240: primary = standby = routed; `qs-verify` MATCH both |
| 2, drain | after resume, at routed 1,201,525 of 1,301,585 | slot `confirmed_flush` 0/2EE69388, mirror applied 0/2ED552C8 (121 held) | `ready`, no hold | caught up in 0.27 s | orders 1,301,585; `qs-verify` MATCH both |
| 3, spool | with a 160,000-row transaction open (4 chunks of 40,000) | 40 small commits held behind it | restart paused: routed = heap 8 of 8; the big transaction committed while paused: still waiting, routed = heap 8 of 8 | caught up in 0.87 s | orders 1,461,625; `qs-verify` MATCH both |

Each run: no lost row, no duplicate (`count(*) = count(DISTINCT id)` on the primary, the standby heap and the
routed read), no divergence. The first attempt (run 0) was killed by a harness bug after its kill and was
restarted; the workload then reused customer names, so 120 orders were written twice in run 1. Both are
harness faults; the comparison is against the primary, so they do not touch the result.

Open point, not a verdict: in runs 2 and 3 the slot's `confirmed_flush` was past the mirror's applied LSN by more
than the usual one byte while transactions were held (run 2: 1.9 MB past, with 121 held; run 3: 1.7 KB past,
with 40 held). Run 1 had the usual one-byte offset. `changestream/spool.go` says a slot is not confirmed
past a transaction still in a spool, and the spool is not durable. Zero loss was seen, so either the held
changes were durable somewhere else (not checked here) or `applied_lsn` in `mirror_status` is not the position
the slot follows. The owner of the sidecar should say which.

**C. Promotion.** Three-node cluster, namespaced (each standby and its sidecar in its own mount namespace, with
its mirror bind-mounted over the same path): primary P (6620), standby A (6621, sidecar health 6623), standby B
(6622, sidecar health 6624); 300,000 orders, 50,000 customers, join domains on. A write every 20 ms throughout.
Pollers on A from before the stop, every 5 ms: routed count, heap count and the routed statement's decision,
once with `quicksilver.max_lag = 0` (default) and once with `1GB`. Timeline (seconds since the start of the
run, run 2):

| t (s) | event |
|---:|---|
| 16.46 | headline plan on A before the stop: heap (default freshness, writes in flight); P's fast stop begins |
| 21.16 | P stopped (the fast stop took 4.7 s) |
| 21.42 | `pg_ctl promote` on A done; A not in recovery at 21.44 |
| 21.46 | strict poller's first answer on A after the promotion: heap, reason `this node is not a standby: its mirror is no longer fed` (QR_NOT_STANDBY) |
| 22.17 | A's sidecar: `this node is the primary — standing down`; it dropped its slot `qs_a_qsint_a` 70 ms later |
| 24.26 | physical slot `qsint_b2` created on A; B's sidecar was retrying P (connection refused) until stopped at 28.30 |
| 29.18 | B's standby restarted in its namespace, streaming from A (`qsint_b2`) |
| 29.68 | B's sidecar ready, 0.5 s after it started; log: `resuming from the existing slot slot=qs_b_qsint_b` (the failover slot A received via `sync_replication_slots`; no re-snapshot) |
| 150.9 | writes stopped (5,316 writes in the run) |
| 151.9 | B caught up (0.9 s) |
| 152.8 | final: A heap = B heap = B routed = 305,316 rows, md5 `35aab906…`; `qs-verify` B MATCH |

Freshness, with the 5 ms pollers. With the default `max_lag = 0`: the last mirror-routed answer on A came at
20.37 s, before the stop; after the stop there were 1,082 polls and none was mirror-routed. The reason
changed to QR_NOT_STANDBY 0.04 s after the promotion completed, 0.30 s after P stopped. So, under default
freshness, no window in which A answered from a mirror that was no longer fed was seen at 5 ms resolution.
B under writes: with `max_lag = 0` its 48 one-second samples were all heap (its mirror is never fresh while
writes continue, so it routes only when writes pause); with `max_lag = 1GB` all 48 were mirror-routed. The
sampling does not separate the mirror's lag from the time between the two reads, so no lag figure is given for B.
B's answers therefore were not checked equal to A's heap during the two minutes; they were checked after the
writes stopped (the final line above).

What A's sidecar left behind: `/readyz` after the stand-down still said `ready: true`, `lag 2.6s`, with the
last applied LSN (not `0/0`); `mirror_status()` reported `applied_lsn` NULL on the same node.

**Bug (for the C owner): a promoted node serves stale routed answers when `quicksilver.max_lag` is above 0.**
With `max_lag = 1GB`, A kept answering from its mirror after the promotion: 1,237 of the 1,246 polls after the
stop were mirror-routed (decision `routed`), and the routed count froze at 300,610 while the heap grew to
305,316 (4,706 rows behind at 150 s, still growing). The first namespaced run (100 ms polls) showed the same
freeze: routed 300,602 against heap 305,327 at 147 s. Reproduce: `bench/owed_live_2026-11-28/c2_setup.sh`, then
`POLL_S=0.005 python3 c_promote2.py out.json`; the `lag1gb` poller is the one that shows it, and
`analyze_c2.py` prints the freeze.

Cause: `qs_fresh` (`ext/pg_quicksilver/pg_quicksilver.c:3932-3985`) checks only the upper bound (`applied > now`)
and the lag bound (`now - applied > max_lag bytes`). After the promotion the applied LSN is still the last one
the sidecar reported, and under 1 GB of WAL the bound is not exceeded, so the mirror passes as fresh. The
"no longer fed" reason is computed by `qs_stale_reason` (`:3920-3930`) only when `qs_fresh` fails, so with a
byte budget it is never reached. At `max_lag = 0` the same check fails at once, which is why only the byte
budget case shows. Suggested fix, for the C owner: a node that is not in recovery is not fresh whatever the
bound (checked on the read path, not only in the registry), and/or the stood-down sidecar clears its report so
that `qs_fresh` takes the "no applied LSN" branch. The disagreement between `mirror_status()` (applied NULL) and
the read path (applied set) should be checked at the same time.

Not done: the 10,000,000-order runs (disk, above); the join-domain completeness change on A (the headline did
not route on A before the stop under default freshness with writes in flight, so there was no routed plan to
change); the per-table `kill -9` with a third table (the sidecar mirrors `orders` and `customers` here).

### Part 1: functional scenarios

Twelve tables on the standby's sidecar: `public.orders` (200k rows, then 10M in Part 3),
`public.customers`, `public.qs_types`, the six `ex.*` tables of the setup, and the new `fv.events`
(uuid key, `numeric(12,2)`, `timestamptz`, text; 200k rows), `fv.accounts` (text key; 200k rows)
and `fv.lookup` (500 rows). The dataset is the repository's setup plus those three tables, not a
bespoke schema; the checks are the same.

| | scenario | result | deciding line (file in `out/`) |
|---|---|---|---|
| a | bootstrap, mixed writes on all tables | **PASS** | `qs-verify`: 12 MATCH (`a_qs_verify.txt`); `tables 12 different 0` (`a_lake_vs_heap.txt`): count, sums, and an md5 over typed text per table |
| b | qualifying aggregates routed on the standby; PK lookups on the heap | **PASS** | `Quicksilver: mirror` for `GROUP BY status`, `sum` with a filter, `GROUP BY label`; each `EQUAL` to `route=heap`; PK lookups `Quicksilver: heap (cheap on the heap)`, `EQUAL` (`b_routing_standby.txt`) |
| c | `kill -9` the sidecar during writes; restart | **PASS** | sidecar pid 22184 killed, writer 4 rounds in; restarted; writer 40/40 committed; after settling, `12 MATCH`, 3 aggregates routed `EQUAL` (`c_recheck_*`). See finding 9 for the unsettled check |
| d | `TRUNCATE fv.events`, reload | **PASS** | `fv.events: EQUAL heap=200000 lake=200000`; 12 MATCH; 3 routed aggregates `EQUAL` (`d_*`) |
| e | `ADD COLUMN tier text NOT NULL DEFAULT 'std'`, then writes | **PASS** | 12 MATCH; `GROUP BY tier` routed to `Quicksilver: mirror`, `rows routed=3 heap=3 EQUAL` (`e_probe.txt`) |
| f | unsupported type change on one table (`fv.lookup.id` integer to bigint) | **PASS** | `/readyz` HTTP 200 (`QS_SERVE_WITH_HALTED_TABLES=on`), `halted_tables: {"fv.lookup": "... RETYPED id integer->bigint ..."}`; 11 MATCH while halted; REBUILD marker and restart: 12 MATCH (`f_*`) |
| g | one transaction updating 500,000 rows | **PASS** | `UPDATE 500000` in one transaction, 2.46 s; 12 MATCH; sidecar VmRSS sampled every 0.2 s: peak 283 MB (`g_rss_samples.txt`). The 500,000-row insert that preceded it was replicated during the sampling window, so the peak is not attributable to the update alone |
| h | ordinary role `fv_app` (not superuser, not in `qs_duck`) | **PASS** | `fv_app`: 3 aggregates `Quicksilver: mirror`, `EQUAL`; `duckdb.query` refused: `DuckDB execution is not allowed because you have not been granted the duckdb.postgres_role`; `fv_nosel` (no SELECT): `ERROR: permission denied for table accounts` (`h_*`) |
| i | `BEGIN` at READ COMMITTED, aggregate, then an INSERT | **PARTIAL** | first half PASS: routed in a block with no writes, `EQUAL`. The second half cannot run: the standby is read-only, `cannot execute INSERT in a read-only transaction`. A SAVEPOINT (the rule's other trigger) moved the statement to the heap: `in a subtransaction (savepoint): pg_duckdb refuses` (`i_run.txt`) |
| j | promote the standby; new standby by `pg_basebackup`; sidecar there | **PASS** (with findings 4 and 5) | primary logged `this node is the primary — standing down`; new standby on 5803: `12 MATCH`; routing `EQUAL` after the config fix in finding 5 (`j_*`) |
| k | two standbys with sidecars; concurrent writes | **PASS** | standby 5803 and standby 5806: each `12 MATCH`; primary: `distinct active logical slots: 2` (`qs_int2_qsint_3`, `qs_int3_qsint_4`). Writer b 15/15 rounds; writer a stopped by a deadlock after 3 rounds (`k_writer_a.txt`), which PostgreSQL resolved. Routing on 5803 was not active at that point (finding 6) |
| l | join domains on a uuid key and foreign key (experimental) | **PASS** on the clean run; **an unexplained wrong answer** on an earlier run | clean run (`l_run4.txt`): `join_domains` 2 rows; `Join keys: 1 rewritten to companions`; `routed=1 heap=1 EQUAL`. With `QS_JOIN_DOMAINS` unset: no rewrite, 0 rows in `join_domains`, `EQUAL`. Earlier run (`l_run3.txt`, finding 7): a routed join returned `('0', 'None')` where the heap returned `('232112', '928463')` |
| m | `QS_TEMPORAL_TYPES=0` on a fresh mirror | **PASS** | 12 MATCH; lake vs heap `tables 12 different 0`, compared as the text the lake holds for timestamps (`--ts-text`); routing `EQUAL` (`m_temporal_off_*`) |

**Part 2: equality harness** (`ext/pg_quicksilver/validate/equality.py`, on standby 5806, after
scenario m, with the default sidecar on a fresh mirror):

```
superuser postgres:  statements 1142, routed_to_mirror 425, stayed_on_heap 717, routed_identical 425, routed_same_error 0, routed_different 0, heap_different 0
ordinary role fv_app: statements 1142, routed_to_mirror 276, stayed_on_heap 866, routed_identical 276, routed_same_error 0, routed_different 0, heap_different 0
```

`routed_different` is 0 for both. (The per-statement summaries are in `p2_equality_*.json`; the
full records were over 300 KB and are summarised.)

### Part 3: benchmark, 10M orders, 500k customers

Set-up: `load.sql` with `rows=10000000 nc=500000` (113 s); `staging.sql` (the change workload's
tables, 50 s); `LOCALE=C` so text `ORDER BY` can route; base backup to the standby; sidecar started
last and timed. Change workload: `workload.py`, 200 transactions: 500,000 status updates,
100,000 payload updates, 100,000 deletes and 100,000 inserts, 3.6 GB of WAL, 50 to 59 s. The earlier
runs in the four-engines results used the same 10M-order dataset (their `ref_A.json` counts sum to
10,000,000), on 3 cores.

**Initial sync and drain**

| measure | this run | earlier (merged build / four engines) |
|---|---:|---:|
| initial sync, sidecar start to `/readyz` 200, 10M rows | **47.2 s** | 63 to 66 s (`initial ready_after_s`) |
| sidecar VmHWM at readiness | 311 MB | 1,395 MB peak RSS (merged initial) |
| backlog drain, sidecar stopped during the workload, to the workload's last LSN | **14.1 s** | 17.9 to 27.4 s (`drain … start_to_visible_s`) |
| sidecar VmHWM after the drain (the same process, from its start) | 1.57 GB | 3,054 MB (earlier initial), 2,972 to 3,040 MB (merged catch-up) |
| idle rewrite (`compaction started` to `compacted`), 10M base + 700k delta | **65.8 s** | (not measured in the earlier results) |

**Commit to visible** (30 commits of 5,000 rows, each a single transaction on the primary; visible =
the sidecar's applied LSN has reached the commit, read from `/readyz` every 10 ms, to within 8 KB):
**median 174 ms, p95 396 ms**, min 86 ms, max 402 ms. The earlier live-lag measurement of the
Quicksilver sidecar was a median of 133 to 207 ms (`live_lag_qs*.json`), a different probe.

**The six queries, three engines, three states** (milliseconds, best / median of 5 warm runs; every
answer equal to the heap's; `(heap)` marks a statement the extension did not route):

| query | state | heap, earlier (pg) | Quicksilver, earlier (qs) | Mason, earlier | heap, this run | routed (extension), this run | DuckDB direct, this run |
|---|---|---:|---:|---:|---:|---:|---:|
| count_by_status | A: fresh after load | 609.5 / 640.2 | 19.2 / 21.3 | 23.0 / 24.1 | 590.7 / 626.0 | 36.4 / 40.7 | 48.2 / 50.3 |
| revenue_by_month_join | A | 9580.0 / 9854.1 | 1376.1 / 1387.0 | 233.5 / 238.2 | 3739.2 / 3866.0 | 3844.7 / 3913.2 (heap) | 3211.2 / 3296.5 |
| pk_lookup | A | 0.1 / 0.1 | 237.9 / 325.1 | 14.5 / 15.3 | 0.1 / 0.2 | 0.2 / 0.4 (heap) | 23.4 / 26.1 |
| count_star | A | 245.3 / 249.3 | 3.0 / 3.1 | 4.0 / 4.0 | 251.9 / 267.6 | 8.5 / 10.1 | 2.8 / 3.1 |
| sum_amount | A | 455.7 / 462.6 | 36.6 / 42.6 | 7.4 / 7.9 | 498.3 / 505.3 | 30.2 / 33.1 | 48.4 / 51.3 |
| month_filter_agg | A | 51.0 / 61.8 | 10.1 / 10.9 | 5.5 / 6.5 | 66.6 / 77.5 | 20.4 / 26.1 | 17.5 / 17.8 |
| count_by_status | B: after the change workload | 700.2 / 714.7 | 112.2 / 120.6 | 28.5 / 35.7 | 759.0 / 789.0 | 71.9 / 78.8 | 97.1 / 102.6 |
| revenue_by_month_join | B | 9667.9 / 9780.0 | 1519.0 / 1566.6 | 259.4 / 265.8 | 5041.6 / 5649.1 | 4194.4 / 4600.2 (heap) | 3298.7 / 3425.8 |
| pk_lookup | B | 0.1 / 0.1 | 296.3 / 305.5 | 14.7 / 15.6 | 0.1 / 0.2 | 0.2 / 0.4 (heap) | 46.5 / 49.0 |
| count_star | B | 1370.3 / 1417.5 | 331.8 / 341.3 | 7.2 / 7.3 | 1538.6 / 1656.7 | 7.5 / 7.9 | 2.8 / 2.9 |
| sum_amount | B | 561.1 / 579.8 | 127.4 / 129.7 | 13.2 / 15.8 | 590.5 / 596.9 | 31.0 / 35.2 | 49.4 / 51.6 |
| month_filter_agg | B | 70.7 / 71.8 | 47.5 / 55.3 | 5.8 / 6.1 | 94.7 / 97.9 | 21.3 / 23.0 | 17.1 / 17.3 |
| count_by_status | C: after the mirror compacted | 618.9 / 687.9 | 120.9 / 137.1 | n/a | 710.2 / 732.1 | 33.8 / 35.5 | 55.0 / 55.7 |
| revenue_by_month_join | C | 9009.6 / 9232.4 | 1506.8 / 1549.5 | n/a | 3683.8 / 3791.8 | 3824.1 / 4014.1 (heap) | 3100.3 / 3129.6 |
| pk_lookup | C | 0.2 / 0.3 | 302.7 / 323.0 | n/a | 0.1 / 0.2 | 0.2 / 0.4 (heap) | 24.4 / 24.8 |
| count_star | C | 1369.8 / 1436.2 | 340.0 / 352.5 | n/a | 1614.1 / 1618.3 | 7.9 / 8.2 | 3.0 / 3.1 |
| sum_amount | C | 534.3 / 541.9 | 138.0 / 144.5 | n/a | 568.8 / 583.6 | 28.8 / 29.8 | 46.7 / 51.8 |
| month_filter_agg | C | 68.0 / 70.4 | 66.5 / 72.0 | n/a | 94.3 / 99.4 | 20.8 / 22.0 | 15.3 / 15.7 |

"Earlier" columns are from `bench/engine_compare/four_engines/results/{pg,qs,mason}_{A,B,C}_warm.json`
(Mason has no state C run there). The merged-build file `merged_build/out/p2_summary.txt` has the same
states for the Quicksilver build of that time (for example count_by_status 19.2 / 21.3 ms fresh,
40.4 / 43.4 ms after the change workload). Regenerate the table with
[`scripts/make_tables.py`](bench/final_validation/scripts/make_tables.py).

Reading the table: the routed column is the extension's answer through PostgreSQL; the DuckDB column
is the same DuckLake read by a Python process with no PostgreSQL in the path. State C is after the
sidecar's own idle compaction. The heap's `count(*)` in state C is slower than in state A (252 to
1,614 ms): the standby's heap carries the workload's dead tuples, and nothing vacuumed it during the
run. The state-B suite ran while the idle compaction was in progress (it started at 22:30:29 and
ended at 22:31:35), so state B includes compaction work.

**Ten concurrent clients** (state C, each client its own connection; time until all ten have answers,
median of three rounds; every answer equal to the heap's):

| query | heap | routed (extension) |
|---|---:|---:|
| `count_by_status` | 6,586 ms | 260 ms |
| revenue by month, join | 43,655 ms | 43,441 ms (heap: not routed) |

### Part 4: memory under the plugin's default sidecar limit

`DefaultSidecarMemory = "1Gi"` (`go/internal/plugin/config.go`) = 1,073,741,824 bytes. Cgroup v1 memory
controller, `/sys/fs/cgroup/memory/qsfinal`, with the sidecar's shell and its children in it. The
usage figure includes page cache, which is reclaimable.

| phase | completed | time | sidecar VmHWM | cgroup peak usage | reclaim events (`failcnt`) | OOM kills |
|---|---|---:|---:|---:|---:|---:|
| initial sync, 10M rows | yes | 49.8 s to ready | 321 MB | 1,073,745,920 bytes (the limit) | 30,369 | 0 |
| backlog drain (700k changes, 3.6 GB WAL), sidecar stopped during the workload | yes | 9.4 s to applied LSN; `qs-verify` MATCH on orders and customers afterwards | 883 MB | at the limit (1,017,565,184 bytes at the end of the drain) | 34,051 | 0 |
| idle rewrite, 10.155M base + 700k delta | yes | 44.2 s | 883 MB (no new peak) | at the limit | 122,969 | 0 |

At the drain's end, `memory.stat` had 536 MB of RSS and 465 MB of cache. Every phase completed and no
process was killed, but the limit was reached in every phase, and the kernel did the work by
reclaiming cache, not by refusing memory. One run each. The unconstrained run's drain peak
(1.57 GB) is larger than the constrained one's (883 MB), and the difference is not explained; the
unconstrained drain followed a sync in the same process, the constrained one a fresh sync, so the
two are not the same experiment.

### Findings, including failures

1. **A column named `at` is not routed.** `WHERE at >= …` on `fv.events`: the mirror's SQL is not
   quoted, DuckDB reads `at` as a keyword: `Parser Error: syntax error at or near ">="`. The heap
   answers, so the result is correct and the query is not faster (`b_routing_standby.txt`).
2. **Text `ORDER BY` needs the C collation to route.** `ORDER BY status COLLATE "C"` is refused: `node
   CollateExpr` is not on the allowlist. `GROUP BY` without `ORDER BY` routes. An ICU database
   (the default of `setup.sh`) does not route text ordering at all.
3. **`date_trunc(text, timestamptz)` is not on the allowlist.** The revenue-by-month join is heap in
   every state. *Superseded 2026-11-27 (§0.1): `date_trunc` is routed for a constant unit in a session
   zone the extension has proven (240 of 499 zones checked, 40 of them for `hour`), and the join is
   routed in UTC and America/New_York, with identical answers.*
4. **After a promotion the old sidecar stands down but does not exit.** Its log says `this node is the
   primary — standing down` and `dropped this Pod's replication slot`. Its process kept running, and its
   `/readyz` stayed `ready: true` with `applied_lsn` frozen and `lag_seconds` rising. It was stopped by
   PID.
5. **A new standby copies `duckdb.allowed_directories` from the primary.** The promoted server's
   setting named the old mirror directory, so the new sidecar's lake could not be attached and every
   aggregate fell back to the heap (`lake.fv.events … schema "lake.fv" does not exist`). The answers
   stayed equal, the routing did not. Adding the new mirror directory to the standby's
   `duckdb.allowed_directories` and reloading fixed it.
6. **On standby 5803, the lake attach failed later and was not isolated.** After scenario k, every
   aggregate fell back to the heap; `quicksilver.attach_lakes()` returned -1 in fresh sessions, while
   `duckdb.raw_query` of the same `ATTACH` succeeded in a psql session. Not resolved in this run.
7. **Two sidecars with different table lists on one primary fight over the registry.** The sidecar with
   `fv.lines` logged `registry brought to the mirrored tables … created=[fv.lines]` 1,140 times in about
   20 minutes; the sidecar with the shorter table list logged `dropped=[fv.lines]` 2,747 times; `deadlock detected` appeared 46 and 24 times (`out/sidecar_registry_contention_*`).
   In that state a routed join answered `('0', 'None')` where the heap answered `('232112', '928463')`.
   On a fresh mirror directory with one sidecar on the cluster, the same join was `EQUAL` (scenario l).
   The wrong answer is not explained. It is the one correctness observation of this run that is not
   closed, and it is recorded here rather than dropped.
8. **`/readyz` is HTTP 200 with a halted table** because the harness sets `QS_SERVE_WITH_HALTED_TABLES=on`
   (the documented behaviour for that setting; the default is 503).
9. **A check run before settling can see rows the mirror has not applied yet.** `qs-verify` run straight
   after a restart reported `missing from mirror` for the replayed rows (scenario c's first check,
   `c_qs_verify.txt`; scenario m's first attempt, where the settle step also timed out with an empty
   applied LSN). A run once the mirror reported its applied LSN was `MATCH` for all tables. `check_all.sh`
   now settles first.
10. **Harness faults, fixed in the scripts, not the product:** a key collision between writer runs, a
    numeric overflow in a workload update, an `int4` overflow in a key expression, a `%` placeholder
    error in psycopg, a DuckDB reserved word in a checksum, and a session `SET` left on in a transaction
    check. Attempt logs are kept as `*_attempt*` files in `out/`.

### What these numbers do not say

- **One machine, 4 vCPU, with a second Quicksilver stack running on it.** The load was never below 2 during
  the timed phases, so absolute milliseconds are not comparable to a quiet host. Ratios within one table
  are the better guide.
- **One run each** for almost every figure. The best/median columns are over five warm runs of one
  sidecar state; no state was repeated from a fresh start except the Part 4 phases.
- **Synthetic data.** The orders are random within the `load.sql` distributions; the join keys are md5 of
  a counter. The change workload is a fixed script, not a production mix.
- **Not on Kubernetes.** No CloudNativePG, no pod limits other than the cgroup, no network hop between
  primary, standby and sidecar.
- **Warm cache only** for the six-query suite. The cold rows of the earlier results are not repeated.
- **Routing covers only what the allowlist names.** Two of the six queries (the join with `date_trunc`, the
  point lookup by design) are heap in the routed engine (the join is routed in proven zones since 2026-11-27,
  §0.1); a DuckDB-native reader is faster than the heap on
  the aggregates and slower on point lookups (24 ms against 0.1 ms), so the routed numbers and the
  DuckDB numbers answer different questions.
- **The mirror is slower than the heap for key lookups, by two orders of magnitude when DuckDB reads it
  directly** (24 ms against 0.1 ms). The routed engine avoids this by staying on the heap.
- **Commit-to-visible is measured through the applied LSN**, which is the published position, and the
  8 KB tolerance is the one approximation in that measurement.
- **The 1 GiB limit was reached in every Part 4 phase**; a larger working set, or a table with wider rows,
  would hit it sooner. Page cache counts toward the cgroup's usage.
- **The initial sync time is "start to ready"**, and readiness is judged on the lag inside the SLO, not on
  byte-level catch-up.
- **Part 1 scenario i's second half, and the join-domain wrong answer of finding 7, are open.**


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

`QS_COMPACT_DEAD_FRACTION` adds a second compaction trigger. It shipped **off**,
and [docs/32](docs/32-the-knob-that-barely-fires.md) is the story of finding out
why that was the right default for the value being tested (0.01).

It now defaults to **0.2** — the same fifth of the base, above the same 25,000-row
floor, that the churn trigger uses — so a workload that updates sees no change in
schedule (churn is tested first and reaches a fifth at the same moment) and a
delete-only workload, which had no trigger at all, gets one at the price already
accepted for everything else. On the `purge` table below that is one rewrite at
80,000 dead instead of none.

What bounds how LONG a base carries a vector is a third trigger, `why=idle`:
a table that has not been written for `QS_COMPACT_IDLE_SECONDS` (default 30,
0 = off) and whose base has a deletion vector is rewritten, and idle rewrites
are spaced by `QS_COMPACT_IDLE_BACKOFF` (default 10) times how long the last
rewrite took. Triggers are evaluated on idle ticks as well as applying ones;
before, a mirror that stopped receiving writes also stopped asking. Measured on
a 1M-row table after an 8% update/delete burst (`count(*)` through the view,
one thread): 1.3 ms fresh, 54.8 ms churned, 1.0 ms after the idle rewrite,
which started 30 s after the last write and took 4.4 s.

The 0.01 A/B, for the record:

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
