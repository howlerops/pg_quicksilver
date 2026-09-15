# 12 — S5: the serving path

**Status: ✅ resolved by a 58-line patch, prototyped and verified. Not upstreamed.**

[Result 7](11-measured-results.md#result-7--s3-rls-holds-but-ordinary-roles-cannot-reach-the-mirror-at-all)
left Architecture A blocked from both ends:

- `pg_duckdb` over the **Postgres heap** is 1.27× *slower* than plain Postgres → the safe
  path is not fast.
- The **Parquet** path is fast but only reachable by roles holding
  `pg_read_server_files` + `pg_write_server_files` → the fast path is not safe.

S5 asked whether that gap can be closed without forking `pg_duckdb` or owning a whole
columnar access path. **It can.**

---

## The fix

A `duckdb.allowed_directories` GUC (`PGC_POSTMASTER`). When set, an unprivileged backend
keeps `LocalFileSystem` but is confined to the named directories, and the configuration is
locked so it cannot be widened from SQL.

```
# postgresql.conf
duckdb.postgres_role       = 'duckdb_users'
duckdb.allowed_directories = '/var/lib/quicksilver/mirror'
```

**58 lines across 4 files.** Full diff and rationale in
[`bench/patches/`](../bench/patches/).

### Why it is this small

The primitive already exists. The bundled DuckDB supports `allowed_directories`,
`allowed_paths` and `lock_configuration`; `pg_duckdb` simply never wires them up, choosing
the blunt `disabled_filesystems = 'LocalFileSystem'` instead. The patch only has to select
the finer-grained mechanism that was already there.

### The trap in the middle

DuckDB's `CanAccessFile()` opens with:

```cpp
if (options.enable_external_access) {
    return true;   // the allowlist is never consulted
}
```

So `enable_external_access=false` **switches the allowlist on** rather than denying
everything — precisely because `allowed_directories` is non-empty. Either setting alone is
useless: the allowlist by itself is ignored, and `false` by itself blocks the mirror too.
Both failure modes were hit before the working combination.

Two further constraints, both found by failing:

1. `lock_configuration=true` must come **last** — after extension loading and secret setup,
   because nothing can be `SET` once it holds.
2. Confinement must run **once per DuckDB instance**. `RefreshConnectionState()` runs on
   every query against a cached instance; the second call otherwise tries to `SET
   allowed_directories` against its own lock and throws.

---

## Verification

`bench/scripts/run_s5_confinement.py`, as a role in `duckdb_users` **and not** in
`pg_read_server_files`:

| | Result |
|---|---|
| Mirror view, aggregate, repeat query, direct `read_parquet` in the allowed dir | ✅ allowed |
| `/etc/passwd`, `pg_hba.conf`, `PG_VERSION` | ✅ denied |
| Path traversal `<allowed>/../pgdata/PG_VERSION` | ✅ denied |
| HTTP exfiltration `read_csv('https://…')` | ✅ denied |
| Widening `duckdb.disabled_filesystems` / `allowed_directories` / `enable_external_access` | ✅ denied |
| Superuser still unrestricted (control) | ✅ allowed |

**No measurable performance cost.** Confined analytical queries ran at 0.72–0.79× of the
S2b superuser timings — that gain is cache warmth between runs, not the patch; the honest
reading is "no cost".

### A methodological note, again

The first version of this test "passed" its widening cases for the wrong reason:
`SELECT * FROM duckdb.query($$ SET … $$)` fails in DuckDB's *parser* ("Expected a single
SELECT statement") and never reaches the lock. That is the second time in this project a
security test went green vacuously — the same trap as
[S3 Phase 1](11-measured-results.md#result-7--s3-rls-holds-but-ordinary-roles-cannot-reach-the-mirror-at-all).
The committed test now uses only vectors a restricted role can actually reach — the Postgres
GUCs, all `PGC_SUSET` or `PGC_POSTMASTER` — and carries a comment explaining why the obvious
test is worthless.

**Standing lesson for this project: on any security test, a clean sweep is a reason to
check the error messages, not to celebrate.**

---

## One thing to investigate before shipping

DuckDB auto-appends its own temp directory to the allowlist. Reading the setting back:

```
allowed_directories = [/var/lib/postgresql/qsbench/parquet/,
                       /var/lib/postgresql/qsbench/pgdata/pg_duckdb/temp/]
```

That is necessary — DuckDB spills there — but it means a confined role can read DuckDB's
spill files. If spilled data from another session's query can persist there, it is a
cross-tenant information-disclosure path. **Open question OQ-16:** are spill files
per-backend, mode-0600, and removed on completion? If not, the mirror directory and the temp
directory need separate treatment. This does not affect the patch's validity but must be
answered before anything ships.

---

## What this changes

| | Before S5 | After S5 |
|---|---|---|
| Architecture A serving path | **Blocked** | Viable with a 58-line patch |
| Scope | Quicksilver must own a columnar access path (TAM/FDW) | Not required for v1 |
| Risk R17 | Blocking | Mitigated, pending upstream |
| Dependency posture | — | Carries a patched `pg_duckdb` until upstreamed |

The fallback options from Result 7 — writing a table access method or an FDW — are no longer
on the v1 critical path. That is a substantial scope reduction.

### Upstreaming

This should go upstream rather than live as a fork. It is small, it uses DuckDB primitives
already present, and the security argument is easy to state: today's choice is between
"no local files" and "arbitrary server files", and a directory allowlist is the obvious
middle. Before opening a PR it needs: regression tests in `pg_duckdb`'s own suite,
documentation, multi-directory parsing (the prototype handles a comma-separated list but is
only tested with one entry), and a decision on the temp-directory question above.

Until then, Quicksilver ships a patched `pg_duckdb` in its image
([docs/07](07-image-strategy.md)) — which the derived-image strategy already accommodates,
though it does add a maintenance obligation that a pure upstream dependency would not.
