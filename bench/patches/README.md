# Patches

## `pg_duckdb-allowed-directories.patch`

Against `pg_duckdb` **v1.1.1**. Adds a `duckdb.allowed_directories` GUC that *confines*
unprivileged backends to named directories instead of disabling the local filesystem
outright.

**Why:** [docs/11 Result 7](../../docs/11-measured-results.md) found that stock `pg_duckdb`
gates local file reads on `AllowRawFileAccess()` — membership of **both**
`pg_read_server_files` and `pg_write_server_files`. That is all-or-nothing: the only way to
let an application role read a Parquet mirror is to also let it `COPY FROM '/etc/passwd'`
and write arbitrary server files. A superuser-owned view does not help, because the check
runs against the *current* user at plan time. That blocked Quicksilver's entire serving path.

**What it does:** when `duckdb.allowed_directories` is set, an unprivileged backend keeps
`LocalFileSystem` but DuckDB is configured with `allowed_directories`, then
`enable_external_access=false`, then `lock_configuration=true`.

**The non-obvious part** — DuckDB's `CanAccessFile()` begins:

```cpp
if (options.enable_external_access) {
    return true;   // allowlist never consulted
}
```

So `enable_external_access=false` *switches the allowlist on* rather than denying
everything, precisely because `allowed_directories` is non-empty. Setting one without the
other is useless in both directions: allowlist alone is ignored, and `false` alone blocks
the mirror too. Both orderings were measured; see the commit history.

**Two implementation constraints, both learned by failing first:**

1. `lock_configuration=true` must be applied **last** — after extension loading and secret
   setup, since nothing can be `SET` afterwards.
2. It must run **once per DuckDB instance**. `RefreshConnectionState()` runs on every query
   against a cached instance, so the second call tries to `SET allowed_directories` against
   its own lock and throws *"Cannot change configuration option … the configuration has been
   locked"*. Guarded with a `filesystem_confined` member, mirroring `secrets_valid`.

**Verified by** `bench/scripts/run_s5_confinement.py` — mirror readable, everything outside
the directory denied (including path traversal and HTTP), confinement not widenable.
No measurable query-performance cost.

**How to build it.** The recipe lives in the `serving` job of
[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml), because a recipe that only
exists in prose is a recipe nobody can check:

```sh
git clone --depth 1 --branch v1.1.1 --recurse-submodules --shallow-submodules \
  https://github.com/duckdb/pg_duckdb /tmp/pg_duckdb
cd /tmp/pg_duckdb
git apply /path/to/pg_duckdb-allowed-directories.patch
PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config make -j"$(nproc)"
sudo PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config make install
```

It compiles DuckDB (v1.4.3, the bundled submodule) as well, so it is about an hour the
first time. CI caches the result on this patch's hash: change the patch and it rebuilds,
change anything else and it does not.

`bench/scripts/serving_pg17.sh` checks for the **GUC**, not the file — `grep -a
duckdb.allowed_directories` over `pg_duckdb.so`. A stock build would pass a file check and
then fail much later as "permission denied" on the mirror, which reads exactly like the bug
this patch exists to fix.

**Status:** prototype, not upstreamed. Intended as the basis for an upstream PR — see
[docs/12](../../docs/12-s5-serving-path.md) for what would need adding first.
