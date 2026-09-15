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

**Status:** prototype, not upstreamed. Intended as the basis for an upstream PR — see
[docs/12](../../docs/12-s5-serving-path.md) for what would need adding first.
