# Patches

## pg_duckdb 1.2.2

`pg_duckdb-1.2.2-allowed-directories.patch` is a `git diff` against the `v1.2.2` tag
of [duckdb/pg_duckdb](https://github.com/duckdb/pg_duckdb). It lets a role that is
**not** a member of both `pg_read_server_files` and `pg_write_server_files` read local
files, and only those under `duckdb.allowed_directories`. Built and tested as described
in the verification section below.

### Why the stock build blocks an unprivileged role

Source: pg_duckdb `v1.2.2` (`4e921ff`), whose DuckDB submodule is `8d78114`
(DuckDB 2.0, reported as `v2.0.0-1168-g8d78114e90a`).

1. `AllowRawFileAccess()` (`src/pg/permissions.cpp:13`) is true only when the current
   user is a member of **both** `pg_write_server_files` and `pg_read_server_files`.
2. `DisabledFileSystems()` (`src/pgduckdb_duckdb.cpp:321`) returns `LocalFileSystem`
   for every other user. `DuckDBManager::RefreshConnectionState()` (line 343) runs
   `SET disabled_filesystems='LocalFileSystem'` on the DuckDB instance before each
   query (line 357). This is the setting that flips.
3. DuckDB's virtual file system then refuses every local path, whatever else is set:
   `File system LocalFileSystem has been disabled by configuration`
   (`third_party/duckdb/src/common/virtual_file_system.cpp:483`).

`duckdb.allowed_directories` takes no part in step 3. pg_duckdb only applies it once,
when the DuckDB instance is created (`SET allowed_directories=[...]`, around line 260),
and DuckDB checks it only while `enable_external_access` is off. The file system has
already been refused by then. So the stock setting is inert for an unprivileged role,
and the only way round it is to grant both `pg_*_server_files` roles.

### What DuckDB 2.0.1 gives us natively

Measured with the python `duckdb` 2.0.1 wheel, with
`allowed_directories=[<dir>/]`, `enable_external_access=false` and
`lock_configuration=true`, in that order (`bench/patches/README.md` records the
results; the probe was run from a scratch directory):

| case | result |
|---|---|
| read a file inside the directory | allowed |
| `/etc/passwd` | denied: `Cannot access file ... file system operations are disabled by configuration` |
| `dir/../outside/file` | denied (paths are canonicalised first) |
| symlink inside the directory pointing outside | denied |
| `https://...` | denied |
| `ATTACH` a database file outside | denied |
| `COPY ... TO` a file outside | denied |
| `COPY ... TO` a file **inside** the directory | **allowed** |
| `SET allowed_directories`, `SET enable_external_access=true`, `SET lock_configuration=false` | denied: `the configuration has been locked` |

So DuckDB enforces every read confinement the goal needs, but **not** write confinement:
`CanAccessFile()` (`src/main/config.cpp:1132`) does not look at the open mode, and
`access_mode=read_only` does not stop a `COPY ... TO` of an external file.

### What the patch does

- `DisabledFileSystems()` no longer adds `LocalFileSystem` for a role that lacks both
  roles when `duckdb.allowed_directories` is set. That role keeps the local file system.
- The reads are confined by DuckDB itself. `RefreshConnectionState()` runs
  `SET enable_external_access=false`, then `SET lock_configuration=true`, which makes
  every later `SET` fail. Nothing in the role's session can widen the allowlist, turn
  external access back on, or unlock.
- Writes are refused by `src/pgduckdb_confined_fs.cpp`, a `LocalFileSystem` subclass
  registered as a DuckDB sub-system before the CA certificate file system. It keeps the
  name `LocalFileSystem`, so `duckdb.disabled_filesystems` still disables it. Once the
  instance is confined it throws on every open for writing, memory mapping for writing,
  create, remove and move. Reads pass through unchanged.
- `duckdb.allowed_directories` stays `PGC_SUSET`, which is what this needs: a role that
  is not a superuser cannot `SET` it, and nothing it can do changes it afterwards.

Three consequences, all deliberate:

- Confinement is per DuckDB instance, and an instance is per backend. A backend that
  first confines a role keeps that confinement, so a superuser who later runs in the
  same backend gets the locked, confined instance too. Fail closed.
- Once confined, the backend no longer refreshes its DuckDB configuration. A new
  extension installed later is not loaded into an already-confined backend, and
  `duckdb.disabled_filesystems` is not reapplied. A new connection picks both up.
- A confined role cannot write temporary files either, so a query that has to spill to
  disk fails rather than writing one.

### Verification

See the section of `bench/scripts/serving_pg17.sh` for the denial list, and the commit
that adds this section for the results on the patched build and on the stock package.

---

## Is the patch still needed on pg_duckdb 1.2.2?

Yes, and it is now the 1.2.2 patch above. The stock 1.2.2 package keeps the local
file system disabled for any role that lacks both server-file roles, so the mirror
cannot be read without them. The v1.1.1 patch that stood here before is retired: the
`serving` job builds the 1.2.2 patch instead, and so does the `postgres` image target
when it is asked to.
