# 07 — Image strategy

The user asked whether we should build our own Postgres image on top of the public one.
**Yes — and it is a smaller commitment than it sounds, provided we stay derivative.**

---

## Why a custom image is required

Quicksilver nodes need things not in the stock CNPG image:

- `pg_duckdb` + the DuckDB runtime (query engine — [04](04-storage-and-query-engine.md))
- The `quicksilver` extension (routing GUCs, `applied_lsn()`, lag views; later the physical-WAL
  background worker — [03](03-wal-ingestion.md))
- The ingest binary (change stream → columnar writer)
- Supporting libraries for Parquet/Arrow

CNPG runs the image it is told to run, and instance pods can only load extensions the image
contains. There is no runtime-install escape hatch, and there shouldn't be — immutable images
are the point.

---

## Base: derive, never fork

Build **`FROM` the official CNPG PostgreSQL image**
(`ghcr.io/cloudnative-pg/postgresql:<major>`). Do not fork `postgres-containers`, and do not
build Postgres from source.

This matters more than it looks. The CNPG images carry a specific layout, entrypoint,
`barman-cloud` tooling, UID/GID conventions, and a security-patch cadence that CNPG's own
release process depends on. Deriving means a Postgres CVE is fixed by bumping a base tag;
forking means we own Postgres patch response forever. That is not a trade worth making for
three extensions.

```dockerfile
ARG PG_MAJOR=17
ARG CNPG_BASE=ghcr.io/cloudnative-pg/postgresql:${PG_MAJOR}

# ---- build stage -------------------------------------------------------
FROM ${CNPG_BASE} AS build
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential postgresql-server-dev-${PG_MAJOR} cmake git ninja-build \
 && rm -rf /var/lib/apt/lists/*

# pg_duckdb (MIT) — pinned to a release tag, never a branch
ARG PG_DUCKDB_REF=v0.4.0
RUN git clone --depth 1 --branch ${PG_DUCKDB_REF} --recurse-submodules \
      https://github.com/duckdb/pg_duckdb.git /src/pg_duckdb \
 && make -C /src/pg_duckdb -j"$(nproc)" install

# quicksilver extension
COPY ext/ /src/quicksilver/
RUN make -C /src/quicksilver -j"$(nproc)" install

# ---- runtime stage -----------------------------------------------------
FROM ${CNPG_BASE}
USER root
COPY --from=build /usr/lib/postgresql/${PG_MAJOR}/lib/     /usr/lib/postgresql/${PG_MAJOR}/lib/
COPY --from=build /usr/share/postgresql/${PG_MAJOR}/extension/ /usr/share/postgresql/${PG_MAJOR}/extension/
COPY --from=build /usr/local/bin/quicksilver-ingest        /usr/local/bin/
USER 26            # postgres uid in the CNPG images — do not invent one
```

Notes that will save time later:

- **Multi-stage is not optional.** Shipping `build-essential` and `postgresql-server-dev` in
  the runtime image roughly doubles its size and its CVE surface.
- **Pin every source ref to a tag**, never a branch. Reproducible builds are the difference
  between "the mirror broke" and "the mirror broke and we can't rebuild last week's image".
- **Do not change the entrypoint or the UID.** CNPG's instance manager owns process startup.
- Build for **`linux/amd64` and `linux/arm64`** — Graviton is common for database workloads
  and the DuckDB/Arrow stack is the part most likely to fight cross-compilation.

---

## Tagging and version coupling

Quicksilver is coupled to the Postgres major version in at least two ways (extension ABI
today, physical WAL record layouts in phase 3), so the major version must be in the tag:

```
ghcr.io/howlerops/pg_quicksilver:17-v0.1.0
ghcr.io/howlerops/pg_quicksilver:17-v0.1.0-<base-digest-prefix>   # fully pinned
ghcr.io/howlerops/pg_quicksilver:17                              # floating latest for major 17
```

Build a matrix across supported majors (start with 17; add 16 and 18 once the extension
builds cleanly on each). Publish an **SBOM** and **cosign signature** per image — several
CNPG adopters are in regulated environments where an unsigned database image is simply not
installable.

---

## Does the *whole cluster* need this image?

Two deployment shapes, and the answer differs. This is worth deciding early because it
changes the adoption story.

**Architecture A (phase 1) — mirror tier is separate.** Only the Quicksilver `StatefulSet`
needs the custom image. The CNPG `Cluster` keeps the stock image, possibly with
`wal_level=logical` set via the `Postgres.EnrichConfiguration` hook.

*Adoption cost: near zero.* Users install a plugin; their existing cluster image is
untouched. This is a significant and underrated advantage of shipping A first — nobody has
to re-image their production database to try it.

**Architecture C (phase 3) — mirror lives inside instance pods.** Now the instance pods *are*
Quicksilver nodes, so `spec.imageName` must point at our image for the whole cluster. That is
a much bigger ask: a full rolling restart of the production cluster onto a third-party image.

Mitigations worth designing for up front:
- Keep the image **strictly additive** — same Postgres, same patch level, same entrypoint,
  plus extensions. Then "switch to our image" is a low-risk rolling update rather than a
  migration.
- Consider publishing the Quicksilver extensions as a **CNPG image-volume / extension
  overlay** if CNPG's support for mounting extensions from a separate image is mature by
  then. That would remove the base-image swap entirely and is worth tracking — see
  **OQ-6** in [09](09-risks-and-open-questions.md).
- Make it possible to run the mirror tier as *dedicated* instances within the cluster
  (`instances` with a role/affinity distinction) so only those pods carry the extra
  footprint, even if all pods carry the image.

---

## Build and supply chain

| Concern | Approach |
|---|---|
| CI | GitHub Actions matrix over `{PG_MAJOR} × {amd64, arm64}`; build on tag and nightly against the floating base to catch upstream breakage early. |
| Base updates | Automated PR when the CNPG base digest changes; rebuild + full test suite. A Postgres CVE should be a merged PR, not a project. |
| Provenance | SBOM (syft), signature (cosign), SLSA provenance attestation. |
| Size | Target < 500 MB compressed. DuckDB + Arrow is the bulk; strip symbols in the runtime stage. |
| Testing | Every image runs the compatibility matrix from [06](06-compatibility-and-semantics.md) before publish. An image that builds but mis-maps `numeric` must not ship. |

---

## Recommendation

1. **Phase 1: build the derived image, but only the mirror tier uses it.** Keeps adoption
   friction at approximately zero while we prove the value proposition, and the image work is
   needed regardless.
2. **Keep it strictly additive from day one**, so the phase-3 requirement to run it
   cluster-wide is a rolling update rather than a migration — and so that the decision to do
   so can be made on evidence rather than forced by a fork we can't back out of.
3. **Never fork upstream Postgres or the CNPG base.** If we ever need a Postgres core patch,
   that is a signal to change the design, not the base image.
