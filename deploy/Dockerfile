# One build, three images, selected by --target.
#
#   docker build --target plugin   -t ghcr.io/howlerops/pg_quicksilver-plugin:dev   -f deploy/Dockerfile .
#   docker build --target mirror   -t ghcr.io/howlerops/pg_quicksilver-mirror:dev   -f deploy/Dockerfile .
#   docker build --target postgres -t ghcr.io/howlerops/pg_quicksilver-postgres:17-dev -f deploy/Dockerfile .
#
# The operand image's tag has to START WITH THE POSTGRESQL MAJOR: CloudNativePG
# reads a Cluster's PostgreSQL version from the front of the image tag, takes
# "0.0.4" for PostgreSQL 0 and refuses "dev" outright.
#
# Base images are fully qualified on purpose. Docker silently resolves a bare
# "golang:1.27" against Docker Hub; podman and buildah refuse it outright unless
# the host has configured unqualified-search registries. Spelling the registry
# out is the only form that works everywhere, and it also removes a way for a
# build host's registry configuration to change which image you get.
#
# The mirror image is NOT a PostgreSQL image. It is a sidecar that shares the
# instance Pod's data volume and talks to PostgreSQL over the network and the
# local socket, so it needs no server binaries and no extensions. That is what
# lets docs/07 hold for the mirror alone: a Cluster that only mirrors keeps the
# stock CloudNativePG image. The `postgres` target at the end is for the one
# feature that does need something in the server: routing reads.
#
# Needs BuildKit (the default builder since Docker 23): the `postgres` target
# uses `ADD --checksum` and `RUN --network=none`.

# ---- what is pinned, in one place -----------------------------------------
# An ARG a FROM uses has to come before the first FROM.
#
# GO_IMAGE      the minor line go/go.mod asks for (`go 1.27`). GOTOOLCHAIN=local
#               below makes a go.mod that outgrows it a build error instead of
#               a toolchain downloaded mid-build.
# CNPG_BASE     the CloudNativePG operand image, by PostgreSQL MINOR, because
#               pg_quicksilver.so is compiled against that minor's headers and
#               the headers are installed at exactly the server's version.
#               17.12 is what docs/40 was validated on.
# PG_DUCKDB_*   the package, by URL and SHA-256. pg_duckdb publishes no
#               packages of its own (its releases are container images), and
#               building it is DuckDB's build: an hour of C++. This is the
#               Debian 12 build from the apt repository the project's
#               validation hosts use, which is where docs/40's 1.2.0 came from.
#               THAT REPOSITORY DROPS OLD VERSIONS: the 1.2.0 this was first
#               pinned to returned 404 within days, so the pin is 1.2.2, the
#               version it serves now, and it will break again the same way.
#               docs/40's measurements are on 1.2.0; the extension job in
#               ci.yml is what says whether a newer one still passes.
#               Fetched as one file with a checksum rather than by adding the
#               repository: nothing else from it is wanted, and a checksum is a
#               stronger pin than a signing key that can sign a newer version.
#               To move it: take Filename and SHA256 for postgresql-17-pg-duckdb
#               from <repo>/dists/bookworm/main/binary-amd64/Packages.
ARG PG_MAJOR=17
ARG GO_IMAGE=docker.io/library/golang:1.27
ARG CNPG_BASE=ghcr.io/cloudnative-pg/postgresql:17.12-standard-bookworm
ARG PG_DUCKDB_DEB_URL=https://repo.pigsty.io/apt/pgsql/bookworm/pool/main/p/pg-duckdb/postgresql-17-pg-duckdb_1.2.2-1PGSTY~bookworm_amd64.deb
ARG PG_DUCKDB_DEB_SHA256=29d3d9543f7766f87595441f215e8a11390a5bddf7f5950efca47e45f5d34a64
# PG_DUCKDB_BUILD selects where pg_duckdb comes from in the postgres target.
#
#   package (default)  the pinned .deb above: stock 1.2.2. An unprivileged role
#                      cannot read local files unless it also holds
#                      pg_write_server_files, so routed reads are superuser-only
#                      mirror reads. Fast: nothing is compiled.
#   source             pg_duckdb PG_DUCKDB_TAG, at PG_DUCKDB_COMMIT, with
#                      bench/patches/pg_duckdb-1.2.2-allowed-directories.patch, then
#                      bench/patches/pg_duckdb-1.2.2-duckdb-keywords.patch.
#                      An unprivileged role reads only duckdb.allowed_directories,
#                      cannot write, and cannot read anything else. About an hour
#                      of C++, and it clones from GitHub during the build.
#
# The build checks that the tag points at PG_DUCKDB_COMMIT, and the patch must
# apply to that commit, so changing either pin is a deliberate act.
ARG PG_DUCKDB_BUILD=package
ARG PG_DUCKDB_TAG=v1.2.2
ARG PG_DUCKDB_COMMIT=4e921ffbe4a16b3caec6cf24ca0151fa9d88032d

FROM ${GO_IMAGE} AS build
WORKDIR /src
# CGO off: the binaries go into distroless/static, which has no libc, and
# nothing in the module needs cgo (the DuckLake catalog is written with a
# pure-Go SQLite driver for exactly this reason).
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
COPY go/go.mod go/go.sum ./
RUN go mod download
COPY go/ ./
ARG VERSION=dev
# qs-verify and qs-query ship in the mirror image because an operator needs
# them INSIDE the Pod, next to the mirror's files: qs-verify answers "is the
# mirror telling the truth", qs-query prints the ATTACH a reader of the lake
# needs. The image has no shell, so `kubectl exec ... -- /qs-verify ...`.
RUN go build -trimpath \
      -ldflags "-s -w -X github.com/howlerops/pg_quicksilver/go/internal/plugin.Version=${VERSION}" \
      -o /out/quicksilver-plugin ./cmd/quicksilver-plugin \
 && go build -trimpath -ldflags "-s -w" -o /out/qs-mirror ./cmd/qs-mirror \
 && go build -trimpath -ldflags "-s -w" -o /out/qs-verify ./cmd/qs-verify \
 && go build -trimpath -ldflags "-s -w" -o /out/qs-query  ./cmd/qs-query

# The plugin never touches PostgreSQL or the filesystem; distroless static is
# the whole of what it needs.
FROM gcr.io/distroless/static-debian12:nonroot AS plugin
COPY --from=build /out/quicksilver-plugin /quicksilver-plugin
USER 65532:65532
ENTRYPOINT ["/quicksilver-plugin"]

# The sidecar runs in the instance Pod and writes to the data volume, so it runs
# as the same UID the CNPG images use for postgres (26). Running as anyone else
# produces files the instance cannot read and a mirror nobody can serve.
#
# distroless publishes no version tags, only :latest and :nonroot (the plugin's
# base above), so there is nothing more specific to name short of a digest.
# hadolint ignore=DL3007
FROM gcr.io/distroless/static-debian12:latest AS mirror
COPY --from=build /out/qs-mirror /qs-mirror
COPY --from=build /out/qs-verify /qs-verify
COPY --from=build /out/qs-query  /qs-query
USER 26:26
# 9188, not 9187: CNPG's instance manager binds 9187 in the same network
# namespace, and the sidecar starting first would stop PostgreSQL booting.
EXPOSE 9188
ENTRYPOINT ["/qs-mirror"]

# ---------------------------------------------------------------------------
# The PostgreSQL operand image, for Clusters that route reads with
# pg_quicksilver. Everything above still holds: the mirror alone needs nothing
# in the server. Routing runs inside PostgreSQL, so the image the Cluster's
# imageName points at has to carry
#
#   pg_duckdb        the engine that reads the mirror (1.2.2, DuckDB 2.0.1)
#   pg_quicksilver   ext/pg_quicksilver, built here against the same minor
#   ducklake, sqlite DuckDB's own extensions, which every backend that routes
#                    loads. pg_duckdb would otherwise fetch them from
#                    extensions.duckdb.org into $PGDATA on first use, and an
#                    instance with no route there never routes.
#
# and the Cluster has to say, in this order and with this directory:
#
#   postgresql:
#     shared_preload_libraries: ["pg_duckdb", "pg_quicksilver"]
#     parameters:
#       duckdb.extension_directory: /usr/lib/quicksilver/duckdb-extensions
#
# examples/cluster-routed.yaml is the whole Cluster; docs/16 has the rest.
#
# Nothing about CloudNativePG's contract changes: same base, same UID 26, same
# PATH, no ENTRYPOINT (the operator supplies the instance manager), and
# nothing added outside /usr, which a Pod with a read-only root filesystem
# reads like any other part of the image.

# A build stage: nothing from it but three files reaches an image, so it stays
# root, which apt and `make install` need.
FROM ${CNPG_BASE} AS extension-build
ARG PG_MAJOR
# hadolint ignore=DL3002
USER root
# The headers at exactly the version of the server in the base image, so the
# .so is built against what will load it. PGDG's main suite keeps the recent
# minors; its archive suite keeps all of them, and is added only when the main
# one no longer has this version, so a base pinned to an older minor still
# builds.
# hadolint ignore=DL3008
RUN set -eux; \
    . /etc/os-release; \
    v=$(dpkg-query -W -f='${Version}' "postgresql-${PG_MAJOR}"); \
    apt-get update; \
    if ! apt-get install -y --no-install-recommends build-essential "postgresql-server-dev-${PG_MAJOR}=${v}"; then \
      echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt-archive.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg-archive main" \
        > /etc/apt/sources.list.d/pgdg-archive.list; \
      apt-get update; \
      apt-get install -y --no-install-recommends build-essential "postgresql-server-dev-${PG_MAJOR}=${v}"; \
    fi; \
    rm -rf /var/lib/apt/lists/*
COPY ext/pg_quicksilver/ /src/pg_quicksilver/
# with_llvm=no: no bitcode, so no clang in the build stage; the JIT does not
# inline into an extension it has no bitcode for and nothing else changes.
RUN make -C /src/pg_quicksilver PG_CONFIG="/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config" with_llvm=no \
 && make -C /src/pg_quicksilver PG_CONFIG="/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config" with_llvm=no \
      DESTDIR=/out install

# pg_duckdb, from the package or from source, as PG_DUCKDB_BUILD says. Each
# stage is the operand base with pg_duckdb installed and nothing else changed.
FROM ${CNPG_BASE} AS pg-duckdb-package
ARG PG_DUCKDB_DEB_URL
ARG PG_DUCKDB_DEB_SHA256
USER root
# DL3020 wants COPY for files; this is a URL behind an ARG, which only ADD
# fetches, and --checksum is what makes fetching it safe.
# hadolint ignore=DL3020
ADD --checksum=sha256:${PG_DUCKDB_DEB_SHA256} ${PG_DUCKDB_DEB_URL} /tmp/pg_duckdb.deb
# The .deb depends on postgresql-17, which the base image has from PGDG, and on
# libraries Debian carries; apt resolves those, so the lists are needed once.
# hadolint ignore=DL3008
RUN apt-get update \
 && apt-get install -y --no-install-recommends /tmp/pg_duckdb.deb \
 && rm -rf /var/lib/apt/lists/* /tmp/pg_duckdb.deb
USER 26

# The source build, for PG_DUCKDB_BUILD=source. It compiles DuckDB as well, so it
# is about an hour on four cores. The headers come from the server's own minor,
# with the same archive fallback as extension-build.
FROM ${CNPG_BASE} AS duckdb-source-build
ARG PG_MAJOR
ARG PG_DUCKDB_TAG
ARG PG_DUCKDB_COMMIT
USER root
# hadolint ignore=DL3008
RUN set -eux; \
    . /etc/os-release; \
    v=$(dpkg-query -W -f='${Version}' "postgresql-${PG_MAJOR}"); \
    apt-get update; \
    if ! apt-get install -y --no-install-recommends build-essential ca-certificates cmake git ninja-build pkg-config \
           libcurl4-openssl-dev liblz4-dev libssl-dev zlib1g-dev "postgresql-server-dev-${PG_MAJOR}=${v}"; then \
      echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt-archive.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg-archive main" \
        > /etc/apt/sources.list.d/pgdg-archive.list; \
      apt-get update; \
      apt-get install -y --no-install-recommends build-essential ca-certificates cmake git ninja-build pkg-config \
           libcurl4-openssl-dev liblz4-dev libssl-dev zlib1g-dev "postgresql-server-dev-${PG_MAJOR}=${v}"; \
    fi; \
    rm -rf /var/lib/apt/lists/*
COPY bench/patches/pg_duckdb-1.2.2-allowed-directories.patch /src/pg_duckdb.patch
COPY bench/patches/pg_duckdb-1.2.2-duckdb-keywords.patch /src/pg_duckdb-keywords.patch
# The tag is cloned and then checked against the commit it names, so a moved tag
# fails the build instead of changing what is built. The submodule is shallow:
# the Makefile passes OVERRIDE_GIT_DESCRIBE, so DuckDB does not need the history.
# hadolint ignore=DL3003
RUN set -eux; \
    git clone --depth 1 --branch "${PG_DUCKDB_TAG}" --recurse-submodules --shallow-submodules \
      https://github.com/duckdb/pg_duckdb /src/pg_duckdb; \
    test "$(git -C /src/pg_duckdb rev-parse HEAD)" = "${PG_DUCKDB_COMMIT}"; \
    git -C /src/pg_duckdb apply /src/pg_duckdb.patch; \
    git -C /src/pg_duckdb apply /src/pg_duckdb-keywords.patch
WORKDIR /src/pg_duckdb
# `install: install-duckdb` puts libduckdb.so in place with a plain install(1),
# which does not create its directory under DESTDIR, so it is made first.
RUN set -eux; \
    pg=/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config; \
    mkdir -p "/out$($pg --pkglibdir)" "/out$($pg --sharedir)/extension"; \
    make -j"$(nproc)" PG_CONFIG="$pg"; \
    make install DESTDIR=/out PG_CONFIG="$pg"
# The same check as the serving job in ci.yml: the patch's own message, because
# stock 1.2.2 also has duckdb.allowed_directories. A build without the patch
# fails here, not later in a query.
RUN set -eux; \
    lib="/out$(/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config --pkglibdir)"; \
    grep -qa "writes to local files are disabled by configuration" "${lib}/pg_duckdb.so"
USER 26

FROM ${CNPG_BASE} AS pg-duckdb-source
USER root
# The runtime libraries the source build links against; the package pulls its own.
# hadolint ignore=DL3008
RUN apt-get update \
 && apt-get install -y --no-install-recommends libcurl4 libssl3 liblz4-1 zlib1g libstdc++6 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=duckdb-source-build /out/ /
USER 26

# The stage the server is built on, chosen by PG_DUCKDB_BUILD. It names a stage
# of this file, not an image, so the untagged-image rule does not apply.
# hadolint ignore=DL3006
FROM pg-duckdb-${PG_DUCKDB_BUILD} AS pg-duckdb

# The server with both extensions in it, and nothing else yet.
FROM pg-duckdb AS postgres-extensions
ARG PG_MAJOR
USER root
COPY --from=extension-build /out/usr/lib/postgresql/${PG_MAJOR}/lib/pg_quicksilver.so \
     /usr/lib/postgresql/${PG_MAJOR}/lib/
COPY --from=extension-build /out/usr/share/postgresql/${PG_MAJOR}/extension/pg_quicksilver* \
     /usr/share/postgresql/${PG_MAJOR}/extension/
COPY --chmod=0755 deploy/duckdb-extensions.sh /usr/local/bin/qs-duckdb-extensions
USER 26

# DuckDB's extensions, fetched by the DuckDB that will load them. This is the
# only stage of this target that needs the network after apt.
FROM postgres-extensions AS duckdb-extensions
ARG PG_MAJOR
USER root
RUN install -d -o 26 -g 26 /usr/lib/quicksilver/duckdb-extensions
USER 26
RUN PG_MAJOR="${PG_MAJOR}" qs-duckdb-extensions install /usr/lib/quicksilver/duckdb-extensions

FROM postgres-extensions AS postgres-assembled
# root-owned: PostgreSQL reads these and must never be able to replace them.
COPY --from=duckdb-extensions --chown=root:root \
     /usr/lib/quicksilver/duckdb-extensions /usr/lib/quicksilver/duckdb-extensions

# The proof, and the reason this is not just a claim in a README. With the
# network OFF and as the user PostgreSQL runs as: both libraries preload in the
# documented order, both extensions are created, the sidecar's own
# duckdb.install_extension succeeds, and a DuckLake on a SQLite catalog is
# written and read back. If any of that would have needed the network, or the
# .so does not load in this server, the image does not build.
FROM postgres-assembled AS postgres-verified
ARG PG_MAJOR
USER 26
RUN --network=none PG_MAJOR="${PG_MAJOR}" PRELOAD=pg_duckdb,pg_quicksilver \
    qs-duckdb-extensions verify /usr/lib/quicksilver/duckdb-extensions > /tmp/verified.txt \
 && cat /tmp/verified.txt

FROM postgres-assembled AS postgres
# Copying the transcript out of the verification stage is what makes that
# stage part of this target's build: a stage nothing depends on is skipped.
COPY --from=postgres-verified /tmp/verified.txt /usr/share/quicksilver/verified.txt
LABEL io.howlerops.quicksilver.shared-preload-libraries="pg_duckdb,pg_quicksilver" \
      io.howlerops.quicksilver.duckdb-extension-directory="/usr/lib/quicksilver/duckdb-extensions"
# The postgres UID of the CNPG images, as for the sidecar: do not invent one.
USER 26
