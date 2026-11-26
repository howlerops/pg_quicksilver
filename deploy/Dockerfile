# Two binaries, one build, two images selected by --target.
#
#   docker build --target plugin -t ghcr.io/howlerops/pg_quicksilver-plugin:dev .
#   docker build --target mirror -t ghcr.io/howlerops/pg_quicksilver-mirror:dev .
#   docker build --target postgres -t <registry>/postgresql-quicksilver:17 .   (optional; see the end)
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
# lets docs/07 hold: the CNPG PostgreSQL image stays the stock upstream one, and
# nothing here forks it.

# For the `postgres` target at the end of this file.
ARG PG_MAJOR=17
ARG CNPG_BASE=ghcr.io/cloudnative-pg/postgresql:17-standard-bookworm

FROM docker.io/library/golang:1.27 AS build
WORKDIR /src
COPY go/go.mod go/go.sum ./
RUN go mod download
COPY go/ ./
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X github.com/howlerops/pg_quicksilver/go/internal/plugin.Version=${VERSION}" \
      -o /out/quicksilver-plugin ./cmd/quicksilver-plugin \
 && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/qs-mirror  ./cmd/qs-mirror \
 && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/qs-verify  ./cmd/qs-verify

# The plugin never touches PostgreSQL or the filesystem; distroless static is
# the whole of what it needs.
FROM gcr.io/distroless/static-debian12:nonroot AS plugin
COPY --from=build /out/quicksilver-plugin /quicksilver-plugin
USER 65532:65532
ENTRYPOINT ["/quicksilver-plugin"]

# The sidecar runs in the instance Pod and writes to the data volume, so it runs
# as the same UID the CNPG images use for postgres (26). Running as anyone else
# produces files the instance cannot read and a mirror nobody can serve.
FROM gcr.io/distroless/static-debian12 AS mirror
COPY --from=build /out/qs-mirror /qs-mirror
COPY --from=build /out/qs-verify /qs-verify
USER 26:26
# 9188, not 9187: CNPG's instance manager binds 9187 in the same network
# namespace, and the sidecar starting first would stop PostgreSQL booting.
EXPOSE 9188
ENTRYPOINT ["/qs-mirror"]

# ---------------------------------------------------------------------------
# The PostgreSQL image, for clusters that route reads with pg_quicksilver.
#
#   docker build --target postgres -t registry.example/postgresql-quicksilver:17 .
#
# Everything above still holds: the sidecar is not a PostgreSQL image and the
# mirror alone needs nothing in the server. Routing does. It runs inside
# PostgreSQL, so the image the Cluster's imageName points at has to carry
#
#   pg_duckdb        the engine that reads the mirror (1.2.0 is what was verified)
#   pg_quicksilver   ext/pg_quicksilver, built here against the same major
#
# and the Cluster has to load them, in this order:
#
#   postgresql:
#     shared_preload_libraries: ["pg_duckdb", "pg_quicksilver"]
#
# docs/16 has the rest (CREATE EXTENSION, the sidecar's ducklake parameter).
#
# pg_duckdb is installed from a package, not built: its build is DuckDB's, an
# hour of C++, and the package is what docs/40 was measured on. PG_DUCKDB_DEB
# names it; the default is the package name in the repository this project's
# validation hosts use, and an image built elsewhere points it at whatever
# carries pg_duckdb 1.2.0 for its distribution (a .deb URL works too).
# (PG_MAJOR and CNPG_BASE are declared at the top of this file: an ARG a FROM
# uses has to come before the first FROM.)
FROM ${CNPG_BASE} AS extension-build
ARG PG_MAJOR
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential postgresql-server-dev-${PG_MAJOR} \
 && rm -rf /var/lib/apt/lists/*
COPY ext/pg_quicksilver/ /src/pg_quicksilver/
# with_llvm=no: no bitcode, so no clang in the build stage; the JIT does not
# inline into an extension it has no bitcode for and nothing else changes.
RUN make -C /src/pg_quicksilver PG_CONFIG=/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config with_llvm=no \
 && make -C /src/pg_quicksilver PG_CONFIG=/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config with_llvm=no \
      DESTDIR=/out install

FROM ${CNPG_BASE} AS postgres
ARG PG_MAJOR
ARG PG_DUCKDB_DEB=postgresql-17-pg-duckdb
USER root
RUN apt-get update && apt-get install -y --no-install-recommends ${PG_DUCKDB_DEB} \
 && rm -rf /var/lib/apt/lists/*
COPY --from=extension-build /out/usr/lib/postgresql/${PG_MAJOR}/lib/pg_quicksilver.so \
     /usr/lib/postgresql/${PG_MAJOR}/lib/
COPY --from=extension-build /out/usr/share/postgresql/${PG_MAJOR}/extension/pg_quicksilver* \
     /usr/share/postgresql/${PG_MAJOR}/extension/
# The postgres UID of the CNPG images, as for the sidecar: do not invent one.
USER 26
