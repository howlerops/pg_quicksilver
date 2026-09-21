# Two binaries, one build, two images selected by --target.
#
#   docker build --target plugin -t ghcr.io/howlerops/pg_quicksilver-plugin:dev .
#   docker build --target mirror -t ghcr.io/howlerops/pg_quicksilver-mirror:dev .
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
