# syntax=docker/dockerfile:1

# ---- build stage ------------------------------------------------------------
FROM golang:1.25.7-alpine AS build

WORKDIR /src

# Cache modules separately from source so dependency layers stay warm.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

# CGO_ENABLED=0 is load-bearing, not hygiene: the default toolchain links the
# net resolver against libc, and that dynamic binary cannot start on
# distroless/static. Forcing it off yields a fully static binary.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" -o /out/broker ./cmd/broker

# Materialize the WAL dir here so we can hand it to the runtime stage already
# owned by the nonroot uid. distroless has no shell, so chown can't happen there.
RUN mkdir -p /data

# ---- runtime stage ----------------------------------------------------------
# static:nonroot ships ca-certificates + tzdata + a uid 65532 nonroot user,
# and runs a static binary with no libc. See ADR for why over scratch.
FROM gcr.io/distroless/static:nonroot AS runtime

# Bring the WAL dir over already owned by nonroot (uid/gid 65532) so wal.Open
# can create hermes.wal. WORKDIR alone would leave it root-owned and unwritable.
COPY --from=build --chown=65532:65532 /data /data
WORKDIR /data
ENV HERMES_WAL_PATH=/data/hermes.wal

COPY --from=build /out/broker /usr/local/bin/broker

EXPOSE 8080
USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/broker"]
