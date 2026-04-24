# syntax=docker/dockerfile:1.7

# Multi-stage build for the otelcol-bazel custom OpenTelemetry Collector.
#
# Stage 1 (builder): run OpenTelemetry Collector Builder (OCB) to generate and
# compile a minimal collector distribution. OCB pulls the besreceiver module at
# the version pinned in builder-config.yaml — the Dockerfile build context does
# NOT need the receiver source checked out.
#
# Stage 2 (runtime): copy only the resulting static binary and default config
# into a distroless image. Runs as nonroot by default.
#
# The four versions that MUST stay in sync (see README.md):
#   - OCB_VERSION                                       (this file)
#   - go.opentelemetry.io/collector/*         stable    (builder-config.yaml)
#   - go.opentelemetry.io/collector/*         beta      (builder-config.yaml)
#   - github.com/chagui/besreceiver                     (builder-config.yaml)
#   - open-telemetry/opentelemetry-collector-contrib/*  (builder-config.yaml)

ARG GO_VERSION=1.26
ARG OCB_VERSION=v0.146.1
ARG DISTROLESS_TAG=nonroot

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION} AS builder

ARG OCB_VERSION
ARG TARGETOS
ARG TARGETARCH

# Build-time Go env overrides. Defaults are safe for public modules indexed
# by the Go checksum database. Override `GOPRIVATE` when the pinned
# besreceiver tag is not yet indexed by sum.golang.org (common when a tag
# was just pushed), or when building against a private fork. Setting
# `GOPRIVATE` implicitly disables GOSUMDB lookups for matched paths.
# Example:
#   docker build --build-arg GOPRIVATE=github.com/chagui/* ...
ARG GOPROXY=https://proxy.golang.org,direct
ARG GOPRIVATE=

# OCB emits a Go module, runs `go build`, and honors GOOS/GOARCH so the
# resulting binary matches the image target platform (enables multi-arch
# builds via `docker buildx build --platform linux/amd64,linux/arm64`).
ENV CGO_ENABLED=0 \
    GOOS=${TARGETOS} \
    GOARCH=${TARGETARCH} \
    GOPROXY=${GOPROXY} \
    GOPRIVATE=${GOPRIVATE}

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go install "go.opentelemetry.io/collector/cmd/builder@${OCB_VERSION}"

WORKDIR /build

COPY builder-config.yaml ./builder-config.yaml

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    builder --config builder-config.yaml

FROM gcr.io/distroless/static-debian12:${DISTROLESS_TAG}

LABEL org.opencontainers.image.title="otelcol-bazel" \
      org.opencontainers.image.description="OpenTelemetry Collector with besreceiver for Bazel Build Event Protocol streams" \
      org.opencontainers.image.source="https://github.com/chagui/besreceiver" \
      org.opencontainers.image.licenses="Apache-2.0"

# distroless/static-debian12:nonroot runs as uid/gid 65532.
COPY --from=builder /build/otelcol-bazel/otelcol-bazel /otelcol-bazel
COPY collector-config.yaml /etc/otelcol-bazel/config.yaml

# BES gRPC ingress (8082) is the only port intended to be reached from outside
# the pod. Health check (13133) and internal prometheus (8888) bind to
# loopback in the default config — they are documented here but not EXPOSEd.
EXPOSE 8082

ENTRYPOINT ["/otelcol-bazel"]
# Default config can be overridden at runtime by bind-mounting a replacement
# at /etc/otelcol-bazel/config.yaml OR by passing `--config` as a command arg.
CMD ["--config=/etc/otelcol-bazel/config.yaml"]
