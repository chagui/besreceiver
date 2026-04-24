# Jaeger

Self-contained stack that sends Bazel BEP events to a local Jaeger all-in-one.
Good for inspecting the span hierarchy produced by `besreceiver` without any
external accounts or network dependencies.

## Prerequisites

- Docker and Docker Compose
- `otelcol-bazel:latest` image built locally (`make docker` from the repo root)
- A Bazel workspace to point at the BES endpoint (see `examples/bazel-project/`)

## Quick start

```bash
docker compose up -d
```

Point Bazel at the collector:

```bash
bazel build //... \
  --bes_backend=grpc://localhost:8082 \
  --build_event_publish_all_actions
```

Open the Jaeger UI at <http://localhost:16686>, pick the `bes` service, and look
for `bazel.build` root spans.

## Verification

```bash
# Collector is healthy
curl -fsS http://localhost:13133

# BES gRPC port is listening
nc -z localhost 8082

# Collector self-metrics (span counts, queue size, etc.)
curl -s http://localhost:8888/metrics | grep otelcol_receiver_accepted_spans
```

## Teardown

```bash
docker compose down
```

## Ports

| Port | Service | Purpose |
|---|---|---|
| `8082` | otelcol-bazel | BES gRPC (Bazel clients connect here) |
| `13133` | otelcol-bazel | health_check extension |
| `8888` | otelcol-bazel | Prometheus self-metrics |
| `16686` | jaeger | Jaeger UI |
| `4317` | jaeger | OTLP gRPC (exposed for debugging; collector uses internal DNS) |
