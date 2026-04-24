# Grafana Tempo

Self-contained stack running Tempo (single-binary) behind Grafana with a
pre-provisioned Tempo datasource. Good for exploring trace data with TraceQL
and the service graph without signing up for a hosted backend.

## Prerequisites

- Docker and Docker Compose
- `otelcol-bazel:latest` image built locally (`make docker` from the repo root)
- A Bazel workspace to point at the BES endpoint

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

Open Grafana at <http://localhost:3000> (anonymous admin is enabled), pick the
Tempo datasource, and run a TraceQL query such as:

```traceql
{ name = "bazel.build" }
```

Individual target spans can be filtered by label, e.g.
`{ span.bazel.target.label =~ "//pkg/.*" }`.

## Verification

```bash
# Collector is healthy
curl -fsS http://localhost:13133

# Tempo ready (returns "ready")
curl -fsS http://localhost:3200/ready

# Grafana ping
curl -fsS http://localhost:3000/api/health
```

## Teardown

```bash
docker compose down -v   # -v also removes the tempo-data volume
```

## Ports

| Port | Service | Purpose |
|---|---|---|
| `8082` | otelcol-bazel | BES gRPC |
| `13133` | otelcol-bazel | health_check |
| `8888` | otelcol-bazel | Prometheus self-metrics |
| `3200` | tempo | Tempo HTTP query API |
| `3000` | grafana | Grafana UI |

## Files

| File | Purpose |
|---|---|
| `collector-config.yaml` | BES receiver → OTLP gRPC → Tempo |
| `tempo.yaml` | Tempo single-binary config (local disk storage, 1h retention) |
| `grafana/provisioning/datasources/tempo.yaml` | Pre-configured Tempo datasource |
| `docker-compose.yaml` | Tempo + Grafana + otelcol-bazel |
