# Generic OTLP

Minimal collector that forwards Bazel BEP traces, metrics, and logs to any
OTLP-compatible gRPC endpoint. Use this as the starting point for backends
without a dedicated example — Honeycomb, New Relic, Uptrace, Signoz, a shared
internal gateway, etc.

## Prerequisites

- An OTLP gRPC endpoint reachable from the collector container
- Docker and Docker Compose
- `otelcol-bazel:latest` image built locally (`make docker` from the repo root)

## Quick start

1. Copy the env template and point at your OTLP endpoint:

   ```bash
   cp .env.example .env
   # edit .env: OTLP_ENDPOINT=your.backend:4317, OTLP_INSECURE=false for TLS
   ```

2. Start the collector:

   ```bash
   docker compose up -d
   ```

3. Point Bazel at the local collector:

   ```bash
   bazel build //... \
     --bes_backend=grpc://localhost:8082 \
     --build_event_publish_all_actions
   ```

## Adding auth headers

Most hosted OTLP backends expect a bearer token or per-vendor header. Uncomment
the `headers:` block in `collector-config.yaml` and set values from environment
variables. Example for Honeycomb:

```yaml
exporters:
  otlp:
    endpoint: ${env:OTLP_ENDPOINT}
    tls:
      insecure: false
    headers:
      x-honeycomb-team: ${env:HONEYCOMB_API_KEY}
      x-honeycomb-dataset: bazel-builds
```

For backends that use OAuth2 bearer tokens, uncomment the
`bearertokenauth/otlp` extension and the matching `auth.authenticator`
reference on the exporter.

## Verification

```bash
# Collector is healthy
curl -fsS http://localhost:13133

# Export counters climb after a Bazel build
curl -s http://localhost:8888/metrics | grep otelcol_exporter_sent

# Watch for export errors (TLS / auth / endpoint unreachable)
docker compose logs -f otelcol-bazel
```

## Teardown

```bash
docker compose down
```

## Files

| File | Purpose |
|---|---|
| `collector-config.yaml` | BES receiver → generic OTLP gRPC exporter |
| `.env.example` | `OTLP_ENDPOINT` + `OTLP_INSECURE` template |
| `docker-compose.yaml` | Runs `otelcol-bazel:latest` with `.env` loaded |
