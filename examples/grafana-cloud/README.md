# Grafana Cloud

Sends Bazel BEP traces, metrics, and logs to Grafana Cloud over OTLP/HTTP with
HTTP Basic authentication. No local Tempo/Mimir/Loki needed — you use the
hosted backends that come with your Grafana Cloud stack.

## Prerequisites

- A Grafana Cloud stack (the free tier is sufficient for evaluation)
- A Cloud Access Policy token with scopes `traces:write`, `metrics:write`, and
  `logs:write` — create one under **Administration → Access policies**
- Docker and Docker Compose
- `otelcol-bazel:latest` image built locally (`make docker` from the repo root)

## Quick start

1. Copy the env template and fill in credentials:

   ```bash
   cp .env.example .env
   # edit .env: GRAFANA_CLOUD_OTLP_ENDPOINT, GRAFANA_CLOUD_OTLP_USERNAME,
   # GRAFANA_CLOUD_OTLP_PASSWORD
   ```

   The three values are shown on the **OTLP Gateway** page of your stack
   (Home → Connections → OpenTelemetry). The username is the numeric
   "Instance ID"; the password is the access policy token.

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

4. In Grafana Cloud, open **Explore**, pick the Tempo datasource for your
   stack, and query:

   ```traceql
   { name = "bazel.build" }
   ```

   Build metrics land in the Mimir datasource under `bazel_*` names; logs land
   in Loki with `service_name="bes"`.

## Verification

```bash
# Collector is healthy locally
curl -fsS http://localhost:13133

# Collector self-metrics show export attempts/successes
curl -s http://localhost:8888/metrics | grep otelcol_exporter_sent_spans

# Inspect collector logs for auth errors (401 / 403) or TLS issues
docker compose logs -f otelcol-bazel
```

If you see `401 Unauthorized`, regenerate the access policy token and confirm
it has all three `*:write` scopes. If you see DNS errors, double-check that
`GRAFANA_CLOUD_OTLP_ENDPOINT` matches your stack's region.

## Teardown

```bash
docker compose down
```

## Files

| File | Purpose |
|---|---|
| `collector-config.yaml` | BES receiver → OTLP/HTTP with basic auth |
| `.env.example` | Template for OTLP endpoint + Basic auth credentials |
| `docker-compose.yaml` | Runs `otelcol-bazel:latest` with `.env` loaded |
