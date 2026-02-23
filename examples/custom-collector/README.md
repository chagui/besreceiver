# Custom Collector Example

Build your own OpenTelemetry Collector with the BES receiver, using whatever base image and exporters your organization requires.

## Quick start

```bash
docker compose up --build
```

Then point Bazel at it:

```bash
bazel build //... \
  --bes_backend=grpc://localhost:8082 \
  --build_event_publish_all_actions
```

Traces are printed to stdout via the `debug` exporter. Replace it in `collector-config.yaml` with your preferred backend (OTLP, Datadog, Jaeger, etc.).

## How it works

The `builder-config.yaml` imports `besreceiver` as a remote Go module — no source checkout needed:

```yaml
receivers:
  - gomod: "github.com/chagui/besreceiver v0.1.0"
    import: "github.com/chagui/besreceiver/receiver/besreceiver"
```

The Dockerfile uses [OCB](https://opentelemetry.io/docs/collector/custom-collector/) to generate and compile the collector in a multi-stage build. Swap the base image on the `final` stage to match your organization's requirements.

## Files

| File | Purpose |
|---|---|
| `builder-config.yaml` | OCB manifest — declares which components to include |
| `collector-config.yaml` | Collector runtime config (receivers, processors, exporters, pipelines) |
| `Dockerfile` | Multi-stage build: OCB generates + compiles, then copies into base image |
| `docker-compose.yaml` | Runs the collector with BES on `:8082` and health check on `:13133` |
