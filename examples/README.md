## Examples

| Directory | Purpose |
|---|---|
| `bazel-project/` | Minimal Bazel workspace with passing and failing tests for generating BES events. |
| `custom-collector/` | Shows how to import `besreceiver` as a remote module and build your own collector image. |
| `datadog/` | Development setup with live-reload (`air`) and a Datadog Agent sidecar. |
| `jaeger/` | Self-contained stack: Jaeger all-in-one + `otelcol-bazel` for browsing spans in the Jaeger UI. |
| `tempo/` | Self-contained stack: Tempo + Grafana (pre-provisioned Tempo datasource) + `otelcol-bazel`. |
| `grafana-cloud/` | Sends traces/metrics/logs to Grafana Cloud over OTLP/HTTP with HTTP Basic auth. |
| `otlp-generic/` | Forwards to any OTLP gRPC endpoint (Honeycomb, New Relic, internal gateway, ...). |

All backend examples use the `otelcol-bazel:latest` image built via `make docker` from the repo root.
