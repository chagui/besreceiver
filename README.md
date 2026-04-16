# besreceiver

[![CI](https://github.com/chagui/besreceiver/actions/workflows/ci.yml/badge.svg)](https://github.com/chagui/besreceiver/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/chagui/besreceiver.svg)](https://pkg.go.dev/github.com/chagui/besreceiver)
[![Go Report Card](https://goreportcard.com/badge/github.com/chagui/besreceiver)](https://goreportcard.com/report/github.com/chagui/besreceiver)
![Stability: Alpha](https://img.shields.io/badge/stability-alpha-orange)

An OpenTelemetry Collector custom receiver designed to receive [Bazel Build Event Protocol (BEP)](https://bazel.build/remote/bep) streams via the BES gRPC API and converts them into the OpenTelemetry trace model. Bazel builds become trace flamegraphs in Datadog, Tempo, Jaeger, or any OTLP backend.

## Quick start

### Build the custom collector

```bash
# Install the OpenTelemetry Collector Builder (OCB)
go install go.opentelemetry.io/collector/cmd/builder@v0.146.1

# Build the collector binary
make ocb
```

### Run it

```bash
./build/otelcol-bazel --config collector-config.yaml
```

### Point Bazel at it

```bash
bazel build //... \
  --bes_backend=grpc://localhost:8082 \
  --build_event_publish_all_actions
```

> The `--build_event_publish_all_actions` flag is required -- without it, only failed actions emit events.

## Trace output

Each Bazel invocation produces one trace:

```
bazel.build (root span, traceID derived from invocation UUID)
├── bazel.target (//pkg:lib)
│   ├── bazel.action (Javac)
│   └── bazel.action (Turbine)
├── bazel.target (//pkg:test)
│   ├── bazel.test (shard=0, run=1, attempt=1)
│   └── bazel.test (shard=1, run=1, attempt=1)
└── bazel.metrics (wall_time, cpu_time, actions_executed)
```

## Configuration

The receiver is configured under `receivers.bes` in the collector YAML. It embeds the standard [`configgrpc.ServerConfig`](https://pkg.go.dev/go.opentelemetry.io/collector/config/configgrpc#ServerConfig) (endpoint, TLS, keepalive) plus BES-specific tuning parameters (`invocation_timeout`, `reaper_interval`). See [`collector-config.yaml`](collector-config.yaml) for a working example and [`config.go`](receiver/besreceiver/config.go) for the full schema.

## Development

```bash
make test       # Run tests (with race detector, no cache)
make lint       # Run golangci-lint
make vet        # Run go vet
make build      # Verify compilation
make ci         # Run the full CI chain: vet → lint → test → build
make test-bench # Run benchmarks
make fuzz       # Run fuzz tests
```

## Documentation

- [Architecture](docs/architecture.md) -- system overview and Mermaid diagrams
- [Sequence diagrams](docs/sequence.md) -- BEP stream processing flow

## License

See [LICENSE](LICENSE).
