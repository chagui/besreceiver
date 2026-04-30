# CLAUDE.md — besreceiver

## What this project is

An OpenTelemetry Collector custom receiver that accepts Bazel Build Event Protocol (BEP) streams via the BES gRPC API and converts them into distributed traces, logs, and metrics. Bazel builds become trace flamegraphs in Datadog, Tempo, Jaeger, or any OTLP backend, with build performance metrics queryable as time-series.

## Module path

```
github.com/chagui/besreceiver
```

The receiver package is at `receiver/besreceiver/`.

## Architecture

```
Bazel --bes_backend=grpc://host:8082
  → besreceiver (gRPC BES server inside OTel Collector)
    → BEP parser (anypb.Any → bep.BuildEvent)
    → Trace builder (BEP events → ptrace.Traces + pmetric.Metrics)
    → consumer.ConsumeTraces() / ConsumeMetrics() / ConsumeLogs()
  → batch processor → OTLP exporter → backend
```

### Span hierarchy per invocation

```
bazel.build (root, traceID derived from invocation UUID)
├── bazel.target (//pkg:lib, structural parent)
│   ├── bazel.action (Javac, with start/end timestamps)
│   └── bazel.action (Turbine)
├── bazel.target (//pkg:test)
│   ├── bazel.test (shard=0, run=1, attempt=1)
│   └── bazel.test (shard=1, run=1, attempt=1)
└── bazel.metrics (wall_time, cpu_time, actions_executed)
```

## Source files

| File | Purpose |
|---|---|
| `third_party/bazel/protobuf/*.proto` | BEP protobuf source files (vendored from Bazel) |
| `internal/bep/` | Generated Go code from proto files (`buf generate third_party/bazel/protobuf`) |
| `receiver/besreceiver/config.go` | Config struct embedding `configgrpc.ServerConfig` |
| `receiver/besreceiver/factory.go` | `NewFactory()`, component type `"bes"`, alpha stability |
| `receiver/besreceiver/receiver.go` | `Start`/`Shutdown`, BES gRPC handler (`PublishBuildToolEventStream`) |
| `receiver/besreceiver/bepparser.go` | `ParseBazelEvent`: `anypb.Any` → `bep.BuildEvent` via `proto.Unmarshal` |
| `receiver/besreceiver/tracebuilder.go` | Core logic: per-invocation state, BEP events → `ptrace.Traces` + `pmetric.Metrics` |
| `receiver/besreceiver/metricsbuilder.go` | Per-invocation gauges + cumulative counters from `BuildMetrics` |

## Commands

```bash
make build      # go build ./...
make test       # go test ./...
make vet        # go vet ./...
make lint       # golangci-lint run ./...
make tidy       # go mod tidy
make generate   # buf generate third_party/bazel/protobuf (regenerate Go code from protos)
make ocb        # build custom collector binary with OCB
make docker     # docker build

make pre-commit-install  # install prek + wire up git pre-commit and pre-push hooks
make pre-commit          # run pre-commit-stage hooks (fast: changed packages)
make pre-push            # run pre-push-stage hooks (slow: full module lint)
```

Two-stage setup: `pre-commit` lints only packages of staged Go files
(`scripts/precommit-golangci-lint.sh`); `pre-push` runs `golangci-lint run ./...`
over the root module before pushing. CI remains the authoritative pass.

## Dependencies

- **BEP types**: vendored proto sources in `third_party/bazel/protobuf/`, generated Go code in `internal/bep/`. Version pinned in `third_party/bazel/VERSION`
- **BES gRPC transport**: `google.golang.org/genproto/googleapis/devtools/build/v1` (pre-compiled `PublishBuildEvent` service)
- **OTel Collector**: `go.opentelemetry.io/collector/{component,consumer,receiver,pdata,config/configgrpc}` — v1.52.0 stable / v0.146.1 experimental

## Key design decisions

- **OTel Collector receiver, not standalone server**: same core logic, gains batching/retry/fan-out/TLS for free
- **OpenTelemetry over Datadog native**: vendor-neutral, multi-backend via YAML config
- **pdata API, not OTel SDK**: we construct traces retrospectively from an event stream, not instrumenting live code — pdata gives explicit control over TraceIDs, SpanIDs, timestamps
- **Deterministic traceID**: SHA-256 of invocation UUID, so the same build always produces the same trace ID
- **Vendored BEP protos**: proto sources in `third_party/bazel/protobuf/`, generated Go code in `internal/bep/` — keeps the project self-contained with no external replace directives. Pinned to a specific Bazel commit in `third_party/bazel/VERSION`; see `third_party/bazel/README.md` for update instructions

## Testing patterns

Tests use `consumertest.TracesSink` and `consumertest.MetricsSink` to capture emitted traces and metrics, then assert span names, attributes, parent-child relationships, status codes, gauge values, and cumulative counter semantics. `receivertest.NewNopSettings(componentType)` provides a no-op receiver settings for unit tests.

## Running with Bazel

```bash
bazel build //... \
  --bes_backend=grpc://localhost:8082 \
  --build_event_publish_all_actions
```

The `--build_event_publish_all_actions` flag is required — without it, only failed actions emit events.

## Collector config

See `collector-config.yaml` for an example. The receiver is configured under `receivers.bes` with a `configgrpc.ServerConfig` (endpoint, TLS, keepalive, etc.).
