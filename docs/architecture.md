# Architecture

## End-to-end data flow

![End-to-end pipeline: Bazel CLI → besreceiver (inside OTel Collector) → batch processor + OTLP exporter → Datadog / Tempo / Jaeger](architecture-pipeline.svg)

## Internal components

![Internal components: concurrent gRPC goroutines feed a buffered eventCh, a single owner goroutine in tracebuilder.go processes events against the invocations map and invocationState, the reaper flushes stale invocations, and metricsbuilder emits gauges and cumulative counters to the traces/logs/metrics consumers](architecture-components.svg)

## Span hierarchy per Bazel invocation

![Span hierarchy: bazel.build root span fans out to bazel.target spans (each with child bazel.action spans and bazel.test shard spans) and one bazel.metrics span; PII-gated attributes and deterministic trace/span IDs called out in side legends](architecture-spans.svg)
