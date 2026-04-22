# Sequence Diagrams

## BEP Stream Processing (single invocation)

![BEP stream processing: Bazel opens a bidirectional PublishBuildToolEventStream, each OrderedBuildEvent is parsed from anypb.Any into a bep.BuildEvent, the Trace Builder creates and mutates invocationState (root bazel.build span, bazel.target spans, bazel.action spans), traces are handed to consumer.Traces for batch+export, and the invocationState is cleaned up on stream EOF after BuildFinished](sequence-bep-stream.svg)

## Concurrent Invocations

![Concurrent invocations: two Bazel builds (uuid=aaa and uuid=bbb) open independent gRPC streams to the besreceiver, their BuildStarted events populate distinct entries in the invocations sync.Map, and inside a par block the receiver processes ActionExecuted events from both streams in parallel by loading each invocationState by UUID and emitting traces to consumer.Traces; BuildFinished for each stream deletes its map entry](sequence-concurrent.svg)

## Collector Startup Lifecycle

![Collector startup lifecycle: otelcol-bazel main() calls Run on the Collector Service, which invokes besreceiver Factory.CreateTraces to obtain a receiver instance, then calls Start; the receiver builds the gRPC server via ServerConfig.ToServer, registers the PublishBuildEvent service, listens on 0.0.0.0:8082, and spawns Serve in a goroutine; on Shutdown the receiver calls GracefulStop to drain active streams](sequence-startup.svg)
