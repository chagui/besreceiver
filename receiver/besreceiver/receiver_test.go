package besreceiver

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "google.golang.org/genproto/googleapis/devtools/build/v1"

	"github.com/chagui/besreceiver/receiver/besreceiver/internal/metadata"
)

// --- Stream handler tests ---

func TestPublishBuildToolEventStream_HappyPath(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	r := &besReceiver{
		logger:       zap.NewNop(),
		traceBuilder: tb,
	}

	stream := &mockBESStream{
		ctx: context.Background(),
		events: []*pb.PublishBuildToolEventStreamRequest{
			makeBuildStartedReq(t, "inv-1", "uuid-stream", "build", 1),
			makeBuildFinishedReq(t, "inv-1", 2, 0, "SUCCESS"),
		},
	}

	if err := r.PublishBuildToolEventStream(stream); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify ACKs: one per event.
	if len(stream.acks) != 2 {
		t.Fatalf("expected 2 ACKs, got %d", len(stream.acks))
	}
	if stream.acks[0].GetSequenceNumber() != 1 {
		t.Errorf("expected ACK seq 1, got %d", stream.acks[0].GetSequenceNumber())
	}
	if stream.acks[1].GetSequenceNumber() != 2 {
		t.Errorf("expected ACK seq 2, got %d", stream.acks[1].GetSequenceNumber())
	}

	// BuildFinished emits 1 span (the complete root span).
	if sink.SpanCount() != 1 {
		t.Fatalf("expected 1 span, got %d", sink.SpanCount())
	}
	span := sink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	if span.Name() != "bazel.build build" {
		t.Errorf("expected span name 'bazel.build build', got %s", span.Name())
	}
}

func TestPublishBuildToolEventStream_EmptyStream(t *testing.T) {
	sink := new(consumertest.TracesSink)
	r := &besReceiver{
		logger:       zap.NewNop(),
		traceBuilder: NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{}),
	}

	stream := &mockBESStream{
		ctx:    context.Background(),
		events: nil, // immediate EOF
	}

	if err := r.PublishBuildToolEventStream(stream); err != nil {
		t.Fatalf("expected nil error for empty stream, got %v", err)
	}
	if len(stream.acks) != 0 {
		t.Errorf("expected 0 ACKs, got %d", len(stream.acks))
	}
}

func TestPublishBuildToolEventStream_SendError(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	r := &besReceiver{
		logger:       zap.NewNop(),
		traceBuilder: tb,
	}

	stream := &mockBESStream{
		ctx: context.Background(),
		events: []*pb.PublishBuildToolEventStreamRequest{
			makeBuildStartedReq(t, "inv-1", "uuid-send-err", "build", 1),
		},
		sendErr: errors.New("send failed"),
	}

	err := r.PublishBuildToolEventStream(stream)
	if err == nil {
		t.Fatal("expected error from Send failure")
	}
	if !errors.Is(err, stream.sendErr) {
		t.Errorf("expected wrapped 'send failed', got %q", err.Error())
	}
}

func TestPublishBuildToolEventStream_RetryableConsumerError(t *testing.T) {
	// A non-permanent consumer error should close the stream so Bazel retries.
	errSink := consumertest.NewErr(errors.New("pipeline overloaded"))
	tb := NewTraceBuilder(errSink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	r := &besReceiver{
		logger:       zap.NewNop(),
		traceBuilder: tb,
	}

	// Send BuildStarted (no span emitted) + BuildFinished (triggers ConsumeTraces → error).
	stream := &mockBESStream{
		ctx: context.Background(),
		events: []*pb.PublishBuildToolEventStreamRequest{
			makeBuildStartedReq(t, "inv-1", "uuid-retry", "build", 1),
			makeBuildFinishedReq(t, "inv-1", 2, 0, "SUCCESS"),
		},
	}

	err := r.PublishBuildToolEventStream(stream)
	if err == nil {
		t.Fatal("expected error from retryable consumer failure")
	}

	// BuildStarted ACKed (no span emission), BuildFinished should NOT be ACKed.
	if len(stream.acks) != 1 {
		t.Errorf("expected 1 ACK (only BuildStarted), got %d", len(stream.acks))
	}
}

func TestPublishBuildToolEventStream_PermanentConsumerError(t *testing.T) {
	// A permanent consumer error should log but continue the stream.
	permErr := consumererror.NewPermanent(errors.New("data rejected"))
	errSink := consumertest.NewErr(permErr)
	tb := NewTraceBuilder(errSink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	r := &besReceiver{
		logger:       zap.NewNop(),
		traceBuilder: tb,
	}

	stream := &mockBESStream{
		ctx: context.Background(),
		events: []*pb.PublishBuildToolEventStreamRequest{
			makeBuildStartedReq(t, "inv-1", "uuid-perm", "build", 1),
			makeBuildFinishedReq(t, "inv-1", 2, 0, "SUCCESS"),
		},
	}

	// Should succeed — permanent errors are logged but stream continues.
	if err := r.PublishBuildToolEventStream(stream); err != nil {
		t.Fatalf("expected nil error for permanent consumer failure, got %v", err)
	}

	// Both events should be ACKed even though ConsumeTraces returned a permanent error.
	if len(stream.acks) != 2 {
		t.Errorf("expected 2 ACKs, got %d", len(stream.acks))
	}
}

// --- Lifecycle tests ---

func TestPublishLifecycleEvent(t *testing.T) {
	r := &besReceiver{logger: zap.NewNop()}

	resp, err := r.PublishLifecycleEvent(context.Background(), &pb.PublishLifecycleEventRequest{
		BuildEvent: &pb.OrderedBuildEvent{
			StreamId: &pb.StreamId{BuildId: "build-123"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestShutdownBeforeStart(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	recv := &besReceiver{
		config: cfg,
		logger: zap.NewNop(),
	}

	// Shutdown without Start should not panic.
	if err := recv.Shutdown(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestShutdownRespectsContextDeadline(t *testing.T) {
	sink := new(consumertest.TracesSink)
	r := &besReceiver{
		logger:       zap.NewNop(),
		traceBuilder: NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{}),
		grpcServer:   grpc.NewServer(),
	}

	// Create a context that is already expired.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond) // ensure deadline passes

	err := r.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

// --- TraceBuilder tests co-located with receiver tests ---

func TestTraceBuilder_TargetConfiguredEmitsSpan(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-tc", "uuid-target", "build", 1)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeTargetConfiguredOBE(t, "inv-tc", "//pkg:lib", 2)); err != nil {
		t.Fatal(err)
	}

	// Spans are batched — nothing emitted yet.
	if sink.SpanCount() != 0 {
		t.Fatalf("expected 0 spans (batched), got %d", sink.SpanCount())
	}

	if err := tb.ProcessOrderedBuildEvent(ctx, makeActionOBE(t, "inv-tc", "//pkg:lib", "Javac", 3, true)); err != nil {
		t.Fatal(err)
	}

	// BuildFinished — flushes the batch.
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-tc", 4, 0, "SUCCESS")); err != nil {
		t.Fatal(err)
	}

	// 3 spans in one batch: target + action + root.
	if sink.SpanCount() != 3 {
		t.Fatalf("expected 3 spans, got %d", sink.SpanCount())
	}

	// Find spans by name in the batched output.
	spans := sink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	var targetSpan, actionSpan ptrace.Span
	for i := range spans.Len() {
		switch spans.At(i).Name() {
		case "bazel.target":
			targetSpan = spans.At(i)
		case "bazel.action Javac":
			actionSpan = spans.At(i)
		}
	}

	if targetSpan.Name() != "bazel.target" {
		t.Fatal("expected to find bazel.target span in batch")
	}

	label, ok := targetSpan.Attributes().Get("bazel.target.label")
	if !ok || label.Str() != "//pkg:lib" {
		t.Errorf("expected bazel.target.label=//pkg:lib, got %v", label)
	}

	ruleKind, ok := targetSpan.Attributes().Get("bazel.target.rule_kind")
	if !ok || ruleKind.Str() != "java_library rule" {
		t.Errorf("expected bazel.target.rule_kind='java_library rule', got %v", ruleKind)
	}

	// Verify the target's spanID is used as parent for the action.
	if actionSpan.Name() != "bazel.action Javac" {
		t.Fatal("expected to find 'bazel.action Javac' span in batch")
	}
	if actionSpan.ParentSpanID() != targetSpan.SpanID() {
		t.Errorf("expected action parent to be target span %v, got %v", targetSpan.SpanID(), actionSpan.ParentSpanID())
	}
}

func TestTraceBuilder_BuildFinishedWithFailure(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-fail", "uuid-fail", "build", 1)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-fail", 2, 1, "BUILD_FAILURE")); err != nil {
		t.Fatal(err)
	}

	if sink.SpanCount() != 1 {
		t.Fatalf("expected 1 span, got %d", sink.SpanCount())
	}

	span := sink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	if span.Status().Code() != ptrace.StatusCodeError {
		t.Errorf("expected error status for failed build, got %v", span.Status().Code())
	}
	if span.Status().Message() != "BUILD_FAILURE" {
		t.Errorf("expected status message BUILD_FAILURE, got %s", span.Status().Message())
	}
}

// --- Integration test: real gRPC server ---

func TestIntegration_RealGRPCServer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	tracesSink := new(consumertest.TracesSink)
	metricsSink := new(consumertest.MetricsSink)
	settings := receivertest.NewNopSettings(metadata.Type)
	cfg := createDefaultConfig().(*Config)
	cfg.NetAddr = confignet.AddrConfig{
		Endpoint:  "localhost:0",
		Transport: confignet.TransportTypeTCP,
	}

	recv := &besReceiver{
		config:          cfg,
		logger:          settings.Logger,
		settings:        settings,
		tracesConsumer:  tracesSink,
		metricsConsumer: metricsSink,
	}

	host := componenttest.NewNopHost()
	if err := recv.Start(ctx, host); err != nil {
		t.Fatalf("failed to start receiver: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := recv.Shutdown(shutdownCtx); err != nil {
			t.Errorf("shutdown error: %v", err)
		}
	}()

	// Dial the actual address the OS assigned.
	actualAddr := recv.Addr().String()
	conn, err := grpc.NewClient(actualAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial gRPC: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("failed to close connection: %v", err)
		}
	}()

	client := pb.NewPublishBuildEventClient(conn)

	stream, err := client.PublishBuildToolEventStream(ctx)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	sendAndACK(t, stream, makeBuildStartedReq(t, "inv-integration", "uuid-integration", "build", 1), 1)
	sendAndACK(t, stream, makeBuildFinishedReq(t, "inv-integration", 2, 0, "SUCCESS"), 2)
	sendAndACK(t, stream, makeBuildMetricsReq(t, "inv-integration", 3, 10000, 5000), 3)

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("failed to close stream: %v", err)
	}

	t.Run("traces", func(t *testing.T) {
		if tracesSink.SpanCount() != 2 {
			t.Fatalf("expected 2 spans, got %d", tracesSink.SpanCount())
		}

		span := tracesSink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		if span.Name() != "bazel.build build" {
			t.Errorf("expected span name 'bazel.build build', got %s", span.Name())
		}
		cmd, ok := span.Attributes().Get("bazel.command")
		if !ok || cmd.Str() != "build" {
			t.Errorf("expected bazel.command=build, got %v", cmd)
		}
	})

	t.Run("metrics", func(t *testing.T) {
		allMetrics := metricsSink.AllMetrics()
		if len(allMetrics) != 1 {
			t.Fatalf("expected 1 ConsumeMetrics call, got %d", len(allMetrics))
		}

		md := allMetrics[0]
		if md.MetricCount() == 0 {
			t.Fatal("expected metrics from BuildMetrics event, got 0")
		}
		if !hasMetricNamed(md, "bazel.invocation.wall_time") {
			t.Error("expected bazel.invocation.wall_time gauge in metrics")
		}
		if !hasMetricNamed(md, "bazel.invocation.count") {
			t.Error("expected bazel.invocation.count counter in metrics")
		}
	})
}
