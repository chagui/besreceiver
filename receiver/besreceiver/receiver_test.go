package besreceiver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-tc", "uuid-target", "build", 1),
		makeTargetConfiguredOBE(t, "inv-tc", "//pkg:lib", 2),
	)

	// Spans are batched — nothing emitted yet.
	require.Equal(t, 0, sink.SpanCount(), "expected 0 spans (batched)")

	processEvents(ctx, t, tb,
		makeActionOBE(t, "inv-tc", "//pkg:lib", "Javac", 3, true),
		// BuildFinished — flushes the batch.
		makeBuildFinishedOBE(t, "inv-tc", 4, 0, "SUCCESS"),
	)

	// 3 spans in one batch: target + action + root.
	require.Equal(t, 3, sink.SpanCount(), "expected 3 spans")

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

	require.Equal(t, "bazel.target", targetSpan.Name(), "expected to find bazel.target span in batch")

	label, ok := targetSpan.Attributes().Get("bazel.target.label")
	assert.True(t, ok, "missing bazel.target.label attribute")
	assert.Equal(t, "//pkg:lib", label.Str())

	ruleKind, ok := targetSpan.Attributes().Get("bazel.target.rule_kind")
	assert.True(t, ok, "missing bazel.target.rule_kind attribute")
	assert.Equal(t, "java_library rule", ruleKind.Str())

	// Verify the target's spanID is used as parent for the action.
	require.Equal(t, "bazel.action Javac", actionSpan.Name(), "expected to find action span in batch")
	assert.Equal(t, targetSpan.SpanID(), actionSpan.ParentSpanID(), "action parent should be target span")
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
	require.NoError(t, recv.Start(ctx, host), "failed to start receiver")
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		assert.NoError(t, recv.Shutdown(shutdownCtx), "shutdown error")
	}()

	// Dial the actual address the OS assigned.
	actualAddr := recv.Addr().String()
	conn, err := grpc.NewClient(actualAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "failed to dial gRPC")
	defer func() {
		assert.NoError(t, conn.Close(), "failed to close connection")
	}()

	client := pb.NewPublishBuildEventClient(conn)

	stream, err := client.PublishBuildToolEventStream(ctx)
	require.NoError(t, err, "failed to open stream")

	sendAndACK(t, stream, makeBuildStartedReq(t, "inv-integration", "uuid-integration", "build", 1), 1)
	sendAndACK(t, stream, makeBuildFinishedReq(t, "inv-integration", 2, 0, "SUCCESS"), 2)
	sendAndACK(t, stream, makeBuildMetricsReq(t, "inv-integration", 3, 10000, 5000), 3)

	require.NoError(t, stream.CloseSend(), "failed to close stream")

	t.Run("traces", func(t *testing.T) {
		require.Equal(t, 2, tracesSink.SpanCount(), "expected 2 spans")

		span := tracesSink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		assert.Equal(t, "bazel.build build", span.Name())
		cmd, ok := span.Attributes().Get("bazel.command")
		assert.True(t, ok, "missing bazel.command attribute")
		assert.Equal(t, "build", cmd.Str())
	})

	t.Run("metrics", func(t *testing.T) {
		allMetrics := metricsSink.AllMetrics()
		require.Len(t, allMetrics, 1, "expected 1 ConsumeMetrics call")

		md := allMetrics[0]
		assert.NotZero(t, md.MetricCount(), "expected metrics from BuildMetrics event")
		assert.True(t, hasMetricNamed(md, "bazel.invocation.wall_time"), "expected wall_time gauge")
		assert.True(t, hasMetricNamed(md, "bazel.invocation.count"), "expected invocation count counter")
	})
}
