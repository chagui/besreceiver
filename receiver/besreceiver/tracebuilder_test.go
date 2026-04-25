package besreceiver

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	buildpb "google.golang.org/genproto/googleapis/devtools/build/v1"

	bep "github.com/chagui/besreceiver/internal/bep/buildeventstream"
	"github.com/chagui/besreceiver/internal/bep/commandline"
)

func TestTraceBuilder_BuildStarted(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-1", "uuid-aaa", "build", 1)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// BuildStarted buffers state but does not emit a span.
	if sink.SpanCount() != 0 {
		t.Fatalf("expected 0 spans (root is deferred), got %d", sink.SpanCount())
	}

	// Verify state was created by completing the invocation and checking emitted spans.
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-1", 2, 0, "SUCCESS")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if sink.SpanCount() != 1 {
		t.Fatalf("expected 1 span, got %d", sink.SpanCount())
	}

	span := sink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	expectedTraceID := traceIDFromUUID("uuid-aaa")
	if span.TraceID() != expectedTraceID {
		t.Errorf("expected traceID %v, got %v", expectedTraceID, span.TraceID())
	}
	cmd, ok := span.Attributes().Get("bazel.command")
	if !ok || cmd.Str() != "build" {
		t.Errorf("expected bazel.command=build, got %v", cmd)
	}
}

func TestTraceBuilder_ActionExecuted(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-2", "uuid-bbb", "build", 1)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeActionOBE(t, "inv-2", "//pkg:lib", "Javac", 2, false)); err != nil {
		t.Fatal(err)
	}

	// Spans are batched — nothing emitted yet.
	if sink.SpanCount() != 0 {
		t.Fatalf("expected 0 spans (batched), got %d", sink.SpanCount())
	}

	// BuildFinished flushes the batch.
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-2", 3, 0, "SUCCESS")); err != nil {
		t.Fatal(err)
	}

	// 2 spans in one batch: action + root.
	if sink.SpanCount() != 2 {
		t.Fatalf("expected 2 spans, got %d", sink.SpanCount())
	}

	// Find the action span in the batched output.
	spans := sink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	var span ptrace.Span
	for i := range spans.Len() {
		if spans.At(i).Name() == "bazel.action Javac" {
			span = spans.At(i)
			break
		}
	}

	if span.Name() != "bazel.action Javac" {
		t.Fatal("expected to find 'bazel.action Javac' span in batch")
	}
	if span.Status().Code() != ptrace.StatusCodeError {
		t.Error("expected error status for failed action")
	}

	mnemonic, ok := span.Attributes().Get("bazel.action.mnemonic")
	if !ok || mnemonic.Str() != "Javac" {
		t.Errorf("expected bazel.action.mnemonic=Javac, got %v", mnemonic)
	}
}

func TestTraceBuilder_BuildFinished(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-3", "uuid-ccc", "test", 1)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-3", 2, 0, "SUCCESS")); err != nil {
		t.Fatal(err)
	}

	// BuildFinished emits a single complete root span.
	if sink.SpanCount() != 1 {
		t.Fatalf("expected 1 span (complete root), got %d", sink.SpanCount())
	}

	span := sink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)

	if span.Name() != "bazel.build test" {
		t.Errorf("expected span name 'bazel.build test', got %s", span.Name())
	}

	// Verify both start and end timestamps are set.
	if span.StartTimestamp() == 0 {
		t.Error("expected start timestamp to be set from BuildStarted")
	}
	if span.EndTimestamp() == 0 {
		t.Error("expected end timestamp to be set from BuildFinished")
	}

	// Verify attributes from BuildStarted.
	cmd, ok := span.Attributes().Get("bazel.command")
	if !ok || cmd.Str() != "test" {
		t.Errorf("expected bazel.command=test, got %v", cmd)
	}

	// Verify attributes from BuildFinished.
	exitName, ok := span.Attributes().Get("bazel.exit_code.name")
	if !ok || exitName.Str() != "SUCCESS" {
		t.Errorf("expected bazel.exit_code.name=SUCCESS, got %v", exitName)
	}
	exitCode, ok := span.Attributes().Get("bazel.exit_code.code")
	if !ok || exitCode.Int() != 0 {
		t.Errorf("expected bazel.exit_code.code=0, got %v", exitCode)
	}

	// State is NOT deleted after BuildFinished — BuildMetrics should still work.
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildMetricsOBE(t, "inv-3", 3, 5000, 3000)); err != nil {
		t.Fatal(err)
	}
	if sink.SpanCount() != 2 {
		t.Fatalf("expected 2 spans (root + metrics), got %d", sink.SpanCount())
	}
}

func TestTraceBuilder_TestResult(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-4", "uuid-ddd", "test", 1)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeTestResultOBE(t, "inv-4", "//pkg:test", 2, bep.TestStatus_PASSED)); err != nil {
		t.Fatal(err)
	}

	// Spans are batched — nothing emitted yet.
	if sink.SpanCount() != 0 {
		t.Fatalf("expected 0 spans (batched), got %d", sink.SpanCount())
	}

	// BuildFinished flushes the batch.
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-4", 3, 0, "SUCCESS")); err != nil {
		t.Fatal(err)
	}

	// 2 spans in one batch: test + root.
	if sink.SpanCount() != 2 {
		t.Fatalf("expected 2 spans, got %d", sink.SpanCount())
	}

	// Find the test span in the batched output.
	spans := sink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	var span ptrace.Span
	for i := range spans.Len() {
		if spans.At(i).Name() == "bazel.test" {
			span = spans.At(i)
			break
		}
	}

	if span.Name() != "bazel.test" {
		t.Fatal("expected to find bazel.test span in batch")
	}

	status, ok := span.Attributes().Get("bazel.test.status")
	if !ok || status.Str() != "PASSED" {
		t.Errorf("expected bazel.test.status=PASSED, got %v", status)
	}
}

func TestTraceBuilder_ConcurrentInvocations(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// Start and finish two invocations
	for _, inv := range []struct{ id, uuid string }{
		{"inv-a", "uuid-aaa"},
		{"inv-b", "uuid-bbb"},
	} {
		if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, inv.id, inv.uuid, "build", 1)); err != nil {
			t.Fatal(err)
		}
		if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, inv.id, 2, 0, "SUCCESS")); err != nil {
			t.Fatal(err)
		}
	}

	if sink.SpanCount() != 2 {
		t.Fatalf("expected 2 spans, got %d", sink.SpanCount())
	}

	// Verify both invocations have separate trace IDs in the emitted spans.
	traceIDs := make(map[pcommon.TraceID]bool)
	for _, traces := range sink.AllTraces() {
		spans := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
		for i := range spans.Len() {
			traceIDs[spans.At(i).TraceID()] = true
		}
	}
	if len(traceIDs) != 2 {
		t.Errorf("expected 2 different trace IDs, got %d", len(traceIDs))
	}
}

func TestTraceBuilder_BuildMetricsDeletesState(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-5", "uuid-eee", "build", 1)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-5", 2, 0, "SUCCESS")); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildMetricsOBE(t, "inv-5", 3, 10000, 5000)); err != nil {
		t.Fatal(err)
	}

	// 2 total spans: root (from BuildFinished) + metrics (from BuildMetrics).
	if sink.SpanCount() != 2 {
		t.Fatalf("expected 2 spans, got %d", sink.SpanCount())
	}

	metricsSpan := sink.AllTraces()[1].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	if metricsSpan.Name() != "bazel.metrics" {
		t.Errorf("expected span name bazel.metrics, got %s", metricsSpan.Name())
	}

	wallTime, ok := metricsSpan.Attributes().Get("bazel.metrics.wall_time_ms")
	if !ok || wallTime.Int() != 10000 {
		t.Errorf("expected bazel.metrics.wall_time_ms=10000, got %v", wallTime)
	}

	// Verify state was cleaned up: sending another BuildMetrics should be a no-op.
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildMetricsOBE(t, "inv-5", 4, 20000, 10000)); err != nil {
		t.Fatal(err)
	}
	if sink.SpanCount() != 2 {
		t.Errorf("expected 2 spans (no new spans after state cleanup), got %d", sink.SpanCount())
	}
}

func TestTraceBuilder_ParseErrorIsPermanent(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// Send an event with corrupt protobuf data.
	obe := &buildpb.OrderedBuildEvent{
		StreamId: &buildpb.StreamId{
			BuildId:      "build-1",
			InvocationId: "inv-corrupt",
		},
		SequenceNumber: 1,
		Event: &buildpb.BuildEvent{
			Event: &buildpb.BuildEvent_BazelEvent{
				BazelEvent: &anypb.Any{
					TypeUrl: "type.googleapis.com/build_event_stream.BuildEvent",
					Value:   []byte("not valid protobuf"),
				},
			},
		},
	}

	err := tb.ProcessOrderedBuildEvent(ctx, obe)
	if err == nil {
		t.Fatal("expected error from corrupt protobuf")
	}
	if !consumererror.IsPermanent(err) {
		t.Errorf("expected permanent error, got non-permanent: %v", err)
	}
}

func TestTraceBuilder_BatchFlushSingleConsumeCall(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-batch", "uuid-batch", "build", 1)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeTargetConfiguredOBE(t, "inv-batch", "//pkg:lib", 2)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeActionOBE(t, "inv-batch", "//pkg:lib", "Javac", 3, true)); err != nil {
		t.Fatal(err)
	}

	// No spans emitted yet — all batched.
	if sink.SpanCount() != 0 {
		t.Fatalf("expected 0 spans before flush, got %d", sink.SpanCount())
	}
	if len(sink.AllTraces()) != 0 {
		t.Fatalf("expected 0 ConsumeTraces calls before flush, got %d", len(sink.AllTraces()))
	}

	// BuildFinished — single flush.
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-batch", 4, 0, "SUCCESS")); err != nil {
		t.Fatal(err)
	}

	// Exactly 1 ConsumeTraces call with all 3 spans (target + action + root).
	if len(sink.AllTraces()) != 1 {
		t.Fatalf("expected 1 ConsumeTraces call, got %d", len(sink.AllTraces()))
	}
	if sink.SpanCount() != 3 {
		t.Fatalf("expected 3 spans in batch, got %d", sink.SpanCount())
	}

	// Verify all span names are present.
	spans := sink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	names := make(map[string]bool)
	for i := range spans.Len() {
		names[spans.At(i).Name()] = true
	}
	for _, want := range []string{"bazel.target", "bazel.action Javac", "bazel.build build"} {
		if !names[want] {
			t.Errorf("expected span %q in batch, got names: %v", want, names)
		}
	}
}

func TestTraceIDFromUUID_Deterministic(t *testing.T) {
	tid1 := traceIDFromUUID("same-uuid")
	tid2 := traceIDFromUUID("same-uuid")
	if tid1 != tid2 {
		t.Error("expected deterministic trace ID from same UUID")
	}

	tid3 := traceIDFromUUID("different-uuid")
	if tid1 == tid3 {
		t.Error("expected different trace IDs from different UUIDs")
	}
}

// --- New tests ---

func TestResolveTargetSpan(t *testing.T) {
	rootSpanID := pcommon.SpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 1})
	exactSpanID := pcommon.SpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 2})
	labelOnlySpanID := pcommon.SpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 3})

	// Synthesize span handles with the desired IDs. Spans live inside a
	// ptrace.Traces; append to a scratch SpanSlice so the handles are valid.
	scratch := ptrace.NewTraces().ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	exactSpan := scratch.AppendEmpty()
	exactSpan.SetSpanID(exactSpanID)
	labelOnlySpan := scratch.AppendEmpty()
	labelOnlySpan.SetSpanID(labelOnlySpanID)

	state := &invocationState{
		rootSpanID: rootSpanID,
		targets: map[string]ptrace.Span{
			targetKey("//pkg:lib", "cfg-1"): exactSpan,
			targetKey("//pkg:lib", ""):      labelOnlySpan,
		},
	}

	tests := []struct {
		name     string
		label    string
		configID string
		want     pcommon.SpanID
	}{
		{
			name:     "exact match with config",
			label:    "//pkg:lib",
			configID: "cfg-1",
			want:     exactSpanID,
		},
		{
			name:     "label-only fallback",
			label:    "//pkg:lib",
			configID: "cfg-unknown",
			want:     labelOnlySpanID,
		},
		{
			name:     "root fallback for unknown label",
			label:    "//pkg:unknown",
			configID: "",
			want:     rootSpanID,
		},
		{
			name:     "empty label falls back to root",
			label:    "",
			configID: "",
			want:     rootSpanID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := state.resolveTargetSpan(tt.label, tt.configID)
			if got != tt.want {
				t.Errorf("resolveTargetSpan(%q, %q) = %v, want %v", tt.label, tt.configID, got, tt.want)
			}
		})
	}
}

func TestTraceBuilder_ConcurrentStreams(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent stress test in short mode")
	}

	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	const numGoroutines = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := range numGoroutines {
		go func(idx int) {
			defer wg.Done()
			invID := fmt.Sprintf("inv-concurrent-%d", idx)
			uuid := fmt.Sprintf("uuid-concurrent-%d", idx)

			// Full invocation: BuildStarted + Action + BuildFinished + BuildMetrics.
			if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, invID, uuid, "build", 1)); err != nil {
				t.Errorf("goroutine %d: BuildStarted error: %v", idx, err)
				return
			}
			if err := tb.ProcessOrderedBuildEvent(ctx, makeActionOBE(t, invID, "//pkg:lib", "Javac", 2, true)); err != nil {
				t.Errorf("goroutine %d: Action error: %v", idx, err)
				return
			}
			if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, invID, 3, 0, "SUCCESS")); err != nil {
				t.Errorf("goroutine %d: BuildFinished error: %v", idx, err)
				return
			}
			if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildMetricsOBE(t, invID, 4, 10000, 5000)); err != nil {
				t.Errorf("goroutine %d: BuildMetrics error: %v", idx, err)
				return
			}
		}(i)
	}

	wg.Wait()

	// Each invocation produces: 2 spans from BuildFinished (action + root) + 1 span from BuildMetrics = 3.
	expectedSpans := numGoroutines * 3
	if sink.SpanCount() != expectedSpans {
		t.Errorf("expected %d spans, got %d", expectedSpans, sink.SpanCount())
	}
}

func TestTraceBuilder_EventBeforeBuildStarted(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// Send various events for an unknown invocation — all should be no-ops.
	events := []*buildpb.OrderedBuildEvent{
		makeActionOBE(t, "inv-unknown", "//pkg:lib", "Javac", 1, true),
		makeTestResultOBE(t, "inv-unknown", "//pkg:test", 2, bep.TestStatus_PASSED),
		makeTargetConfiguredOBE(t, "inv-unknown", "//pkg:lib", 3),
		makeBuildFinishedOBE(t, "inv-unknown", 4, 0, "SUCCESS"),
		makeBuildMetricsOBE(t, "inv-unknown", 5, 10000, 5000),
	}

	for i, obe := range events {
		if err := tb.ProcessOrderedBuildEvent(ctx, obe); err != nil {
			t.Errorf("event %d: expected no error, got %v", i, err)
		}
	}

	if sink.SpanCount() != 0 {
		t.Errorf("expected 0 spans for unknown invocation, got %d", sink.SpanCount())
	}
}

func TestTraceBuilder_EventAfterStateCleanup(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// Full lifecycle: Start → Finish → Metrics (state deleted).
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-cleanup", "uuid-cleanup", "build", 1)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-cleanup", 2, 0, "SUCCESS")); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildMetricsOBE(t, "inv-cleanup", 3, 10000, 5000)); err != nil {
		t.Fatal(err)
	}

	spansBefore := sink.SpanCount()

	// Send more events for the same invocation — all should be no-ops.
	if err := tb.ProcessOrderedBuildEvent(ctx, makeActionOBE(t, "inv-cleanup", "//pkg:lib", "Javac", 4, true)); err != nil {
		t.Fatalf("expected no error for cleaned-up invocation, got %v", err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeTestResultOBE(t, "inv-cleanup", "//pkg:test", 5, bep.TestStatus_PASSED)); err != nil {
		t.Fatalf("expected no error for cleaned-up invocation, got %v", err)
	}

	if sink.SpanCount() != spansBefore {
		t.Errorf("expected no new spans after state cleanup, got %d new", sink.SpanCount()-spansBefore)
	}
}

func TestReapStale(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{
		InvocationTimeout: time.Nanosecond,
		ReaperInterval:    time.Nanosecond,
	})

	// Build invocation state directly with pending spans.
	state := newInvocationState(
		traceIDFromUUID("uuid-stale"),
		spanIDFromIdentity("uuid-stale", "root"),
		&bep.BuildStarted{Uuid: "uuid-stale", Command: "build"},
		time.Now().Add(-time.Hour),
		PIIConfig{},
		HighCardinalityCaps{},
		nil,
		SummaryConfig{},
	)
	span := state.appendSpan()
	span.SetSpanID(spanIDFromIdentity("uuid-stale", "action", "//pkg:lib", "Javac", ""))
	span.SetName("bazel.action")

	invocations := map[string]*invocationState{
		"inv-stale": state,
	}

	tb.reapStale(invocations, time.Now())

	if len(invocations) != 0 {
		t.Error("expected stale invocation to be reaped")
	}
	if sink.SpanCount() != 1 {
		t.Errorf("expected 1 span flushed by reaper, got %d", sink.SpanCount())
	}
}

func TestTraceIDFromUUID_NoCollisions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping collision test in short mode")
	}

	seen := make(map[pcommon.TraceID]string, 10000)
	for i := range 10000 {
		uuid := fmt.Sprintf("uuid-%d", i)
		tid := traceIDFromUUID(uuid)
		if prev, ok := seen[tid]; ok {
			t.Fatalf("collision: %q and %q both map to %v", prev, uuid, tid)
		}
		seen[tid] = uuid
	}
}

func TestSpanIDFromIdentity_Deterministic(t *testing.T) {
	cases := []struct {
		name     string
		partsA   []string
		partsB   []string
		wantSame bool
	}{
		{"same parts produce same ID", []string{"u", "root"}, []string{"u", "root"}, true},
		{"different role differs", []string{"u", "root"}, []string{"u", "target"}, false},
		{"different uuid differs", []string{"u1", "root"}, []string{"u2", "root"}, false},
		{"different label differs",
			[]string{"u", "target", "//a:b"},
			[]string{"u", "target", "//a:c"},
			false,
		},
		{"action identity differs by primary output",
			[]string{"u", "action", "//a:b", "Javac", "bazel-out/x"},
			[]string{"u", "action", "//a:b", "Javac", "bazel-out/y"},
			false,
		},
		{"test identity differs by attempt",
			[]string{"u", "test", "//a:b", "1", "0", "1"},
			[]string{"u", "test", "//a:b", "1", "0", "2"},
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := spanIDFromIdentity(tc.partsA...)
			b := spanIDFromIdentity(tc.partsB...)
			if tc.wantSame && a != b {
				t.Errorf("expected identical span IDs, got %x and %x", a, b)
			}
			if !tc.wantSame && a == b {
				t.Errorf("expected different span IDs, both were %x", a)
			}
		})
	}
}

func TestSpanIDFromIdentity_NoCollisions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping collision test in short mode")
	}

	seen := make(map[pcommon.SpanID]string, 10000)
	for i := range 10000 {
		identity := fmt.Sprintf("uuid-%d\x00role\x00label", i)
		sid := spanIDFromIdentity(identity)
		if prev, ok := seen[sid]; ok {
			t.Fatalf("collision: %q and %q both map to %x", prev, identity, sid)
		}
		seen[sid] = identity
	}
}

// TestSpanIDs_ReplayDeterministic replays an identical event stream through two
// independent TraceBuilders and asserts that every emitted span has the same
// name, SpanID, and ParentSpanID in both runs.
func TestSpanIDs_ReplayDeterministic(t *testing.T) {
	collect := func() []spanTuple {
		sink := new(consumertest.TracesSink)
		tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
		tb.Start()
		defer tb.Stop()
		ctx := context.Background()
		processEvents(ctx, t, tb,
			makeBuildStartedOBE(t, "inv-1", "uuid-xyz", "build", 1),
			makeTargetConfiguredOBE(t, "inv-1", "//pkg:lib", 2),
			makeActionOBE(t, "inv-1", "//pkg:lib", "Javac", 3, true),
			makeTestResultOBE(t, "inv-1", "//pkg:lib", 4, bep.TestStatus_PASSED),
			makeBuildFinishedOBE(t, "inv-1", 5, 0, "SUCCESS"),
			makeBuildMetricsOBE(t, "inv-1", 6, 100, 50),
		)
		return collectSpanTuples(sink)
	}

	run1 := collect()
	run2 := collect()

	if len(run1) == 0 {
		t.Fatal("expected at least one span")
	}
	if len(run1) != len(run2) {
		t.Fatalf("span count mismatch: %d vs %d", len(run1), len(run2))
	}
	for i := range run1 {
		if run1[i] != run2[i] {
			t.Errorf("span %d mismatch:\n  run1: %+v\n  run2: %+v", i, run1[i], run2[i])
		}
	}
}

type spanTuple struct {
	name     string
	spanID   pcommon.SpanID
	parentID pcommon.SpanID
}

func collectSpanTuples(sink *consumertest.TracesSink) []spanTuple {
	var out []spanTuple
	for _, traces := range sink.AllTraces() {
		for i := range traces.ResourceSpans().Len() {
			rs := traces.ResourceSpans().At(i)
			for j := range rs.ScopeSpans().Len() {
				ss := rs.ScopeSpans().At(j)
				for k := range ss.Spans().Len() {
					s := ss.Spans().At(k)
					out = append(out, spanTuple{
						name:     s.Name(),
						spanID:   s.SpanID(),
						parentID: s.ParentSpanID(),
					})
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].name != out[j].name {
			return out[i].name < out[j].name
		}
		return fmt.Sprintf("%x", out[i].spanID) < fmt.Sprintf("%x", out[j].spanID)
	})
	return out
}

func TestTargetKey_Uniqueness(t *testing.T) {
	tests := []struct {
		name              string
		labelA, configIDA string
		labelB, configIDB string
		wantEqual         bool
	}{
		{
			name:   "different configID",
			labelA: "//pkg:lib", configIDA: "cfg-1",
			labelB: "//pkg:lib", configIDB: "cfg-2",
			wantEqual: false,
		},
		{
			name:   "with vs without configID",
			labelA: "//pkg:lib", configIDA: "cfg-1",
			labelB: "//pkg:lib", configIDB: "",
			wantEqual: false,
		},
		{
			name:   "same label same configID",
			labelA: "//pkg:lib", configIDA: "cfg-1",
			labelB: "//pkg:lib", configIDB: "cfg-1",
			wantEqual: true,
		},
		{
			name:   "different labels",
			labelA: "//pkg:a", configIDA: "",
			labelB: "//pkg:b", configIDB: "",
			wantEqual: false,
		},
		{
			name:   "empty strings",
			labelA: "", configIDA: "",
			labelB: "", configIDB: "",
			wantEqual: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyA := targetKey(tt.labelA, tt.configIDA)
			keyB := targetKey(tt.labelB, tt.configIDB)
			got := keyA == keyB
			if got != tt.wantEqual {
				t.Errorf("targetKey(%q, %q) == targetKey(%q, %q): got %v, want %v",
					tt.labelA, tt.configIDA, tt.labelB, tt.configIDB, got, tt.wantEqual)
			}
		})
	}
}

func TestTraceBuilder_LogsEmitted(t *testing.T) {
	tracesSink := new(consumertest.TracesSink)
	logsSink := new(consumertest.LogsSink)
	tb := NewTraceBuilder(tracesSink, logsSink, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// Full lifecycle: Started → TargetConfigured → Action → TestResult → Finished → Metrics.
	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-log", "uuid-log", "test", 1),
		makeTargetConfiguredOBE(t, "inv-log", "//pkg:lib", 2),
		makeActionOBE(t, "inv-log", "//pkg:lib", "Javac", 3, true),
		makeTestResultOBE(t, "inv-log", "//pkg:test", 4, bep.TestStatus_PASSED),
		makeBuildFinishedOBE(t, "inv-log", 5, 0, "SUCCESS"),
		makeBuildMetricsOBE(t, "inv-log", 6, 10000, 5000),
	)

	// 6 events → 6 log records.
	require.Equal(t, 6, logsSink.LogRecordCount(), "expected 6 log records")

	// Verify event names in order.
	expectedEvents := []string{
		"bazel.build.started",
		"bazel.target.configured",
		"bazel.action.completed",
		"bazel.test.result",
		"bazel.build.finished",
		"bazel.build.metrics",
	}
	expectedTraceID := traceIDFromUUID("uuid-log")
	for i, logs := range logsSink.AllLogs() {
		lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)

		eventName, ok := lr.Attributes().Get("event.name")
		require.True(t, ok, "log record %d: missing event.name attribute", i)
		assert.Equal(t, expectedEvents[i], eventName.Str(), "log record %d: event.name", i)

		// All log records should have the invocation ID.
		invID, ok := lr.Attributes().Get("bazel.invocation_id")
		assert.True(t, ok, "log record %d: missing bazel.invocation_id", i)
		assert.Equal(t, "inv-log", invID.Str(), "log record %d: bazel.invocation_id", i)

		// All log records should have the trace ID for correlation.
		assert.Equal(t, expectedTraceID, lr.TraceID(), "log record %d: traceID", i)

		// Resource should have service.name=bazel.
		svc, ok := logs.ResourceLogs().At(0).Resource().Attributes().Get("service.name")
		assert.True(t, ok, "log record %d: missing service.name", i)
		assert.Equal(t, "bazel", svc.Str(), "log record %d: service.name", i)

		// Scope should be besreceiver.
		assert.Equal(t, "besreceiver", logs.ResourceLogs().At(0).ScopeLogs().At(0).Scope().Name(), "log record %d: scope", i)
	}

	// Traces should also be emitted normally.
	assert.NotZero(t, tracesSink.SpanCount(), "expected traces to be emitted alongside logs")
}

func TestTraceBuilder_LogsOnly(t *testing.T) {
	logsSink := new(consumertest.LogsSink)
	tb := NewTraceBuilder(nil, logsSink, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-lo", "uuid-lo", "build", 1)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-lo", 2, 0, "SUCCESS")); err != nil {
		t.Fatal(err)
	}

	// Logs should be emitted even with nil tracesConsumer.
	if logsSink.LogRecordCount() != 2 {
		t.Fatalf("expected 2 log records, got %d", logsSink.LogRecordCount())
	}
}

func TestTraceBuilder_TracesOnly(t *testing.T) {
	tracesSink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(tracesSink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-to", "uuid-to", "build", 1)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-to", 2, 0, "SUCCESS")); err != nil {
		t.Fatal(err)
	}

	// Traces should be emitted even with nil logsConsumer.
	if tracesSink.SpanCount() != 1 {
		t.Fatalf("expected 1 span, got %d", tracesSink.SpanCount())
	}
}

func TestTraceBuilder_LogSeverity(t *testing.T) {
	logsSink := new(consumertest.LogsSink)
	tb := NewTraceBuilder(nil, logsSink, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// Failed action should produce ERROR severity.
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-sev", "uuid-sev", "build", 1)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeActionOBE(t, "inv-sev", "//pkg:lib", "Javac", 2, false)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-sev", 3, 1, "BUILD_FAILURE")); err != nil {
		t.Fatal(err)
	}

	// 3 log records: started (INFO), action failed (ERROR), finished (ERROR).
	if logsSink.LogRecordCount() != 3 {
		t.Fatalf("expected 3 log records, got %d", logsSink.LogRecordCount())
	}

	allLogs := logsSink.AllLogs()

	// BuildStarted → INFO.
	startedLR := allLogs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	if startedLR.SeverityNumber() != plog.SeverityNumberInfo {
		t.Errorf("expected INFO severity for BuildStarted, got %d", startedLR.SeverityNumber())
	}

	// Failed action → ERROR.
	actionLR := allLogs[1].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	if actionLR.SeverityNumber() != plog.SeverityNumberError {
		t.Errorf("expected ERROR severity for failed action, got %d", actionLR.SeverityNumber())
	}

	// Failed build → ERROR.
	finishedLR := allLogs[2].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	if finishedLR.SeverityNumber() != plog.SeverityNumberError {
		t.Errorf("expected ERROR severity for failed build, got %d", finishedLR.SeverityNumber())
	}
}

func TestTraceBuilder_LogConsumerErrorDoesNotBreakTraces(t *testing.T) {
	tracesSink := new(consumertest.TracesSink)
	errLogsSink := consumertest.NewErr(errors.New("logs pipeline overloaded"))

	tb := NewTraceBuilder(tracesSink, errLogsSink, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// Full lifecycle — logs consumer always errors.
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-lerr", "uuid-lerr", "build", 1)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-lerr", 2, 0, "SUCCESS")); err != nil {
		t.Fatal(err)
	}

	// Traces should still be emitted despite log consumer failures.
	if tracesSink.SpanCount() != 1 {
		t.Fatalf("expected 1 span despite log consumer errors, got %d", tracesSink.SpanCount())
	}
}

func TestActionSpanName_EmptyMnemonic(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-em", "uuid-em", "build", 1)); err != nil {
		t.Fatal(err)
	}
	// Action with empty mnemonic.
	if err := tb.ProcessOrderedBuildEvent(ctx, makeActionOBE(t, "inv-em", "//pkg:lib", "", 2, true)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-em", 3, 0, "SUCCESS")); err != nil {
		t.Fatal(err)
	}

	spans := sink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	for i := range spans.Len() {
		if spans.At(i).Name() == "bazel.action" {
			return // Pass — no trailing space.
		}
	}
	// Collect names for diagnostics.
	var names []string
	for i := range spans.Len() {
		names = append(names, spans.At(i).Name())
	}
	t.Fatalf("expected 'bazel.action' span (no trailing space) in batch, got names: %v", names)
}

func TestBuildSpanName_EmptyCommand(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// BuildStarted with empty command.
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-ec", "uuid-ec", "", 1)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-ec", 2, 0, "SUCCESS")); err != nil {
		t.Fatal(err)
	}

	span := sink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	if span.Name() != "bazel.build" {
		t.Errorf("expected span name 'bazel.build' (no trailing space), got %q", span.Name())
	}
}

// --- Ordering assumption canary tests ---
//
// These tests document the current behavior when BEP events arrive out of the
// expected order. They serve as canaries: if a future Bazel version changes the
// event ordering contract, these tests pin exactly what breaks (or doesn't).

func TestTraceBuilder_ActionBeforeTargetConfigured(t *testing.T) {
	// When ActionExecuted arrives before TargetConfigured for the same target,
	// the action is initially parented to root, then reparented under the target
	// when TargetConfigured arrives (deferred reparenting).
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-ooo", "uuid-ooo", "build", 1)); err != nil {
		t.Fatal(err)
	}
	// Action arrives BEFORE its target is configured.
	if err := tb.ProcessOrderedBuildEvent(ctx, makeActionOBE(t, "inv-ooo", "//pkg:lib", "Javac", 2, true)); err != nil {
		t.Fatal(err)
	}
	// Target configured arrives AFTER the action — triggers reparenting.
	if err := tb.ProcessOrderedBuildEvent(ctx, makeTargetConfiguredOBE(t, "inv-ooo", "//pkg:lib", 3)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-ooo", 4, 0, "SUCCESS")); err != nil {
		t.Fatal(err)
	}

	// All 3 spans should be present: action, target, root.
	if sink.SpanCount() != 3 {
		t.Fatalf("expected 3 spans, got %d", sink.SpanCount())
	}

	spans := sink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	var actionSpan, targetSpan ptrace.Span
	for i := range spans.Len() {
		switch {
		case spans.At(i).Name() == "bazel.action Javac":
			actionSpan = spans.At(i)
		case spans.At(i).Name() == "bazel.target":
			targetSpan = spans.At(i)
		}
	}

	if actionSpan.Name() == "" || targetSpan.Name() == "" {
		t.Fatal("expected to find both action and target spans")
	}

	// After reparenting, the action should be a child of the target span,
	// not the root. This verifies deferred reparenting works correctly.
	if actionSpan.ParentSpanID() != targetSpan.SpanID() {
		t.Errorf("expected action parent to be target span %v (reparented), got %v",
			targetSpan.SpanID(), actionSpan.ParentSpanID())
	}
}

func TestTraceBuilder_TargetConfiguredAfterBuildFinished(t *testing.T) {
	// TargetConfigured arriving after BuildFinished should be emitted in a
	// standalone ptrace.Traces payload (same pattern as BuildMetrics) rather
	// than silently dropped. The late target span rides on the same traceID
	// and parents to the root span so downstream backends stitch the build
	// tree together.
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-late-tc", "uuid-late-tc", "build", 1)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-late-tc", 2, 0, "SUCCESS")); err != nil {
		t.Fatal(err)
	}

	preFlushBatches := len(sink.AllTraces())
	preFlushSpans := sink.SpanCount()

	// TargetConfigured arrives after flush — should emit a standalone batch
	// with the target span parented under the existing root span.
	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx, makeTargetConfiguredOBE(t, "inv-late-tc", "//pkg:lib", 3)))

	allTraces := sink.AllTraces()
	require.Len(t, allTraces, preFlushBatches+1, "expected a separate ConsumeTraces call for the late TargetConfigured")
	require.Equal(t, preFlushSpans+1, sink.SpanCount(), "expected exactly one new span for the late TargetConfigured")

	lateBatch := allTraces[len(allTraces)-1]
	lateSpans := lateBatch.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	require.Equal(t, 1, lateSpans.Len(), "late batch should carry exactly the target span")
	lateTarget := lateSpans.At(0)
	assert.Equal(t, "bazel.target", lateTarget.Name())
	label, ok := lateTarget.Attributes().Get("bazel.target.label")
	require.True(t, ok)
	assert.Equal(t, "//pkg:lib", label.Str())

	// Parent should be the root span of the original batch (deterministic
	// SpanID derived from the invocation uuid).
	rootSpanID := spanIDFromIdentity("uuid-late-tc", "root")
	assert.Equal(t, rootSpanID, lateTarget.ParentSpanID())

	// TraceID must match the flushed batch so the late span joins the trace.
	flushedRoot := findRootSpan(t, sink)
	assert.Equal(t, flushedRoot.TraceID(), lateTarget.TraceID())
}

func TestTraceBuilder_ActionAfterBuildFinishedBeforeMetrics(t *testing.T) {
	// ActionExecuted arriving after BuildFinished (but before BuildMetrics
	// cleans up state) should be emitted in a standalone ptrace.Traces
	// payload. If the action's target was already seen pre-flush, the late
	// span parents to the target span; otherwise it parents to the root.
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-late-act", "uuid-late-act", "build", 1)))
	// Include a TargetConfigured pre-flush so the late action has a resolvable parent.
	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx, makeTargetConfiguredOBE(t, "inv-late-act", "//pkg:lib", 2)))
	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-late-act", 3, 0, "SUCCESS")))

	preFlushBatches := len(sink.AllTraces())
	preFlushSpans := sink.SpanCount()

	// Action arrives after flush but before BuildMetrics.
	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx, makeActionOBE(t, "inv-late-act", "//pkg:lib", "Javac", 4, true)))

	allTraces := sink.AllTraces()
	require.Len(t, allTraces, preFlushBatches+1, "expected a separate ConsumeTraces call for the late action")
	require.Equal(t, preFlushSpans+1, sink.SpanCount(), "expected exactly one new span for the late action")

	lateBatch := allTraces[len(allTraces)-1]
	lateSpans := lateBatch.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	require.Equal(t, 1, lateSpans.Len())
	lateAction := lateSpans.At(0)
	assert.Equal(t, "bazel.action Javac", lateAction.Name())
	label, ok := lateAction.Attributes().Get("bazel.target.label")
	require.True(t, ok)
	assert.Equal(t, "//pkg:lib", label.Str())

	// Parent should be the target span from the flushed batch (same SpanID,
	// derived deterministically from the invocation uuid + label).
	expectedParent := spanIDFromIdentity("uuid-late-act", "target", "//pkg:lib")
	assert.Equal(t, expectedParent, lateAction.ParentSpanID(),
		"late action should parent into the pre-flush target span")

	// BuildMetrics should still work normally after the late action and
	// increment the span count by one more batch.
	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx, makeBuildMetricsOBE(t, "inv-late-act", 5, 10000, 5000)))
	require.Equal(t, preFlushSpans+2, sink.SpanCount(),
		"expected one additional span from BuildMetrics on top of the late action")
}

func TestTraceBuilder_TestResultAfterBuildFinishedBeforeMetrics(t *testing.T) {
	// TestResult arriving after BuildFinished (but before BuildMetrics)
	// should emit a standalone bazel.test span parented under the existing
	// target span when the target is known, mirroring the late-action path.
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-late-tr", "uuid-late-tr", "test", 1)))
	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx, makeTargetConfiguredOBE(t, "inv-late-tr", "//pkg:test", 2)))
	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-late-tr", 3, 0, "SUCCESS")))

	preFlushBatches := len(sink.AllTraces())
	preFlushSpans := sink.SpanCount()

	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx, makeTestResultOBE(t, "inv-late-tr", "//pkg:test", 4, bep.TestStatus_PASSED)))

	allTraces := sink.AllTraces()
	require.Len(t, allTraces, preFlushBatches+1)
	require.Equal(t, preFlushSpans+1, sink.SpanCount())

	lateSpans := allTraces[len(allTraces)-1].ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	require.Equal(t, 1, lateSpans.Len())
	lateTest := lateSpans.At(0)
	assert.Equal(t, "bazel.test", lateTest.Name())
	label, ok := lateTest.Attributes().Get("bazel.target.label")
	require.True(t, ok)
	assert.Equal(t, "//pkg:test", label.Str())

	expectedParent := spanIDFromIdentity("uuid-late-tr", "target", "//pkg:test")
	assert.Equal(t, expectedParent, lateTest.ParentSpanID())

	// A second late TestResult (e.g., retry attempt) should also emit.
	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx, makeTestResultOBE(t, "inv-late-tr", "//pkg:test", 5, bep.TestStatus_PASSED)))
	require.Equal(t, preFlushSpans+2, sink.SpanCount(),
		"each late TestResult should produce a separate span")
}

func TestTraceBuilder_LateActionWithoutKnownTarget(t *testing.T) {
	// When a late action arrives for a label whose TargetConfigured never
	// appeared pre-flush, the span parents to the root rather than being
	// dropped. No post-flush reparenting — orphan tracking is disabled once
	// the batch is emitted.
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-late-orphan", "uuid-late-orphan", "build", 1)))
	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-late-orphan", 2, 0, "SUCCESS")))

	preFlushSpans := sink.SpanCount()

	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx, makeActionOBE(t, "inv-late-orphan", "//unknown:lib", "Javac", 3, true)))

	require.Equal(t, preFlushSpans+1, sink.SpanCount())
	lateBatch := sink.AllTraces()[len(sink.AllTraces())-1]
	lateAction := lateBatch.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	rootSpanID := spanIDFromIdentity("uuid-late-orphan", "root")
	assert.Equal(t, rootSpanID, lateAction.ParentSpanID(),
		"late action with no known target falls back to the root span")
}

func TestTraceBuilder_CumulativeCountersAcrossInvocations(t *testing.T) {
	tracesSink := new(consumertest.TracesSink)
	metricsSink := new(consumertest.MetricsSink)
	tb := NewTraceBuilder(tracesSink, nil, metricsSink, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// First invocation.
	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-c1", "uuid-c1", "build", 1),
		makeBuildFinishedOBE(t, "inv-c1", 2, 0, "SUCCESS"),
		makeBuildMetricsOBE(t, "inv-c1", 3, 10000, 5000),
	)

	// Second invocation.
	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-c2", "uuid-c2", "test", 1),
		makeBuildFinishedOBE(t, "inv-c2", 2, 0, "SUCCESS"),
		makeBuildMetricsOBE(t, "inv-c2", 3, 20000, 8000),
	)

	// Should have 2 ConsumeMetrics calls (one per BuildMetrics event).
	allMetrics := metricsSink.AllMetrics()
	require.Len(t, allMetrics, 2, "expected 2 ConsumeMetrics calls")

	// Find the cumulative invocation count in the second batch — should be 2.
	lastMD := allMetrics[1]

	invCount, found := findMetricIntValue(lastMD, "bazel.invocation.count")
	require.True(t, found, "expected to find bazel.invocation.count metric in second batch")
	assert.Equal(t, int64(2), invCount, "expected invocation count=2 after two builds")

	if wallTime, found := findMetricIntValue(lastMD, "bazel.invocation.wall_time.total"); found {
		assert.Equal(t, int64(30000), wallTime, "expected total wall_time=30000")
	}
}

// TestActionBeforeTarget verifies that an action arriving before its target's
// TargetConfigured event is still correctly parented under the target span.
//
// In real Bazel BES streams, event ordering is by completion time, not
// dependency order. ActionExecuted can arrive before TargetConfigured.
func TestActionBeforeTarget(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// Event order: Started → Action → Target → Finished
	// The action for //pkg:lib arrives BEFORE TargetConfigured for //pkg:lib.
	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx,
		makeBuildStartedOBE(t, "inv-order", "uuid-order", "build", 1)))
	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx,
		makeActionOBE(t, "inv-order", "//pkg:lib", "Javac", 2, true)))
	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx,
		makeTargetConfiguredOBE(t, "inv-order", "//pkg:lib", 3)))
	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx,
		makeBuildFinishedOBE(t, "inv-order", 4, 0, "SUCCESS")))

	require.Equal(t, 3, sink.SpanCount(), "expected 3 spans: root + target + action")

	spans := collectSpans(t, sink)

	var actionSpan, targetSpan spanInfo
	for _, s := range spans {
		switch s.Name {
		case "bazel.action Javac":
			actionSpan = s
		case "bazel.target":
			targetSpan = s
		}
	}

	require.NotEmpty(t, actionSpan.Name, "action span not found")
	require.NotEmpty(t, targetSpan.Name, "target span not found")

	assert.Equal(t, targetSpan.SpanID, actionSpan.ParentSpanID,
		"action should be child of target even when action arrives first")
}

// TestTestResultBeforeTarget verifies that a test result arriving before its
// target's TargetConfigured event is reparented under the target span.
func TestTestResultBeforeTarget(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// Event order: Started → TestResult → Target → Finished
	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx,
		makeBuildStartedOBE(t, "inv-test-order", "uuid-test-order", "test", 1)))
	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx,
		makeTestResultOBE(t, "inv-test-order", "//pkg:test", 2, bep.TestStatus_PASSED)))
	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx,
		makeTargetConfiguredOBE(t, "inv-test-order", "//pkg:test", 3)))
	require.NoError(t, tb.ProcessOrderedBuildEvent(ctx,
		makeBuildFinishedOBE(t, "inv-test-order", 4, 0, "SUCCESS")))

	require.Equal(t, 3, sink.SpanCount(), "expected 3 spans: root + target + test")

	spans := collectSpans(t, sink)

	var testSpan, targetSpan spanInfo
	for _, s := range spans {
		switch s.Name {
		case "bazel.test":
			testSpan = s
		case "bazel.target":
			targetSpan = s
		}
	}

	require.NotEmpty(t, testSpan.Name, "test span not found")
	require.NotEmpty(t, targetSpan.Name, "target span not found")

	assert.Equal(t, targetSpan.SpanID, testSpan.ParentSpanID,
		"test should be child of target even when test result arrives first")
}

// TestReparent_CounterMetric verifies that bes.events.reparented is
// incremented by one per action/test span reparented onto a late-arriving
// target span, and is not incremented when events arrive in order.
func TestReparent_CounterMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{
		MeterProvider: mp,
	})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// Mixed: one action and one test both arrive before their target, a
	// second target arrives in-order (no reparent). Expected counter = 2.
	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-reparent", "uuid-reparent", "build", 1),
		// Out-of-order: action + test arrive before //pkg:lib.
		makeActionOBE(t, "inv-reparent", "//pkg:lib", "Javac", 2, true),
		makeTestResultOBE(t, "inv-reparent", "//pkg:lib", 3, bep.TestStatus_PASSED),
		makeTargetConfiguredOBE(t, "inv-reparent", "//pkg:lib", 4),
		// In-order: target arrives before its action.
		makeTargetConfiguredOBE(t, "inv-reparent", "//pkg:other", 5),
		makeActionOBE(t, "inv-reparent", "//pkg:other", "GoCompile", 6, true),
		makeBuildFinishedOBE(t, "inv-reparent", 7, 0, "SUCCESS"),
	)

	assert.Equal(t, int64(2), readCounter(t, reader, "bes.events.reparented"),
		"expected 2 reparented spans (one action + one test for //pkg:lib)")
}

// TestReparent_CounterNotIncrementedWhenInOrder verifies that the
// bes.events.reparented counter stays at zero for fully in-order streams.
func TestReparent_CounterNotIncrementedWhenInOrder(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{
		MeterProvider: mp,
	})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-inorder", "uuid-inorder", "build", 1),
		makeTargetConfiguredOBE(t, "inv-inorder", "//pkg:lib", 2),
		makeActionOBE(t, "inv-inorder", "//pkg:lib", "Javac", 3, true),
		makeBuildFinishedOBE(t, "inv-inorder", 4, 0, "SUCCESS"),
	)

	assert.Equal(t, int64(0), readCounter(t, reader, "bes.events.reparented"),
		"in-order streams must not increment the reparent counter")
}

// TestReparent_OrphanAtFinalize covers the case where an action arrives
// before its target and TargetConfigured never arrives. The action span
// stays parented to root (explicit orphan), no reparenting happens, and
// the counter is not incremented.
func TestReparent_OrphanAtFinalize(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{
		MeterProvider: mp,
	})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// Action arrives, then BuildFinished — no TargetConfigured ever.
	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-orphan", "uuid-orphan", "build", 1),
		makeActionOBE(t, "inv-orphan", "//pkg:lib", "Javac", 2, true),
		makeBuildFinishedOBE(t, "inv-orphan", 3, 0, "SUCCESS"),
	)

	require.Equal(t, 2, sink.SpanCount(), "expected 2 spans: root + orphaned action")

	spans := collectSpans(t, sink)
	var actionSpan, rootSpan spanInfo
	for _, s := range spans {
		switch {
		case s.Name == "bazel.action Javac":
			actionSpan = s
		case strings.HasPrefix(s.Name, "bazel.build"):
			rootSpan = s
		}
	}
	require.NotEmpty(t, actionSpan.Name, "action span not found")
	require.NotEmpty(t, rootSpan.Name, "root span not found")

	assert.Equal(t, rootSpan.SpanID, actionSpan.ParentSpanID,
		"orphan action without TargetConfigured must stay parented to root")
	assert.Equal(t, int64(0), readCounter(t, reader, "bes.events.reparented"),
		"orphan at finalize must not increment reparent counter")
}

// readCounter reads the int sum value of a counter metric by name from a
// ManualReader. Returns 0 if the metric is not present.
func readCounter(t testing.TB, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %s is not Sum[int64]: %T", name, m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	return 0
}

// --- Aborted event tests (issue #20) ---

// findRootSpan returns the single bazel.build span from a sink, failing the
// test if zero or more than one match.
func findRootSpan(t *testing.T, sink *consumertest.TracesSink) ptrace.Span {
	t.Helper()
	var match ptrace.Span
	var found int
	for _, tr := range sink.AllTraces() {
		for i := range tr.ResourceSpans().Len() {
			rs := tr.ResourceSpans().At(i)
			for j := range rs.ScopeSpans().Len() {
				ss := rs.ScopeSpans().At(j)
				for k := range ss.Spans().Len() {
					s := ss.Spans().At(k)
					if s.Name() == "bazel.build" || strings.HasPrefix(s.Name(), "bazel.build ") {
						match = s
						found++
					}
				}
			}
		}
	}
	require.Equal(t, 1, found, "expected exactly one bazel.build span")
	return match
}

func TestTraceBuilder_Aborted_StampsRootSpan(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-abort-1", "uuid-abort-1", "build", 1),
		makeAbortedOBE(t, "inv-abort-1", 2, bep.Aborted_USER_INTERRUPTED, "ctrl-c", nil),
		makeBuildFinishedOBE(t, "inv-abort-1", 3, 8, "INTERRUPTED"),
	)

	span := findRootSpan(t, sink)

	reason, ok := span.Attributes().Get("bazel.abort.reason")
	require.True(t, ok, "missing bazel.abort.reason")
	assert.Equal(t, "user_interrupted", reason.Str())

	description, ok := span.Attributes().Get("bazel.abort.description")
	require.True(t, ok, "missing bazel.abort.description")
	assert.Equal(t, "ctrl-c", description.Str())

	exitName, ok := span.Attributes().Get("bazel.exit_code.name")
	require.True(t, ok, "missing bazel.exit_code.name")
	assert.Equal(t, "INTERRUPTED", exitName.Str(), "exit code attrs preserved alongside abort")

	assert.Equal(t, ptrace.StatusCodeError, span.Status().Code())
	assert.Equal(t, "aborted: user_interrupted: ctrl-c", span.Status().Message())
}

func TestTraceBuilder_Aborted_OnlyFirstRecorded(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-abort-2", "uuid-abort-2", "build", 1),
		makeAbortedOBE(t, "inv-abort-2", 2, bep.Aborted_OUT_OF_MEMORY, "heap", nil),
		makeAbortedOBE(t, "inv-abort-2", 3, bep.Aborted_INTERNAL, "crash", nil),
		makeBuildFinishedOBE(t, "inv-abort-2", 4, 37, "OOM_ERROR"),
	)

	span := findRootSpan(t, sink)

	reason, _ := span.Attributes().Get("bazel.abort.reason")
	assert.Equal(t, "out_of_memory", reason.Str(), "first abort reason preserved")

	description, _ := span.Attributes().Get("bazel.abort.description")
	assert.Equal(t, "heap", description.Str(), "first abort description preserved")
}

func TestTraceBuilder_Aborted_WithoutBuildFinished(t *testing.T) {
	// Exercise the reaper path directly: an invocation that recorded an
	// abort but never saw BuildFinished must still emit the root span when
	// flushOrphaned is invoked.
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{
		InvocationTimeout: time.Nanosecond,
	})

	state := newInvocationState(
		traceIDFromUUID("uuid-abort-3"),
		spanIDFromIdentity("uuid-abort-3", "root"),
		&bep.BuildStarted{Uuid: "uuid-abort-3", Command: "build"},
		time.Now().Add(-time.Hour),
		PIIConfig{},
		HighCardinalityCaps{},
		nil,
		SummaryConfig{},
	)
	state.recordAbort(&bep.Aborted{Reason: bep.Aborted_TIME_OUT, Description: "deadline"})

	invocations := map[string]*invocationState{"inv-abort-3": state}
	tb.reapStale(invocations, time.Now())

	assert.Empty(t, invocations, "expected invocation to be reaped")

	span := findRootSpan(t, sink)
	reason, ok := span.Attributes().Get("bazel.abort.reason")
	require.True(t, ok, "missing bazel.abort.reason on reaped aborted invocation")
	assert.Equal(t, "time_out", reason.Str())

	_, hasExit := span.Attributes().Get("bazel.exit_code.name")
	assert.False(t, hasExit, "exit code should not be present without BuildFinished")

	assert.Equal(t, ptrace.StatusCodeError, span.Status().Code())
}

func TestTraceBuilder_BuildFinished_NonAborted_NoAbortAttrs(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-no-abort", "uuid-no-abort", "build", 1),
		makeBuildFinishedOBE(t, "inv-no-abort", 2, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)

	_, hasReason := span.Attributes().Get("bazel.abort.reason")
	assert.False(t, hasReason, "non-aborted build should not have abort reason")
	_, hasDesc := span.Attributes().Get("bazel.abort.description")
	assert.False(t, hasDesc, "non-aborted build should not have abort description")
	assert.Equal(t, ptrace.StatusCodeUnset, span.Status().Code())
}

func TestTraceBuilder_Aborted_OnArbitraryEventID(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// Abort rides on a TargetConfigured event ID instead of BuildFinished.
	targetID := &bep.BuildEventId{
		Id: &bep.BuildEventId_TargetConfigured{
			TargetConfigured: &bep.BuildEventId_TargetConfiguredId{Label: "//pkg:lib"},
		},
	}

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-abort-4", "uuid-abort-4", "build", 1),
		makeAbortedOBE(t, "inv-abort-4", 2, bep.Aborted_LOADING_FAILURE, "bad BUILD file", targetID),
		makeBuildFinishedOBE(t, "inv-abort-4", 3, 1, "FAILURE"),
	)

	span := findRootSpan(t, sink)
	reason, ok := span.Attributes().Get("bazel.abort.reason")
	require.True(t, ok)
	assert.Equal(t, "loading_failure", reason.Str())
}

func TestAbortReasonString_AllEnumValues(t *testing.T) {
	cases := []struct {
		reason bep.Aborted_AbortReason
		want   string
	}{
		{bep.Aborted_UNKNOWN, "unknown"},
		{bep.Aborted_USER_INTERRUPTED, "user_interrupted"},
		{bep.Aborted_NO_ANALYZE, "no_analyze"},
		{bep.Aborted_NO_BUILD, "no_build"},
		{bep.Aborted_TIME_OUT, "time_out"},
		{bep.Aborted_REMOTE_ENVIRONMENT_FAILURE, "remote_environment_failure"},
		{bep.Aborted_INTERNAL, "internal"},
		{bep.Aborted_LOADING_FAILURE, "loading_failure"},
		{bep.Aborted_ANALYSIS_FAILURE, "analysis_failure"},
		{bep.Aborted_SKIPPED, "skipped"},
		{bep.Aborted_INCOMPLETE, "incomplete"},
		{bep.Aborted_OUT_OF_MEMORY, "out_of_memory"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			assert.Equal(t, tc.want, abortReasonString(tc.reason))
		})
	}
}

// --- WorkspaceStatus + BuildMetadata tests (issue #22) ---

func TestWorkspaceStatusAttributesOnRootSpan(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{
		PII: PIIConfig{IncludeWorkspaceStatus: true},
	})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-ws-1", "uuid-ws-1", "build", 1),
		makeWorkspaceStatusOBE(t, "inv-ws-1", 2, map[string]string{
			"BUILD_SCM_REVISION": "abc123",
			"STABLE_GIT_BRANCH":  "main",
		}),
		makeBuildFinishedOBE(t, "inv-ws-1", 3, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)

	rev, ok := span.Attributes().Get("bazel.workspace.build_scm_revision")
	require.True(t, ok, "missing bazel.workspace.build_scm_revision")
	assert.Equal(t, "abc123", rev.Str())

	branch, ok := span.Attributes().Get("bazel.workspace.stable_git_branch")
	require.True(t, ok, "missing bazel.workspace.stable_git_branch")
	assert.Equal(t, "main", branch.Str())
}

func TestBuildMetadataAttributesOnRootSpan(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{
		PII: PIIConfig{IncludeBuildMetadata: true},
	})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-md-1", "uuid-md-1", "build", 1),
		makeBuildMetadataOBE(t, "inv-md-1", 2, map[string]string{
			"ci_pipeline_id": "pipeline-42",
			"PR Number":      "123",
		}),
		makeBuildFinishedOBE(t, "inv-md-1", 3, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)

	pipeline, ok := span.Attributes().Get("bazel.metadata.ci_pipeline_id")
	require.True(t, ok)
	assert.Equal(t, "pipeline-42", pipeline.Str())

	pr, ok := span.Attributes().Get("bazel.metadata.pr_number")
	require.True(t, ok, "expected sanitized 'PR Number' → pr_number")
	assert.Equal(t, "123", pr.Str())
}

func TestWorkspaceItemsCappedAt30(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{
		PII: PIIConfig{IncludeWorkspaceStatus: true},
	})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	items := make(map[string]string, 50)
	for i := range 50 {
		items[fmt.Sprintf("KEY_%03d", i)] = fmt.Sprintf("val-%d", i)
	}

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-ws-cap", "uuid-ws-cap", "build", 1),
		makeWorkspaceStatusOBE(t, "inv-ws-cap", 2, items),
		makeBuildFinishedOBE(t, "inv-ws-cap", 3, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)
	count := 0
	span.Attributes().Range(func(k string, _ pcommon.Value) bool {
		if strings.HasPrefix(k, "bazel.workspace.") {
			count++
		}
		return true
	})
	assert.Equal(t, maxWorkspaceItems, count)
}

func TestBuildMetadataCappedAt20(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{
		PII: PIIConfig{IncludeBuildMetadata: true},
	})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	entries := make(map[string]string, 40)
	for i := range 40 {
		entries[fmt.Sprintf("key_%03d", i)] = fmt.Sprintf("val-%d", i)
	}

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-md-cap", "uuid-md-cap", "build", 1),
		makeBuildMetadataOBE(t, "inv-md-cap", 2, entries),
		makeBuildFinishedOBE(t, "inv-md-cap", 3, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)
	count := 0
	span.Attributes().Range(func(k string, _ pcommon.Value) bool {
		if strings.HasPrefix(k, "bazel.metadata.") {
			count++
		}
		return true
	})
	assert.Equal(t, maxBuildMetadataEntries, count)
}

func TestAttributeValueTruncatedAt256Chars(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{
		PII: PIIConfig{IncludeWorkspaceStatus: true},
	})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	bigVal := strings.Repeat("x", 500)
	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-trunc", "uuid-trunc", "build", 1),
		makeWorkspaceStatusOBE(t, "inv-trunc", 2, map[string]string{"BIG": bigVal}),
		makeBuildFinishedOBE(t, "inv-trunc", 3, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)
	got, ok := span.Attributes().Get("bazel.workspace.big")
	require.True(t, ok)
	assert.Len(t, got.Str(), maxAttrValueLen)
}

func TestMissingWorkspaceAndMetadataEventsDoNotBreakTraces(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-nows", "uuid-nows", "build", 1),
		makeBuildFinishedOBE(t, "inv-nows", 2, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)

	// No bazel.workspace.* or bazel.metadata.* should be present.
	span.Attributes().Range(func(k string, _ pcommon.Value) bool {
		if strings.HasPrefix(k, "bazel.workspace.") || strings.HasPrefix(k, "bazel.metadata.") {
			t.Errorf("unexpected context attribute on root span: %s", k)
		}
		return true
	})
}

func TestWorkspaceEventArrivingAfterFinishedIsIgnored(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-post", "uuid-post", "build", 1),
		makeBuildFinishedOBE(t, "inv-post", 2, 0, "SUCCESS"),
		// WorkspaceStatus after finalize — should be ignored.
		makeWorkspaceStatusOBE(t, "inv-post", 3, map[string]string{"LATE_KEY": "late_val"}),
	)

	span := findRootSpan(t, sink)
	_, has := span.Attributes().Get("bazel.workspace.late_key")
	assert.False(t, has, "workspace events after BuildFinished must not mutate the emitted root span")
}

// --- Target lifecycle tests (issue #19) ---

// findTargetSpan returns the sole bazel.target span with the given label,
// failing the test if zero or more than one match.
func findTargetSpan(t *testing.T, sink *consumertest.TracesSink, label string) ptrace.Span {
	t.Helper()
	var match ptrace.Span
	var found int
	for _, tr := range sink.AllTraces() {
		for i := range tr.ResourceSpans().Len() {
			rs := tr.ResourceSpans().At(i)
			for j := range rs.ScopeSpans().Len() {
				ss := rs.ScopeSpans().At(j)
				for k := range ss.Spans().Len() {
					s := ss.Spans().At(k)
					if s.Name() != "bazel.target" {
						continue
					}
					got, ok := s.Attributes().Get("bazel.target.label")
					if ok && got.Str() == label {
						match = s
						found++
					}
				}
			}
		}
	}
	require.Equal(t, 1, found, "expected exactly one bazel.target span for %q", label)
	return match
}

func TestTraceBuilder_TargetComplete_Success(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-tc-1", "uuid-tc-1", "build", 1),
		makeTargetConfiguredOBE(t, "inv-tc-1", "//pkg:lib", 2),
		makeTargetCompleteOBE(t, "inv-tc-1", "//pkg:lib", 3, true, 0, "", 2),
		makeBuildFinishedOBE(t, "inv-tc-1", 4, 0, "SUCCESS"),
	)

	span := findTargetSpan(t, sink, "//pkg:lib")
	success, ok := span.Attributes().Get("bazel.target.success")
	require.True(t, ok)
	assert.True(t, success.Bool())

	ogCount, ok := span.Attributes().Get("bazel.target.output_group_count")
	require.True(t, ok)
	assert.Equal(t, int64(2), ogCount.Int())

	assert.Equal(t, ptrace.StatusCodeUnset, span.Status().Code())
}

func TestTraceBuilder_TargetComplete_Failure(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-tc-2", "uuid-tc-2", "build", 1),
		makeTargetConfiguredOBE(t, "inv-tc-2", "//pkg:lib", 2),
		makeTargetCompleteOBE(t, "inv-tc-2", "//pkg:lib", 3, false, 0, "compilation failed", 0),
		makeBuildFinishedOBE(t, "inv-tc-2", 4, 1, "FAILURE"),
	)

	span := findTargetSpan(t, sink, "//pkg:lib")
	success, _ := span.Attributes().Get("bazel.target.success")
	assert.False(t, success.Bool())

	detail, ok := span.Attributes().Get("bazel.target.failure_detail")
	require.True(t, ok)
	assert.Equal(t, "compilation failed", detail.Str())

	assert.Equal(t, ptrace.StatusCodeError, span.Status().Code())
	assert.Equal(t, "compilation failed", span.Status().Message())
}

func TestTraceBuilder_TargetComplete_TestTimeout(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-tc-3", "uuid-tc-3", "build", 1),
		makeTargetConfiguredOBE(t, "inv-tc-3", "//pkg:test", 2),
		makeTargetCompleteOBE(t, "inv-tc-3", "//pkg:test", 3, true, 60*time.Second, "", 0),
		makeBuildFinishedOBE(t, "inv-tc-3", 4, 0, "SUCCESS"),
	)

	span := findTargetSpan(t, sink, "//pkg:test")
	timeout, ok := span.Attributes().Get("bazel.target.test_timeout_s")
	require.True(t, ok)
	assert.Equal(t, 60.0, timeout.Double())
}

func TestTraceBuilder_TestSummary(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-ts-1", "uuid-ts-1", "test", 1),
		makeTargetConfiguredOBE(t, "inv-ts-1", "//pkg:test", 2),
		makeTestSummaryOBE(t, "inv-ts-1", "//pkg:test", 3, bep.TestStatus_FAILED, 6, 3, 2, 1500*time.Millisecond),
		makeBuildFinishedOBE(t, "inv-ts-1", 4, 1, "FAILURE"),
	)

	span := findTargetSpan(t, sink, "//pkg:test")

	status, ok := span.Attributes().Get("bazel.target.test.overall_status")
	require.True(t, ok)
	assert.Equal(t, "FAILED", status.Str())

	runCount, ok := span.Attributes().Get("bazel.target.test.total_run_count")
	require.True(t, ok)
	assert.Equal(t, int64(6), runCount.Int())

	shards, _ := span.Attributes().Get("bazel.target.test.shard_count")
	assert.Equal(t, int64(3), shards.Int())

	cached, _ := span.Attributes().Get("bazel.target.test.total_num_cached")
	assert.Equal(t, int64(2), cached.Int())

	dur, ok := span.Attributes().Get("bazel.target.test.total_run_duration_ms")
	require.True(t, ok)
	assert.Equal(t, int64(1500), dur.Int())
}

func TestTraceBuilder_TargetComplete_WithoutConfigured(t *testing.T) {
	// TargetComplete arriving without a prior TargetConfigured is a no-op.
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-tc-nc", "uuid-tc-nc", "build", 1),
		makeTargetCompleteOBE(t, "inv-tc-nc", "//pkg:ghost", 2, false, 0, "boom", 0),
		makeBuildFinishedOBE(t, "inv-tc-nc", 3, 1, "FAILURE"),
	)

	// No bazel.target span should exist (no TargetConfigured emitted it).
	for _, tr := range sink.AllTraces() {
		for i := range tr.ResourceSpans().Len() {
			rs := tr.ResourceSpans().At(i)
			for j := range rs.ScopeSpans().Len() {
				ss := rs.ScopeSpans().At(j)
				for k := range ss.Spans().Len() {
					assert.NotEqual(t, "bazel.target", ss.Spans().At(k).Name(),
						"no target span should exist without TargetConfigured")
				}
			}
		}
	}
}

func TestTraceBuilder_TestSummary_WithoutConfigured(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-ts-nc", "uuid-ts-nc", "test", 1),
		makeTestSummaryOBE(t, "inv-ts-nc", "//pkg:ghost", 2, bep.TestStatus_PASSED, 1, 1, 0, 0),
		makeBuildFinishedOBE(t, "inv-ts-nc", 3, 0, "SUCCESS"),
	)

	// No target span, no panic, trace still emits the root span.
	span := findRootSpan(t, sink)
	assert.NotNil(t, span)
}

func TestTraceBuilder_NoTargetComplete_LeavesTargetAttrsMinimal(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// TargetConfigured without matching TargetComplete (e.g. aborted build).
	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-no-tc", "uuid-no-tc", "build", 1),
		makeTargetConfiguredOBE(t, "inv-no-tc", "//pkg:abort", 2),
		makeBuildFinishedOBE(t, "inv-no-tc", 3, 1, "FAILURE"),
	)

	span := findTargetSpan(t, sink, "//pkg:abort")
	_, hasSuccess := span.Attributes().Get("bazel.target.success")
	assert.False(t, hasSuccess, "target without TargetComplete should not have success attr")
	assert.Equal(t, ptrace.StatusCodeUnset, span.Status().Code())
}

func TestTraceBuilder_NoTestSummary_LeavesTestAttrsAbsent(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// Non-test target path: TargetConfigured + TargetComplete, no TestSummary.
	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-no-ts", "uuid-no-ts", "build", 1),
		makeTargetConfiguredOBE(t, "inv-no-ts", "//pkg:lib", 2),
		makeTargetCompleteOBE(t, "inv-no-ts", "//pkg:lib", 3, true, 0, "", 1),
		makeBuildFinishedOBE(t, "inv-no-ts", 4, 0, "SUCCESS"),
	)

	span := findTargetSpan(t, sink, "//pkg:lib")
	span.Attributes().Range(func(k string, _ pcommon.Value) bool {
		assert.False(t, strings.HasPrefix(k, "bazel.target.test."),
			"non-test target should not have bazel.target.test.* attrs; got %s", k)
		return true
	})
}

// --- Build configuration / command line context tests (issue #21) ---

func TestTraceBuilder_Configuration_StampsTargetConfigAttrs(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-cfg-1", "uuid-cfg-1", "build", 1),
		makeConfigurationOBE(t, "inv-cfg-1", "k8-opt", "k8-opt", "linux-x86_64", "k8", false, 2),
		makeTargetConfiguredWithConfigOBE(t, "inv-cfg-1", "//pkg:lib", "k8-opt", 3),
		makeBuildFinishedOBE(t, "inv-cfg-1", 4, 0, "SUCCESS"),
	)

	span := findTargetSpan(t, sink, "//pkg:lib")

	mnemonic, ok := span.Attributes().Get("bazel.target.config.mnemonic")
	require.True(t, ok)
	assert.Equal(t, "k8-opt", mnemonic.Str())

	platform, ok := span.Attributes().Get("bazel.target.config.platform")
	require.True(t, ok)
	assert.Equal(t, "linux-x86_64", platform.Str())

	cpu, ok := span.Attributes().Get("bazel.target.config.cpu")
	require.True(t, ok)
	assert.Equal(t, "k8", cpu.Str())

	isTool, ok := span.Attributes().Get("bazel.target.config.is_tool")
	require.True(t, ok)
	assert.False(t, isTool.Bool())
}

func TestTraceBuilder_Configuration_ArrivesAfterTarget(t *testing.T) {
	// Documents the no-back-patching contract: if Configuration arrives after
	// the TargetConfigured it serves, the target span will lack config attrs.
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-cfg-late", "uuid-cfg-late", "build", 1),
		makeTargetConfiguredWithConfigOBE(t, "inv-cfg-late", "//pkg:lib", "k8-opt", 2),
		makeConfigurationOBE(t, "inv-cfg-late", "k8-opt", "k8-opt", "linux-x86_64", "k8", false, 3),
		makeBuildFinishedOBE(t, "inv-cfg-late", 4, 0, "SUCCESS"),
	)

	span := findTargetSpan(t, sink, "//pkg:lib")
	_, hasMnemonic := span.Attributes().Get("bazel.target.config.mnemonic")
	assert.False(t, hasMnemonic, "late-arriving Configuration is not back-patched")
}

func TestTraceBuilder_Configuration_MissingConfigDoesNotBreak(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-cfg-miss", "uuid-cfg-miss", "build", 1),
		makeTargetConfiguredWithConfigOBE(t, "inv-cfg-miss", "//pkg:lib", "never-sent", 2),
		makeBuildFinishedOBE(t, "inv-cfg-miss", 3, 0, "SUCCESS"),
	)

	span := findTargetSpan(t, sink, "//pkg:lib")
	_, hasMnemonic := span.Attributes().Get("bazel.target.config.mnemonic")
	assert.False(t, hasMnemonic)
}

func TestTraceBuilder_Configuration_NoneConfigIgnored(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-cfg-none", "uuid-cfg-none", "build", 1),
		// A "none" child config id should be treated as no config.
		makeConfigurationOBE(t, "inv-cfg-none", "none", "phantom", "phantom-platform", "phantom", false, 2),
		makeTargetConfiguredWithConfigOBE(t, "inv-cfg-none", "//pkg:lib", "none", 3),
		makeBuildFinishedOBE(t, "inv-cfg-none", 4, 0, "SUCCESS"),
	)

	span := findTargetSpan(t, sink, "//pkg:lib")
	_, hasMnemonic := span.Attributes().Get("bazel.target.config.mnemonic")
	assert.False(t, hasMnemonic, "'none' config id should never stamp attrs")
}

func TestTraceBuilder_OptionsParsed_RootAttrs(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-op", "uuid-op", "build", 1),
		makeOptionsParsedOBE(t, "inv-op",
			"gazelle-ci",
			[]string{"--output_base=/tmp/x"},
			[]string{"--config=ci", "//..."},
			2,
		),
		makeBuildFinishedOBE(t, "inv-op", 3, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)

	tag, ok := span.Attributes().Get("bazel.tool_tag")
	require.True(t, ok)
	assert.Equal(t, "gazelle-ci", tag.Str())

	startup, ok := span.Attributes().Get("bazel.options.startup_count")
	require.True(t, ok)
	assert.Equal(t, int64(1), startup.Int())

	cmd, ok := span.Attributes().Get("bazel.options.command_count")
	require.True(t, ok)
	assert.Equal(t, int64(2), cmd.Int())
}

func TestTraceBuilder_StructuredCommandLine_OriginalOnly(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{
		PII: PIIConfig{IncludeCommandLine: true},
	})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	originalSections := []*commandline.CommandLineSection{
		chunkSection("bazel"),
		optionSection("--foo=bar"),
		chunkSection("build"),
		chunkSection("//..."),
	}
	canonicalSections := []*commandline.CommandLineSection{
		chunkSection("bazel"),
		optionSection("--foo=canonical"),
		chunkSection("build"),
		chunkSection("//..."),
	}

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-cmd", "uuid-cmd", "build", 1),
		makeStructuredCommandLineOBE(t, "inv-cmd", "original", originalSections, 2),
		makeStructuredCommandLineOBE(t, "inv-cmd", "canonical", canonicalSections, 3),
		makeBuildFinishedOBE(t, "inv-cmd", 4, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)
	cmd, ok := span.Attributes().Get("bazel.command_line")
	require.True(t, ok)
	assert.Equal(t, "bazel --foo=bar build //...", cmd.Str())
	assert.NotContains(t, cmd.Str(), "canonical", "canonical variant must not leak into output")
}

// --- PII controls (issue #14) ---

// makePIIRichBuildStartedOBE emits a BuildStarted event populated with every
// PII-gated field so the matrix tests can assert each flag individually.
func makePIIRichBuildStartedOBE(t *testing.T, invID string, seqNum int64) *buildpb.OrderedBuildEvent {
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_Started{Started: &bep.BuildEventId_BuildStartedId{}},
		},
		Payload: &bep.BuildEvent_Started{
			Started: &bep.BuildStarted{
				Uuid:               "uuid-pii",
				Command:            "build",
				StartTime:          &timestamppb.Timestamp{Seconds: 1700000000},
				Host:               "ci-runner-42",
				User:               "jenkins",
				WorkspaceDirectory: "/home/jenkins/workspace/foo",
				WorkingDirectory:   "/home/jenkins/workspace/foo/subdir",
				BuildToolVersion:   "7.0.0",
			},
		},
	})
}

// makePIIRichActionOBE emits an ActionExecuted with the gated PII fields set.
func makePIIRichActionOBE(t *testing.T, invID string, seqNum int64) *buildpb.OrderedBuildEvent {
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_ActionCompleted{
				ActionCompleted: &bep.BuildEventId_ActionCompletedId{Label: "//pkg:lib"},
			},
		},
		Payload: &bep.BuildEvent_Action{
			Action: &bep.ActionExecuted{
				Success:     true,
				Type:        "Javac",
				CommandLine: []string{"javac", "-d", "out", "Foo.java"},
				PrimaryOutput: &bep.File{
					Name: "pkg/Foo.class",
				},
				StartTime: &timestamppb.Timestamp{Seconds: 1700000001},
				EndTime:   &timestamppb.Timestamp{Seconds: 1700000002},
			},
		},
	})
}

func findActionSpan(t *testing.T, sink *consumertest.TracesSink, label string) ptrace.Span {
	t.Helper()
	var match ptrace.Span
	var found int
	for _, tr := range sink.AllTraces() {
		for i := range tr.ResourceSpans().Len() {
			rs := tr.ResourceSpans().At(i)
			for j := range rs.ScopeSpans().Len() {
				ss := rs.ScopeSpans().At(j)
				for k := range ss.Spans().Len() {
					s := ss.Spans().At(k)
					if !strings.HasPrefix(s.Name(), "bazel.action") {
						continue
					}
					got, ok := s.Attributes().Get("bazel.target.label")
					if ok && got.Str() == label {
						match = s
						found++
					}
				}
			}
		}
	}
	require.Equal(t, 1, found, "expected exactly one bazel.action span for %q", label)
	return match
}

func runPIITestStream(t *testing.T, pii PIIConfig) *consumertest.TracesSink {
	t.Helper()
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{PII: pii})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makePIIRichBuildStartedOBE(t, "inv-pii", 1),
		makeTargetConfiguredOBE(t, "inv-pii", "//pkg:lib", 2),
		makePIIRichActionOBE(t, "inv-pii", 3),
		makeBuildFinishedOBE(t, "inv-pii", 4, 0, "SUCCESS"),
	)
	return sink
}

func TestPII_DefaultRedactsEverything(t *testing.T) {
	sink := runPIITestStream(t, PIIConfig{})

	root := findRootSpan(t, sink)
	for _, key := range []string{"bazel.host", "bazel.user", "bazel.workspace_directory", "bazel.working_directory"} {
		_, has := root.Attributes().Get(key)
		assert.False(t, has, "default config must not emit %s", key)
	}

	// Non-PII attrs remain visible regardless of PII config.
	cmd, ok := root.Attributes().Get("bazel.command")
	require.True(t, ok)
	assert.Equal(t, "build", cmd.Str())

	action := findActionSpan(t, sink, "//pkg:lib")
	_, hasCmd := action.Attributes().Get("bazel.action.command_line")
	assert.False(t, hasCmd)
	_, hasOut := action.Attributes().Get("bazel.action.primary_output")
	assert.False(t, hasOut)
}

func TestPII_Matrix(t *testing.T) {
	cases := []struct {
		name       string
		pii        PIIConfig
		rootAttr   string
		rootWant   string
		actionAttr string
		actionWant string
	}{
		{"hostname", PIIConfig{IncludeHostname: true}, "bazel.host", "ci-runner-42", "", ""},
		{"username", PIIConfig{IncludeUsername: true}, "bazel.user", "jenkins", "", ""},
		{"workspace_dir", PIIConfig{IncludeWorkspaceDir: true}, "bazel.workspace_directory", "/home/jenkins/workspace/foo", "", ""},
		{"working_dir", PIIConfig{IncludeWorkingDir: true}, "bazel.working_directory", "/home/jenkins/workspace/foo/subdir", "", ""},
		{"action_output", PIIConfig{IncludeActionOutputPaths: true}, "", "", "bazel.action.primary_output", "pkg/Foo.class"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink := runPIITestStream(t, tc.pii)
			if tc.rootAttr != "" {
				root := findRootSpan(t, sink)
				got, ok := root.Attributes().Get(tc.rootAttr)
				require.True(t, ok, "expected %s on root span", tc.rootAttr)
				assert.Equal(t, tc.rootWant, got.Str())
			}
			if tc.actionAttr != "" {
				action := findActionSpan(t, sink, "//pkg:lib")
				got, ok := action.Attributes().Get(tc.actionAttr)
				require.True(t, ok, "expected %s on action span", tc.actionAttr)
				assert.Equal(t, tc.actionWant, got.Str())
			}
		})
	}
}

func TestPII_CommandArgs_Slice(t *testing.T) {
	sink := runPIITestStream(t, PIIConfig{IncludeCommandArgs: true})
	action := findActionSpan(t, sink, "//pkg:lib")
	slice, ok := action.Attributes().Get("bazel.action.command_line")
	require.True(t, ok)
	require.Equal(t, pcommon.ValueTypeSlice, slice.Type())
	require.Equal(t, 4, slice.Slice().Len())
	assert.Equal(t, "javac", slice.Slice().At(0).Str())
	assert.Equal(t, "Foo.java", slice.Slice().At(3).Str())
}

func TestPII_AllFlags_On(t *testing.T) {
	sink := runPIITestStream(t, PIIConfig{
		IncludeHostname:          true,
		IncludeUsername:          true,
		IncludeWorkspaceDir:      true,
		IncludeWorkingDir:        true,
		IncludeCommandArgs:       true,
		IncludeActionOutputPaths: true,
	})

	root := findRootSpan(t, sink)
	for _, key := range []string{"bazel.host", "bazel.user", "bazel.workspace_directory", "bazel.working_directory"} {
		_, has := root.Attributes().Get(key)
		assert.True(t, has, "expected %s with all flags on", key)
	}

	action := findActionSpan(t, sink, "//pkg:lib")
	_, hasCmd := action.Attributes().Get("bazel.action.command_line")
	assert.True(t, hasCmd)
	_, hasOut := action.Attributes().Get("bazel.action.primary_output")
	assert.True(t, hasOut)
}

func TestPII_WorkspaceDirectory_RequiresFlag(t *testing.T) {
	// Back-compat documentation test for the breaking change: bazel.workspace_directory
	// is no longer emitted by default and requires include_workspace_dir.
	sinkDefault := runPIITestStream(t, PIIConfig{})
	root := findRootSpan(t, sinkDefault)
	_, hasDefault := root.Attributes().Get("bazel.workspace_directory")
	assert.False(t, hasDefault, "default config must NOT emit bazel.workspace_directory (breaking change from prior versions)")

	sinkEnabled := runPIITestStream(t, PIIConfig{IncludeWorkspaceDir: true})
	rootEnabled := findRootSpan(t, sinkEnabled)
	got, ok := rootEnabled.Attributes().Get("bazel.workspace_directory")
	require.True(t, ok)
	assert.Equal(t, "/home/jenkins/workspace/foo", got.Str())
}

// runFullPIITestStream runs an event stream exercising every PII-sensitive
// pathway: BuildStarted with host/user/dirs, WorkspaceStatus with host-like
// and user-like keys plus a safe key, BuildMetadata with a PII-like user
// key plus a safe key, and a StructuredCommandLine. Used by the composition
// matrix tests below.
func runFullPIITestStream(t *testing.T, pii PIIConfig) *consumertest.TracesSink {
	t.Helper()
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{PII: pii})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makePIIRichBuildStartedOBE(t, "inv-full-pii", 1),
		makeWorkspaceStatusOBE(t, "inv-full-pii", 2, map[string]string{
			"STABLE_GIT_BRANCH": "main",
			"BUILD_HOST":        "ws-host",
			"BUILD_USER":        "ws-user",
			"WORKING_DIRECTORY": "/ws-wd",
		}),
		makeBuildMetadataOBE(t, "inv-full-pii", 3, map[string]string{
			"pr_number": "1337",
			"user":      "md-user",
			"host":      "md-host",
		}),
		makeStructuredCommandLineOBE(t, "inv-full-pii", "original", []*commandline.CommandLineSection{
			chunkSection("bazel"),
			chunkSection("build"),
			chunkSection("//..."),
		}, 4),
		makeBuildFinishedOBE(t, "inv-full-pii", 5, 0, "SUCCESS"),
	)
	return sink
}

// TestPII_WorkspaceStatusComposition verifies that the coarse
// IncludeWorkspaceStatus gate composes with per-field PII flags: the
// pathway can be opened while individual host/user/dir keys are still
// filtered out per the finer flags.
func TestPII_WorkspaceStatusComposition(t *testing.T) {
	cases := []struct {
		name string
		pii  PIIConfig
		// want["bazel.workspace.<k>"] = true  → attribute must exist
		// want["bazel.workspace.<k>"] = false → attribute must NOT exist
		want map[string]bool
	}{
		{
			name: "status gate off suppresses everything",
			pii:  PIIConfig{IncludeHostname: true, IncludeUsername: true, IncludeWorkingDir: true},
			want: map[string]bool{
				"bazel.workspace.stable_git_branch": false,
				"bazel.workspace.build_host":        false,
				"bazel.workspace.build_user":        false,
				"bazel.workspace.working_directory": false,
			},
		},
		{
			name: "status gate on, all finer flags off",
			pii:  PIIConfig{IncludeWorkspaceStatus: true},
			want: map[string]bool{
				"bazel.workspace.stable_git_branch": true,  // safe key passes
				"bazel.workspace.build_host":        false, // hostname filtered
				"bazel.workspace.build_user":        false, // username filtered
				"bazel.workspace.working_directory": false, // working dir filtered
			},
		},
		{
			name: "status gate on, hostname on",
			pii:  PIIConfig{IncludeWorkspaceStatus: true, IncludeHostname: true},
			want: map[string]bool{
				"bazel.workspace.stable_git_branch": true,
				"bazel.workspace.build_host":        true,
				"bazel.workspace.build_user":        false,
				"bazel.workspace.working_directory": false,
			},
		},
		{
			name: "status gate on, username on",
			pii:  PIIConfig{IncludeWorkspaceStatus: true, IncludeUsername: true},
			want: map[string]bool{
				"bazel.workspace.stable_git_branch": true,
				"bazel.workspace.build_host":        false,
				"bazel.workspace.build_user":        true,
				"bazel.workspace.working_directory": false,
			},
		},
		{
			name: "status gate on, working_dir on",
			pii:  PIIConfig{IncludeWorkspaceStatus: true, IncludeWorkingDir: true},
			want: map[string]bool{
				"bazel.workspace.stable_git_branch": true,
				"bazel.workspace.build_host":        false,
				"bazel.workspace.build_user":        false,
				"bazel.workspace.working_directory": true,
			},
		},
		{
			name: "status gate on, all PII flags on",
			pii: PIIConfig{
				IncludeWorkspaceStatus: true,
				IncludeHostname:        true,
				IncludeUsername:        true,
				IncludeWorkingDir:      true,
				IncludeWorkspaceDir:    true,
			},
			want: map[string]bool{
				"bazel.workspace.stable_git_branch": true,
				"bazel.workspace.build_host":        true,
				"bazel.workspace.build_user":        true,
				"bazel.workspace.working_directory": true,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink := runFullPIITestStream(t, tc.pii)
			root := findRootSpan(t, sink)
			for attr, want := range tc.want {
				_, got := root.Attributes().Get(attr)
				assert.Equal(t, want, got, "%s: want present=%v, got present=%v", attr, want, got)
			}
		})
	}
}

// TestPII_BuildMetadataComposition exercises the same composition for the
// bazel.metadata.* pathway with a user/host key collision: when the coarse
// gate is open, PII-like keys are still filtered per the finer flags.
func TestPII_BuildMetadataComposition(t *testing.T) {
	cases := []struct {
		name string
		pii  PIIConfig
		want map[string]bool
	}{
		{
			name: "metadata gate off suppresses everything",
			pii:  PIIConfig{IncludeHostname: true, IncludeUsername: true},
			want: map[string]bool{
				"bazel.metadata.pr_number": false,
				"bazel.metadata.host":      false,
				"bazel.metadata.user":      false,
			},
		},
		{
			name: "metadata gate on, PII flags off",
			pii:  PIIConfig{IncludeBuildMetadata: true},
			want: map[string]bool{
				"bazel.metadata.pr_number": true,  // safe key passes
				"bazel.metadata.host":      false, // host filtered
				"bazel.metadata.user":      false, // user filtered
			},
		},
		{
			name: "metadata + hostname",
			pii:  PIIConfig{IncludeBuildMetadata: true, IncludeHostname: true},
			want: map[string]bool{
				"bazel.metadata.pr_number": true,
				"bazel.metadata.host":      true,
				"bazel.metadata.user":      false,
			},
		},
		{
			name: "metadata + username",
			pii:  PIIConfig{IncludeBuildMetadata: true, IncludeUsername: true},
			want: map[string]bool{
				"bazel.metadata.pr_number": true,
				"bazel.metadata.host":      false,
				"bazel.metadata.user":      true,
			},
		},
		{
			name: "metadata + all",
			pii:  PIIConfig{IncludeBuildMetadata: true, IncludeHostname: true, IncludeUsername: true},
			want: map[string]bool{
				"bazel.metadata.pr_number": true,
				"bazel.metadata.host":      true,
				"bazel.metadata.user":      true,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink := runFullPIITestStream(t, tc.pii)
			root := findRootSpan(t, sink)
			for attr, want := range tc.want {
				_, got := root.Attributes().Get(attr)
				assert.Equal(t, want, got, "%s: want present=%v, got present=%v", attr, want, got)
			}
		})
	}
}

// TestPII_CommandLine_Gated verifies bazel.command_line is all-or-nothing
// under IncludeCommandLine. No per-key filtering is attempted; operators
// needing finer control should use the redactionprocessor.
func TestPII_CommandLine_Gated(t *testing.T) {
	t.Run("default off suppresses command_line", func(t *testing.T) {
		sink := runFullPIITestStream(t, PIIConfig{})
		root := findRootSpan(t, sink)
		_, has := root.Attributes().Get("bazel.command_line")
		assert.False(t, has, "bazel.command_line must default to suppressed")
	})
	t.Run("flag on emits command_line", func(t *testing.T) {
		sink := runFullPIITestStream(t, PIIConfig{IncludeCommandLine: true})
		root := findRootSpan(t, sink)
		got, ok := root.Attributes().Get("bazel.command_line")
		require.True(t, ok)
		assert.Equal(t, "bazel build //...", got.Str())
	})
	t.Run("other PII flags do not gate command_line", func(t *testing.T) {
		// IncludeHostname alone should not turn command_line on.
		sink := runFullPIITestStream(t, PIIConfig{IncludeHostname: true})
		root := findRootSpan(t, sink)
		_, has := root.Attributes().Get("bazel.command_line")
		assert.False(t, has, "command_line requires its own flag")
	})
}

// TestPII_NoDuplication_HostnameAttr catches the specific bug that motivated
// the composition model: when IncludeHostname is true but IncludeWorkspaceStatus
// is false, the hostname should appear exactly once (on bazel.host from
// BuildStarted) — not duplicated via the workspace-status pathway.
func TestPII_NoDuplication_HostnameAttr(t *testing.T) {
	sink := runFullPIITestStream(t, PIIConfig{IncludeHostname: true})
	root := findRootSpan(t, sink)

	host, ok := root.Attributes().Get("bazel.host")
	require.True(t, ok, "bazel.host should appear when IncludeHostname=true")
	assert.Equal(t, "ci-runner-42", host.Str())

	_, hasWs := root.Attributes().Get("bazel.workspace.build_host")
	assert.False(t, hasWs, "workspace-status host must be suppressed when the status gate is closed")
	_, hasMd := root.Attributes().Get("bazel.metadata.host")
	assert.False(t, hasMd, "metadata host must be suppressed when the metadata gate is closed")
}

func TestTraceBuilder_StructuredCommandLine_Truncated(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{
		PII: PIIConfig{IncludeCommandLine: true},
	})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// 200 chunks of 10 chars each = 2000 bytes plus spaces, well over 1024.
	longChunks := make([]string, 200)
	for i := range longChunks {
		longChunks[i] = strings.Repeat("x", 10)
	}
	sections := []*commandline.CommandLineSection{chunkSection(longChunks...)}

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-long", "uuid-long", "build", 1),
		makeStructuredCommandLineOBE(t, "inv-long", "original", sections, 2),
		makeBuildFinishedOBE(t, "inv-long", 3, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)
	cmd, ok := span.Attributes().Get("bazel.command_line")
	require.True(t, ok)
	// <= commandLineMaxBytes + ellipsis rune (3 bytes).
	assert.LessOrEqual(t, len(cmd.Str()), commandLineMaxBytes+3)
	assert.True(t, strings.HasSuffix(cmd.Str(), "…"), "expected truncation marker")
}

// summaryAttrInt fetches a bazel.summary.* Int64 attribute from the root span.
// Asserts presence and returns the value so callers can compare against
// per-test expectations inline.
func summaryAttrInt(t *testing.T, span ptrace.Span, key string) int64 {
	t.Helper()
	v, ok := span.Attributes().Get(key)
	require.Truef(t, ok, "expected attribute %s on root span", key)
	require.Equalf(t, pcommon.ValueTypeInt, v.Type(), "expected %s to be Int64", key)
	return v.Int()
}

// TestTraceBuilder_Summary_AccurateCounts feeds a representative event stream
// (2 targets, 3 actions with a 2/1 success/fail split, 3 tests covering the
// passed/flaky/failed verdicts) and asserts every bazel.summary.* value.
func TestTraceBuilder_Summary_AccurateCounts(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{
		Summary: SummaryConfig{Enabled: true},
	})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-sum", "uuid-sum", "build", 1),
		makeTargetConfiguredOBE(t, "inv-sum", "//pkg:lib", 2),
		makeTargetConfiguredOBE(t, "inv-sum", "//pkg:test", 3),
		makeActionOBE(t, "inv-sum", "//pkg:lib", "Javac", 4, true),
		makeActionOBE(t, "inv-sum", "//pkg:lib", "Turbine", 5, true),
		makeActionOBE(t, "inv-sum", "//pkg:lib", "Javac", 6, false),
		makeTestSummaryOBE(t, "inv-sum", "//pkg:test/pass", 7, bep.TestStatus_PASSED, 1, 1, 0, 0),
		makeTestSummaryOBE(t, "inv-sum", "//pkg:test/flake", 8, bep.TestStatus_FLAKY, 1, 1, 0, 0),
		makeTestSummaryOBE(t, "inv-sum", "//pkg:test/fail", 9, bep.TestStatus_FAILED, 1, 1, 0, 0),
		makeBuildFinishedOBE(t, "inv-sum", 10, 0, "SUCCESS"),
	)

	root := findRootSpan(t, sink)
	assert.Equal(t, int64(2), summaryAttrInt(t, root, "bazel.summary.total_targets"))
	assert.Equal(t, int64(3), summaryAttrInt(t, root, "bazel.summary.total_actions"))
	assert.Equal(t, int64(2), summaryAttrInt(t, root, "bazel.summary.success_actions"))
	assert.Equal(t, int64(1), summaryAttrInt(t, root, "bazel.summary.failed_actions"))
	assert.Equal(t, int64(3), summaryAttrInt(t, root, "bazel.summary.total_tests"))
	assert.Equal(t, int64(1), summaryAttrInt(t, root, "bazel.summary.passed_tests"))
	assert.Equal(t, int64(1), summaryAttrInt(t, root, "bazel.summary.failed_tests"))
	assert.Equal(t, int64(1), summaryAttrInt(t, root, "bazel.summary.flaky_tests"))
}

// TestTraceBuilder_Summary_ZeroCountersEmittedWhenEnabled verifies that a
// build without any targets / actions / tests still produces zero-valued
// Int64 attributes — the spec requires attribute presence regardless of
// event count so downstream queries can rely on the keys existing.
func TestTraceBuilder_Summary_ZeroCountersEmittedWhenEnabled(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{
		Summary: SummaryConfig{Enabled: true},
	})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-zero", "uuid-zero", "build", 1),
		makeBuildFinishedOBE(t, "inv-zero", 2, 0, "SUCCESS"),
	)

	root := findRootSpan(t, sink)
	for _, key := range []string{
		"bazel.summary.total_targets",
		"bazel.summary.total_actions",
		"bazel.summary.success_actions",
		"bazel.summary.failed_actions",
		"bazel.summary.total_tests",
		"bazel.summary.passed_tests",
		"bazel.summary.failed_tests",
		"bazel.summary.flaky_tests",
	} {
		assert.Equal(t, int64(0), summaryAttrInt(t, root, key), "%s should be 0", key)
	}
}

// TestTraceBuilder_Summary_DisabledOmitsAllAttributes asserts the enabled=false
// path omits every bazel.summary.* attribute — not just zeros, but the keys
// themselves. Mirrors the feature-gate contract in the issue acceptance
// criteria.
func TestTraceBuilder_Summary_DisabledOmitsAllAttributes(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{
		Summary: SummaryConfig{Enabled: false},
	})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-off", "uuid-off", "build", 1),
		makeTargetConfiguredOBE(t, "inv-off", "//pkg:lib", 2),
		makeActionOBE(t, "inv-off", "//pkg:lib", "Javac", 3, true),
		makeTestSummaryOBE(t, "inv-off", "//pkg:test", 4, bep.TestStatus_PASSED, 1, 1, 0, 0),
		makeBuildFinishedOBE(t, "inv-off", 5, 0, "SUCCESS"),
	)

	root := findRootSpan(t, sink)
	root.Attributes().Range(func(k string, _ pcommon.Value) bool {
		assert.Falsef(t, strings.HasPrefix(k, "bazel.summary."),
			"no bazel.summary.* attribute should be emitted when disabled, found %s", k)
		return true
	})
}

// TestTraceBuilder_Summary_DefaultFactoryEnablesEmission guards the default
// config path — createDefaultConfig() must set Summary.Enabled=true so users
// who never set the block still see the aggregate counters.
func TestTraceBuilder_Summary_DefaultFactoryEnablesEmission(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	assert.True(t, cfg.Summary.Enabled, "summary.enabled must default to true")
}

// --- Progress → logs tests ---

// newProgressTB builds a TraceBuilder with Progress enabled at the given cap.
// Returns the builder and a logs sink for assertion.
func newProgressTB(t *testing.T, maxChunkSize int) (*TraceBuilder, *consumertest.LogsSink) {
	t.Helper()
	logsSink := new(consumertest.LogsSink)
	tb := NewTraceBuilder(nil, logsSink, nil, zap.NewNop(), TraceBuilderConfig{
		Progress: ProgressConfig{Enabled: true, MaxChunkSize: maxChunkSize},
	})
	tb.Start()
	t.Cleanup(tb.Stop)
	return tb, logsSink
}

// firstProgressRecord returns the single log record at index i, failing if
// the shape is unexpected.
func firstProgressRecord(t *testing.T, sink *consumertest.LogsSink, i int) plog.LogRecord {
	t.Helper()
	logs := sink.AllLogs()
	require.Greater(t, len(logs), i, "missing log record at index %d", i)
	rl := logs[i].ResourceLogs()
	require.Equal(t, 1, rl.Len())
	sl := rl.At(0).ScopeLogs()
	require.Equal(t, 1, sl.Len())
	lrs := sl.At(0).LogRecords()
	require.Equal(t, 1, lrs.Len())
	return lrs.At(0)
}

func TestProgress_DisabledByDefault(t *testing.T) {
	logsSink := new(consumertest.LogsSink)
	// Zero-value TraceBuilderConfig.Progress → Enabled=false.
	tb := NewTraceBuilder(nil, logsSink, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-dis", "uuid-dis", "build", 1),
		makeProgressOBE(t, "inv-dis", 2, "stdout chunk\n", "stderr chunk\n"),
	)

	// Only the BuildStarted log record; Progress must be suppressed entirely.
	require.Equal(t, 1, logsSink.LogRecordCount())
	lr := firstProgressRecord(t, logsSink, 0)
	evt, _ := lr.Attributes().Get("event.name")
	assert.Equal(t, "bazel.build.started", evt.Str())
}

func TestProgress_Enabled_StdoutOnly(t *testing.T) {
	tb, logsSink := newProgressTB(t, 1024)
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-so", "uuid-so", "build", 1),
		makeProgressOBE(t, "inv-so", 2, "compiling //pkg:lib\n", ""),
	)

	// 1 BuildStarted + 1 stdout record = 2.
	require.Equal(t, 2, logsSink.LogRecordCount())
	lr := firstProgressRecord(t, logsSink, 1)
	assert.Equal(t, plog.SeverityNumberInfo, lr.SeverityNumber())
	assert.Equal(t, "compiling //pkg:lib\n", lr.Body().Str())

	evt, _ := lr.Attributes().Get("event.name")
	assert.Equal(t, "bazel.progress", evt.Str())
	stream, _ := lr.Attributes().Get("bazel.progress.stream")
	assert.Equal(t, "stdout", stream.Str())
	invID, _ := lr.Attributes().Get("bazel.invocation_id")
	assert.Equal(t, "inv-so", invID.Str())

	// bytes + truncated are always stamped so consumers can rely on presence.
	bytes, ok := lr.Attributes().Get("bazel.progress.bytes")
	require.True(t, ok, "bazel.progress.bytes must always be stamped")
	assert.Equal(t, int64(len("compiling //pkg:lib\n")), bytes.Int())

	trunc, ok := lr.Attributes().Get("bazel.progress.truncated")
	require.True(t, ok, "bazel.progress.truncated must always be stamped")
	assert.False(t, trunc.Bool())

	// opaque_count is propagated from BuildEventId so consumers can re-establish
	// chunk order across stdout/stderr.
	op, ok := lr.Attributes().Get("bazel.progress.opaque_count")
	require.True(t, ok)
	assert.Equal(t, int64(2), op.Int())

	// TraceID ties the record back to the invocation.
	assert.Equal(t, traceIDFromUUID("uuid-so"), lr.TraceID())
}

func TestProgress_Enabled_StderrOnly(t *testing.T) {
	tb, logsSink := newProgressTB(t, 1024)
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-se", "uuid-se", "build", 1),
		makeProgressOBE(t, "inv-se", 2, "", "WARNING: dangling reference\n"),
	)

	require.Equal(t, 2, logsSink.LogRecordCount())
	lr := firstProgressRecord(t, logsSink, 1)
	// stderr maps to INFO — Bazel routes normal-build chatter through stderr.
	assert.Equal(t, plog.SeverityNumberInfo, lr.SeverityNumber())
	assert.Equal(t, "WARNING: dangling reference\n", lr.Body().Str())
	stream, _ := lr.Attributes().Get("bazel.progress.stream")
	assert.Equal(t, "stderr", stream.Str())
}

func TestProgress_Enabled_BothStreams_StderrFirst(t *testing.T) {
	tb, logsSink := newProgressTB(t, 1024)
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-both", "uuid-both", "build", 1),
		makeProgressOBE(t, "inv-both", 2, "out-chunk\n", "err-chunk\n"),
	)

	// 1 BuildStarted + 2 Progress (stderr first per BEP spec, then stdout).
	require.Equal(t, 3, logsSink.LogRecordCount())

	stderrLR := firstProgressRecord(t, logsSink, 1)
	stream1, _ := stderrLR.Attributes().Get("bazel.progress.stream")
	assert.Equal(t, "stderr", stream1.Str(), "stderr must be emitted first per BEP spec")
	assert.Equal(t, plog.SeverityNumberInfo, stderrLR.SeverityNumber())
	assert.Equal(t, "err-chunk\n", stderrLR.Body().Str())

	stdoutLR := firstProgressRecord(t, logsSink, 2)
	stream2, _ := stdoutLR.Attributes().Get("bazel.progress.stream")
	assert.Equal(t, "stdout", stream2.Str())
	assert.Equal(t, plog.SeverityNumberInfo, stdoutLR.SeverityNumber())
	assert.Equal(t, "out-chunk\n", stdoutLR.Body().Str())

	// Both records share the same opaque_count so consumers can pair them.
	op1, _ := stderrLR.Attributes().Get("bazel.progress.opaque_count")
	op2, _ := stdoutLR.Attributes().Get("bazel.progress.opaque_count")
	assert.Equal(t, op1.Int(), op2.Int())
}

func TestProgress_Enabled_EmptyStreamsSuppressed(t *testing.T) {
	tb, logsSink := newProgressTB(t, 1024)
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-empty", "uuid-empty", "build", 1),
		// Both streams empty — no Progress record must be emitted.
		makeProgressOBE(t, "inv-empty", 2, "", ""),
	)

	require.Equal(t, 1, logsSink.LogRecordCount(), "empty Progress must emit nothing")
}

func TestProgress_Truncation_SingleStream(t *testing.T) {
	const maxChunkSize = 16
	tb, logsSink := newProgressTB(t, maxChunkSize)
	ctx := context.Background()

	// 100 bytes of stdout; must be truncated to 16 and stamped.
	stdout := strings.Repeat("x", 100)
	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-trunc", "uuid-trunc", "build", 1),
		makeProgressOBE(t, "inv-trunc", 2, stdout, ""),
	)

	require.Equal(t, 2, logsSink.LogRecordCount())
	lr := firstProgressRecord(t, logsSink, 1)
	assert.Equal(t, maxChunkSize, len(lr.Body().Str()), "body must be truncated to MaxChunkSize")
	assert.Equal(t, strings.Repeat("x", maxChunkSize), lr.Body().Str())

	trunc, ok := lr.Attributes().Get("bazel.progress.truncated")
	require.True(t, ok)
	assert.True(t, trunc.Bool())

	bytes, ok := lr.Attributes().Get("bazel.progress.bytes")
	require.True(t, ok)
	assert.Equal(t, int64(100), bytes.Int(), "bytes reflects original length, not clipped length")
}

func TestProgress_Truncation_RuneSafe(t *testing.T) {
	// Cap that lands mid-rune for the test fixture — verify the body is
	// clipped at a rune boundary rather than producing invalid UTF-8.
	const maxChunkSize = 5
	tb, logsSink := newProgressTB(t, maxChunkSize)
	ctx := context.Background()

	// "héllo" — h(1) + é(2) + l(1) + l(1) + o(1) = 6 bytes; cap of 5 lands
	// inside é, so truncation must back off to either 1 byte ("h") or 3 bytes
	// ("hé"). Either way the result must be valid UTF-8.
	stdout := "héllo"
	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-rune", "uuid-rune", "build", 1),
		makeProgressOBE(t, "inv-rune", 2, stdout, ""),
	)

	lr := firstProgressRecord(t, logsSink, 1)
	body := lr.Body().Str()
	assert.True(t, utf8.ValidString(body), "truncated body must be valid UTF-8, got %q", body)
	assert.LessOrEqual(t, len(body), maxChunkSize)
}

func TestProgress_CapDisabled_ZeroMeansUnlimited(t *testing.T) {
	// MaxChunkSize == 0 disables the cap — even very large bodies pass
	// through verbatim. truncated is stamped as false; bytes carries the
	// original length.
	tb, logsSink := newProgressTB(t, 0)
	ctx := context.Background()

	stdout := strings.Repeat("y", 1<<20) // 1 MiB
	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-unl", "uuid-unl", "build", 1),
		makeProgressOBE(t, "inv-unl", 2, stdout, ""),
	)

	require.Equal(t, 2, logsSink.LogRecordCount())
	lr := firstProgressRecord(t, logsSink, 1)
	assert.Equal(t, len(stdout), len(lr.Body().Str()), "body must pass through unmodified when cap is 0")
	trunc, _ := lr.Attributes().Get("bazel.progress.truncated")
	assert.False(t, trunc.Bool())
	bytes, _ := lr.Attributes().Get("bazel.progress.bytes")
	assert.Equal(t, int64(len(stdout)), bytes.Int())
}

func TestProgress_PostFlushDropped(t *testing.T) {
	// Progress arriving after BuildFinished (which flushes the trace batch)
	// must be silently dropped — matches the gate every other handler uses.
	tb, logsSink := newProgressTB(t, 1024)
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-postf", "uuid-postf", "build", 1),
		makeBuildFinishedOBE(t, "inv-postf", 2, 0, "SUCCESS"),
		// Late Progress — the batch is already flushed.
		makeProgressOBE(t, "inv-postf", 3, "late stdout\n", "late stderr\n"),
	)

	for i := range logsSink.LogRecordCount() {
		lr := firstProgressRecord(t, logsSink, i)
		evt, _ := lr.Attributes().Get("event.name")
		assert.NotEqual(t, "bazel.progress", evt.Str(),
			"post-flush Progress must not emit a log record")
	}
}

func TestProgress_UnknownInvocationDropped(t *testing.T) {
	// Progress arrives before BuildStarted — no invocation state yet, so the
	// record must be silently dropped (matches the pattern used by other
	// handlers).
	tb, logsSink := newProgressTB(t, 1024)
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeProgressOBE(t, "inv-orphan", 1, "out\n", "err\n"),
	)

	require.Equal(t, 0, logsSink.LogRecordCount())
}

// --- ExecRequestConstructed / bazel.run.* (issue #63) ---

// runExecRequestStream runs a `bazel run` style event sequence (BuildStarted →
// ExecRequestConstructed → BuildFinished) through the TraceBuilder with the
// given PII config and returns the captured traces sink.
func runExecRequestStream(t *testing.T, pii PIIConfig) *consumertest.TracesSink {
	t.Helper()
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{PII: pii})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-run", "uuid-run", "run", 1),
		makeExecRequestConstructedOBE(t, "inv-run", 2,
			[]string{"/bin/bazel-out/.../bin/tool", "--flag", "value"},
			"/home/user/workspace",
			map[string]string{"FOO": "bar", "PATH": "/usr/bin"},
			true,
		),
		makeBuildFinishedOBE(t, "inv-run", 3, 0, "SUCCESS"),
	)
	return sink
}

// TestExecRequest_AllPIIOn verifies every bazel.run.* attribute is stamped
// when the necessary PII gates are open.
func TestExecRequest_AllPIIOn(t *testing.T) {
	sink := runExecRequestStream(t, PIIConfig{
		IncludeCommandArgs: true,
		IncludeWorkingDir:  true,
	})
	root := findRootSpan(t, sink)

	// Always-on: count + should_exec.
	count, ok := root.Attributes().Get("bazel.run.environment_variable_count")
	require.True(t, ok)
	assert.Equal(t, int64(2), count.Int())

	se, ok := root.Attributes().Get("bazel.run.should_exec")
	require.True(t, ok)
	assert.True(t, se.Bool())

	// Working directory (gated by IncludeWorkingDir).
	wd, ok := root.Attributes().Get("bazel.run.working_directory")
	require.True(t, ok)
	assert.Equal(t, "/home/user/workspace", wd.Str())

	// argv (gated by IncludeCommandArgs) as Slice[string].
	argv, ok := root.Attributes().Get("bazel.run.argv")
	require.True(t, ok)
	require.Equal(t, pcommon.ValueTypeSlice, argv.Type())
	require.Equal(t, 3, argv.Slice().Len())
	assert.Equal(t, "/bin/bazel-out/.../bin/tool", argv.Slice().At(0).Str())
	assert.Equal(t, "--flag", argv.Slice().At(1).Str())
	assert.Equal(t, "value", argv.Slice().At(2).Str())

	// environment (gated by IncludeCommandArgs) as Slice[Map{name,value}],
	// sorted by key thanks to the test helper.
	env, ok := root.Attributes().Get("bazel.run.environment")
	require.True(t, ok)
	require.Equal(t, pcommon.ValueTypeSlice, env.Type())
	require.Equal(t, 2, env.Slice().Len())

	first := env.Slice().At(0).Map()
	name, ok := first.Get("name")
	require.True(t, ok)
	assert.Equal(t, "FOO", name.Str())
	value, ok := first.Get("value")
	require.True(t, ok)
	assert.Equal(t, "bar", value.Str())

	second := env.Slice().At(1).Map()
	name2, ok := second.Get("name")
	require.True(t, ok)
	assert.Equal(t, "PATH", name2.Str())
	value2, ok := second.Get("value")
	require.True(t, ok)
	assert.Equal(t, "/usr/bin", value2.Str())
}

// TestExecRequest_PIIOff_CountAndShouldExecStillEmitted asserts the default
// (redacted) path still emits the safe count/should_exec attrs when the
// ExecRequestConstructed event arrives, so operators retain a "run
// invocation" signal.
func TestExecRequest_PIIOff_CountAndShouldExecStillEmitted(t *testing.T) {
	sink := runExecRequestStream(t, PIIConfig{})
	root := findRootSpan(t, sink)

	// Always-on attrs present.
	count, ok := root.Attributes().Get("bazel.run.environment_variable_count")
	require.True(t, ok)
	assert.Equal(t, int64(2), count.Int())

	se, ok := root.Attributes().Get("bazel.run.should_exec")
	require.True(t, ok)
	assert.True(t, se.Bool())

	// Gated attrs redacted.
	for _, key := range []string{
		"bazel.run.argv",
		"bazel.run.working_directory",
		"bazel.run.environment",
	} {
		_, has := root.Attributes().Get(key)
		assert.False(t, has, "expected %s to be redacted with PII off", key)
	}
}

// TestExecRequest_WorkingDirGate_OnlyDir asserts IncludeWorkingDir alone
// surfaces only working_directory; argv and environment stay redacted.
func TestExecRequest_WorkingDirGate_OnlyDir(t *testing.T) {
	sink := runExecRequestStream(t, PIIConfig{IncludeWorkingDir: true})
	root := findRootSpan(t, sink)

	wd, ok := root.Attributes().Get("bazel.run.working_directory")
	require.True(t, ok)
	assert.Equal(t, "/home/user/workspace", wd.Str())

	_, hasArgv := root.Attributes().Get("bazel.run.argv")
	assert.False(t, hasArgv, "argv must stay redacted without IncludeCommandArgs")
	_, hasEnv := root.Attributes().Get("bazel.run.environment")
	assert.False(t, hasEnv, "environment must stay redacted without IncludeCommandArgs")
}

// TestExecRequest_CommandArgsGate_ArgvAndEnv asserts IncludeCommandArgs
// surfaces both argv and environment (shared gate per issue #63) while
// keeping working_directory redacted.
func TestExecRequest_CommandArgsGate_ArgvAndEnv(t *testing.T) {
	sink := runExecRequestStream(t, PIIConfig{IncludeCommandArgs: true})
	root := findRootSpan(t, sink)

	argv, ok := root.Attributes().Get("bazel.run.argv")
	require.True(t, ok)
	require.Equal(t, pcommon.ValueTypeSlice, argv.Type())
	assert.Equal(t, 3, argv.Slice().Len())

	env, ok := root.Attributes().Get("bazel.run.environment")
	require.True(t, ok)
	require.Equal(t, pcommon.ValueTypeSlice, env.Type())
	assert.Equal(t, 2, env.Slice().Len())

	_, hasWD := root.Attributes().Get("bazel.run.working_directory")
	assert.False(t, hasWD, "working_directory must stay redacted without IncludeWorkingDir")
}

// TestExecRequest_NoWorkingDirectory asserts that an ExecRequestConstructed
// event with an empty working_directory does not emit a blank attribute even
// when IncludeWorkingDir is true.
func TestExecRequest_NoWorkingDirectory(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{
		PII: PIIConfig{IncludeWorkingDir: true, IncludeCommandArgs: true},
	})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-run-nowd", "uuid-run-nowd", "run", 1),
		makeExecRequestConstructedOBE(t, "inv-run-nowd", 2,
			[]string{"tool"},
			"", // empty working directory
			map[string]string{"HOME": "/root"},
			false,
		),
		makeBuildFinishedOBE(t, "inv-run-nowd", 3, 0, "SUCCESS"),
	)

	root := findRootSpan(t, sink)
	_, has := root.Attributes().Get("bazel.run.working_directory")
	assert.False(t, has, "empty working_directory must not emit the attribute")

	se, ok := root.Attributes().Get("bazel.run.should_exec")
	require.True(t, ok)
	assert.False(t, se.Bool())
}

// TestExecRequest_BazelBuild_NoAttrsEmitted asserts that a regular `bazel
// build` invocation (which does not emit ExecRequestConstructed) carries
// none of the bazel.run.* attributes. Regression guard for the "only stamp
// when the event arrived" contract.
func TestExecRequest_BazelBuild_NoAttrsEmitted(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{
		PII: PIIConfig{IncludeWorkingDir: true, IncludeCommandArgs: true},
	})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-build", "uuid-build", "build", 1),
		makeBuildFinishedOBE(t, "inv-build", 2, 0, "SUCCESS"),
	)

	root := findRootSpan(t, sink)
	for _, key := range []string{
		"bazel.run.argv",
		"bazel.run.working_directory",
		"bazel.run.environment_variable_count",
		"bazel.run.environment",
		"bazel.run.should_exec",
	} {
		_, has := root.Attributes().Get(key)
		assert.False(t, has, "bazel build must not emit %s (no ExecRequestConstructed event)", key)
	}
}
