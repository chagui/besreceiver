package besreceiver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"

	buildpb "google.golang.org/genproto/googleapis/devtools/build/v1"

	bep "github.com/chagui/besreceiver/internal/bep/buildeventstream"
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

	state := &invocationState{
		rootSpanID: rootSpanID,
		targets: map[string]pcommon.SpanID{
			targetKey("//pkg:lib", "cfg-1"): exactSpanID,
			targetKey("//pkg:lib", ""):      labelOnlySpanID,
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
			got := state.resolveTargetSpan(tt.label, tt.configID)
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
		newSpanID(),
		&bep.BuildStarted{Uuid: "uuid-stale", Command: "build"},
		time.Now().Add(-time.Hour),
	)
	span := state.appendSpan()
	span.SetSpanID(newSpanID())
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
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildStartedOBE(t, "inv-log", "uuid-log", "test", 1)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeTargetConfiguredOBE(t, "inv-log", "//pkg:lib", 2)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeActionOBE(t, "inv-log", "//pkg:lib", "Javac", 3, true)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeTestResultOBE(t, "inv-log", "//pkg:test", 4, bep.TestStatus_PASSED)); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildFinishedOBE(t, "inv-log", 5, 0, "SUCCESS")); err != nil {
		t.Fatal(err)
	}
	if err := tb.ProcessOrderedBuildEvent(ctx, makeBuildMetricsOBE(t, "inv-log", 6, 10000, 5000)); err != nil {
		t.Fatal(err)
	}

	// 6 events → 6 log records.
	if logsSink.LogRecordCount() != 6 {
		t.Fatalf("expected 6 log records, got %d", logsSink.LogRecordCount())
	}

	// Verify event names in order.
	expectedEvents := []string{
		"bazel.build.started",
		"bazel.target.configured",
		"bazel.action.completed",
		"bazel.test.result",
		"bazel.build.finished",
		"bazel.build.metrics",
	}
	for i, logs := range logsSink.AllLogs() {
		lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
		eventName, ok := lr.Attributes().Get("event.name")
		if !ok {
			t.Errorf("log record %d: missing event.name attribute", i)
			continue
		}
		if eventName.Str() != expectedEvents[i] {
			t.Errorf("log record %d: expected event.name=%q, got %q", i, expectedEvents[i], eventName.Str())
		}

		// All log records should have the invocation ID.
		invID, ok := lr.Attributes().Get("bazel.invocation_id")
		if !ok || invID.Str() != "inv-log" {
			t.Errorf("log record %d: expected bazel.invocation_id=inv-log, got %v", i, invID)
		}

		// All log records should have the trace ID for correlation.
		expectedTraceID := traceIDFromUUID("uuid-log")
		if lr.TraceID() != expectedTraceID {
			t.Errorf("log record %d: expected traceID %v, got %v", i, expectedTraceID, lr.TraceID())
		}

		// Resource should have service.name=bazel.
		svc, ok := logs.ResourceLogs().At(0).Resource().Attributes().Get("service.name")
		if !ok || svc.Str() != "bazel" {
			t.Errorf("log record %d: expected service.name=bazel, got %v", i, svc)
		}

		// Scope should be besreceiver.
		scope := logs.ResourceLogs().At(0).ScopeLogs().At(0).Scope().Name()
		if scope != "besreceiver" {
			t.Errorf("log record %d: expected scope=besreceiver, got %q", i, scope)
		}
	}

	// Traces should also be emitted normally.
	if tracesSink.SpanCount() == 0 {
		t.Error("expected traces to be emitted alongside logs")
	}
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
	tb := NewTraceBuilder(sink, nil, zap.NewNop(), TraceBuilderConfig{})
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
	tb := NewTraceBuilder(sink, nil, zap.NewNop(), TraceBuilderConfig{})
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
