package besreceiver

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/types/known/durationpb"

	bep "github.com/chagui/besreceiver/internal/bep/buildeventstream"
	pkgmetrics "github.com/chagui/besreceiver/internal/bep/packagemetrics"
)

// newInvocationStateForHighCardTest creates a minimal invocation state suitable
// for exercising buildMetricsSpan in isolation.
func newInvocationStateForHighCardTest(caps HighCardinalityCaps) *invocationState {
	var traceID pcommon.TraceID
	copy(traceID[:], "0123456789abcdef")
	var rootSpanID pcommon.SpanID
	copy(rootSpanID[:], "01234567")
	return &invocationState{
		traceID:    traceID,
		rootSpanID: rootSpanID,
		uuid:       "uuid-test",
		started:    &bep.BuildStarted{Command: "build", Uuid: "uuid-test"},
		createdAt:  time.Now(),
		caps:       caps,
	}
}

func TestBuildMetricsSpan_GarbageMetrics(t *testing.T) {
	state := newInvocationStateForHighCardTest(defaultHighCardinalityCaps())
	metrics := &bep.BuildMetrics{
		MemoryMetrics: &bep.BuildMetrics_MemoryMetrics{
			GarbageMetrics: []*bep.BuildMetrics_MemoryMetrics_GarbageMetrics{
				{Type: "G1 Old Gen", GarbageCollected: 1000},
				{Type: "G1 Young Gen", GarbageCollected: 2000},
			},
		},
	}

	traces, _ := state.buildMetricsSpan(metrics, actionDataOptions{})
	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	attr, ok := span.Attributes().Get("bazel.metrics.garbage")
	if !ok {
		t.Fatal("expected bazel.metrics.garbage attribute")
	}
	if attr.Type() != pcommon.ValueTypeSlice {
		t.Fatalf("expected slice type, got %s", attr.Type())
	}
	slice := attr.Slice()
	if slice.Len() != 2 {
		t.Fatalf("expected 2 entries, got %d", slice.Len())
	}

	// Validate first entry.
	entry0 := slice.At(0).Map()
	gotType, _ := entry0.Get("type")
	if gotType.Str() != "G1 Old Gen" {
		t.Errorf("entry 0: expected type=%q, got %q", "G1 Old Gen", gotType.Str())
	}
	gotBytes, _ := entry0.Get("garbage_collected_bytes")
	if gotBytes.Int() != 1000 {
		t.Errorf("entry 0: expected garbage_collected_bytes=1000, got %d", gotBytes.Int())
	}
}

func TestBuildMetricsSpan_PackageLoad(t *testing.T) {
	state := newInvocationStateForHighCardTest(defaultHighCardinalityCaps())
	metrics := &bep.BuildMetrics{
		PackageMetrics: &bep.BuildMetrics_PackageMetrics{
			PackageLoadMetrics: []*pkgmetrics.PackageLoadMetrics{
				{
					Name:                        new("//pkg:a"),
					LoadDuration:                durationpb.New(250 * time.Millisecond),
					NumTargets:                  new(uint64(7)),
					ComputationSteps:            new(uint64(42)),
					NumTransitiveLoads:          new(uint64(3)),
					PackageOverhead:             new(uint64(123)),
					GlobFilesystemOperationCost: new(uint64(9)),
				},
			},
		},
	}

	traces, _ := state.buildMetricsSpan(metrics, actionDataOptions{})
	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	attr, ok := span.Attributes().Get("bazel.metrics.package_load")
	if !ok {
		t.Fatal("expected bazel.metrics.package_load attribute")
	}
	slice := attr.Slice()
	if slice.Len() != 1 {
		t.Fatalf("expected 1 entry, got %d", slice.Len())
	}
	entry := slice.At(0).Map()
	if v, _ := entry.Get("name"); v.Str() != "//pkg:a" {
		t.Errorf("name: got %q", v.Str())
	}
	if v, _ := entry.Get("load_duration_ms"); v.Int() != 250 {
		t.Errorf("load_duration_ms: got %d, want 250", v.Int())
	}
	if v, _ := entry.Get("num_targets"); v.Int() != 7 {
		t.Errorf("num_targets: got %d", v.Int())
	}
	if v, _ := entry.Get("computation_steps"); v.Int() != 42 {
		t.Errorf("computation_steps: got %d", v.Int())
	}
	if v, _ := entry.Get("num_transitive_loads"); v.Int() != 3 {
		t.Errorf("num_transitive_loads: got %d", v.Int())
	}
	if v, _ := entry.Get("package_overhead"); v.Int() != 123 {
		t.Errorf("package_overhead: got %d", v.Int())
	}
	if v, _ := entry.Get("glob_filesystem_operation_cost"); v.Int() != 9 {
		t.Errorf("glob_filesystem_operation_cost: got %d", v.Int())
	}
}

func TestBuildMetricsSpan_PackageLoad_NilDuration(t *testing.T) {
	state := newInvocationStateForHighCardTest(defaultHighCardinalityCaps())
	metrics := &bep.BuildMetrics{
		PackageMetrics: &bep.BuildMetrics_PackageMetrics{
			PackageLoadMetrics: []*pkgmetrics.PackageLoadMetrics{
				{
					Name:       new("//pkg:a"),
					NumTargets: new(uint64(1)),
					// LoadDuration explicitly nil.
				},
			},
		},
	}

	traces, _ := state.buildMetricsSpan(metrics, actionDataOptions{})
	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	attr, ok := span.Attributes().Get("bazel.metrics.package_load")
	if !ok {
		t.Fatal("expected bazel.metrics.package_load attribute")
	}
	entry := attr.Slice().At(0).Map()
	if v, ok := entry.Get("load_duration_ms"); ok && v.Int() != 0 {
		t.Errorf("expected load_duration_ms to be 0 or absent for nil duration, got %d", v.Int())
	}
}

func TestBuildMetricsSpan_GraphValues(t *testing.T) {
	state := newInvocationStateForHighCardTest(defaultHighCardinalityCaps())
	metrics := &bep.BuildMetrics{
		BuildGraphMetrics: &bep.BuildMetrics_BuildGraphMetrics{
			DirtiedValues: []*bep.BuildMetrics_EvaluationStat{
				{SkyfunctionName: "FILE", Count: 10},
			},
			ChangedValues: []*bep.BuildMetrics_EvaluationStat{
				{SkyfunctionName: "FILE_STATE", Count: 5},
			},
			BuiltValues: []*bep.BuildMetrics_EvaluationStat{
				{SkyfunctionName: "ACTION_EXECUTION", Count: 20},
			},
			CleanedValues: []*bep.BuildMetrics_EvaluationStat{
				{SkyfunctionName: "GLOB", Count: 2},
			},
			EvaluatedValues: []*bep.BuildMetrics_EvaluationStat{
				{SkyfunctionName: "CONFIGURED_TARGET", Count: 100},
			},
		},
	}

	traces, _ := state.buildMetricsSpan(metrics, actionDataOptions{})
	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)

	cases := map[string]struct {
		skyFn string
		count int64
	}{
		"bazel.metrics.graph.dirtied_values":   {"FILE", 10},
		"bazel.metrics.graph.changed_values":   {"FILE_STATE", 5},
		"bazel.metrics.graph.built_values":     {"ACTION_EXECUTION", 20},
		"bazel.metrics.graph.cleaned_values":   {"GLOB", 2},
		"bazel.metrics.graph.evaluated_values": {"CONFIGURED_TARGET", 100},
	}
	for name, want := range cases {
		attr, ok := span.Attributes().Get(name)
		if !ok {
			t.Errorf("missing attribute %s", name)
			continue
		}
		slice := attr.Slice()
		if slice.Len() != 1 {
			t.Errorf("%s: expected 1 entry, got %d", name, slice.Len())
			continue
		}
		entry := slice.At(0).Map()
		if v, _ := entry.Get("skyfunction"); v.Str() != want.skyFn {
			t.Errorf("%s: skyfunction got %q want %q", name, v.Str(), want.skyFn)
		}
		if v, _ := entry.Get("count"); v.Int() != want.count {
			t.Errorf("%s: count got %d want %d", name, v.Int(), want.count)
		}
	}
}

func TestBuildMetricsSpan_NilSubMessages(t *testing.T) {
	state := newInvocationStateForHighCardTest(defaultHighCardinalityCaps())
	// BuildMetrics with only non-high-cardinality sub-messages present and
	// nil MemoryMetrics / PackageMetrics / BuildGraphMetrics.
	metrics := &bep.BuildMetrics{}

	traces, truncs := state.buildMetricsSpan(metrics, actionDataOptions{})
	if len(truncs) != 0 {
		t.Errorf("expected no truncations, got %v", truncs)
	}

	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	for _, name := range []string{
		"bazel.metrics.garbage",
		"bazel.metrics.package_load",
		"bazel.metrics.graph.dirtied_values",
		"bazel.metrics.graph.changed_values",
		"bazel.metrics.graph.built_values",
		"bazel.metrics.graph.cleaned_values",
		"bazel.metrics.graph.evaluated_values",
	} {
		if _, ok := span.Attributes().Get(name); ok {
			t.Errorf("expected attribute %s to be absent for nil sub-messages", name)
		}
	}
}

func TestBuildMetricsSpan_EmptyLists(t *testing.T) {
	state := newInvocationStateForHighCardTest(defaultHighCardinalityCaps())
	metrics := &bep.BuildMetrics{
		MemoryMetrics: &bep.BuildMetrics_MemoryMetrics{
			GarbageMetrics: []*bep.BuildMetrics_MemoryMetrics_GarbageMetrics{},
		},
		PackageMetrics: &bep.BuildMetrics_PackageMetrics{
			PackageLoadMetrics: []*pkgmetrics.PackageLoadMetrics{},
		},
		BuildGraphMetrics: &bep.BuildMetrics_BuildGraphMetrics{
			DirtiedValues:   []*bep.BuildMetrics_EvaluationStat{},
			ChangedValues:   []*bep.BuildMetrics_EvaluationStat{},
			BuiltValues:     []*bep.BuildMetrics_EvaluationStat{},
			CleanedValues:   []*bep.BuildMetrics_EvaluationStat{},
			EvaluatedValues: []*bep.BuildMetrics_EvaluationStat{},
		},
	}

	traces, _ := state.buildMetricsSpan(metrics, actionDataOptions{})
	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)

	for _, name := range []string{
		"bazel.metrics.garbage",
		"bazel.metrics.package_load",
		"bazel.metrics.graph.dirtied_values",
		"bazel.metrics.graph.changed_values",
		"bazel.metrics.graph.built_values",
		"bazel.metrics.graph.cleaned_values",
		"bazel.metrics.graph.evaluated_values",
	} {
		if _, ok := span.Attributes().Get(name); ok {
			t.Errorf("expected attribute %s to be absent for empty list", name)
		}
	}
}

func TestBuildMetricsSpan_CapEnforcement_Garbage(t *testing.T) {
	caps := HighCardinalityCaps{Garbage: 2, PackageLoad: 100, GraphValues: 50}
	state := newInvocationStateForHighCardTest(caps)

	var garbage []*bep.BuildMetrics_MemoryMetrics_GarbageMetrics
	for i := range 5 {
		garbage = append(garbage, &bep.BuildMetrics_MemoryMetrics_GarbageMetrics{
			Type: "gen", GarbageCollected: int64(i),
		})
	}
	metrics := &bep.BuildMetrics{
		MemoryMetrics: &bep.BuildMetrics_MemoryMetrics{GarbageMetrics: garbage},
	}

	traces, truncs := state.buildMetricsSpan(metrics, actionDataOptions{})
	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	attr, ok := span.Attributes().Get("bazel.metrics.garbage")
	if !ok {
		t.Fatal("expected bazel.metrics.garbage attribute")
	}
	if got := attr.Slice().Len(); got != 2 {
		t.Errorf("expected capped length 2, got %d", got)
	}
	if len(truncs) != 1 {
		t.Fatalf("expected 1 truncation, got %d: %v", len(truncs), truncs)
	}
	got := truncs[0]
	if got.attribute != "bazel.metrics.garbage" {
		t.Errorf("truncation: attribute=%q", got.attribute)
	}
	if got.original != 5 || got.kept != 2 {
		t.Errorf("truncation: original=%d kept=%d (want 5,2)", got.original, got.kept)
	}
}

func TestBuildMetricsSpan_CapEnforcement_Graph(t *testing.T) {
	caps := HighCardinalityCaps{Garbage: 20, PackageLoad: 100, GraphValues: 3}
	state := newInvocationStateForHighCardTest(caps)

	var stats []*bep.BuildMetrics_EvaluationStat
	for i := range 10 {
		stats = append(stats, &bep.BuildMetrics_EvaluationStat{SkyfunctionName: "FN", Count: int64(i)})
	}
	metrics := &bep.BuildMetrics{
		BuildGraphMetrics: &bep.BuildMetrics_BuildGraphMetrics{
			DirtiedValues: stats,
		},
	}

	traces, truncs := state.buildMetricsSpan(metrics, actionDataOptions{})
	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	attr, _ := span.Attributes().Get("bazel.metrics.graph.dirtied_values")
	if got := attr.Slice().Len(); got != 3 {
		t.Errorf("expected capped length 3, got %d", got)
	}
	if len(truncs) != 1 {
		t.Fatalf("expected 1 truncation, got %d", len(truncs))
	}
	if truncs[0].attribute != "bazel.metrics.graph.dirtied_values" {
		t.Errorf("truncation: attribute=%q", truncs[0].attribute)
	}
	if truncs[0].original != 10 || truncs[0].kept != 3 {
		t.Errorf("truncation: original=%d kept=%d (want 10,3)", truncs[0].original, truncs[0].kept)
	}
}

func TestBuildMetricsSpan_CapEnforcement_PackageLoad(t *testing.T) {
	caps := HighCardinalityCaps{Garbage: 20, PackageLoad: 1, GraphValues: 50}
	state := newInvocationStateForHighCardTest(caps)

	pkgs := []*pkgmetrics.PackageLoadMetrics{
		{Name: new("//a")},
		{Name: new("//b")},
		{Name: new("//c")},
	}
	metrics := &bep.BuildMetrics{
		PackageMetrics: &bep.BuildMetrics_PackageMetrics{PackageLoadMetrics: pkgs},
	}

	traces, truncs := state.buildMetricsSpan(metrics, actionDataOptions{})
	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	attr, _ := span.Attributes().Get("bazel.metrics.package_load")
	if got := attr.Slice().Len(); got != 1 {
		t.Errorf("expected capped length 1, got %d", got)
	}
	if len(truncs) != 1 {
		t.Fatalf("expected 1 truncation, got %d", len(truncs))
	}
	if truncs[0].original != 3 || truncs[0].kept != 1 {
		t.Errorf("truncation: original=%d kept=%d", truncs[0].original, truncs[0].kept)
	}
}

func TestHandleBuildMetrics_LogsTruncationWarning(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	caps := HighCardinalityCaps{Garbage: 1, PackageLoad: 100, GraphValues: 50}
	tb := NewTraceBuilder(nil, nil, nil, logger, TraceBuilderConfig{Caps: caps})
	tb.Start()
	defer tb.Stop()

	state := newInvocationStateForHighCardTest(caps)
	state.pii = tb.pii
	invocations := map[string]*invocationState{"inv-trunc": state}

	metrics := &bep.BuildMetrics{
		MemoryMetrics: &bep.BuildMetrics_MemoryMetrics{
			GarbageMetrics: []*bep.BuildMetrics_MemoryMetrics_GarbageMetrics{
				{Type: "A", GarbageCollected: 1},
				{Type: "B", GarbageCollected: 2},
				{Type: "C", GarbageCollected: 3},
			},
		},
	}

	// Directly invoke handleBuildMetrics to avoid going through gRPC path.
	if err := tb.handleBuildMetrics(context.Background(), invocations, "inv-trunc", metrics); err != nil {
		t.Fatalf("handleBuildMetrics: %v", err)
	}

	entries := logs.FilterMessageSnippet("high-cardinality").All()
	if len(entries) == 0 {
		t.Fatal("expected a warning log about high-cardinality truncation")
	}
	fields := entries[0].ContextMap()
	if fields["invocation_id"] != "inv-trunc" {
		t.Errorf("expected invocation_id=inv-trunc in warning, got %v", fields["invocation_id"])
	}
	if fields["attribute"] != "bazel.metrics.garbage" {
		t.Errorf("expected attribute=bazel.metrics.garbage, got %v", fields["attribute"])
	}
	if fields["original_count"] != int64(3) {
		t.Errorf("expected original_count=3, got %v", fields["original_count"])
	}
	if fields["kept"] != int64(1) {
		t.Errorf("expected kept=1, got %v", fields["kept"])
	}
}

func TestDefaultHighCardinalityCaps(t *testing.T) {
	caps := defaultHighCardinalityCaps()
	if caps.Garbage != 20 {
		t.Errorf("Garbage default: got %d want 20", caps.Garbage)
	}
	if caps.PackageLoad != 100 {
		t.Errorf("PackageLoad default: got %d want 100", caps.PackageLoad)
	}
	if caps.GraphValues != 50 {
		t.Errorf("GraphValues default: got %d want 50", caps.GraphValues)
	}
}

func TestConfigDefault_IncludesHighCardinalityCaps(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if cfg.HighCardinalityCaps.Garbage != 20 {
		t.Errorf("HighCardinalityCaps.Garbage: got %d want 20", cfg.HighCardinalityCaps.Garbage)
	}
	if cfg.HighCardinalityCaps.PackageLoad != 100 {
		t.Errorf("HighCardinalityCaps.PackageLoad: got %d want 100", cfg.HighCardinalityCaps.PackageLoad)
	}
	if cfg.HighCardinalityCaps.GraphValues != 50 {
		t.Errorf("HighCardinalityCaps.GraphValues: got %d want 50", cfg.HighCardinalityCaps.GraphValues)
	}
}
