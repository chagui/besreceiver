package besreceiver

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"google.golang.org/protobuf/types/known/durationpb"

	bep "github.com/chagui/besreceiver/internal/bep/buildeventstream"
)

// newMetricsSpanState constructs the minimal invocationState needed to exercise
// buildMetricsSpan. Shared across the extended-metrics attribute tests below.
func newMetricsSpanState() *invocationState {
	return &invocationState{
		traceID:    pcommon.TraceID{1},
		rootSpanID: pcommon.SpanID{1},
		uuid:       "uuid-metrics",
		createdAt:  time.Now(),
	}
}

func metricsSpanAttrs(t *testing.T, metrics *bep.BuildMetrics) pcommon.Map {
	t.Helper()
	state := newMetricsSpanState()
	traces, _ := state.buildMetricsSpan(metrics)
	if traces.SpanCount() != 1 {
		t.Fatalf("expected 1 span, got %d", traces.SpanCount())
	}
	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	if got := span.Name(); got != "bazel.metrics" {
		t.Fatalf("expected span name bazel.metrics, got %q", got)
	}
	return span.Attributes()
}

func mustInt(t *testing.T, attrs pcommon.Map, key string, want int64) {
	t.Helper()
	v, ok := attrs.Get(key)
	if !ok {
		t.Errorf("missing attribute %q", key)
		return
	}
	if v.Int() != want {
		t.Errorf("attr %q: got %d, want %d", key, v.Int(), want)
	}
}

func mustAbsent(t *testing.T, attrs pcommon.Map, key string) {
	t.Helper()
	if _, ok := attrs.Get(key); ok {
		t.Errorf("attribute %q unexpectedly present", key)
	}
}

func TestBuildMetricsSpan_NilBuildMetricsEmitsEmptySpan(t *testing.T) {
	attrs := metricsSpanAttrs(t, &bep.BuildMetrics{})
	if attrs.Len() != 0 {
		t.Errorf("expected no attributes when all sub-messages nil, got %d", attrs.Len())
	}
}

func TestBuildMetricsSpan_TimingExtras(t *testing.T) {
	metrics := &bep.BuildMetrics{
		TimingMetrics: &bep.BuildMetrics_TimingMetrics{
			WallTimeInMs:              10000,
			CpuTimeInMs:               5000,
			AnalysisPhaseTimeInMs:     1000,
			ExecutionPhaseTimeInMs:    2000,
			ActionsExecutionStartInMs: 500,
			CriticalPathTime:          durationpb.New(3 * time.Second),
		},
	}
	attrs := metricsSpanAttrs(t, metrics)
	mustInt(t, attrs, "bazel.metrics.wall_time_ms", 10000)
	mustInt(t, attrs, "bazel.metrics.cpu_time_ms", 5000)
	mustInt(t, attrs, "bazel.metrics.analysis_phase_time_ms", 1000)
	mustInt(t, attrs, "bazel.metrics.execution_phase_time_ms", 2000)
	mustInt(t, attrs, "bazel.metrics.actions_execution_start_ms", 500)
	mustInt(t, attrs, "bazel.metrics.critical_path_time_ms", 3000)
}

// Critical-path duration is a proto sub-message; missing means "unmeasured".
// We don't want to emit a misleading zero, so ensure the attribute is absent.
func TestBuildMetricsSpan_TimingWithoutCriticalPath(t *testing.T) {
	metrics := &bep.BuildMetrics{
		TimingMetrics: &bep.BuildMetrics_TimingMetrics{
			WallTimeInMs: 100,
		},
	}
	attrs := metricsSpanAttrs(t, metrics)
	mustInt(t, attrs, "bazel.metrics.wall_time_ms", 100)
	mustAbsent(t, attrs, "bazel.metrics.critical_path_time_ms")
}

func TestBuildMetricsSpan_ActionSummary(t *testing.T) {
	metrics := &bep.BuildMetrics{
		ActionSummary: &bep.BuildMetrics_ActionSummary{
			ActionsCreated:                    500,
			ActionsCreatedNotIncludingAspects: 480,
			ActionsExecuted:                   42,
		},
	}
	attrs := metricsSpanAttrs(t, metrics)
	mustInt(t, attrs, "bazel.metrics.actions_created", 500)
	mustInt(t, attrs, "bazel.metrics.actions_created_not_including_aspects", 480)
	mustInt(t, attrs, "bazel.metrics.actions_executed", 42)
}

func TestBuildMetricsSpan_Memory(t *testing.T) {
	metrics := &bep.BuildMetrics{
		MemoryMetrics: &bep.BuildMetrics_MemoryMetrics{
			UsedHeapSizePostBuild:          1 << 30,
			PeakPostGcHeapSize:             2 << 30,
			PeakPostGcTenuredSpaceHeapSize: 3 << 30,
		},
	}
	attrs := metricsSpanAttrs(t, metrics)
	mustInt(t, attrs, "bazel.metrics.memory.used_heap_size_post_build", 1<<30)
	mustInt(t, attrs, "bazel.metrics.memory.peak_post_gc_heap_size", 2<<30)
	mustInt(t, attrs, "bazel.metrics.memory.peak_post_gc_tenured_space_heap_size", 3<<30)
}

// Deprecated targets_loaded stays off the span — verify it is absent even when
// populated upstream (Bazel versions still set it).
func TestBuildMetricsSpan_TargetSkipsDeprecatedTargetsLoaded(t *testing.T) {
	metrics := &bep.BuildMetrics{
		TargetMetrics: &bep.BuildMetrics_TargetMetrics{
			TargetsLoaded:                        9999,
			TargetsConfigured:                    1234,
			TargetsConfiguredNotIncludingAspects: 1200,
		},
	}
	attrs := metricsSpanAttrs(t, metrics)
	mustAbsent(t, attrs, "bazel.metrics.targets_loaded")
	mustInt(t, attrs, "bazel.metrics.targets_configured", 1234)
	mustInt(t, attrs, "bazel.metrics.targets_configured_not_including_aspects", 1200)
}

func TestBuildMetricsSpan_Package(t *testing.T) {
	metrics := &bep.BuildMetrics{
		PackageMetrics: &bep.BuildMetrics_PackageMetrics{
			PackagesLoaded: 321,
		},
	}
	attrs := metricsSpanAttrs(t, metrics)
	mustInt(t, attrs, "bazel.metrics.packages_loaded", 321)
}

func TestBuildMetricsSpan_BuildGraph(t *testing.T) {
	metrics := &bep.BuildMetrics{
		BuildGraphMetrics: &bep.BuildMetrics_BuildGraphMetrics{
			ActionLookupValueCount:                    10,
			ActionLookupValueCountNotIncludingAspects: 9,
			ActionCount:                     100,
			ActionCountNotIncludingAspects:  95,
			InputFileConfiguredTargetCount:  50,
			OutputFileConfiguredTargetCount: 25,
			OtherConfiguredTargetCount:      5,
			OutputArtifactCount:             200,
			PostInvocationSkyframeNodeCount: 5000,
		},
	}
	attrs := metricsSpanAttrs(t, metrics)
	mustInt(t, attrs, "bazel.metrics.graph.action_lookup_value_count", 10)
	mustInt(t, attrs, "bazel.metrics.graph.action_lookup_value_count_not_including_aspects", 9)
	mustInt(t, attrs, "bazel.metrics.graph.action_count", 100)
	mustInt(t, attrs, "bazel.metrics.graph.action_count_not_including_aspects", 95)
	mustInt(t, attrs, "bazel.metrics.graph.input_file_configured_target_count", 50)
	mustInt(t, attrs, "bazel.metrics.graph.output_file_configured_target_count", 25)
	mustInt(t, attrs, "bazel.metrics.graph.other_configured_target_count", 5)
	mustInt(t, attrs, "bazel.metrics.graph.output_artifact_count", 200)
	mustInt(t, attrs, "bazel.metrics.graph.post_invocation_skyframe_node_count", 5000)
}

func TestBuildMetricsSpan_Artifacts(t *testing.T) {
	metrics := &bep.BuildMetrics{
		ArtifactMetrics: &bep.BuildMetrics_ArtifactMetrics{
			SourceArtifactsRead: &bep.BuildMetrics_ArtifactMetrics_FilesMetric{
				SizeInBytes: 1000, Count: 10,
			},
			OutputArtifactsSeen: &bep.BuildMetrics_ArtifactMetrics_FilesMetric{
				SizeInBytes: 2000, Count: 20,
			},
			OutputArtifactsFromActionCache: &bep.BuildMetrics_ArtifactMetrics_FilesMetric{
				SizeInBytes: 3000, Count: 30,
			},
			TopLevelArtifacts: &bep.BuildMetrics_ArtifactMetrics_FilesMetric{
				SizeInBytes: 4000, Count: 4,
			},
		},
	}
	attrs := metricsSpanAttrs(t, metrics)
	mustInt(t, attrs, "bazel.metrics.artifacts.source_read_count", 10)
	mustInt(t, attrs, "bazel.metrics.artifacts.source_read_bytes", 1000)
	mustInt(t, attrs, "bazel.metrics.artifacts.output_count", 20)
	mustInt(t, attrs, "bazel.metrics.artifacts.output_bytes", 2000)
	mustInt(t, attrs, "bazel.metrics.artifacts.from_action_cache_count", 30)
	mustInt(t, attrs, "bazel.metrics.artifacts.from_action_cache_bytes", 3000)
	mustInt(t, attrs, "bazel.metrics.artifacts.top_level_count", 4)
	mustInt(t, attrs, "bazel.metrics.artifacts.top_level_bytes", 4000)
}

func TestBuildMetricsSpan_ArtifactsPartial(t *testing.T) {
	// Only one FilesMetric populated — the others must remain absent, not zero-filled.
	metrics := &bep.BuildMetrics{
		ArtifactMetrics: &bep.BuildMetrics_ArtifactMetrics{
			SourceArtifactsRead: &bep.BuildMetrics_ArtifactMetrics_FilesMetric{
				SizeInBytes: 42, Count: 2,
			},
		},
	}
	attrs := metricsSpanAttrs(t, metrics)
	mustInt(t, attrs, "bazel.metrics.artifacts.source_read_count", 2)
	mustInt(t, attrs, "bazel.metrics.artifacts.source_read_bytes", 42)
	mustAbsent(t, attrs, "bazel.metrics.artifacts.output_count")
	mustAbsent(t, attrs, "bazel.metrics.artifacts.output_bytes")
	mustAbsent(t, attrs, "bazel.metrics.artifacts.from_action_cache_count")
	mustAbsent(t, attrs, "bazel.metrics.artifacts.top_level_count")
}

func TestBuildMetricsSpan_Network(t *testing.T) {
	metrics := &bep.BuildMetrics{
		NetworkMetrics: &bep.BuildMetrics_NetworkMetrics{
			SystemNetworkStats: &bep.BuildMetrics_NetworkMetrics_SystemNetworkStats{
				BytesSent:             100,
				BytesRecv:             200,
				PacketsSent:           10,
				PacketsRecv:           20,
				PeakBytesSentPerSec:   1000,
				PeakBytesRecvPerSec:   2000,
				PeakPacketsSentPerSec: 100,
				PeakPacketsRecvPerSec: 200,
			},
		},
	}
	attrs := metricsSpanAttrs(t, metrics)
	mustInt(t, attrs, "bazel.metrics.network.bytes_sent", 100)
	mustInt(t, attrs, "bazel.metrics.network.bytes_recv", 200)
	mustInt(t, attrs, "bazel.metrics.network.packets_sent", 10)
	mustInt(t, attrs, "bazel.metrics.network.packets_recv", 20)
	mustInt(t, attrs, "bazel.metrics.network.peak_bytes_sent_per_sec", 1000)
	mustInt(t, attrs, "bazel.metrics.network.peak_bytes_recv_per_sec", 2000)
	mustInt(t, attrs, "bazel.metrics.network.peak_packets_sent_per_sec", 100)
	mustInt(t, attrs, "bazel.metrics.network.peak_packets_recv_per_sec", 200)
}

// NetworkMetrics present but SystemNetworkStats nil is indistinguishable from
// "Bazel didn't collect network data"; emitting zeros would mislead dashboards.
func TestBuildMetricsSpan_NetworkWithoutSystemStats(t *testing.T) {
	metrics := &bep.BuildMetrics{
		NetworkMetrics: &bep.BuildMetrics_NetworkMetrics{},
	}
	attrs := metricsSpanAttrs(t, metrics)
	mustAbsent(t, attrs, "bazel.metrics.network.bytes_sent")
	mustAbsent(t, attrs, "bazel.metrics.network.peak_bytes_sent_per_sec")
}

func TestBuildMetricsSpan_Cumulative(t *testing.T) {
	metrics := &bep.BuildMetrics{
		CumulativeMetrics: &bep.BuildMetrics_CumulativeMetrics{
			NumAnalyses: 3,
			NumBuilds:   2,
		},
	}
	attrs := metricsSpanAttrs(t, metrics)
	mustInt(t, attrs, "bazel.metrics.cumulative.num_analyses", 3)
	mustInt(t, attrs, "bazel.metrics.cumulative.num_builds", 2)
}

func TestBuildMetricsSpan_DynamicExecution(t *testing.T) {
	metrics := &bep.BuildMetrics{
		DynamicExecutionMetrics: &bep.BuildMetrics_DynamicExecutionMetrics{
			RaceStatistics: []*bep.BuildMetrics_DynamicExecutionMetrics_RaceStatistics{
				{Mnemonic: "Javac", LocalRunner: "local", RemoteRunner: "remote", LocalWins: 5, RemoteWins: 10},
				{Mnemonic: "CppCompile", LocalRunner: "local", RemoteRunner: "remote", LocalWins: 3, RemoteWins: 7},
			},
		},
	}
	attrs := metricsSpanAttrs(t, metrics)
	v, ok := attrs.Get("bazel.metrics.dynamic_execution")
	if !ok {
		t.Fatal("missing bazel.metrics.dynamic_execution")
	}
	slice := v.Slice()
	if slice.Len() != 2 {
		t.Fatalf("expected 2 race stats, got %d", slice.Len())
	}
	first := slice.At(0).Map()
	if m, _ := first.Get("mnemonic"); m.Str() != "Javac" {
		t.Errorf("expected first mnemonic Javac, got %q", m.Str())
	}
	if w, _ := first.Get("local_wins"); w.Int() != 5 {
		t.Errorf("expected local_wins=5, got %d", w.Int())
	}
	if w, _ := first.Get("remote_wins"); w.Int() != 10 {
		t.Errorf("expected remote_wins=10, got %d", w.Int())
	}
	if r, _ := first.Get("local_runner"); r.Str() != "local" {
		t.Errorf("expected local_runner=local, got %q", r.Str())
	}
	if r, _ := first.Get("remote_runner"); r.Str() != "remote" {
		t.Errorf("expected remote_runner=remote, got %q", r.Str())
	}
}

// Cardinality cap: 25 incoming entries -> 20 emitted. Protects trace backends
// against pathological Bazel workspaces with thousands of distinct mnemonics.
func TestBuildMetricsSpan_DynamicExecutionCap(t *testing.T) {
	var stats []*bep.BuildMetrics_DynamicExecutionMetrics_RaceStatistics
	for i := range 25 {
		stats = append(stats, &bep.BuildMetrics_DynamicExecutionMetrics_RaceStatistics{
			Mnemonic: "m", LocalWins: int32(i),
		})
	}
	metrics := &bep.BuildMetrics{
		DynamicExecutionMetrics: &bep.BuildMetrics_DynamicExecutionMetrics{RaceStatistics: stats},
	}
	attrs := metricsSpanAttrs(t, metrics)
	v, ok := attrs.Get("bazel.metrics.dynamic_execution")
	if !ok {
		t.Fatal("missing bazel.metrics.dynamic_execution")
	}
	if v.Slice().Len() != maxBuildMetricsSliceEntries {
		t.Errorf("expected slice capped at %d, got %d", maxBuildMetricsSliceEntries, v.Slice().Len())
	}
}

// Empty RaceStatistics should leave the attr unset, not emit an empty slice.
func TestBuildMetricsSpan_DynamicExecutionEmpty(t *testing.T) {
	metrics := &bep.BuildMetrics{
		DynamicExecutionMetrics: &bep.BuildMetrics_DynamicExecutionMetrics{},
	}
	attrs := metricsSpanAttrs(t, metrics)
	mustAbsent(t, attrs, "bazel.metrics.dynamic_execution")
}

func TestBuildMetricsSpan_WorkerPool(t *testing.T) {
	metrics := &bep.BuildMetrics{
		WorkerPoolMetrics: &bep.BuildMetrics_WorkerPoolMetrics{
			WorkerPoolStats: []*bep.BuildMetrics_WorkerPoolMetrics_WorkerPoolStats{
				{Mnemonic: "Javac", CreatedCount: 5, DestroyedCount: 2, EvictedCount: 1, AliveCount: 3},
			},
		},
	}
	attrs := metricsSpanAttrs(t, metrics)
	v, ok := attrs.Get("bazel.metrics.worker_pool")
	if !ok {
		t.Fatal("missing bazel.metrics.worker_pool")
	}
	slice := v.Slice()
	if slice.Len() != 1 {
		t.Fatalf("expected 1 worker pool stat, got %d", slice.Len())
	}
	first := slice.At(0).Map()
	if m, _ := first.Get("mnemonic"); m.Str() != "Javac" {
		t.Errorf("expected mnemonic=Javac, got %q", m.Str())
	}
	if c, _ := first.Get("created_count"); c.Int() != 5 {
		t.Errorf("expected created_count=5, got %d", c.Int())
	}
	if c, _ := first.Get("destroyed_count"); c.Int() != 2 {
		t.Errorf("expected destroyed_count=2, got %d", c.Int())
	}
	if c, _ := first.Get("evicted_count"); c.Int() != 1 {
		t.Errorf("expected evicted_count=1, got %d", c.Int())
	}
	if c, _ := first.Get("alive_count"); c.Int() != 3 {
		t.Errorf("expected alive_count=3, got %d", c.Int())
	}
}

func TestBuildMetricsSpan_WorkerPoolCap(t *testing.T) {
	var stats []*bep.BuildMetrics_WorkerPoolMetrics_WorkerPoolStats
	for range 50 {
		stats = append(stats, &bep.BuildMetrics_WorkerPoolMetrics_WorkerPoolStats{Mnemonic: "m"})
	}
	metrics := &bep.BuildMetrics{
		WorkerPoolMetrics: &bep.BuildMetrics_WorkerPoolMetrics{WorkerPoolStats: stats},
	}
	attrs := metricsSpanAttrs(t, metrics)
	v, ok := attrs.Get("bazel.metrics.worker_pool")
	if !ok {
		t.Fatal("missing bazel.metrics.worker_pool")
	}
	if v.Slice().Len() != maxBuildMetricsSliceEntries {
		t.Errorf("expected slice capped at %d, got %d", maxBuildMetricsSliceEntries, v.Slice().Len())
	}
}

func TestBuildMetricsSpan_WorkerPoolEmpty(t *testing.T) {
	metrics := &bep.BuildMetrics{
		WorkerPoolMetrics: &bep.BuildMetrics_WorkerPoolMetrics{},
	}
	attrs := metricsSpanAttrs(t, metrics)
	mustAbsent(t, attrs, "bazel.metrics.worker_pool")
}

// Intentionally-skipped sub-messages after #29 landed: RuleClassCount and
// AspectCount on BuildGraphMetrics, WorkerMetrics, and
// RemoteAnalysisCacheStatistics. GarbageMetrics / PackageLoadMetrics /
// EvaluationStat slices are now surfaced by #29 and covered by highcard_test.go.
func TestBuildMetricsSpan_SkipsHighCardinalitySubMessages(t *testing.T) {
	metrics := &bep.BuildMetrics{
		MemoryMetrics: &bep.BuildMetrics_MemoryMetrics{
			UsedHeapSizePostBuild: 1,
			GarbageMetrics: []*bep.BuildMetrics_MemoryMetrics_GarbageMetrics{
				{Type: "G1 Young"},
			},
		},
		BuildGraphMetrics: &bep.BuildMetrics_BuildGraphMetrics{
			ActionCount:     1,
			DirtiedValues:   []*bep.BuildMetrics_EvaluationStat{{SkyfunctionName: "x", Count: 1}},
			ChangedValues:   []*bep.BuildMetrics_EvaluationStat{{SkyfunctionName: "x", Count: 1}},
			BuiltValues:     []*bep.BuildMetrics_EvaluationStat{{SkyfunctionName: "x", Count: 1}},
			EvaluatedValues: []*bep.BuildMetrics_EvaluationStat{{SkyfunctionName: "x", Count: 1}},
			RuleClass:       []*bep.BuildMetrics_BuildGraphMetrics_RuleClassCount{{RuleClass: "rule", Count: 1}},
			Aspect:          []*bep.BuildMetrics_BuildGraphMetrics_AspectCount{{AspectName: "a", Count: 1}},
		},
		WorkerMetrics: []*bep.BuildMetrics_WorkerMetrics{
			{WorkerIds: []uint32{1}, Mnemonic: "Javac"},
		},
		RemoteAnalysisCacheStatistics: &bep.BuildMetrics_RemoteAnalysisCacheStatistics{
			CacheHits: 1,
		},
	}

	attrs := metricsSpanAttrs(t, metrics)
	// Scalar fields from Memory + BuildGraph ARE present; slices ARE NOT.
	mustInt(t, attrs, "bazel.metrics.memory.used_heap_size_post_build", 1)
	mustInt(t, attrs, "bazel.metrics.graph.action_count", 1)

	skippedKeys := []string{
		"bazel.metrics.graph.rule_class",
		"bazel.metrics.graph.aspect",
		"bazel.metrics.worker_metrics",
		"bazel.metrics.remote_analysis_cache",
	}
	for _, k := range skippedKeys {
		mustAbsent(t, attrs, k)
	}
}
