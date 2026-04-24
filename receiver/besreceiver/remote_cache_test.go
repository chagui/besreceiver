package besreceiver

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/chagui/besreceiver/internal/bep/actioncache"
	bep "github.com/chagui/besreceiver/internal/bep/buildeventstream"
)

// --- Shared helpers ---------------------------------------------------------

// newStateWithTarget produces an invocationState with one pre-configured
// target span so addTestResult can resolve its parent without triggering the
// orphan/reparent path. Label is "//pkg:test" by convention.
func newStateWithTarget(t *testing.T) *invocationState {
	t.Helper()
	traces, ss := newTracesPayload()
	state := &invocationState{
		traceID:       pcommon.TraceID{0x01, 0x02},
		rootSpanID:    pcommon.SpanID{0x0a, 0x0b},
		uuid:          "uuid-test",
		targets:       make(map[string]ptrace.Span),
		pendingTraces: traces,
		scopeSpans:    ss,
	}
	state.addTarget("//pkg:test", "test_suite rule", "")
	return state
}

// findTestSpan returns the first bazel.test span in the state's batch.
func findTestSpan(t *testing.T, state *invocationState) ptrace.Span {
	t.Helper()
	spans := state.scopeSpans.Spans()
	for i := range spans.Len() {
		if spans.At(i).Name() == "bazel.test" {
			return spans.At(i)
		}
	}
	t.Fatalf("no bazel.test span found in %d spans", spans.Len())
	return ptrace.Span{}
}

// --- bazel.test ExecutionInfo -----------------------------------------------

func TestAddTestResult_NilExecutionInfo(t *testing.T) {
	state := newStateWithTarget(t)
	result := &bep.TestResult{
		Status: bep.TestStatus_PASSED,
		// ExecutionInfo: nil — must not break existing behavior.
	}
	tr := &bep.BuildEventId_TestResultId{Label: "//pkg:test", Run: 1}

	state.addTestResult("//pkg:test", "", tr, result)

	span := findTestSpan(t, state)
	// Status still present.
	v, ok := span.Attributes().Get("bazel.test.status")
	require.True(t, ok)
	assert.Equal(t, "PASSED", v.Str())
	// cached_locally always emitted (even zero).
	cl, ok := span.Attributes().Get("bazel.test.cached_locally")
	require.True(t, ok)
	assert.False(t, cl.Bool())
	// No execution attrs emitted.
	_, ok = span.Attributes().Get("bazel.test.execution.strategy")
	assert.False(t, ok, "expected no execution.strategy on nil ExecutionInfo")
	_, ok = span.Attributes().Get("bazel.test.execution.hostname")
	assert.False(t, ok)
}

func TestAddTestResult_CachedLocally(t *testing.T) {
	state := newStateWithTarget(t)
	result := &bep.TestResult{
		Status:        bep.TestStatus_PASSED,
		CachedLocally: true,
	}
	tr := &bep.BuildEventId_TestResultId{Label: "//pkg:test", Run: 1}

	state.addTestResult("//pkg:test", "", tr, result)

	span := findTestSpan(t, state)
	v, ok := span.Attributes().Get("bazel.test.cached_locally")
	require.True(t, ok)
	assert.True(t, v.Bool())
}

func TestAddTestResult_ExecutionInfo_Basic(t *testing.T) {
	state := newStateWithTarget(t)
	result := &bep.TestResult{
		Status: bep.TestStatus_PASSED,
		ExecutionInfo: &bep.TestResult_ExecutionInfo{
			Strategy:       "remote",
			CachedRemotely: true,
			Hostname:       "worker-42.example.com",
		},
	}
	tr := &bep.BuildEventId_TestResultId{Label: "//pkg:test", Run: 1}

	state.addTestResult("//pkg:test", "", tr, result)

	span := findTestSpan(t, state)

	strategy, ok := span.Attributes().Get("bazel.test.execution.strategy")
	require.True(t, ok)
	assert.Equal(t, "remote", strategy.Str())

	cached, ok := span.Attributes().Get("bazel.test.execution.cached_remotely")
	require.True(t, ok)
	assert.True(t, cached.Bool())

	host, ok := span.Attributes().Get("bazel.test.execution.hostname")
	require.True(t, ok)
	assert.Equal(t, "worker-42.example.com", host.Str())
}

func TestAddTestResult_ExecutionInfo_EmptyStringsAreOmitted(t *testing.T) {
	state := newStateWithTarget(t)
	result := &bep.TestResult{
		Status: bep.TestStatus_PASSED,
		ExecutionInfo: &bep.TestResult_ExecutionInfo{
			Strategy:       "", // omitted
			CachedRemotely: false,
			Hostname:       "", // omitted
		},
	}
	tr := &bep.BuildEventId_TestResultId{Label: "//pkg:test", Run: 1}

	state.addTestResult("//pkg:test", "", tr, result)

	span := findTestSpan(t, state)
	_, ok := span.Attributes().Get("bazel.test.execution.strategy")
	assert.False(t, ok, "expected no strategy for empty string")
	_, ok = span.Attributes().Get("bazel.test.execution.hostname")
	assert.False(t, ok, "expected no hostname for empty string")
	// cached_remotely is always emitted (even zero value).
	cr, ok := span.Attributes().Get("bazel.test.execution.cached_remotely")
	require.True(t, ok)
	assert.False(t, cr.Bool())
}

func TestAddTestResult_TimingBreakdown_Flattened(t *testing.T) {
	state := newStateWithTarget(t)
	result := &bep.TestResult{
		Status: bep.TestStatus_PASSED,
		ExecutionInfo: &bep.TestResult_ExecutionInfo{
			TimingBreakdown: &bep.TestResult_ExecutionInfo_TimingBreakdown{
				Name: "total",
				Time: durationpb.New(5 * time.Second),
				Child: []*bep.TestResult_ExecutionInfo_TimingBreakdown{
					{Name: "setup", Time: durationpb.New(1 * time.Second)},
					{Name: "run", Time: durationpb.New(4 * time.Second)},
				},
			},
		},
	}
	tr := &bep.BuildEventId_TestResultId{Label: "//pkg:test", Run: 1}

	state.addTestResult("//pkg:test", "", tr, result)

	span := findTestSpan(t, state)

	total, ok := span.Attributes().Get("bazel.test.timing.total_ms")
	require.True(t, ok)
	assert.EqualValues(t, 5000, total.Int())

	setup, ok := span.Attributes().Get("bazel.test.timing.setup_ms")
	require.True(t, ok)
	assert.EqualValues(t, 1000, setup.Int())

	run, ok := span.Attributes().Get("bazel.test.timing.run_ms")
	require.True(t, ok)
	assert.EqualValues(t, 4000, run.Int())
}

func TestAddTestResult_TimingBreakdown_DepthCapAt2(t *testing.T) {
	state := newStateWithTarget(t)
	// 3-level tree: root → level2 → level3. level3 must be dropped (depth cap 2).
	result := &bep.TestResult{
		Status: bep.TestStatus_PASSED,
		ExecutionInfo: &bep.TestResult_ExecutionInfo{
			TimingBreakdown: &bep.TestResult_ExecutionInfo_TimingBreakdown{
				Name: "total",
				Time: durationpb.New(10 * time.Second),
				Child: []*bep.TestResult_ExecutionInfo_TimingBreakdown{
					{
						Name: "setup",
						Time: durationpb.New(4 * time.Second),
						Child: []*bep.TestResult_ExecutionInfo_TimingBreakdown{
							{Name: "setup_detail", Time: durationpb.New(2 * time.Second)},
						},
					},
				},
			},
		},
	}
	tr := &bep.BuildEventId_TestResultId{Label: "//pkg:test", Run: 1}

	state.addTestResult("//pkg:test", "", tr, result)

	span := findTestSpan(t, state)

	_, ok := span.Attributes().Get("bazel.test.timing.total_ms")
	assert.True(t, ok, "depth-1 root must be present")
	_, ok = span.Attributes().Get("bazel.test.timing.setup_ms")
	assert.True(t, ok, "depth-2 child must be present")
	_, ok = span.Attributes().Get("bazel.test.timing.setup_detail_ms")
	assert.False(t, ok, "depth-3 grandchild must be dropped by cap")
}

func TestAddTestResult_TimingBreakdown_FallsBackToTimeMillis(t *testing.T) {
	state := newStateWithTarget(t)
	result := &bep.TestResult{
		Status: bep.TestStatus_PASSED,
		ExecutionInfo: &bep.TestResult_ExecutionInfo{
			TimingBreakdown: &bep.TestResult_ExecutionInfo_TimingBreakdown{
				Name: "total",
				// Time is nil; rely on deprecated TimeMillis.
				TimeMillis: 2500,
			},
		},
	}
	tr := &bep.BuildEventId_TestResultId{Label: "//pkg:test", Run: 1}

	state.addTestResult("//pkg:test", "", tr, result)

	span := findTestSpan(t, state)
	total, ok := span.Attributes().Get("bazel.test.timing.total_ms")
	require.True(t, ok)
	assert.EqualValues(t, 2500, total.Int())
}

func TestAddTestResult_TimingBreakdown_EmptyNameSkipped(t *testing.T) {
	state := newStateWithTarget(t)
	result := &bep.TestResult{
		Status: bep.TestStatus_PASSED,
		ExecutionInfo: &bep.TestResult_ExecutionInfo{
			TimingBreakdown: &bep.TestResult_ExecutionInfo_TimingBreakdown{
				Name: "", // nothing to key on — skip, but still walk children
				Child: []*bep.TestResult_ExecutionInfo_TimingBreakdown{
					{Name: "run", Time: durationpb.New(3 * time.Second)},
				},
			},
		},
	}
	tr := &bep.BuildEventId_TestResultId{Label: "//pkg:test", Run: 1}

	state.addTestResult("//pkg:test", "", tr, result)

	span := findTestSpan(t, state)
	// Child still emitted.
	run, ok := span.Attributes().Get("bazel.test.timing.run_ms")
	require.True(t, ok)
	assert.EqualValues(t, 3000, run.Int())
}

func TestAddTestResult_ResourceUsage(t *testing.T) {
	state := newStateWithTarget(t)
	result := &bep.TestResult{
		Status: bep.TestStatus_PASSED,
		ExecutionInfo: &bep.TestResult_ExecutionInfo{
			ResourceUsage: []*bep.TestResult_ExecutionInfo_ResourceUsage{
				{Name: "memory_bytes", Value: 104857600},
				{Name: "cpu_seconds", Value: 42},
			},
		},
	}
	tr := &bep.BuildEventId_TestResultId{Label: "//pkg:test", Run: 1}

	state.addTestResult("//pkg:test", "", tr, result)

	span := findTestSpan(t, state)
	mem, ok := span.Attributes().Get("bazel.test.resource.memory_bytes")
	require.True(t, ok)
	assert.EqualValues(t, 104857600, mem.Int())

	cpu, ok := span.Attributes().Get("bazel.test.resource.cpu_seconds")
	require.True(t, ok)
	assert.EqualValues(t, 42, cpu.Int())
}

func TestAddTestResult_ResourceUsage_CapAt20(t *testing.T) {
	state := newStateWithTarget(t)
	// Build 25 entries; only the first 20 must be stamped.
	var usage []*bep.TestResult_ExecutionInfo_ResourceUsage
	for i := range 25 {
		usage = append(usage, &bep.TestResult_ExecutionInfo_ResourceUsage{
			Name:  fmt.Sprintf("metric_%02d", i),
			Value: int64(i),
		})
	}
	result := &bep.TestResult{
		Status: bep.TestStatus_PASSED,
		ExecutionInfo: &bep.TestResult_ExecutionInfo{
			ResourceUsage: usage,
		},
	}
	tr := &bep.BuildEventId_TestResultId{Label: "//pkg:test", Run: 1}

	state.addTestResult("//pkg:test", "", tr, result)

	span := findTestSpan(t, state)

	kept := 0
	span.Attributes().Range(func(k string, _ pcommon.Value) bool {
		if len(k) > len("bazel.test.resource.") && k[:len("bazel.test.resource.")] == "bazel.test.resource." {
			kept++
		}
		return true
	})
	assert.Equal(t, 20, kept, "expected cap at 20 resource entries")

	// First 20 must be present; index 20..24 must be absent.
	_, ok := span.Attributes().Get("bazel.test.resource.metric_00")
	assert.True(t, ok)
	_, ok = span.Attributes().Get("bazel.test.resource.metric_19")
	assert.True(t, ok)
	_, ok = span.Attributes().Get("bazel.test.resource.metric_20")
	assert.False(t, ok, "entries past the cap must be dropped")
}

// --- bazel.metrics ActionCacheStatistics / runners --------------------------

func TestBuildMetricsSpan_NilActionCacheStatistics(t *testing.T) {
	state := &invocationState{
		traceID:    pcommon.TraceID{0x01},
		rootSpanID: pcommon.SpanID{0x0a},
		uuid:       "uuid-metrics",
	}
	metrics := &bep.BuildMetrics{
		ActionSummary: &bep.BuildMetrics_ActionSummary{
			ActionsCreated:  5,
			ActionsExecuted: 3,
			// ActionCacheStatistics: nil — must not break.
		},
	}

	traces := state.buildMetricsSpan(metrics)
	require.Equal(t, 1, traces.SpanCount())
	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	assert.Equal(t, "bazel.metrics", span.Name())

	// Existing attrs still present.
	created, ok := span.Attributes().Get("bazel.metrics.actions_created")
	require.True(t, ok)
	assert.EqualValues(t, 5, created.Int())

	// No cache attrs emitted.
	_, ok = span.Attributes().Get("bazel.metrics.action_cache.hits")
	assert.False(t, ok)
	_, ok = span.Attributes().Get("bazel.metrics.action_cache.misses")
	assert.False(t, ok)

	// remote_cache_hits is emitted whenever ActionSummary exists (zero-valued).
	rch, ok := span.Attributes().Get("bazel.metrics.remote_cache_hits")
	require.True(t, ok)
	assert.EqualValues(t, 0, rch.Int())
}

func TestBuildMetricsSpan_NilActionSummary(t *testing.T) {
	state := &invocationState{
		traceID:    pcommon.TraceID{0x01},
		rootSpanID: pcommon.SpanID{0x0a},
		uuid:       "uuid-metrics",
	}
	metrics := &bep.BuildMetrics{} // all sub-messages nil

	traces := state.buildMetricsSpan(metrics)
	require.Equal(t, 1, traces.SpanCount())
	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)

	// Neither ActionSummary nor ActionCacheStatistics attrs emitted.
	_, ok := span.Attributes().Get("bazel.metrics.actions_created")
	assert.False(t, ok)
	_, ok = span.Attributes().Get("bazel.metrics.action_cache.hits")
	assert.False(t, ok)
	_, ok = span.Attributes().Get("bazel.metrics.remote_cache_hits")
	assert.False(t, ok)
}

func TestBuildMetricsSpan_ActionCacheStatistics(t *testing.T) {
	state := &invocationState{
		traceID:    pcommon.TraceID{0x01},
		rootSpanID: pcommon.SpanID{0x0a},
		uuid:       "uuid-metrics",
	}
	metrics := &bep.BuildMetrics{
		ActionSummary: &bep.BuildMetrics_ActionSummary{
			RemoteCacheHits: 17,
			ActionCacheStatistics: &actioncache.ActionCacheStatistics{
				Hits:         123,
				Misses:       45,
				SizeInBytes:  1 << 30, // 1 GiB
				LoadTimeInMs: 250,
				SaveTimeInMs: 80,
				MissDetails: []*actioncache.ActionCacheStatistics_MissDetail{
					{Reason: actioncache.ActionCacheStatistics_NOT_CACHED, Count: 30},
					{Reason: actioncache.ActionCacheStatistics_DIGEST_MISMATCH, Count: 15},
				},
			},
		},
	}

	traces := state.buildMetricsSpan(metrics)
	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)

	hits, ok := span.Attributes().Get("bazel.metrics.action_cache.hits")
	require.True(t, ok)
	assert.EqualValues(t, 123, hits.Int())

	misses, ok := span.Attributes().Get("bazel.metrics.action_cache.misses")
	require.True(t, ok)
	assert.EqualValues(t, 45, misses.Int())

	size, ok := span.Attributes().Get("bazel.metrics.action_cache.size_bytes")
	require.True(t, ok)
	assert.EqualValues(t, 1<<30, size.Int())

	load, ok := span.Attributes().Get("bazel.metrics.action_cache.load_time_ms")
	require.True(t, ok)
	assert.EqualValues(t, 250, load.Int())

	save, ok := span.Attributes().Get("bazel.metrics.action_cache.save_time_ms")
	require.True(t, ok)
	assert.EqualValues(t, 80, save.Int())

	rch, ok := span.Attributes().Get("bazel.metrics.remote_cache_hits")
	require.True(t, ok)
	assert.EqualValues(t, 17, rch.Int())

	// Per-reason miss details.
	notCached, ok := span.Attributes().Get("bazel.metrics.action_cache.miss.not_cached")
	require.True(t, ok)
	assert.EqualValues(t, 30, notCached.Int())
	digest, ok := span.Attributes().Get("bazel.metrics.action_cache.miss.digest_mismatch")
	require.True(t, ok)
	assert.EqualValues(t, 15, digest.Int())

	// Reasons not present in the payload must not be emitted.
	_, ok = span.Attributes().Get("bazel.metrics.action_cache.miss.different_files")
	assert.False(t, ok)
}

func TestBuildMetricsSpan_ActionCacheStatistics_AllEightMissReasons(t *testing.T) {
	state := &invocationState{
		traceID:    pcommon.TraceID{0x01},
		rootSpanID: pcommon.SpanID{0x0a},
		uuid:       "uuid-metrics",
	}
	// Cover every enum value (8 total per BEP spec).
	reasons := []actioncache.ActionCacheStatistics_MissReason{
		actioncache.ActionCacheStatistics_DIFFERENT_ACTION_KEY,
		actioncache.ActionCacheStatistics_DIFFERENT_DEPS,
		actioncache.ActionCacheStatistics_DIFFERENT_ENVIRONMENT,
		actioncache.ActionCacheStatistics_DIFFERENT_FILES,
		actioncache.ActionCacheStatistics_CORRUPTED_CACHE_ENTRY,
		actioncache.ActionCacheStatistics_NOT_CACHED,
		actioncache.ActionCacheStatistics_UNCONDITIONAL_EXECUTION,
		actioncache.ActionCacheStatistics_DIGEST_MISMATCH,
	}
	var missDetails []*actioncache.ActionCacheStatistics_MissDetail
	for i, r := range reasons {
		missDetails = append(missDetails, &actioncache.ActionCacheStatistics_MissDetail{
			Reason: r,
			Count:  int32(i + 1),
		})
	}

	metrics := &bep.BuildMetrics{
		ActionSummary: &bep.BuildMetrics_ActionSummary{
			ActionCacheStatistics: &actioncache.ActionCacheStatistics{MissDetails: missDetails},
		},
	}

	traces := state.buildMetricsSpan(metrics)
	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)

	for i, r := range reasons {
		key := "bazel.metrics.action_cache.miss." + lowerEnumForTest(r.String())
		v, ok := span.Attributes().Get(key)
		require.True(t, ok, "missing miss reason attr %q", key)
		assert.EqualValues(t, i+1, v.Int())
	}
}

// lowerEnumForTest mirrors the lower-casing the production code applies to
// enum names. Kept local so tests don't accidentally share state with the
// implementation.
func lowerEnumForTest(s string) string {
	b := make([]byte, len(s))
	for i := range len(s) {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 32
		}
		b[i] = c
	}
	return string(b)
}

func TestBuildMetricsSpan_Runners(t *testing.T) {
	state := &invocationState{
		traceID:    pcommon.TraceID{0x01},
		rootSpanID: pcommon.SpanID{0x0a},
		uuid:       "uuid-metrics",
	}
	metrics := &bep.BuildMetrics{
		ActionSummary: &bep.BuildMetrics_ActionSummary{
			RunnerCount: []*bep.BuildMetrics_ActionSummary_RunnerCount{
				{Name: "local", Count: 42, ExecKind: "local"},
				{Name: "remote", Count: 100, ExecKind: "remote"},
				{Name: "worker", Count: 7, ExecKind: "worker"},
			},
		},
	}

	traces := state.buildMetricsSpan(metrics)
	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)

	runners, ok := span.Attributes().Get("bazel.metrics.runners")
	require.True(t, ok)
	require.Equal(t, pcommon.ValueTypeSlice, runners.Type())

	sl := runners.Slice()
	require.Equal(t, 3, sl.Len())

	first := sl.At(0).Map()
	name, _ := first.Get("name")
	assert.Equal(t, "local", name.Str())
	count, _ := first.Get("count")
	assert.EqualValues(t, 42, count.Int())
	ek, _ := first.Get("exec_kind")
	assert.Equal(t, "local", ek.Str())

	second := sl.At(1).Map()
	name, _ = second.Get("name")
	assert.Equal(t, "remote", name.Str())
	count, _ = second.Get("count")
	assert.EqualValues(t, 100, count.Int())
}

func TestBuildMetricsSpan_Runners_Empty(t *testing.T) {
	state := &invocationState{
		traceID:    pcommon.TraceID{0x01},
		rootSpanID: pcommon.SpanID{0x0a},
		uuid:       "uuid-metrics",
	}
	metrics := &bep.BuildMetrics{
		ActionSummary: &bep.BuildMetrics_ActionSummary{
			// RunnerCount is nil.
		},
	}

	traces := state.buildMetricsSpan(metrics)
	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)

	// No bazel.metrics.runners attribute emitted when there are no runners.
	_, ok := span.Attributes().Get("bazel.metrics.runners")
	assert.False(t, ok)
}
