package besreceiver

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"google.golang.org/protobuf/types/known/durationpb"

	bep "github.com/chagui/besreceiver/internal/bep/buildeventstream"
)

func TestBuildInvocationMetrics_Timing(t *testing.T) {
	ts := pcommon.NewTimestampFromTime(time.Now())
	state := &invocationState{
		started: &bep.BuildStarted{Command: "build"},
	}
	metrics := &bep.BuildMetrics{
		TimingMetrics: &bep.BuildMetrics_TimingMetrics{
			WallTimeInMs:              12345,
			CpuTimeInMs:               6789,
			AnalysisPhaseTimeInMs:     1000,
			ExecutionPhaseTimeInMs:    2000,
			ActionsExecutionStartInMs: 500,
			CriticalPathTime:          durationpb.New(3 * time.Second),
		},
	}

	md := buildInvocationGauges("inv-1", state, metrics, ts, actionDataOptions{})

	// 6 timing gauges.
	if md.MetricCount() != 6 {
		t.Fatalf("expected 6 metrics, got %d", md.MetricCount())
	}

	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	wantGauges := map[string]int64{
		"bazel.invocation.wall_time":                    12345,
		"bazel.invocation.cpu_time":                     6789,
		"bazel.invocation.analysis_phase_time":          1000,
		"bazel.invocation.execution_phase_time":         2000,
		"bazel.invocation.actions_execution_start_time": 500,
		"bazel.invocation.critical_path_time":           3000,
	}

	for i := range sm.Metrics().Len() {
		m := sm.Metrics().At(i)
		want, ok := wantGauges[m.Name()]
		if !ok {
			t.Errorf("unexpected metric %q", m.Name())
			continue
		}
		dp := m.Gauge().DataPoints().At(0)
		if dp.IntValue() != want {
			t.Errorf("metric %q: got %d, want %d", m.Name(), dp.IntValue(), want)
		}
		if dp.Timestamp() != ts {
			t.Errorf("metric %q: timestamp mismatch", m.Name())
		}
		// Verify attributes.
		invID, ok := dp.Attributes().Get("bazel.invocation_id")
		if !ok || invID.Str() != "inv-1" {
			t.Errorf("metric %q: expected bazel.invocation_id=inv-1", m.Name())
		}
		cmd, ok := dp.Attributes().Get("bazel.command")
		if !ok || cmd.Str() != "build" {
			t.Errorf("metric %q: expected bazel.command=build", m.Name())
		}
		delete(wantGauges, m.Name())
	}
	for name := range wantGauges {
		t.Errorf("missing metric %q", name)
	}
}

func TestBuildInvocationMetrics_ActionSummary(t *testing.T) {
	ts := pcommon.NewTimestampFromTime(time.Now())
	state := &invocationState{
		started: &bep.BuildStarted{Command: "test"},
	}
	metrics := &bep.BuildMetrics{
		ActionSummary: &bep.BuildMetrics_ActionSummary{
			ActionsCreated:                    500,
			ActionsCreatedNotIncludingAspects: 480,
			ActionsExecuted:                   42,
		},
	}

	md := buildInvocationGauges("inv-2", state, metrics, ts, actionDataOptions{})

	if md.MetricCount() != 3 {
		t.Fatalf("expected 3 metrics, got %d", md.MetricCount())
	}

	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	wantGauges := map[string]int64{
		"bazel.invocation.actions_created":                       500,
		"bazel.invocation.actions_created_not_including_aspects": 480,
		"bazel.invocation.actions_executed":                      42,
	}

	for i := range sm.Metrics().Len() {
		m := sm.Metrics().At(i)
		want, ok := wantGauges[m.Name()]
		if !ok {
			t.Errorf("unexpected metric %q", m.Name())
			continue
		}
		if m.Gauge().DataPoints().At(0).IntValue() != want {
			t.Errorf("metric %q: got %d, want %d", m.Name(), m.Gauge().DataPoints().At(0).IntValue(), want)
		}
		delete(wantGauges, m.Name())
	}
	for name := range wantGauges {
		t.Errorf("missing metric %q", name)
	}
}

func TestBuildInvocationMetrics_Memory(t *testing.T) {
	ts := pcommon.NewTimestampFromTime(time.Now())
	state := &invocationState{
		started: &bep.BuildStarted{Command: "build"},
	}
	metrics := &bep.BuildMetrics{
		MemoryMetrics: &bep.BuildMetrics_MemoryMetrics{
			UsedHeapSizePostBuild: 1073741824,
			PeakPostGcHeapSize:    2147483648,
		},
	}

	md := buildInvocationGauges("inv-3", state, metrics, ts, actionDataOptions{})

	if md.MetricCount() != 2 {
		t.Fatalf("expected 2 metrics, got %d", md.MetricCount())
	}

	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	wantGauges := map[string]int64{
		"bazel.invocation.memory.used_heap_post_build": 1073741824,
		"bazel.invocation.memory.peak_post_gc_heap":    2147483648,
	}

	for i := range sm.Metrics().Len() {
		m := sm.Metrics().At(i)
		want, ok := wantGauges[m.Name()]
		if !ok {
			t.Errorf("unexpected metric %q", m.Name())
			continue
		}
		if m.Gauge().DataPoints().At(0).IntValue() != want {
			t.Errorf("metric %q: got %d, want %d", m.Name(), m.Gauge().DataPoints().At(0).IntValue(), want)
		}
		delete(wantGauges, m.Name())
	}
	for name := range wantGauges {
		t.Errorf("missing metric %q", name)
	}
}

func TestBuildInvocationMetrics_NilSubMessages(t *testing.T) {
	ts := pcommon.NewTimestampFromTime(time.Now())
	state := &invocationState{
		started: &bep.BuildStarted{Command: "build"},
	}
	metrics := &bep.BuildMetrics{} // all sub-messages nil

	md := buildInvocationGauges("inv-nil", state, metrics, ts, actionDataOptions{})

	if md.MetricCount() != 0 {
		t.Fatalf("expected 0 metrics for nil sub-messages, got %d", md.MetricCount())
	}
}

func TestCumulativeCounters_Record(t *testing.T) {
	startTime := pcommon.NewTimestampFromTime(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	c := newCumulativeCounters(startTime)

	// First build.
	c.record(&bep.BuildMetrics{
		TimingMetrics: &bep.BuildMetrics_TimingMetrics{
			WallTimeInMs: 10000,
			CpuTimeInMs:  5000,
		},
		ActionSummary: &bep.BuildMetrics_ActionSummary{
			ActionsCreated:  100,
			ActionsExecuted: 50,
		},
	})

	if c.totalInvocations != 1 {
		t.Errorf("expected 1 invocation, got %d", c.totalInvocations)
	}
	if c.totalWallTimeMs != 10000 {
		t.Errorf("expected totalWallTimeMs=10000, got %d", c.totalWallTimeMs)
	}
	if c.totalCPUTimeMs != 5000 {
		t.Errorf("expected totalCPUTimeMs=5000, got %d", c.totalCPUTimeMs)
	}
	if c.totalActionsCreated != 100 {
		t.Errorf("expected totalActionsCreated=100, got %d", c.totalActionsCreated)
	}
	if c.totalActionsExecuted != 50 {
		t.Errorf("expected totalActionsExecuted=50, got %d", c.totalActionsExecuted)
	}

	// Second build — totals accumulate.
	c.record(&bep.BuildMetrics{
		TimingMetrics: &bep.BuildMetrics_TimingMetrics{
			WallTimeInMs: 20000,
			CpuTimeInMs:  8000,
		},
		ActionSummary: &bep.BuildMetrics_ActionSummary{
			ActionsCreated:  200,
			ActionsExecuted: 100,
		},
	})

	if c.totalInvocations != 2 {
		t.Errorf("expected 2 invocations, got %d", c.totalInvocations)
	}
	if c.totalWallTimeMs != 30000 {
		t.Errorf("expected totalWallTimeMs=30000, got %d", c.totalWallTimeMs)
	}
	if c.totalCPUTimeMs != 13000 {
		t.Errorf("expected totalCPUTimeMs=13000, got %d", c.totalCPUTimeMs)
	}
	if c.totalActionsCreated != 300 {
		t.Errorf("expected totalActionsCreated=300, got %d", c.totalActionsCreated)
	}
	if c.totalActionsExecuted != 150 {
		t.Errorf("expected totalActionsExecuted=150, got %d", c.totalActionsExecuted)
	}
}

func TestCumulativeCounters_Record_NilSubMessages(t *testing.T) {
	startTime := pcommon.NewTimestampFromTime(time.Now())
	c := newCumulativeCounters(startTime)

	// Should not panic with nil sub-messages.
	c.record(&bep.BuildMetrics{})

	if c.totalInvocations != 1 {
		t.Errorf("expected 1 invocation, got %d", c.totalInvocations)
	}
	if c.totalWallTimeMs != 0 {
		t.Errorf("expected totalWallTimeMs=0, got %d", c.totalWallTimeMs)
	}
}

func TestCumulativeCounters_AppendTo(t *testing.T) {
	startTime := pcommon.NewTimestampFromTime(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	c := newCumulativeCounters(startTime)
	c.record(&bep.BuildMetrics{
		TimingMetrics: &bep.BuildMetrics_TimingMetrics{
			WallTimeInMs: 10000,
			CpuTimeInMs:  5000,
		},
		ActionSummary: &bep.BuildMetrics_ActionSummary{
			ActionsCreated:  100,
			ActionsExecuted: 50,
		},
	})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	ts := pcommon.NewTimestampFromTime(time.Now())
	c.appendTo(sm, ts)

	// 5 cumulative counters.
	if sm.Metrics().Len() != 5 {
		t.Fatalf("expected 5 metrics, got %d", sm.Metrics().Len())
	}

	wantCounters := map[string]int64{
		"bazel.invocation.count":                  1,
		"bazel.invocation.wall_time.total":        10000,
		"bazel.invocation.cpu_time.total":         5000,
		"bazel.invocation.actions_created.total":  100,
		"bazel.invocation.actions_executed.total": 50,
	}

	for i := range sm.Metrics().Len() {
		m := sm.Metrics().At(i)
		want, ok := wantCounters[m.Name()]
		if !ok {
			t.Errorf("unexpected metric %q", m.Name())
			continue
		}
		sum := m.Sum()
		if !sum.IsMonotonic() {
			t.Errorf("metric %q: expected monotonic", m.Name())
		}
		if sum.AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
			t.Errorf("metric %q: expected cumulative temporality", m.Name())
		}
		dp := sum.DataPoints().At(0)
		if dp.IntValue() != want {
			t.Errorf("metric %q: got %d, want %d", m.Name(), dp.IntValue(), want)
		}
		if dp.StartTimestamp() != startTime {
			t.Errorf("metric %q: expected start timestamp to match counter creation time", m.Name())
		}
		if dp.Timestamp() != ts {
			t.Errorf("metric %q: timestamp mismatch", m.Name())
		}
		// Cumulative counters should have no per-invocation attributes.
		if dp.Attributes().Len() != 0 {
			t.Errorf("metric %q: expected 0 attributes, got %d", m.Name(), dp.Attributes().Len())
		}
		delete(wantCounters, m.Name())
	}
	for name := range wantCounters {
		t.Errorf("missing metric %q", name)
	}
}

func TestBuildInvocationMetrics_AllCategories(t *testing.T) {
	ts := pcommon.NewTimestampFromTime(time.Now())
	state := &invocationState{
		started: &bep.BuildStarted{Command: "build"},
	}
	metrics := &bep.BuildMetrics{
		TimingMetrics: &bep.BuildMetrics_TimingMetrics{
			WallTimeInMs: 10000,
			CpuTimeInMs:  5000,
		},
		ActionSummary: &bep.BuildMetrics_ActionSummary{
			ActionsCreated:  100,
			ActionsExecuted: 50,
		},
		MemoryMetrics: &bep.BuildMetrics_MemoryMetrics{
			UsedHeapSizePostBuild: 512000000,
			PeakPostGcHeapSize:    1024000000,
		},
	}

	md := buildInvocationGauges("inv-all", state, metrics, ts, actionDataOptions{})

	// 5 timing (no CriticalPathTime since it's nil) + 3 action + 2 memory = 10.
	if md.MetricCount() != 10 {
		t.Fatalf("expected 10 metrics, got %d", md.MetricCount())
	}
}
