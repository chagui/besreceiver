package besreceiver

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

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

	md := buildInvocationGauges("inv-1", state, metrics, ts)

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
			ActionsCreated:  500,
			ActionsExecuted: 42,
		},
	}

	md := buildInvocationGauges("inv-2", state, metrics, ts)

	if md.MetricCount() != 2 {
		t.Fatalf("expected 2 metrics, got %d", md.MetricCount())
	}

	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	wantGauges := map[string]int64{
		"bazel.invocation.actions_created":  500,
		"bazel.invocation.actions_executed": 42,
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

	md := buildInvocationGauges("inv-3", state, metrics, ts)

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

	md := buildInvocationGauges("inv-nil", state, metrics, ts)

	if md.MetricCount() != 0 {
		t.Fatalf("expected 0 metrics for nil sub-messages, got %d", md.MetricCount())
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

	md := buildInvocationGauges("inv-all", state, metrics, ts)

	// 5 timing (no CriticalPathTime since it's nil) + 2 action + 2 memory = 9.
	if md.MetricCount() != 9 {
		t.Fatalf("expected 9 metrics, got %d", md.MetricCount())
	}
}
