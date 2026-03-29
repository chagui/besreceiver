package besreceiver

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	bep "github.com/chagui/besreceiver/internal/bep/buildeventstream"
)

// buildInvocationGauges constructs per-invocation gauge metrics from a
// BuildMetrics event. Returns a pmetric.Metrics containing gauges for timing,
// action summary, and memory sub-messages. Nil sub-messages are skipped.
func buildInvocationGauges(invocationID string, state *invocationState, metrics *bep.BuildMetrics, ts pcommon.Timestamp) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "bazel")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("besreceiver")

	attrs := pcommon.NewMap()
	attrs.PutStr("bazel.invocation_id", invocationID)
	if state.started != nil {
		attrs.PutStr("bazel.command", state.started.GetCommand())
	}

	addTimingGauges(sm, metrics.GetTimingMetrics(), ts, attrs)
	addActionSummaryGauges(sm, metrics.GetActionSummary(), ts, attrs)
	addMemoryGauges(sm, metrics.GetMemoryMetrics(), ts, attrs)

	return md
}

func addTimingGauges(sm pmetric.ScopeMetrics, tm *bep.BuildMetrics_TimingMetrics, ts pcommon.Timestamp, attrs pcommon.Map) {
	if tm == nil {
		return
	}
	addGaugeInt64(sm, "bazel.invocation.wall_time", "ms", "Wall time of the build invocation", ts, tm.GetWallTimeInMs(), attrs)
	addGaugeInt64(sm, "bazel.invocation.cpu_time", "ms", "CPU time consumed by the build invocation", ts, tm.GetCpuTimeInMs(), attrs)
	addGaugeInt64(sm, "bazel.invocation.analysis_phase_time", "ms", "Elapsed wall time during analysis phase", ts, tm.GetAnalysisPhaseTimeInMs(), attrs)
	addGaugeInt64(sm, "bazel.invocation.execution_phase_time", "ms", "Elapsed wall time during execution phase", ts, tm.GetExecutionPhaseTimeInMs(), attrs)
	addGaugeInt64(sm, "bazel.invocation.actions_execution_start_time", "ms", "Elapsed wall time until first action execution", ts, tm.GetActionsExecutionStartInMs(), attrs)

	if cpt := tm.GetCriticalPathTime(); cpt != nil {
		addGaugeInt64(sm, "bazel.invocation.critical_path_time", "ms", "Critical path wall time", ts, cpt.AsDuration().Milliseconds(), attrs)
	}
}

func addActionSummaryGauges(sm pmetric.ScopeMetrics, as *bep.BuildMetrics_ActionSummary, ts pcommon.Timestamp, attrs pcommon.Map) {
	if as == nil {
		return
	}
	addGaugeInt64(sm, "bazel.invocation.actions_created", "{action}", "Total actions created during the build", ts, as.GetActionsCreated(), attrs)
	addGaugeInt64(sm, "bazel.invocation.actions_executed", "{action}", "Total actions executed during the build", ts, as.GetActionsExecuted(), attrs)
}

func addMemoryGauges(sm pmetric.ScopeMetrics, mm *bep.BuildMetrics_MemoryMetrics, ts pcommon.Timestamp, attrs pcommon.Map) {
	if mm == nil {
		return
	}
	addGaugeInt64(sm, "bazel.invocation.memory.used_heap_post_build", "By", "JVM heap size post build", ts, mm.GetUsedHeapSizePostBuild(), attrs)
	addGaugeInt64(sm, "bazel.invocation.memory.peak_post_gc_heap", "By", "Peak JVM heap size post GC", ts, mm.GetPeakPostGcHeapSize(), attrs)
}

// cumulativeCounters tracks running totals across invocations.
// All fields are owned by the TraceBuilder's run goroutine — no mutex needed.
type cumulativeCounters struct {
	totalWallTimeMs      int64
	totalCPUTimeMs       int64
	totalActionsCreated  int64
	totalActionsExecuted int64
	totalInvocations     int64
	startTime            pcommon.Timestamp
}

func newCumulativeCounters(startTime pcommon.Timestamp) *cumulativeCounters {
	return &cumulativeCounters{startTime: startTime}
}

// record increments running totals from a BuildMetrics event.
func (c *cumulativeCounters) record(metrics *bep.BuildMetrics) {
	c.totalInvocations++
	if tm := metrics.GetTimingMetrics(); tm != nil {
		c.totalWallTimeMs += tm.GetWallTimeInMs()
		c.totalCPUTimeMs += tm.GetCpuTimeInMs()
	}
	if as := metrics.GetActionSummary(); as != nil {
		c.totalActionsCreated += as.GetActionsCreated()
		c.totalActionsExecuted += as.GetActionsExecuted()
	}
}

// appendTo appends cumulative Sum data points to the given ScopeMetrics.
func (c *cumulativeCounters) appendTo(sm pmetric.ScopeMetrics, ts pcommon.Timestamp) {
	addSumInt64(sm, "bazel.invocation.count", "{invocation}", "Total BuildMetrics events processed", ts, c.startTime, c.totalInvocations)
	addSumInt64(sm, "bazel.invocation.wall_time.total", "ms", "Cumulative wall time across invocations", ts, c.startTime, c.totalWallTimeMs)
	addSumInt64(sm, "bazel.invocation.cpu_time.total", "ms", "Cumulative CPU time across invocations", ts, c.startTime, c.totalCPUTimeMs)
	addSumInt64(sm, "bazel.invocation.actions_created.total", "{action}", "Cumulative actions created across invocations", ts, c.startTime, c.totalActionsCreated)
	addSumInt64(sm, "bazel.invocation.actions_executed.total", "{action}", "Cumulative actions executed across invocations", ts, c.startTime, c.totalActionsExecuted)
}

// addSumInt64 appends a single monotonic cumulative Sum metric with one Int64 data point.
func addSumInt64(sm pmetric.ScopeMetrics, name, unit, description string, ts, startTS pcommon.Timestamp, value int64) {
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	m.SetUnit(unit)
	m.SetDescription(description)
	sum := m.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	dp := sum.DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetStartTimestamp(startTS)
	dp.SetIntValue(value)
}

// addGaugeInt64 appends a single Gauge metric with one Int64 data point.
func addGaugeInt64(sm pmetric.ScopeMetrics, name, unit, description string, ts pcommon.Timestamp, value int64, attrs pcommon.Map) {
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	m.SetUnit(unit)
	m.SetDescription(description)
	m.SetEmptyGauge()

	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetIntValue(value)
	attrs.CopyTo(dp.Attributes())
}
