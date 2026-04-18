package besreceiver

import (
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	bep "github.com/chagui/besreceiver/internal/bep/buildeventstream"
)

// invocationState tracks the trace state for a single Bazel invocation.
// All fields are owned by the TraceBuilder's run goroutine — no mutex needed.
//
// Ordering contract: the code assumes Bazel emits BEP events in a specific
// order within each invocation stream:
//
//	BuildStarted → TargetConfigured* → ActionExecuted*/TestResult* → BuildFinished → BuildMetrics
//
// Three consequences if this ordering is violated:
//
//  1. Events before BuildStarted are dropped (no invocationState exists yet).
//  2. ActionExecuted/TestResult arriving before their TargetConfigured are
//     initially parented to the root span, then reparented under the correct
//     target when TargetConfigured arrives (see recordOrphan / reparentOrphans).
//  3. Events after BuildFinished (except BuildMetrics) are silently discarded
//     because finalize sets flushed=true, and addTarget/addAction/addTestResult
//     all short-circuit on that flag.
//
// Bazel has historically maintained this ordering in practice, and the BES gRPC
// transport (OrderedBuildEvent) preserves stream order.
type invocationState struct {
	traceID       pcommon.TraceID
	rootSpanID    pcommon.SpanID
	uuid          string                    // Bazel invocation UUID, used to seed span-identity strings
	targets       map[string]pcommon.SpanID // "label\x00configID" → spanID
	started       *bep.BuildStarted         // buffered for deferred root span emission
	createdAt     time.Time
	pendingTraces ptrace.Traces     // batch of spans accumulated during the build
	scopeSpans    ptrace.ScopeSpans // reference into pendingTraces for span appending
	flushed       bool              // true after BuildFinished flushes the batch

	// orphanedSpans tracks spans whose target hadn't arrived yet at insertion time.
	// Keyed by label, each entry is a list of span indices (into scopeSpans.Spans())
	// that should be reparented when addTarget is called for that label.
	orphanedSpans map[string][]int

	// abortRecorded is set on the first Aborted event. Subsequent aborts are
	// logged but do not overwrite the original reason / description, which are
	// preserved as the root-cause signal on the root span.
	abortRecorded    bool
	abortReason      string
	abortDescription string
}

func newInvocationState(traceID pcommon.TraceID, rootSpanID pcommon.SpanID, started *bep.BuildStarted, now time.Time) *invocationState {
	traces, ss := newTracesPayload()
	return &invocationState{
		traceID:       traceID,
		rootSpanID:    rootSpanID,
		uuid:          started.GetUuid(),
		targets:       make(map[string]pcommon.SpanID),
		started:       started,
		createdAt:     now,
		pendingTraces: traces,
		scopeSpans:    ss,
	}
}

// newTracesPayload creates a pdata Traces with a single ResourceSpans/ScopeSpans,
// pre-configured with service.name=bazel and scope=besreceiver.
func newTracesPayload() (ptrace.Traces, ptrace.ScopeSpans) {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "bazel")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("besreceiver")
	return traces, ss
}

func (s *invocationState) addTarget(label, ruleKind string) {
	if s.flushed {
		return
	}

	spanID := spanIDFromIdentity(s.uuid, "target", label)
	s.targets[targetKey(label, "")] = spanID

	span := s.appendSpan()
	span.SetSpanID(spanID)
	span.SetParentSpanID(s.rootSpanID)
	span.SetName("bazel.target")
	span.SetKind(ptrace.SpanKindInternal)

	span.Attributes().PutStr("bazel.target.label", label)
	if ruleKind != "" {
		span.Attributes().PutStr("bazel.target.rule_kind", ruleKind)
	}

	// Reparent any actions/tests that arrived before this target.
	s.reparentOrphans(label, spanID)
}

func (s *invocationState) addAction(label, configID, primaryOutput string, action *bep.ActionExecuted) {
	if s.flushed {
		return
	}

	parentSpanID, resolved := s.resolveTargetSpan(label, configID)

	spanIdx := s.scopeSpans.Spans().Len()
	span := s.appendSpan()
	span.SetSpanID(spanIDFromIdentity(s.uuid, "action", label, action.GetType(), primaryOutput))

	// If the target hasn't been configured yet, record this span for
	// deferred reparenting when addTarget is called.
	if !resolved && label != "" {
		s.recordOrphan(label, spanIdx)
	}
	span.SetParentSpanID(parentSpanID)
	if mnemonic := action.GetType(); mnemonic != "" {
		span.SetName("bazel.action " + mnemonic)
	} else {
		span.SetName("bazel.action")
	}
	span.SetKind(ptrace.SpanKindInternal)

	if action.GetStartTime() != nil {
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(action.GetStartTime().AsTime()))
	}
	if action.GetEndTime() != nil {
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(action.GetEndTime().AsTime()))
	}

	span.Attributes().PutStr("bazel.action.mnemonic", action.GetType())
	span.Attributes().PutInt("bazel.action.exit_code", int64(action.GetExitCode()))
	span.Attributes().PutBool("bazel.action.success", action.GetSuccess())
	if label != "" {
		span.Attributes().PutStr("bazel.target.label", label)
	}

	if !action.GetSuccess() {
		span.Status().SetCode(ptrace.StatusCodeError)
		span.Status().SetMessage("action failed")
	}
}

func (s *invocationState) addTestResult(label, configID string, tr *bep.BuildEventId_TestResultId, result *bep.TestResult) {
	if s.flushed {
		return
	}

	parentSpanID, resolved := s.resolveTargetSpan(label, configID)

	spanIdx := s.scopeSpans.Spans().Len()
	span := s.appendSpan()
	span.SetSpanID(spanIDFromIdentity(s.uuid, "test", label,
		strconv.Itoa(int(tr.GetRun())),
		strconv.Itoa(int(tr.GetShard())),
		strconv.Itoa(int(tr.GetAttempt())),
	))

	if !resolved && label != "" {
		s.recordOrphan(label, spanIdx)
	}
	span.SetParentSpanID(parentSpanID)
	span.SetName("bazel.test")
	span.SetKind(ptrace.SpanKindInternal)

	if result.GetTestAttemptStart() != nil {
		startTime := result.GetTestAttemptStart().AsTime()
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(startTime))
		if result.GetTestAttemptDuration() != nil {
			endTime := startTime.Add(result.GetTestAttemptDuration().AsDuration())
			span.SetEndTimestamp(pcommon.NewTimestampFromTime(endTime))
		}
	}

	span.Attributes().PutStr("bazel.test.status", result.GetStatus().String())
	if label != "" {
		span.Attributes().PutStr("bazel.target.label", label)
	}
	if tr != nil {
		span.Attributes().PutInt("bazel.test.shard", int64(tr.GetShard()))
		span.Attributes().PutInt("bazel.test.run", int64(tr.GetRun()))
		span.Attributes().PutInt("bazel.test.attempt", int64(tr.GetAttempt()))
	}

	if result.GetStatus() != bep.TestStatus_PASSED {
		span.Status().SetCode(ptrace.StatusCodeError)
		span.Status().SetMessage(result.GetStatus().String())
	}
}

// finalize appends the root span with start/end timestamps and exit code,
// marks the invocation as flushed, and returns the accumulated traces batch.
func (s *invocationState) finalize(finished *bep.BuildFinished) (ptrace.Traces, bool) {
	if s.flushed {
		return ptrace.Traces{}, false
	}

	s.writeRootSpan(finished)

	s.flushed = true
	s.orphanedSpans = nil
	return s.pendingTraces, true
}

// writeRootSpan appends the root bazel.build span to the pending traces batch.
// finished may be nil when the root span is emitted without BuildFinished
// (e.g. the reaper flushing an aborted build that never sent BuildFinished).
//
// When abortRecorded is true, abort attributes are written alongside any
// existing exit-code attributes and the status message is set to
// "aborted: <reason>: <description>". Abort is the root cause, so its
// status takes precedence; the exit code is the consequence and its
// attributes remain for observability.
func (s *invocationState) writeRootSpan(finished *bep.BuildFinished) {
	span := s.appendSpan()
	span.SetSpanID(s.rootSpanID)
	// command is a free-form string in the proto, but in practice bounded to
	// Bazel's fixed command set (~20: build, test, run, query, clean, etc.).
	// Only a subset (~5-7) emit BES events that reach this code path.
	if s.started != nil && s.started.GetCommand() != "" {
		span.SetName("bazel.build " + s.started.GetCommand())
	} else {
		span.SetName("bazel.build")
	}
	span.SetKind(ptrace.SpanKindServer)

	if s.started != nil && s.started.GetStartTime() != nil {
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(s.started.GetStartTime().AsTime()))
	}
	if finished != nil && finished.GetFinishTime() != nil {
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(finished.GetFinishTime().AsTime()))
	}

	if s.started != nil {
		span.Attributes().PutStr("bazel.command", s.started.GetCommand())
		span.Attributes().PutStr("bazel.uuid", s.started.GetUuid())
		span.Attributes().PutStr("bazel.build_tool_version", s.started.GetBuildToolVersion())
		span.Attributes().PutStr("bazel.workspace_directory", s.started.GetWorkspaceDirectory())
	}

	if finished != nil {
		exitCode := finished.GetExitCode()
		span.Attributes().PutStr("bazel.exit_code.name", exitCode.GetName())
		span.Attributes().PutInt("bazel.exit_code.code", int64(exitCode.GetCode()))

		if exitCode.GetCode() != 0 {
			span.Status().SetCode(ptrace.StatusCodeError)
			span.Status().SetMessage(exitCode.GetName())
		}
	}

	if s.abortRecorded {
		span.Attributes().PutStr("bazel.abort.reason", s.abortReason)
		span.Attributes().PutStr("bazel.abort.description", s.abortDescription)
		span.Status().SetCode(ptrace.StatusCodeError)
		span.Status().SetMessage("aborted: " + s.abortReason + ": " + s.abortDescription)
	}
}

// recordAbort stores the first abort reason and description on the invocation
// state. Subsequent calls are no-ops — the root cause is preserved and later
// aborts are consequences that the caller logs but does not propagate here.
// Returns true only on the first call.
func (s *invocationState) recordAbort(a *bep.Aborted) bool {
	if s.abortRecorded {
		return false
	}
	s.abortRecorded = true
	s.abortReason = abortReasonString(a.GetReason())
	s.abortDescription = a.GetDescription()
	return true
}

// abortReasonString returns the lowercased BEP enum name for an abort reason.
// Stable identifier for downstream queries; new enum values added by future
// Bazel versions are handled automatically via the generated String() method.
func abortReasonString(r bep.Aborted_AbortReason) string {
	return strings.ToLower(r.String())
}

// buildMetricsSpan creates a standalone Traces payload for BuildMetrics.
// Used for post-flush events that arrive after the main batch is flushed.
func (s *invocationState) buildMetricsSpan(metrics *bep.BuildMetrics) ptrace.Traces {
	traces, ss := newTracesPayload()

	span := ss.Spans().AppendEmpty()
	span.SetTraceID(s.traceID)
	span.SetSpanID(spanIDFromIdentity(s.uuid, "metrics"))
	span.SetParentSpanID(s.rootSpanID)
	span.SetName("bazel.metrics")
	span.SetKind(ptrace.SpanKindInternal)

	if tm := metrics.GetTimingMetrics(); tm != nil {
		span.Attributes().PutInt("bazel.metrics.wall_time_ms", tm.GetWallTimeInMs())
		span.Attributes().PutInt("bazel.metrics.cpu_time_ms", tm.GetCpuTimeInMs())
		span.Attributes().PutInt("bazel.metrics.analysis_phase_time_ms", tm.GetAnalysisPhaseTimeInMs())
		span.Attributes().PutInt("bazel.metrics.execution_phase_time_ms", tm.GetExecutionPhaseTimeInMs())
	}
	if as := metrics.GetActionSummary(); as != nil {
		span.Attributes().PutInt("bazel.metrics.actions_created", as.GetActionsCreated())
		span.Attributes().PutInt("bazel.metrics.actions_executed", as.GetActionsExecuted())
	}

	return traces
}

// flushOrphaned returns pending traces for invocations that were never finished.
// Used by the reaper to flush spans before deleting stale state.
//
// When an abort was recorded but BuildFinished never arrived, also emit the
// root span (stamped with abort attrs via writeRootSpan) so the abort signal
// is preserved. Otherwise the existing pending spans are flushed as-is,
// matching prior behavior for timed-out but non-aborted invocations.
func (s *invocationState) flushOrphaned() (ptrace.Traces, bool) {
	if s.flushed {
		return ptrace.Traces{}, false
	}
	if !s.abortRecorded && s.pendingTraces.SpanCount() == 0 {
		return ptrace.Traces{}, false
	}
	if s.abortRecorded {
		s.writeRootSpan(nil)
	}
	s.flushed = true
	return s.pendingTraces, true
}

// resolveTargetSpan looks up the parent span for an action or test result.
// It tries an exact (label, configID) match first, then a label-only match,
// and falls back to the root span if no target has been registered yet.
//
// The bool return indicates whether the target was found. When false, the
// caller records the span as an orphan so that addTarget can reparent it
// when the TargetConfigured event arrives. See recordOrphan / reparentOrphans.
func (s *invocationState) resolveTargetSpan(label, configID string) (pcommon.SpanID, bool) {
	if spanID, ok := s.targets[targetKey(label, configID)]; ok {
		return spanID, true
	}
	if spanID, ok := s.targets[targetKey(label, "")]; ok {
		return spanID, true
	}
	return s.rootSpanID, false
}

// recordOrphan tracks a span index that needs reparenting when its target arrives.
func (s *invocationState) recordOrphan(label string, spanIdx int) {
	if s.orphanedSpans == nil {
		s.orphanedSpans = make(map[string][]int)
	}
	s.orphanedSpans[label] = append(s.orphanedSpans[label], spanIdx)
}

// reparentOrphans updates any previously-orphaned spans for the given label
// to be children of the target span.
func (s *invocationState) reparentOrphans(label string, targetSpanID pcommon.SpanID) {
	indices, ok := s.orphanedSpans[label]
	if !ok {
		return
	}
	spans := s.scopeSpans.Spans()
	for _, idx := range indices {
		if idx < spans.Len() {
			spans.At(idx).SetParentSpanID(targetSpanID)
		}
	}
	delete(s.orphanedSpans, label)
}

// appendSpan appends a new span to the invocation's pending batch.
func (s *invocationState) appendSpan() ptrace.Span {
	span := s.scopeSpans.Spans().AppendEmpty()
	span.SetTraceID(s.traceID)
	return span
}
