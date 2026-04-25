package besreceiver

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/chagui/besreceiver/internal/bep/actioncache"
	bep "github.com/chagui/besreceiver/internal/bep/buildeventstream"
)

// Cardinality caps for test-execution and action-cache attribute emission.
// The attribute space is controlled by Bazel — caps defend against
// pathological breakdowns / resource maps without hiding realistic data.
const (
	// maxTestTimingDepth bounds how deep the TimingBreakdown tree is walked.
	// 2 = root + direct children, which covers Bazel's current shape.
	maxTestTimingDepth = 2
	// maxTestResourceEntries bounds per-test ResourceUsage emission.
	maxTestResourceEntries = 20
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
//     target when TargetConfigured arrives (see recordPendingReparent /
//     drainPendingReparent). Orphans whose TargetConfigured never arrives
//     remain parented to root at finalize time.
//  3. TargetConfigured/ActionExecuted/TestResult arriving after BuildFinished
//     but before BuildMetrics (which deletes the state) are emitted in a
//     standalone ptrace.Traces payload by the corresponding lateXSpan
//     method — same pattern as buildMetricsSpan. The main span batch has
//     already been flushed, so these late spans ride on the same traceID
//     and parent into the existing target (if known) or the root. Orphan
//     reparenting is disabled post-flush because the pre-flush batch is
//     already out.
//
// Bazel has historically maintained this ordering in practice, and the BES gRPC
// transport (OrderedBuildEvent) preserves stream order.
type invocationState struct {
	traceID       pcommon.TraceID
	rootSpanID    pcommon.SpanID
	uuid          string                 // Bazel invocation UUID, used to seed span-identity strings
	targets       map[string]ptrace.Span // "label\x00configID" → target span handle
	started       *bep.BuildStarted      // buffered for deferred root span emission
	createdAt     time.Time
	pendingTraces ptrace.Traces     // batch of spans accumulated during the build
	scopeSpans    ptrace.ScopeSpans // reference into pendingTraces for span appending
	flushed       bool              // true after BuildFinished flushes the batch

	// pendingReparent tracks spans whose target hadn't arrived yet at
	// insertion time. Keyed by label, each entry is a list of span handles
	// to be reparented when addTarget is called for that label. pdata spans
	// are reference types, so the stored handle can be mutated in place via
	// SetParentSpanID after creation.
	pendingReparent map[string][]ptrace.Span

	// abortRecorded is set on the first Aborted event. Subsequent aborts are
	// logged but do not overwrite the original reason / description, which are
	// preserved as the root-cause signal on the root span.
	abortRecorded    bool
	abortReason      string
	abortDescription string

	// workspaceItems buffers sanitized WorkspaceStatus entries until the root
	// span is emitted. Keys are pre-sanitized; values are pre-truncated.
	workspaceItems map[string]string
	// buildMetadata buffers sanitized --build_metadata entries until the root
	// span is emitted. Keys are pre-sanitized; values are pre-truncated.
	buildMetadata map[string]string

	// configurations maps config id → Configuration payload. Populated by
	// handleConfiguration so handleTargetConfigured can stamp config attrs
	// on the target span at creation time. No back-patching: if a
	// Configuration arrives after its TargetConfigured, the attrs are
	// skipped (documented ordering assumption).
	configurations map[string]*bep.Configuration

	// Root-span attrs buffered from OptionsParsed + StructuredCommandLine,
	// applied in writeRootSpan.
	toolTag      string
	startupCount int64
	commandCount int64
	commandLine  string

	// execRequest buffers the ExecRequestConstructed payload for emission as
	// bazel.run.* attrs on the root span. Only one ExecRequestConstructed
	// event fires per invocation (on successful `bazel run`); later arrivals
	// would overwrite, but the pattern matches WorkspaceStatus /
	// StructuredCommandLine buffering where final-value-wins is acceptable.
	execRequest *bep.ExecRequestConstructed

	// pii opts specific BEP fields into span emission; see PIIConfig.
	pii PIIConfig

	// caps bounds the size of Slice[Map] attributes emitted on the
	// bazel.metrics span for high-cardinality BuildMetrics sub-messages.
	caps HighCardinalityCaps

	// filter gates per-target span emission. Never nil — the TraceBuilder
	// installs a pass-through filter when the operator has not configured
	// one, so callers can dereference without a guard.
	filter *Filter

	// summary toggles the bazel.summary.* block on the root span. Counters in
	// `summaryCounts` are populated unconditionally so enabling the feature
	// mid-build (unsupported today) would still produce consistent totals.
	summary SummaryConfig
	// summaryCounts accumulates aggregate counters for emission on the root
	// span during finalize. Recording happens in the event mutators
	// (addTarget / addAction / addTestResult / summarizeTarget) before any
	// filtering or span emission, so totals reflect the full build even when
	// detail-level filtering (see #13) suppresses the matching span.
	summaryCounts BuildSummary
}

// BuildSummary holds per-invocation aggregate counters emitted as
// bazel.summary.* attributes on the root bazel.build span. Fields match the
// issue #15 spec; CachedLocally is populated from TestResult.cached_locally
// but not stamped on the span today (reserved for a future attribute).
type BuildSummary struct {
	TotalTargets   int64
	TotalActions   int64
	FailedActions  int64
	SuccessActions int64
	CachedLocally  int64
	TotalTests     int64
	PassedTests    int64
	FailedTests    int64
	FlakyTests     int64
}

func newInvocationState(traceID pcommon.TraceID, rootSpanID pcommon.SpanID, started *bep.BuildStarted, now time.Time, pii PIIConfig, caps HighCardinalityCaps, filter *Filter, summary SummaryConfig) *invocationState {
	traces, ss := newTracesPayload()
	if filter == nil {
		filter = NewFilter(FilterConfig{})
	}
	return &invocationState{
		traceID:       traceID,
		rootSpanID:    rootSpanID,
		uuid:          started.GetUuid(),
		targets:       make(map[string]ptrace.Span),
		started:       started,
		createdAt:     now,
		pendingTraces: traces,
		scopeSpans:    ss,
		pii:           pii,
		caps:          caps,
		filter:        filter,
		summary:       summary,
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

// addTarget creates the bazel.target span for label and reparents any
// previously-buffered action/test spans that arrived before this target.
// Returns the number of spans reparented, so the caller can track a
// reparenting counter metric.
func (s *invocationState) addTarget(label, ruleKind, configID string) int {
	if s.flushed {
		return 0
	}
	// Summary records before any filtering — the count reflects the full
	// build, not only the targets whose spans are ultimately emitted.
	s.summaryCounts.TotalTargets++

	// Filter runs at event-processing time, not emission time: skipping here
	// avoids any allocation for targets the operator has dropped. The root
	// bazel.build and bazel.metrics spans bypass the filter entirely and
	// are emitted elsewhere.
	if !s.filter.LevelForTarget(label).allowTarget() {
		return 0
	}

	span := s.appendSpan()
	spanID := s.populateTargetSpan(span, label, ruleKind, configID)

	// Store the span handle so later TargetComplete / TestSummary events can
	// mutate attributes on the existing target span.
	s.targets[targetKey(label, "")] = span

	// Reparent any actions/tests that arrived before this target.
	return s.drainPendingReparent(label, spanID)
}

// populateTargetSpan stamps identity, parenting, and attributes on a
// pre-appended target span. Returns the span id so callers can wire it into
// the targets map and reparent orphans. Shared by the pre-flush addTarget
// path and the post-flush lateTargetSpan path.
func (s *invocationState) populateTargetSpan(span ptrace.Span, label, ruleKind, configID string) pcommon.SpanID {
	spanID := spanIDFromIdentity(s.uuid, "target", label)
	span.SetSpanID(spanID)
	span.SetParentSpanID(s.rootSpanID)
	span.SetName("bazel.target")
	span.SetKind(ptrace.SpanKindInternal)

	span.Attributes().PutStr("bazel.target.label", label)
	if ruleKind != "" {
		span.Attributes().PutStr("bazel.target.rule_kind", ruleKind)
	}

	// Stamp configuration attributes if we already have the Configuration
	// payload for this target's config id. No back-patching if it arrives
	// later — Bazel emits Configuration before TargetConfigured in practice.
	s.stampTargetConfig(span, configID)
	return spanID
}

// addAction appends a bazel.action span for the given ActionExecuted event.
// Conditionals (PII gate, filter gate, orphan reparent, mnemonic name,
// optional timestamps) live inline; splitting them into helpers would
// fragment the per-span build across several stateful methods without any
// real readability gain.
func (s *invocationState) addAction(label, configID, primaryOutput string, action *bep.ActionExecuted) {
	if s.flushed {
		return
	}
	// Summary records before any filtering — the counts reflect the full
	// build, not only the actions whose spans are ultimately emitted.
	s.recordActionSummary(action)

	// Action-label filtering uses the owning target's label — actions are
	// structural children. When label is empty the filter falls through to
	// DefaultLevel, which is the only correct default (the action can't be
	// attributed to any rule).
	if !s.filter.LevelForTarget(label).allowAction() {
		return
	}

	parentSpanID, resolved := s.resolveTargetSpan(label, configID)

	span := s.appendSpan()
	s.populateActionSpan(span, parentSpanID, label, primaryOutput, action)

	// If the target hasn't been configured yet, buffer the span handle for
	// deferred reparenting when addTarget is called for this label. pdata
	// spans are reference types — the stored handle remains valid until the
	// batch is flushed.
	if !resolved && label != "" {
		s.recordPendingReparent(label, span)
	}
}

// populateActionSpan stamps identity, parenting, and attributes on a
// pre-appended action span. Shared by the pre-flush addAction path and the
// post-flush lateActionSpan path. Caller handles orphan recording (only
// meaningful pre-flush).
func (s *invocationState) populateActionSpan(span ptrace.Span, parentSpanID pcommon.SpanID, label, primaryOutput string, action *bep.ActionExecuted) {
	span.SetSpanID(spanIDFromIdentity(s.uuid, "action", label, action.GetType(), primaryOutput))
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

	if s.pii.IncludeCommandArgs {
		if args := action.GetCommandLine(); len(args) > 0 {
			slice := span.Attributes().PutEmptySlice("bazel.action.command_line")
			for _, arg := range args {
				slice.AppendEmpty().SetStr(arg)
			}
		}
	}
	if s.pii.IncludeActionOutputPaths {
		if po := action.GetPrimaryOutput(); po != nil && po.GetName() != "" {
			span.Attributes().PutStr("bazel.action.primary_output", po.GetName())
		}
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
	// Summary records CachedLocally per-attempt before filtering; the per-test
	// pass/fail/flaky tallies come from TestSummary (one per target) in
	// summarizeTarget so retries don't double-count.
	if result.GetCachedLocally() {
		s.summaryCounts.CachedLocally++
	}

	// Tests share the action gate: allowAction()==true only at verbose.
	// If the operator wants per-target summary without per-shard detail,
	// DetailLevelTargets suppresses both.
	if !s.filter.LevelForTarget(label).allowAction() {
		return
	}

	parentSpanID, resolved := s.resolveTargetSpan(label, configID)

	span := s.appendSpan()
	s.populateTestResultSpan(span, parentSpanID, label, tr, result)

	if !resolved && label != "" {
		s.recordPendingReparent(label, span)
	}
}

// populateTestResultSpan stamps identity, parenting, and attributes on a
// pre-appended test span. Shared by the pre-flush addTestResult path and
// the post-flush lateTestResultSpan path. Caller handles orphan recording
// (only meaningful pre-flush).
func (s *invocationState) populateTestResultSpan(span ptrace.Span, parentSpanID pcommon.SpanID, label string, tr *bep.BuildEventId_TestResultId, result *bep.TestResult) {
	span.SetSpanID(spanIDFromIdentity(s.uuid, "test", label,
		strconv.Itoa(int(tr.GetRun())),
		strconv.Itoa(int(tr.GetShard())),
		strconv.Itoa(int(tr.GetAttempt())),
	))
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
	span.Attributes().PutBool("bazel.test.cached_locally", result.GetCachedLocally())
	if label != "" {
		span.Attributes().PutStr("bazel.target.label", label)
	}
	if tr != nil {
		span.Attributes().PutInt("bazel.test.shard", int64(tr.GetShard()))
		span.Attributes().PutInt("bazel.test.run", int64(tr.GetRun()))
		span.Attributes().PutInt("bazel.test.attempt", int64(tr.GetAttempt()))
	}

	applyTestExecutionInfo(span, result.GetExecutionInfo())

	if result.GetStatus() != bep.TestStatus_PASSED {
		span.Status().SetCode(ptrace.StatusCodeError)
		span.Status().SetMessage(result.GetStatus().String())
	}
}

// applyTestExecutionInfo stamps remote-execution metadata on the test span.
// No-op on nil; bounded by maxTestTimingDepth and maxTestResourceEntries.
func applyTestExecutionInfo(span ptrace.Span, ei *bep.TestResult_ExecutionInfo) {
	if ei == nil {
		return
	}
	if s := ei.GetStrategy(); s != "" {
		span.Attributes().PutStr("bazel.test.execution.strategy", s)
	}
	span.Attributes().PutBool("bazel.test.execution.cached_remotely", ei.GetCachedRemotely())
	if h := ei.GetHostname(); h != "" {
		span.Attributes().PutStr("bazel.test.execution.hostname", h)
	}
	flattenTimingBreakdown(span, ei.GetTimingBreakdown(), 1)
	applyResourceUsage(span, ei.GetResourceUsage())
}

// flattenTimingBreakdown walks the TimingBreakdown tree up to maxTestTimingDepth
// and stamps each node as bazel.test.timing.<sanitized_name>_ms. Nodes with an
// empty or duplicate sanitized name are skipped (last-write-wins would leak
// non-determinism across backends, so we keep the first one).
func flattenTimingBreakdown(span ptrace.Span, node *bep.TestResult_ExecutionInfo_TimingBreakdown, depth int) {
	if node == nil || depth > maxTestTimingDepth {
		return
	}
	key := sanitizeAttrKey(node.GetName())
	if key != "" {
		attrKey := "bazel.test.timing." + key + "_ms"
		if _, exists := span.Attributes().Get(attrKey); !exists {
			span.Attributes().PutInt(attrKey, timingBreakdownMs(node))
		}
	}
	for _, child := range node.GetChild() {
		flattenTimingBreakdown(span, child, depth+1)
	}
}

// timingBreakdownMs prefers the typed Duration field over the deprecated
// time_millis field, mirroring Bazel's preference.
func timingBreakdownMs(node *bep.TestResult_ExecutionInfo_TimingBreakdown) int64 {
	if d := node.GetTime(); d != nil {
		return d.AsDuration().Milliseconds()
	}
	return node.GetTimeMillis() //nolint:staticcheck // SA1019: retained as fallback for Bazel releases that only populate time_millis.
}

// applyResourceUsage stamps ResourceUsage entries as bazel.test.resource.<name>,
// preserving source order and capping at maxTestResourceEntries. Duplicates by
// sanitized name are skipped (first wins).
func applyResourceUsage(span ptrace.Span, usage []*bep.TestResult_ExecutionInfo_ResourceUsage) {
	if len(usage) == 0 {
		return
	}
	kept := 0
	for _, u := range usage {
		if kept >= maxTestResourceEntries {
			break
		}
		key := sanitizeAttrKey(u.GetName())
		if key == "" {
			continue
		}
		attrKey := "bazel.test.resource." + key
		if _, exists := span.Attributes().Get(attrKey); exists {
			continue
		}
		span.Attributes().PutInt(attrKey, u.GetValue())
		kept++
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
	// Any entries left in pendingReparent represent actions/tests whose
	// TargetConfigured never arrived. Their spans stay parented to root
	// (explicit orphan), matching prior behavior.
	s.pendingReparent = nil
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

	s.applyStartedAttributes(span)

	if s.toolTag != "" {
		span.Attributes().PutStr("bazel.tool_tag", s.toolTag)
	}
	if s.startupCount > 0 {
		span.Attributes().PutInt("bazel.options.startup_count", s.startupCount)
	}
	if s.commandCount > 0 {
		span.Attributes().PutInt("bazel.options.command_count", s.commandCount)
	}
	if s.pii.IncludeCommandLine && s.commandLine != "" {
		span.Attributes().PutStr("bazel.command_line", s.commandLine)
	}

	s.applyExecRequestAttrs(span)

	s.applyContextAttributes(span)

	s.applySummaryAttributes(span)

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

// applyStartedAttributes writes root-span attributes derived from the
// BuildStarted payload, gating PII fields on PIIConfig flags.
func (s *invocationState) applyStartedAttributes(span ptrace.Span) {
	if s.started == nil {
		return
	}
	span.Attributes().PutStr("bazel.command", s.started.GetCommand())
	span.Attributes().PutStr("bazel.uuid", s.started.GetUuid())
	span.Attributes().PutStr("bazel.build_tool_version", s.started.GetBuildToolVersion())
	if s.pii.IncludeWorkspaceDir {
		span.Attributes().PutStr("bazel.workspace_directory", s.started.GetWorkspaceDirectory())
	}
	if s.pii.IncludeWorkingDir {
		span.Attributes().PutStr("bazel.working_directory", s.started.GetWorkingDirectory())
	}
	if s.pii.IncludeHostname {
		if host := s.started.GetHost(); host != "" {
			span.Attributes().PutStr("bazel.host", host)
		}
	}
	if s.pii.IncludeUsername {
		if user := s.started.GetUser(); user != "" {
			span.Attributes().PutStr("bazel.user", user)
		}
	}
}

// applyExecRequestAttrs stamps bazel.run.* attributes from a buffered
// ExecRequestConstructed. No-op when the event was never buffered (the
// common case for `bazel build` / `bazel test` invocations, which never
// emit ExecRequestConstructed).
//
// Gating matrix:
//   - bazel.run.argv:                     PII.IncludeCommandArgs
//   - bazel.run.environment:              PII.IncludeCommandArgs (env often
//     carries argv-adjacent secrets, so it reuses the same gate)
//   - bazel.run.working_directory:        PII.IncludeWorkingDir
//   - bazel.run.environment_variable_count: always emitted (count only, safe)
//   - bazel.run.should_exec:              always emitted (bool, safe)
//
// The BEP proto types argv / working_directory / env name+value as bytes,
// not string — they are filesystem paths and OS environment values that
// may contain arbitrary bytes. We cast to string for attribute emission
// (matching the convention used for other byte-typed BEP fields).
func (s *invocationState) applyExecRequestAttrs(span ptrace.Span) {
	er := s.execRequest
	if er == nil {
		return
	}

	// Always-on attrs first: count and should_exec are safe to emit even
	// when details are redacted, giving operators an "event arrived" signal.
	span.Attributes().PutInt("bazel.run.environment_variable_count", int64(len(er.GetEnvironmentVariable())))
	span.Attributes().PutBool("bazel.run.should_exec", er.GetShouldExec())

	if s.pii.IncludeWorkingDir {
		if wd := er.GetWorkingDirectory(); len(wd) > 0 {
			span.Attributes().PutStr("bazel.run.working_directory", string(wd))
		}
	}

	if s.pii.IncludeCommandArgs {
		if argv := er.GetArgv(); len(argv) > 0 {
			slice := span.Attributes().PutEmptySlice("bazel.run.argv")
			for _, a := range argv {
				slice.AppendEmpty().SetStr(string(a))
			}
		}
		if env := er.GetEnvironmentVariable(); len(env) > 0 {
			slice := span.Attributes().PutEmptySlice("bazel.run.environment")
			for _, v := range env {
				m := slice.AppendEmpty().SetEmptyMap()
				m.PutStr("name", string(v.GetName()))
				m.PutStr("value", string(v.GetValue()))
			}
		}
	}
}

// setExecRequest buffers an ExecRequestConstructed payload for emission on
// the root span. No-op after the invocation has been flushed.
func (s *invocationState) setExecRequest(er *bep.ExecRequestConstructed) {
	if s.flushed || er == nil {
		return
	}
	s.execRequest = er
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

// addWorkspaceItems buffers a WorkspaceStatus payload for emission on the
// root span. Items are kept in source order; the first maxWorkspaceItems
// with non-empty sanitized keys are stored (later duplicates overwrite on
// sanitization collision — documented trade-off).
func (s *invocationState) addWorkspaceItems(items []*bep.WorkspaceStatus_Item) {
	if s.flushed || !s.pii.IncludeWorkspaceStatus {
		return
	}
	if s.workspaceItems == nil {
		s.workspaceItems = make(map[string]string)
	}
	kept := len(s.workspaceItems)
	for _, item := range items {
		if kept >= maxWorkspaceItems {
			break
		}
		key := sanitizeAttrKey(item.GetKey())
		if key == "" || s.isPIIKeySuppressed(key) {
			continue
		}
		s.workspaceItems[key] = truncateAttrValue(item.GetValue())
		kept = len(s.workspaceItems)
	}
}

// addBuildMetadata buffers a BuildMetadata payload for emission on the root
// span. BEP maps are unordered, so keys are sorted alphabetically before the
// cap is applied to make truncation deterministic across reruns.
func (s *invocationState) addBuildMetadata(md map[string]string) {
	if s.flushed || !s.pii.IncludeBuildMetadata {
		return
	}
	if s.buildMetadata == nil {
		s.buildMetadata = make(map[string]string)
	}
	keys := make([]string, 0, len(md))
	for k := range md {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	kept := len(s.buildMetadata)
	for _, rawKey := range keys {
		if kept >= maxBuildMetadataEntries {
			break
		}
		key := sanitizeAttrKey(rawKey)
		if key == "" || s.isPIIKeySuppressed(key) {
			continue
		}
		s.buildMetadata[key] = truncateAttrValue(md[rawKey])
		kept = len(s.buildMetadata)
	}
}

// piiHostKeys, piiUserKeys, piiWorkingDirKeys, piiWorkspaceDirKeys enumerate
// sanitized key names that carry hostname, username, and directory-path PII.
// Matches are exact (the sanitizer has already lowercased and normalized
// punctuation). Keep each set small and obvious — the intent is to catch the
// common workspace-status / build_metadata conventions, not to be exhaustive.
//
//nolint:gochecknoglobals // immutable lookup tables; Go has no map const
var (
	piiHostKeys         = map[string]struct{}{"host": {}, "hostname": {}, "build_host": {}}
	piiUserKeys         = map[string]struct{}{"user": {}, "username": {}, "build_user": {}}
	piiWorkingDirKeys   = map[string]struct{}{"working_dir": {}, "working_directory": {}}
	piiWorkspaceDirKeys = map[string]struct{}{"workspace_dir": {}, "workspace_directory": {}}
)

// isPIIKeySuppressed returns true when a sanitized workspace-status or
// build-metadata key carries PII whose specific PII flag is off. Used to
// filter per-key even when the coarse IncludeWorkspaceStatus /
// IncludeBuildMetadata gate is open.
func (s *invocationState) isPIIKeySuppressed(sanitizedKey string) bool {
	if _, ok := piiHostKeys[sanitizedKey]; ok && !s.pii.IncludeHostname {
		return true
	}
	if _, ok := piiUserKeys[sanitizedKey]; ok && !s.pii.IncludeUsername {
		return true
	}
	if _, ok := piiWorkingDirKeys[sanitizedKey]; ok && !s.pii.IncludeWorkingDir {
		return true
	}
	if _, ok := piiWorkspaceDirKeys[sanitizedKey]; ok && !s.pii.IncludeWorkspaceDir {
		return true
	}
	return false
}

// completeTarget applies TargetComplete attributes to the target span stored
// under the given label. No-op when the target span is missing (aborted build
// or TargetComplete arriving before TargetConfigured — same silent-degradation
// pattern as the action-before-target case) or when the state is already
// flushed.
func (s *invocationState) completeTarget(label string, tc *bep.TargetComplete) {
	if s.flushed {
		return
	}
	span, ok := s.targets[targetKey(label, "")]
	if !ok {
		return
	}
	span.Attributes().PutBool("bazel.target.success", tc.GetSuccess())
	if tc.GetTestTimeout() != nil {
		span.Attributes().PutDouble("bazel.target.test_timeout_s", tc.GetTestTimeout().AsDuration().Seconds())
	}
	failureDetail := tc.GetFailureDetail().GetMessage()
	if failureDetail != "" {
		span.Attributes().PutStr("bazel.target.failure_detail", failureDetail)
	}
	span.Attributes().PutInt("bazel.target.output_group_count", int64(len(tc.GetOutputGroup())))
	if !tc.GetSuccess() {
		span.Status().SetCode(ptrace.StatusCodeError)
		msg := "target failed"
		if failureDetail != "" {
			msg = failureDetail
		}
		span.Status().SetMessage(msg)
	}
}

// summarizeTarget applies TestSummary attributes to the target span. No-op
// when the target span is missing (non-test target or out-of-order) or when
// the state is already flushed.
//
// Summary counts are driven by TestSummary.overall_status (one per test
// target, post-retry verdict) rather than individual TestResult events, so
// the totals reflect unique tests rather than attempts. The counter update
// runs unconditionally — including the missing-span case — so a test target
// whose span was dropped by detail-level filtering still contributes.
func (s *invocationState) summarizeTarget(label string, ts *bep.TestSummary) {
	if s.flushed {
		return
	}
	s.summaryCounts.TotalTests++
	switch ts.GetOverallStatus() {
	case bep.TestStatus_PASSED:
		s.summaryCounts.PassedTests++
	case bep.TestStatus_FLAKY:
		s.summaryCounts.FlakyTests++
	case bep.TestStatus_FAILED, bep.TestStatus_TIMEOUT, bep.TestStatus_INCOMPLETE,
		bep.TestStatus_REMOTE_FAILURE, bep.TestStatus_FAILED_TO_BUILD,
		bep.TestStatus_TOOL_HALTED_BEFORE_TESTING:
		s.summaryCounts.FailedTests++
	case bep.TestStatus_NO_STATUS:
		// Unreported status — include in the total but don't tag a verdict.
	}
	span, ok := s.targets[targetKey(label, "")]
	if !ok {
		return
	}
	span.Attributes().PutStr("bazel.target.test.overall_status", ts.GetOverallStatus().String())
	span.Attributes().PutInt("bazel.target.test.total_run_count", int64(ts.GetTotalRunCount()))
	span.Attributes().PutInt("bazel.target.test.shard_count", int64(ts.GetShardCount()))
	span.Attributes().PutInt("bazel.target.test.total_num_cached", int64(ts.GetTotalNumCached()))
	if ts.GetTotalRunDuration() != nil {
		span.Attributes().PutInt("bazel.target.test.total_run_duration_ms", ts.GetTotalRunDuration().AsDuration().Milliseconds())
	}
}

// storeConfiguration buffers a Configuration payload keyed by its config id.
// Later invocations of addTarget look the payload up to stamp config attrs.
func (s *invocationState) storeConfiguration(configID string, cfg *bep.Configuration) {
	if s.flushed || configID == "" || configID == nullConfigID {
		return
	}
	if s.configurations == nil {
		s.configurations = make(map[string]*bep.Configuration)
	}
	s.configurations[configID] = cfg
}

// setOptionsParsed captures tool_tag plus startup / command option counts for
// emission on the root span during finalize.
func (s *invocationState) setOptionsParsed(op *bep.OptionsParsed) {
	if s.flushed {
		return
	}
	s.toolTag = op.GetToolTag()
	s.startupCount = int64(len(op.GetExplicitStartupOptions()))
	s.commandCount = int64(len(op.GetExplicitCmdLine()))
}

// applyContextAttributes writes buffered WorkspaceStatus + BuildMetadata
// entries onto the root span. Keys are emitted in sorted order for
// deterministic attribute listings across backends.
func (s *invocationState) applyContextAttributes(span ptrace.Span) {
	if len(s.workspaceItems) > 0 {
		keys := make([]string, 0, len(s.workspaceItems))
		for k := range s.workspaceItems {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			span.Attributes().PutStr("bazel.workspace."+k, s.workspaceItems[k])
		}
	}
	if len(s.buildMetadata) > 0 {
		keys := make([]string, 0, len(s.buildMetadata))
		for k := range s.buildMetadata {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			span.Attributes().PutStr("bazel.metadata."+k, s.buildMetadata[k])
		}
	}
}

// recordActionSummary increments per-invocation action counters for a single
// ActionExecuted event. Extracted to keep addAction below the cognitive
// complexity budget.
func (s *invocationState) recordActionSummary(action *bep.ActionExecuted) {
	s.summaryCounts.TotalActions++
	if action.GetSuccess() {
		s.summaryCounts.SuccessActions++
		return
	}
	s.summaryCounts.FailedActions++
}

// applySummaryAttributes writes bazel.summary.* aggregate counters on the
// root span. No-op when Summary.Enabled is false. Zero-valued counters are
// emitted so downstream queries can rely on attribute presence rather than
// guarding against absent keys.
func (s *invocationState) applySummaryAttributes(span ptrace.Span) {
	if !s.summary.Enabled {
		return
	}
	attrs := span.Attributes()
	attrs.PutInt("bazel.summary.total_targets", s.summaryCounts.TotalTargets)
	attrs.PutInt("bazel.summary.total_actions", s.summaryCounts.TotalActions)
	attrs.PutInt("bazel.summary.success_actions", s.summaryCounts.SuccessActions)
	attrs.PutInt("bazel.summary.failed_actions", s.summaryCounts.FailedActions)
	attrs.PutInt("bazel.summary.total_tests", s.summaryCounts.TotalTests)
	attrs.PutInt("bazel.summary.passed_tests", s.summaryCounts.PassedTests)
	attrs.PutInt("bazel.summary.failed_tests", s.summaryCounts.FailedTests)
	attrs.PutInt("bazel.summary.flaky_tests", s.summaryCounts.FlakyTests)
}

// nullConfigID is Bazel's sentinel for the null configuration — a TargetCompletedId
// carrying this id has no associated Configuration payload.
const nullConfigID = "none"

// stampTargetConfig writes bazel.target.config.* attrs from the buffered
// Configuration (if any) for the given config id.
func (s *invocationState) stampTargetConfig(span ptrace.Span, configID string) {
	if configID == "" || configID == nullConfigID {
		return
	}
	cfg, ok := s.configurations[configID]
	if !ok {
		return
	}
	if m := cfg.GetMnemonic(); m != "" {
		span.Attributes().PutStr("bazel.target.config.mnemonic", m)
	}
	if p := cfg.GetPlatformName(); p != "" {
		span.Attributes().PutStr("bazel.target.config.platform", p)
	}
	if c := cfg.GetCpu(); c != "" {
		span.Attributes().PutStr("bazel.target.config.cpu", c)
	}
	span.Attributes().PutBool("bazel.target.config.is_tool", cfg.GetIsTool())
}

// lateTargetSpan emits a standalone bazel.target span for a TargetConfigured
// event that arrived after the main batch was flushed. The target span handle
// is recorded on s.targets so later post-flush actions/tests for the same
// label can parent correctly. Orphan reparenting is a no-op here because the
// pre-flush batch has already been handed to the consumer.
func (s *invocationState) lateTargetSpan(label, ruleKind, configID string) ptrace.Traces {
	traces, ss := newTracesPayload()
	span := ss.Spans().AppendEmpty()
	span.SetTraceID(s.traceID)
	s.populateTargetSpan(span, label, ruleKind, configID)
	s.targets[targetKey(label, "")] = span
	return traces
}

// lateActionSpan emits a standalone bazel.action span for an ActionExecuted
// event that arrived after the main batch was flushed. Parent is resolved
// against s.targets (which persists across flush); if unknown the span
// parents to the root. Orphan recording is skipped because no future
// TargetConfigured can reparent into an already-emitted batch.
func (s *invocationState) lateActionSpan(label, configID, primaryOutput string, action *bep.ActionExecuted) ptrace.Traces {
	traces, ss := newTracesPayload()
	span := ss.Spans().AppendEmpty()
	span.SetTraceID(s.traceID)
	parentSpanID, _ := s.resolveTargetSpan(label, configID)
	s.populateActionSpan(span, parentSpanID, label, primaryOutput, action)
	return traces
}

// lateTestResultSpan emits a standalone bazel.test span for a TestResult
// event that arrived after the main batch was flushed. Parent resolution
// follows the same rules as lateActionSpan.
func (s *invocationState) lateTestResultSpan(label, configID string, tr *bep.BuildEventId_TestResultId, result *bep.TestResult) ptrace.Traces {
	traces, ss := newTracesPayload()
	span := ss.Spans().AppendEmpty()
	span.SetTraceID(s.traceID)
	parentSpanID, _ := s.resolveTargetSpan(label, configID)
	s.populateTestResultSpan(span, parentSpanID, label, tr, result)
	return traces
}

// truncationRecord describes one high-cardinality list that exceeded its
// configured cap. Emitted by buildMetricsSpan so callers can log a warning
// with invocation context.
type truncationRecord struct {
	attribute string
	original  int
	kept      int
}

// buildMetricsSpan creates a standalone Traces payload for BuildMetrics.
// Used for post-flush events that arrive after the main batch is flushed.
//
// Sub-messages are stamped via dedicated helpers that each no-op on nil, so
// partially-populated BuildMetrics payloads are safe. `opts` configures the
// per-mnemonic ActionData emission (cap, logger, invocation id). The returned
// []truncationRecord is non-empty when one or more high-cardinality Slice[Map]
// attributes was truncated to the configured cap; the caller is expected to
// emit a warning log per record.
func (s *invocationState) buildMetricsSpan(metrics *bep.BuildMetrics, opts actionDataOptions) (ptrace.Traces, []truncationRecord) {
	traces, ss := newTracesPayload()

	span := ss.Spans().AppendEmpty()
	span.SetTraceID(s.traceID)
	span.SetSpanID(spanIDFromIdentity(s.uuid, "metrics"))
	span.SetParentSpanID(s.rootSpanID)
	span.SetName("bazel.metrics")
	span.SetKind(ptrace.SpanKindInternal)

	attrs := span.Attributes()
	stampTimingAttrs(attrs, metrics.GetTimingMetrics())
	stampActionSummaryAttrs(attrs, metrics.GetActionSummary())
	applyActionDataAttrs(attrs, metrics.GetActionSummary().GetActionData(), opts)
	stampMemoryAttrs(attrs, metrics.GetMemoryMetrics())
	stampTargetAttrs(attrs, metrics.GetTargetMetrics())
	stampPackageAttrs(attrs, metrics.GetPackageMetrics())
	stampBuildGraphAttrs(attrs, metrics.GetBuildGraphMetrics())
	stampArtifactAttrs(attrs, metrics.GetArtifactMetrics())
	stampNetworkAttrs(attrs, metrics.GetNetworkMetrics())
	stampCumulativeAttrs(attrs, metrics.GetCumulativeMetrics())
	stampDynamicExecutionAttrs(attrs, metrics.GetDynamicExecutionMetrics())
	stampWorkerPoolAttrs(attrs, metrics.GetWorkerPoolMetrics())

	caps := s.caps.withDefaults()
	var truncs []truncationRecord
	truncs = appendGarbageMetrics(span, metrics.GetMemoryMetrics(), caps.Garbage, truncs)
	truncs = appendPackageLoadMetrics(span, metrics.GetPackageMetrics(), caps.PackageLoad, truncs)
	truncs = appendGraphMetrics(span, metrics.GetBuildGraphMetrics(), caps.GraphValues, truncs)

	return traces, truncs
}

// appendGarbageMetrics emits bazel.metrics.garbage as a Slice[Map] from
// MemoryMetrics.garbage_metrics. Empty/nil lists are skipped (no attribute).
// Entries beyond cap are dropped and one truncationRecord is appended.
func appendGarbageMetrics(span ptrace.Span, mm *bep.BuildMetrics_MemoryMetrics, limit int, truncs []truncationRecord) []truncationRecord {
	if mm == nil {
		return truncs
	}
	entries := mm.GetGarbageMetrics()
	if len(entries) == 0 {
		return truncs
	}
	original := len(entries)
	kept := original
	if limit > 0 && original > limit {
		entries = entries[:limit]
		kept = limit
	}
	slice := span.Attributes().PutEmptySlice("bazel.metrics.garbage")
	for _, g := range entries {
		m := slice.AppendEmpty().SetEmptyMap()
		m.PutStr("type", g.GetType())
		m.PutInt("garbage_collected_bytes", g.GetGarbageCollected())
	}
	if kept < original {
		truncs = append(truncs, truncationRecord{
			attribute: "bazel.metrics.garbage",
			original:  original,
			kept:      kept,
		})
	}
	return truncs
}

// appendPackageLoadMetrics emits bazel.metrics.package_load as a Slice[Map]
// from PackageMetrics.package_load_metrics. Durations are normalized to
// milliseconds.
func appendPackageLoadMetrics(span ptrace.Span, pm *bep.BuildMetrics_PackageMetrics, limit int, truncs []truncationRecord) []truncationRecord {
	if pm == nil {
		return truncs
	}
	entries := pm.GetPackageLoadMetrics()
	if len(entries) == 0 {
		return truncs
	}
	original := len(entries)
	kept := original
	if limit > 0 && original > limit {
		entries = entries[:limit]
		kept = limit
	}
	slice := span.Attributes().PutEmptySlice("bazel.metrics.package_load")
	for _, p := range entries {
		m := slice.AppendEmpty().SetEmptyMap()
		m.PutStr("name", p.GetName())
		// LoadDuration is a google.protobuf.Duration — convert to ms to match
		// sibling attributes like bazel.metrics.wall_time_ms. Missing duration
		// renders as 0 via the generated getter.
		if d := p.GetLoadDuration(); d != nil {
			m.PutInt("load_duration_ms", d.AsDuration().Milliseconds())
		} else {
			m.PutInt("load_duration_ms", 0)
		}
		m.PutInt("num_targets", int64(p.GetNumTargets()))                                     //nolint:gosec // G115: per-package target count within int64 range for all realistic packages.
		m.PutInt("computation_steps", int64(p.GetComputationSteps()))                         //nolint:gosec // G115: per-package computation steps within int64 range for all realistic packages.
		m.PutInt("num_transitive_loads", int64(p.GetNumTransitiveLoads()))                    //nolint:gosec // G115: per-package transitive load count within int64 range for all realistic packages.
		m.PutInt("package_overhead", int64(p.GetPackageOverhead()))                           //nolint:gosec // G115: per-package overhead within int64 range for all realistic packages.
		m.PutInt("glob_filesystem_operation_cost", int64(p.GetGlobFilesystemOperationCost())) //nolint:gosec // G115: per-package glob cost within int64 range for all realistic packages.
	}
	if kept < original {
		truncs = append(truncs, truncationRecord{
			attribute: "bazel.metrics.package_load",
			original:  original,
			kept:      kept,
		})
	}
	return truncs
}

// appendGraphMetrics emits the five bazel.metrics.graph.*_values Slice[Map]
// attributes. Each applies the same cap independently.
func appendGraphMetrics(span ptrace.Span, bgm *bep.BuildMetrics_BuildGraphMetrics, limit int, truncs []truncationRecord) []truncationRecord {
	if bgm == nil {
		return truncs
	}
	categories := []struct {
		attr    string
		entries []*bep.BuildMetrics_EvaluationStat
	}{
		{"bazel.metrics.graph.dirtied_values", bgm.GetDirtiedValues()},
		{"bazel.metrics.graph.changed_values", bgm.GetChangedValues()},
		{"bazel.metrics.graph.built_values", bgm.GetBuiltValues()},
		{"bazel.metrics.graph.cleaned_values", bgm.GetCleanedValues()},
		{"bazel.metrics.graph.evaluated_values", bgm.GetEvaluatedValues()},
	}
	for _, c := range categories {
		if len(c.entries) == 0 {
			continue
		}
		original := len(c.entries)
		kept := original
		entries := c.entries
		if limit > 0 && original > limit {
			entries = entries[:limit]
			kept = limit
		}
		slice := span.Attributes().PutEmptySlice(c.attr)
		for _, e := range entries {
			m := slice.AppendEmpty().SetEmptyMap()
			// Issue #29 attribute keys: "skyfunction" (not the proto's
			// skyfunction_name) for backend-agnostic query ergonomics.
			m.PutStr("skyfunction", e.GetSkyfunctionName())
			m.PutInt("count", e.GetCount())
		}
		if kept < original {
			truncs = append(truncs, truncationRecord{
				attribute: c.attr,
				original:  original,
				kept:      kept,
			})
		}
	}
	return truncs
}

// maxBuildMetricsSliceEntries caps slice-of-map span attributes (DynamicExecutionMetrics,
// WorkerPoolMetrics). Bazel can emit one entry per mnemonic; 20 is enough to capture
// the common case without unbounded cardinality blowing up trace backends.
const maxBuildMetricsSliceEntries = 20

// stampTimingAttrs writes wall/cpu/phase timings plus the additional critical-path
// and actions-execution-start fields added in issue #25.
func stampTimingAttrs(attrs pcommon.Map, tm *bep.BuildMetrics_TimingMetrics) {
	if tm == nil {
		return
	}
	attrs.PutInt("bazel.metrics.wall_time_ms", tm.GetWallTimeInMs())
	attrs.PutInt("bazel.metrics.cpu_time_ms", tm.GetCpuTimeInMs())
	attrs.PutInt("bazel.metrics.analysis_phase_time_ms", tm.GetAnalysisPhaseTimeInMs())
	attrs.PutInt("bazel.metrics.execution_phase_time_ms", tm.GetExecutionPhaseTimeInMs())
	attrs.PutInt("bazel.metrics.actions_execution_start_ms", tm.GetActionsExecutionStartInMs())
	if cpt := tm.GetCriticalPathTime(); cpt != nil {
		attrs.PutInt("bazel.metrics.critical_path_time_ms", cpt.AsDuration().Milliseconds())
	}
}

// stampActionSummaryAttrs writes the scalar totals plus the remote-cache /
// action-cache / runner-count enrichment established by issue #18.
func stampActionSummaryAttrs(attrs pcommon.Map, as *bep.BuildMetrics_ActionSummary) {
	if as == nil {
		return
	}
	attrs.PutInt("bazel.metrics.actions_created", as.GetActionsCreated())
	attrs.PutInt("bazel.metrics.actions_created_not_including_aspects", as.GetActionsCreatedNotIncludingAspects())
	attrs.PutInt("bazel.metrics.actions_executed", as.GetActionsExecuted())
	attrs.PutInt("bazel.metrics.remote_cache_hits", as.GetRemoteCacheHits()) //nolint:staticcheck // SA1019: field deprecated upstream but still populated by current Bazel releases; issue #18 explicitly surfaces it.
	applyActionCacheStatistics(attrs, as.GetActionCacheStatistics())
	applyRunnerCounts(attrs, as.GetRunnerCount())
}

// applyActionCacheStatistics stamps ActionCacheStatistics onto the metrics span.
// No-op on nil. Per-miss-reason counts are emitted as
// bazel.metrics.action_cache.miss.<reason_lowercased>; only reasons present in
// the payload are emitted (up to 8 enum values per Bazel spec).
func applyActionCacheStatistics(attrs pcommon.Map, acs *actioncache.ActionCacheStatistics) {
	if acs == nil {
		return
	}
	attrs.PutInt("bazel.metrics.action_cache.hits", int64(acs.GetHits()))
	attrs.PutInt("bazel.metrics.action_cache.misses", int64(acs.GetMisses()))
	// SizeInBytes / LoadTimeInMs / SaveTimeInMs are uint64 in the proto; in
	// practice they fit comfortably in int64 (total build cache size, not a
	// cumulative throughput counter). Cast is intentional.
	attrs.PutInt("bazel.metrics.action_cache.size_bytes", int64(acs.GetSizeInBytes()))    //nolint:gosec // G115: uint64 cache size within int64 range for all realistic builds.
	attrs.PutInt("bazel.metrics.action_cache.load_time_ms", int64(acs.GetLoadTimeInMs())) //nolint:gosec // G115: uint64 load time within int64 range for all realistic builds.
	attrs.PutInt("bazel.metrics.action_cache.save_time_ms", int64(acs.GetSaveTimeInMs())) //nolint:gosec // G115: uint64 save time within int64 range for all realistic builds.

	for _, md := range acs.GetMissDetails() {
		if md == nil {
			continue
		}
		reason := strings.ToLower(md.GetReason().String())
		if reason == "" {
			continue
		}
		attrs.PutInt("bazel.metrics.action_cache.miss."+reason, int64(md.GetCount()))
	}
}

// applyRunnerCounts emits bazel.metrics.runners as a slice of maps, one per
// RunnerCount entry. Order mirrors the payload.
func applyRunnerCounts(attrs pcommon.Map, runners []*bep.BuildMetrics_ActionSummary_RunnerCount) {
	if len(runners) == 0 {
		return
	}
	slice := attrs.PutEmptySlice("bazel.metrics.runners")
	for _, rc := range runners {
		if rc == nil {
			continue
		}
		entry := slice.AppendEmpty().SetEmptyMap()
		entry.PutStr("name", rc.GetName())
		entry.PutInt("count", int64(rc.GetCount()))
		entry.PutStr("exec_kind", rc.GetExecKind())
	}
}

func stampMemoryAttrs(attrs pcommon.Map, mm *bep.BuildMetrics_MemoryMetrics) {
	if mm == nil {
		return
	}
	attrs.PutInt("bazel.metrics.memory.used_heap_size_post_build", mm.GetUsedHeapSizePostBuild())
	attrs.PutInt("bazel.metrics.memory.peak_post_gc_heap_size", mm.GetPeakPostGcHeapSize())
	attrs.PutInt("bazel.metrics.memory.peak_post_gc_tenured_space_heap_size", mm.GetPeakPostGcTenuredSpaceHeapSize())
}

// stampTargetAttrs omits the deprecated targets_loaded field — it never measured
// what the proto comment claims. See build_event_stream.proto:1011-1015.
func stampTargetAttrs(attrs pcommon.Map, tm *bep.BuildMetrics_TargetMetrics) {
	if tm == nil {
		return
	}
	attrs.PutInt("bazel.metrics.targets_configured", tm.GetTargetsConfigured())
	attrs.PutInt("bazel.metrics.targets_configured_not_including_aspects", tm.GetTargetsConfiguredNotIncludingAspects())
}

func stampPackageAttrs(attrs pcommon.Map, pm *bep.BuildMetrics_PackageMetrics) {
	if pm == nil {
		return
	}
	attrs.PutInt("bazel.metrics.packages_loaded", pm.GetPackagesLoaded())
}

func stampBuildGraphAttrs(attrs pcommon.Map, bgm *bep.BuildMetrics_BuildGraphMetrics) {
	if bgm == nil {
		return
	}
	attrs.PutInt("bazel.metrics.graph.action_lookup_value_count", int64(bgm.GetActionLookupValueCount()))
	attrs.PutInt("bazel.metrics.graph.action_lookup_value_count_not_including_aspects", int64(bgm.GetActionLookupValueCountNotIncludingAspects()))
	attrs.PutInt("bazel.metrics.graph.action_count", int64(bgm.GetActionCount()))
	attrs.PutInt("bazel.metrics.graph.action_count_not_including_aspects", int64(bgm.GetActionCountNotIncludingAspects()))
	attrs.PutInt("bazel.metrics.graph.input_file_configured_target_count", int64(bgm.GetInputFileConfiguredTargetCount()))
	attrs.PutInt("bazel.metrics.graph.output_file_configured_target_count", int64(bgm.GetOutputFileConfiguredTargetCount()))
	attrs.PutInt("bazel.metrics.graph.other_configured_target_count", int64(bgm.GetOtherConfiguredTargetCount()))
	attrs.PutInt("bazel.metrics.graph.output_artifact_count", int64(bgm.GetOutputArtifactCount()))
	attrs.PutInt("bazel.metrics.graph.post_invocation_skyframe_node_count", int64(bgm.GetPostInvocationSkyframeNodeCount()))
}

// stampArtifactAttrs emits one count + one size attr per artifact category. A nil
// FilesMetric leaves the pair unset — common for categories Bazel didn't populate
// (e.g. top-level artifacts without --output_groups).
func stampArtifactAttrs(attrs pcommon.Map, am *bep.BuildMetrics_ArtifactMetrics) {
	if am == nil {
		return
	}
	stampFilesMetric(attrs, "bazel.metrics.artifacts.source_read", am.GetSourceArtifactsRead())
	stampFilesMetric(attrs, "bazel.metrics.artifacts.output", am.GetOutputArtifactsSeen())
	stampFilesMetric(attrs, "bazel.metrics.artifacts.from_action_cache", am.GetOutputArtifactsFromActionCache())
	stampFilesMetric(attrs, "bazel.metrics.artifacts.top_level", am.GetTopLevelArtifacts())
}

func stampFilesMetric(attrs pcommon.Map, prefix string, fm *bep.BuildMetrics_ArtifactMetrics_FilesMetric) {
	if fm == nil {
		return
	}
	attrs.PutInt(prefix+"_count", int64(fm.GetCount()))
	attrs.PutInt(prefix+"_bytes", fm.GetSizeInBytes())
}

// stampNetworkAttrs reaches into the SystemNetworkStats sub-message for the 8
// throughput + peak fields. NetworkMetrics itself being non-nil but
// SystemNetworkStats nil is treated as "no network data collected". The proto
// exposes uint64 counters; saturateUint64 avoids flipping them negative on the
// (implausible but possible) overflow into int64.
func stampNetworkAttrs(attrs pcommon.Map, nm *bep.BuildMetrics_NetworkMetrics) {
	if nm == nil {
		return
	}
	stats := nm.GetSystemNetworkStats()
	if stats == nil {
		return
	}
	attrs.PutInt("bazel.metrics.network.bytes_sent", saturateUint64(stats.GetBytesSent()))
	attrs.PutInt("bazel.metrics.network.bytes_recv", saturateUint64(stats.GetBytesRecv()))
	attrs.PutInt("bazel.metrics.network.packets_sent", saturateUint64(stats.GetPacketsSent()))
	attrs.PutInt("bazel.metrics.network.packets_recv", saturateUint64(stats.GetPacketsRecv()))
	attrs.PutInt("bazel.metrics.network.peak_bytes_sent_per_sec", saturateUint64(stats.GetPeakBytesSentPerSec()))
	attrs.PutInt("bazel.metrics.network.peak_bytes_recv_per_sec", saturateUint64(stats.GetPeakBytesRecvPerSec()))
	attrs.PutInt("bazel.metrics.network.peak_packets_sent_per_sec", saturateUint64(stats.GetPeakPacketsSentPerSec()))
	attrs.PutInt("bazel.metrics.network.peak_packets_recv_per_sec", saturateUint64(stats.GetPeakPacketsRecvPerSec()))
}

// saturateUint64 clamps a uint64 to math.MaxInt64 so we never emit a negative
// "count" to metric backends. Real network counters should never approach this
// boundary during a single invocation, but the conversion is worth making
// explicit.
func saturateUint64(v uint64) int64 {
	if v > uint64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(v)
}

func stampCumulativeAttrs(attrs pcommon.Map, cm *bep.BuildMetrics_CumulativeMetrics) {
	if cm == nil {
		return
	}
	attrs.PutInt("bazel.metrics.cumulative.num_analyses", int64(cm.GetNumAnalyses()))
	attrs.PutInt("bazel.metrics.cumulative.num_builds", int64(cm.GetNumBuilds()))
}

// stampDynamicExecutionAttrs emits a slice-of-map attribute so each mnemonic's
// race outcome stays together. Capped at maxBuildMetricsSliceEntries to bound
// attribute payload size even if Bazel emits one entry per mnemonic in a large
// workspace.
func stampDynamicExecutionAttrs(attrs pcommon.Map, dem *bep.BuildMetrics_DynamicExecutionMetrics) {
	if dem == nil {
		return
	}
	stats := dem.GetRaceStatistics()
	if len(stats) == 0 {
		return
	}
	slice := attrs.PutEmptySlice("bazel.metrics.dynamic_execution")
	for i, rs := range stats {
		if i >= maxBuildMetricsSliceEntries {
			break
		}
		entry := slice.AppendEmpty().SetEmptyMap()
		entry.PutStr("mnemonic", rs.GetMnemonic())
		entry.PutStr("local_runner", rs.GetLocalRunner())
		entry.PutStr("remote_runner", rs.GetRemoteRunner())
		entry.PutInt("local_wins", int64(rs.GetLocalWins()))
		entry.PutInt("remote_wins", int64(rs.GetRemoteWins()))
	}
}

// stampWorkerPoolAttrs emits a slice-of-map attribute for per-mnemonic worker
// pool lifecycle stats. Same cap rationale as DynamicExecutionMetrics.
func stampWorkerPoolAttrs(attrs pcommon.Map, wpm *bep.BuildMetrics_WorkerPoolMetrics) {
	if wpm == nil {
		return
	}
	stats := wpm.GetWorkerPoolStats()
	if len(stats) == 0 {
		return
	}
	slice := attrs.PutEmptySlice("bazel.metrics.worker_pool")
	for i, ws := range stats {
		if i >= maxBuildMetricsSliceEntries {
			break
		}
		entry := slice.AppendEmpty().SetEmptyMap()
		entry.PutStr("mnemonic", ws.GetMnemonic())
		entry.PutInt("created_count", ws.GetCreatedCount())
		entry.PutInt("destroyed_count", ws.GetDestroyedCount())
		entry.PutInt("evicted_count", ws.GetEvictedCount())
		entry.PutInt("alive_count", ws.GetAliveCount())
	}
}

// actionDataOptions bundles the cap and the context needed to log truncation
// warnings when emitting ActionData as span attributes / gauges. Threaded
// together so the two emission paths (span attribute + gauges) truncate
// identically and log from the same source.
type actionDataOptions struct {
	maxEntries   int
	logger       *zap.Logger
	invocationID string
}

// applyActionDataAttrs emits the per-mnemonic ActionData breakdown as a
// Slice[Map] attribute on the bazel.metrics span. Empty input → no attribute,
// so downstream queries can distinguish "absent" from "present but empty".
// When len(entries) > opts.maxEntries the list is truncated to the first N
// (Bazel already ranks by execution count at the source) and a warning log
// carries the invocation id plus original count so operators know data was
// dropped.
func applyActionDataAttrs(attrs pcommon.Map, entries []*bep.BuildMetrics_ActionSummary_ActionData, opts actionDataOptions) {
	if len(entries) == 0 {
		return
	}
	limit := len(entries)
	if opts.maxEntries > 0 && limit > opts.maxEntries {
		limit = opts.maxEntries
		if opts.logger != nil {
			opts.logger.Warn("Truncating ActionData entries on bazel.metrics span",
				zap.String("invocation_id", opts.invocationID),
				zap.Int("original_count", len(entries)),
				zap.Int("limit", opts.maxEntries),
			)
		}
	}
	slice := attrs.PutEmptySlice("bazel.metrics.action_data")
	for i := range limit {
		ad := entries[i] //nolint:gosec // G602: limit <= len(entries) by the clamp above.
		m := slice.AppendEmpty().SetEmptyMap()
		m.PutStr("mnemonic", ad.GetMnemonic())
		m.PutInt("actions_executed", ad.GetActionsExecuted())
		m.PutInt("actions_created", ad.GetActionsCreated())
		m.PutInt("first_started_ms", ad.GetFirstStartedMs())
		m.PutInt("last_ended_ms", ad.GetLastEndedMs())
		m.PutInt("system_time_ms", durationToMs(ad.GetSystemTime()))
		m.PutInt("user_time_ms", durationToMs(ad.GetUserTime()))
	}
}

// durationToMs converts a protobuf Duration to milliseconds, mapping nil to 0.
// Used for ActionData.system_time / user_time where Bazel may omit the field.
func durationToMs(d *durationpb.Duration) int64 {
	if d == nil {
		return 0
	}
	return d.AsDuration().Milliseconds()
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
// caller buffers the span for deferred reparenting so that addTarget can
// reparent it when the TargetConfigured event arrives. See
// recordPendingReparent / drainPendingReparent.
func (s *invocationState) resolveTargetSpan(label, configID string) (pcommon.SpanID, bool) {
	if span, ok := s.targets[targetKey(label, configID)]; ok {
		return span.SpanID(), true
	}
	if span, ok := s.targets[targetKey(label, "")]; ok {
		return span.SpanID(), true
	}
	return s.rootSpanID, false
}

// recordPendingReparent buffers a span handle that needs its parent updated
// when TargetConfigured arrives for label. pdata spans are reference types,
// so the handle stored here remains mutable via SetParentSpanID.
func (s *invocationState) recordPendingReparent(label string, span ptrace.Span) {
	if s.pendingReparent == nil {
		s.pendingReparent = make(map[string][]ptrace.Span)
	}
	s.pendingReparent[label] = append(s.pendingReparent[label], span)
}

// drainPendingReparent retroactively points any buffered spans for label at
// targetSpanID, then clears the entry. Returns the number of spans reparented
// so the caller can increment a counter metric.
func (s *invocationState) drainPendingReparent(label string, targetSpanID pcommon.SpanID) int {
	spans, ok := s.pendingReparent[label]
	if !ok {
		return 0
	}
	for _, span := range spans {
		span.SetParentSpanID(targetSpanID)
	}
	delete(s.pendingReparent, label)
	return len(spans)
}

// appendSpan appends a new span to the invocation's pending batch.
func (s *invocationState) appendSpan() ptrace.Span {
	span := s.scopeSpans.Spans().AppendEmpty()
	span.SetTraceID(s.traceID)
	return span
}
