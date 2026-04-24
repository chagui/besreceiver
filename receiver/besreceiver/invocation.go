package besreceiver

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

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

	// pii opts specific BEP fields into span emission; see PIIConfig.
	pii PIIConfig
}

func newInvocationState(traceID pcommon.TraceID, rootSpanID pcommon.SpanID, started *bep.BuildStarted, now time.Time, pii PIIConfig) *invocationState {
	traces, ss := newTracesPayload()
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

func (s *invocationState) addAction(label, configID, primaryOutput string, action *bep.ActionExecuted) {
	if s.flushed {
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

	s.applyContextAttributes(span)

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
func (s *invocationState) summarizeTarget(label string, ts *bep.TestSummary) {
	if s.flushed {
		return
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
		span.Attributes().PutInt("bazel.metrics.remote_cache_hits", as.GetRemoteCacheHits()) //nolint:staticcheck // SA1019: field deprecated upstream but still populated by current Bazel releases; issue #18 explicitly surfaces it.
		applyActionCacheStatistics(span, as.GetActionCacheStatistics())
		applyRunnerCounts(span, as.GetRunnerCount())
	}

	return traces
}

// applyActionCacheStatistics stamps ActionCacheStatistics onto the metrics span.
// No-op on nil. Per-miss-reason counts are emitted as
// bazel.metrics.action_cache.miss.<reason_lowercased>; only reasons present in
// the payload are emitted (up to 8 enum values per Bazel spec).
func applyActionCacheStatistics(span ptrace.Span, acs *actioncache.ActionCacheStatistics) {
	if acs == nil {
		return
	}
	span.Attributes().PutInt("bazel.metrics.action_cache.hits", int64(acs.GetHits()))
	span.Attributes().PutInt("bazel.metrics.action_cache.misses", int64(acs.GetMisses()))
	// SizeInBytes / LoadTimeInMs / SaveTimeInMs are uint64 in the proto; in
	// practice they fit comfortably in int64 (total build cache size, not a
	// cumulative throughput counter). Cast is intentional.
	span.Attributes().PutInt("bazel.metrics.action_cache.size_bytes", int64(acs.GetSizeInBytes()))    //nolint:gosec // G115: uint64 cache size within int64 range for all realistic builds.
	span.Attributes().PutInt("bazel.metrics.action_cache.load_time_ms", int64(acs.GetLoadTimeInMs())) //nolint:gosec // G115: uint64 load time within int64 range for all realistic builds.
	span.Attributes().PutInt("bazel.metrics.action_cache.save_time_ms", int64(acs.GetSaveTimeInMs())) //nolint:gosec // G115: uint64 save time within int64 range for all realistic builds.

	for _, md := range acs.GetMissDetails() {
		if md == nil {
			continue
		}
		reason := strings.ToLower(md.GetReason().String())
		if reason == "" {
			continue
		}
		span.Attributes().PutInt("bazel.metrics.action_cache.miss."+reason, int64(md.GetCount()))
	}
}

// applyRunnerCounts emits bazel.metrics.runners as a slice of maps, one per
// RunnerCount entry. Order mirrors the payload; empty slice yields an empty
// attribute so downstream queries can assert presence without a special case.
func applyRunnerCounts(span ptrace.Span, runners []*bep.BuildMetrics_ActionSummary_RunnerCount) {
	if len(runners) == 0 {
		return
	}
	slice := span.Attributes().PutEmptySlice("bazel.metrics.runners")
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
