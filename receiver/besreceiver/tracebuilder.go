package besreceiver

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	pb "google.golang.org/genproto/googleapis/devtools/build/v1"

	bep "github.com/chagui/besreceiver/internal/bep/buildeventstream"
	"github.com/chagui/besreceiver/internal/bep/commandline"
)

const (
	defaultInvocationTimeout = 1 * time.Hour
	defaultReaperInterval    = 5 * time.Minute
)

// eventMsg is sent from gRPC handler goroutines to the owner goroutine.
type eventMsg struct {
	ctx          context.Context
	invocationID string
	event        *bep.BuildEvent
	errCh        chan<- error
}

// TraceBuilderConfig holds tuning parameters for a TraceBuilder.
type TraceBuilderConfig struct {
	InvocationTimeout time.Duration
	ReaperInterval    time.Duration
	MeterProvider     metric.MeterProvider
	PII               PIIConfig
	// Caps bounds Slice[Map] attributes on the bazel.metrics span for
	// high-cardinality BuildMetrics sub-messages. Zero-valued fields fall back
	// to defaults via HighCardinalityCaps.withDefaults.
	Caps HighCardinalityCaps
}

// TraceBuilder converts BEP events into OTel traces, logs, and metrics.
// All invocation state is owned by a single goroutine started via Start().
type TraceBuilder struct {
	tracesConsumer    consumer.Traces
	logsConsumer      consumer.Logs
	metricsConsumer   consumer.Metrics
	logger            *zap.Logger
	invocationTimeout time.Duration
	reaperInterval    time.Duration
	eventCh           chan eventMsg
	stopCh            chan struct{}
	doneCh            chan struct{}
	stopOnce          sync.Once

	// PII gates sensitive-field emission on spans. Threaded into per-invocation
	// state so finalize/addAction can make the decision locally without re-
	// reaching into the builder.
	pii PIIConfig

	// caps bounds high-cardinality Slice[Map] attributes on bazel.metrics.
	// Threaded into per-invocation state for local truncation decisions.
	caps HighCardinalityCaps

	// Cumulative counters for cross-invocation metrics.
	counters *cumulativeCounters

	// Internal metrics for observability.
	activeInvocations metric.Int64UpDownCounter
	eventsProcessed   metric.Int64Counter
	eventsReparented  metric.Int64Counter
	invocationsReaped metric.Int64Counter
	consumerErrors    metric.Int64Counter
}

// NewTraceBuilder creates a new TraceBuilder.
// Any consumer may be nil if that signal is not configured.
// Zero-value fields in cfg get sensible defaults.
func NewTraceBuilder(tracesConsumer consumer.Traces, logsConsumer consumer.Logs, metricsConsumer consumer.Metrics, logger *zap.Logger, cfg TraceBuilderConfig) *TraceBuilder {
	if cfg.InvocationTimeout <= 0 {
		cfg.InvocationTimeout = defaultInvocationTimeout
	}
	if cfg.ReaperInterval <= 0 {
		cfg.ReaperInterval = defaultReaperInterval
	}
	if cfg.MeterProvider == nil {
		cfg.MeterProvider = noop.NewMeterProvider()
	}
	meter := cfg.MeterProvider.Meter("besreceiver")
	activeInvocations, _ := meter.Int64UpDownCounter("bes.invocations.active",
		metric.WithDescription("Number of active (in-flight) build invocations"),
	)
	eventsProcessed, _ := meter.Int64Counter("bes.events.processed",
		metric.WithDescription("Total BEP events processed"),
	)
	eventsReparented, _ := meter.Int64Counter("bes.events.reparented",
		metric.WithDescription("Total action/test spans reparented onto a late-arriving target span"),
	)
	invocationsReaped, _ := meter.Int64Counter("bes.invocations.reaped",
		metric.WithDescription("Total invocations reaped by the stale-invocation reaper"),
	)
	consumerErrors, _ := meter.Int64Counter("bes.consumer.errors",
		metric.WithDescription("Total errors from the traces consumer"),
	)
	return &TraceBuilder{
		tracesConsumer:    tracesConsumer,
		logsConsumer:      logsConsumer,
		metricsConsumer:   metricsConsumer,
		logger:            logger,
		invocationTimeout: cfg.InvocationTimeout,
		reaperInterval:    cfg.ReaperInterval,
		pii:               cfg.PII,
		caps:              cfg.Caps.withDefaults(),
		counters:          newCumulativeCounters(pcommon.NewTimestampFromTime(time.Now())),
		activeInvocations: activeInvocations,
		eventsProcessed:   eventsProcessed,
		eventsReparented:  eventsReparented,
		invocationsReaped: invocationsReaped,
		consumerErrors:    consumerErrors,
	}
}

// Start begins the owner goroutine that processes events and reaps stale invocations.
func (tb *TraceBuilder) Start() {
	tb.eventCh = make(chan eventMsg)
	tb.stopCh = make(chan struct{})
	tb.doneCh = make(chan struct{})
	go tb.run()
}

// Stop signals the owner goroutine to exit and waits for it to finish.
// It is safe to call multiple times.
func (tb *TraceBuilder) Stop() {
	tb.stopOnce.Do(func() {
		if tb.stopCh != nil {
			close(tb.stopCh)
		}
	})
	if tb.doneCh != nil {
		<-tb.doneCh
	}
}

func (tb *TraceBuilder) run() {
	invocations := make(map[string]*invocationState)
	ticker := time.NewTicker(tb.reaperInterval)
	defer ticker.Stop()
	defer close(tb.doneCh)

	for {
		select {
		case msg := <-tb.eventCh:
			err := tb.processEvent(msg.ctx, invocations, msg.invocationID, msg.event)
			msg.errCh <- err
		case <-ticker.C:
			tb.reapStale(invocations, time.Now())
		case <-tb.stopCh:
			return
		}
	}
}

// ProcessOrderedBuildEvent processes a single BES OrderedBuildEvent.
func (tb *TraceBuilder) ProcessOrderedBuildEvent(ctx context.Context, obe *pb.OrderedBuildEvent) error {
	bazelEvent := obe.GetEvent().GetBazelEvent()
	if bazelEvent == nil {
		return nil
	}

	event, err := ParseBazelEvent(bazelEvent)
	if err != nil {
		return consumererror.NewPermanent(err)
	}

	invocationID := obe.GetStreamId().GetInvocationId()
	errCh := make(chan error, 1)

	select {
	case tb.eventCh <- eventMsg{
		ctx:          ctx,
		invocationID: invocationID,
		event:        event,
		errCh:        errCh,
	}:
	case <-ctx.Done():
		return fmt.Errorf("sending event to builder: %w", ctx.Err())
	}

	select {
	case processErr := <-errCh:
		return processErr
	case <-ctx.Done():
		return fmt.Errorf("waiting for event processing: %w", ctx.Err())
	}
}

//nolint:gocyclo,gocognit,cyclop,funlen // Flat payload dispatch: grows as the receiver handles more BEP event types. Splitting by payload family would hurt discoverability.
func (tb *TraceBuilder) processEvent(ctx context.Context, invocations map[string]*invocationState, invocationID string, event *bep.BuildEvent) error {
	tb.eventsProcessed.Add(ctx, 1, metric.WithAttributes(
		attribute.String("event_type", eventTypeName(event)),
	))
	payload := event.GetPayload()
	if payload == nil {
		return nil
	}
	switch p := payload.(type) {
	case *bep.BuildEvent_Started:
		if p.Started == nil {
			return nil
		}
		return tb.handleBuildStarted(ctx, invocations, invocationID, p.Started)
	case *bep.BuildEvent_Configured:
		if p.Configured == nil {
			return nil
		}
		return tb.handleTargetConfigured(ctx, invocations, invocationID, event, p.Configured)
	case *bep.BuildEvent_Configuration:
		if p.Configuration == nil {
			return nil
		}
		return tb.handleConfiguration(ctx, invocations, invocationID, event.GetId(), p.Configuration)
	case *bep.BuildEvent_OptionsParsed:
		if p.OptionsParsed == nil {
			return nil
		}
		return tb.handleOptionsParsed(ctx, invocations, invocationID, p.OptionsParsed)
	case *bep.BuildEvent_StructuredCommandLine:
		if p.StructuredCommandLine == nil {
			return nil
		}
		return tb.handleStructuredCommandLine(ctx, invocations, invocationID, p.StructuredCommandLine)
	case *bep.BuildEvent_Action:
		if p.Action == nil {
			return nil
		}
		return tb.handleActionExecuted(ctx, invocations, invocationID, event.GetId(), p.Action)
	case *bep.BuildEvent_TestResult:
		if p.TestResult == nil {
			return nil
		}
		return tb.handleTestResult(ctx, invocations, invocationID, event.GetId(), p.TestResult)
	case *bep.BuildEvent_Finished:
		if p.Finished == nil {
			return nil
		}
		return tb.handleBuildFinished(ctx, invocations, invocationID, p.Finished)
	case *bep.BuildEvent_BuildMetrics:
		if p.BuildMetrics == nil {
			return nil
		}
		return tb.handleBuildMetrics(ctx, invocations, invocationID, p.BuildMetrics)
	case *bep.BuildEvent_Aborted:
		if p.Aborted == nil {
			return nil
		}
		return tb.handleAborted(ctx, invocations, invocationID, p.Aborted)
	case *bep.BuildEvent_WorkspaceStatus:
		if p.WorkspaceStatus == nil {
			return nil
		}
		return tb.handleWorkspaceStatus(ctx, invocations, invocationID, p.WorkspaceStatus)
	case *bep.BuildEvent_BuildMetadata:
		if p.BuildMetadata == nil {
			return nil
		}
		return tb.handleBuildMetadata(ctx, invocations, invocationID, p.BuildMetadata)
	case *bep.BuildEvent_Completed:
		if p.Completed == nil {
			return nil
		}
		return tb.handleTargetComplete(ctx, invocations, invocationID, event.GetId(), p.Completed)
	case *bep.BuildEvent_TestSummary:
		if p.TestSummary == nil {
			return nil
		}
		return tb.handleTestSummary(ctx, invocations, invocationID, event.GetId(), p.TestSummary)
	}
	return nil
}

// eventTypeName returns a short name for the BEP event type, used as a metric attribute.
func eventTypeName(event *bep.BuildEvent) string {
	switch event.GetPayload().(type) {
	case *bep.BuildEvent_Started:
		return "started"
	case *bep.BuildEvent_Configured:
		return "configured"
	case *bep.BuildEvent_Action:
		return "action"
	case *bep.BuildEvent_TestResult:
		return "test_result"
	case *bep.BuildEvent_Finished:
		return "finished"
	case *bep.BuildEvent_BuildMetrics:
		return "build_metrics"
	case *bep.BuildEvent_Aborted:
		return "aborted"
	case *bep.BuildEvent_WorkspaceStatus:
		return "workspace_status"
	case *bep.BuildEvent_BuildMetadata:
		return "build_metadata"
	case *bep.BuildEvent_Completed:
		return "target_complete"
	case *bep.BuildEvent_TestSummary:
		return "test_summary"
	case *bep.BuildEvent_Configuration:
		return "configuration"
	case *bep.BuildEvent_OptionsParsed:
		return "options_parsed"
	case *bep.BuildEvent_StructuredCommandLine:
		return "structured_command_line"
	default:
		return "other"
	}
}

func (tb *TraceBuilder) handleBuildStarted(ctx context.Context, invocations map[string]*invocationState, invocationID string, started *bep.BuildStarted) error {
	traceID := traceIDFromUUID(started.GetUuid())
	rootSpanID := spanIDFromIdentity(started.GetUuid(), "root")
	invocations[invocationID] = newInvocationState(traceID, rootSpanID, started, time.Now(), tb.pii, tb.caps)
	tb.activeInvocations.Add(ctx, 1)

	tb.logger.Debug("Build started",
		zap.String("invocation_id", invocationID),
		zap.String("uuid", started.GetUuid()),
		zap.String("command", started.GetCommand()),
	)

	state := invocations[invocationID]
	var ts time.Time
	if started.GetStartTime() != nil {
		ts = started.GetStartTime().AsTime()
	}
	tb.emitLog(ctx, state, invocationID, "bazel.build.started",
		fmt.Sprintf("Build started: %s", started.GetCommand()),
		plog.SeverityNumberInfo, ts,
		map[string]string{
			"bazel.command": started.GetCommand(),
			"bazel.uuid":    started.GetUuid(),
		},
	)

	// Root span is emitted in handleBuildFinished with both start and end timestamps.
	return nil
}

func (tb *TraceBuilder) handleTargetConfigured(ctx context.Context, invocations map[string]*invocationState, invocationID string, event *bep.BuildEvent, configured *bep.TargetConfigured) error {
	state := invocations[invocationID]
	if state == nil {
		return nil
	}

	tc := event.GetId().GetTargetConfigured()
	if tc == nil {
		return nil
	}

	// Config id for this target lives on the TargetCompleted child event id,
	// not on the TargetConfigured payload itself. Pick the first non-empty
	// id that isn't "none" (the null config per BEP spec).
	configID := ""
	for _, child := range event.GetChildren() {
		id := child.GetTargetCompleted().GetConfiguration().GetId()
		if id != "" && id != nullConfigID {
			configID = id
			break
		}
	}

	tb.logger.Debug("Target configured",
		zap.String("invocation_id", invocationID),
		zap.String("label", tc.GetLabel()),
	)

	tb.emitLog(ctx, state, invocationID, "bazel.target.configured",
		fmt.Sprintf("Target configured: %s", tc.GetLabel()),
		plog.SeverityNumberInfo, time.Time{},
		map[string]string{
			"bazel.target.label":     tc.GetLabel(),
			"bazel.target.rule_kind": configured.GetTargetKind(),
		},
	)

	// Post-flush arrivals emit a standalone traces payload, matching the
	// pattern established for BuildMetrics (see handleBuildMetrics). This
	// preserves the span instead of silently dropping it when a future Bazel
	// version emits TargetConfigured after BuildFinished.
	if state.flushed {
		traces := state.lateTargetSpan(tc.GetLabel(), configured.GetTargetKind(), configID)
		return tb.consumeAndRecord(ctx, traces)
	}

	reparented := state.addTarget(tc.GetLabel(), configured.GetTargetKind(), configID)
	if reparented > 0 {
		tb.eventsReparented.Add(ctx, int64(reparented))
		tb.logger.Debug("Reparented out-of-order spans under target",
			zap.String("invocation_id", invocationID),
			zap.String("label", tc.GetLabel()),
			zap.Int("count", reparented),
		)
	}
	return nil
}

func (tb *TraceBuilder) handleActionExecuted(ctx context.Context, invocations map[string]*invocationState, invocationID string, eventID *bep.BuildEventId, action *bep.ActionExecuted) error {
	state := invocations[invocationID]
	if state == nil {
		return nil
	}

	ac := eventID.GetActionCompleted()
	label := ""
	configID := ""
	primaryOutput := ""
	if ac != nil {
		label = ac.GetLabel()
		configID = ac.GetConfiguration().GetId()
		primaryOutput = ac.GetPrimaryOutput()
	}

	severity := plog.SeverityNumberInfo
	body := fmt.Sprintf("Action completed: %s %s", action.GetType(), label)
	if !action.GetSuccess() {
		severity = plog.SeverityNumberError
		body = fmt.Sprintf("Action failed: %s %s", action.GetType(), label)
	}
	var actionTS time.Time
	if action.GetStartTime() != nil {
		actionTS = action.GetStartTime().AsTime()
	}
	tb.emitLog(ctx, state, invocationID, "bazel.action.completed",
		body, severity, actionTS,
		map[string]string{
			"bazel.action.mnemonic": action.GetType(),
			"bazel.target.label":    label,
		},
	)

	// Post-flush arrivals emit a standalone traces payload (see handleBuildMetrics).
	if state.flushed {
		traces := state.lateActionSpan(label, configID, primaryOutput, action)
		return tb.consumeAndRecord(ctx, traces)
	}

	state.addAction(label, configID, primaryOutput, action)
	return nil
}

func (tb *TraceBuilder) handleTestResult(ctx context.Context, invocations map[string]*invocationState, invocationID string, eventID *bep.BuildEventId, result *bep.TestResult) error {
	state := invocations[invocationID]
	if state == nil {
		return nil
	}

	tr := eventID.GetTestResult()
	label := ""
	configID := ""
	if tr != nil {
		label = tr.GetLabel()
		configID = tr.GetConfiguration().GetId()
	}

	severity := plog.SeverityNumberInfo
	body := fmt.Sprintf("Test passed: %s", label)
	if result.GetStatus() != bep.TestStatus_PASSED {
		severity = plog.SeverityNumberError
		body = fmt.Sprintf("Test failed: %s", label)
	}
	var testTS time.Time
	if result.GetTestAttemptStart() != nil {
		testTS = result.GetTestAttemptStart().AsTime()
	}
	tb.emitLog(ctx, state, invocationID, "bazel.test.result",
		body, severity, testTS,
		map[string]string{
			"bazel.test.status":  result.GetStatus().String(),
			"bazel.target.label": label,
		},
	)

	// Post-flush arrivals emit a standalone traces payload (see handleBuildMetrics).
	if state.flushed {
		traces := state.lateTestResultSpan(label, configID, tr, result)
		return tb.consumeAndRecord(ctx, traces)
	}

	state.addTestResult(label, configID, tr, result)
	return nil
}

func (tb *TraceBuilder) handleBuildFinished(ctx context.Context, invocations map[string]*invocationState, invocationID string, finished *bep.BuildFinished) error {
	state := invocations[invocationID]
	if state == nil {
		return nil
	}

	traces, ok := state.finalize(finished)
	if !ok {
		return nil
	}

	tb.logger.Debug("Build finished",
		zap.String("invocation_id", invocationID),
		zap.String("exit_code", finished.GetExitCode().GetName()),
		zap.Int32("code", finished.GetExitCode().GetCode()),
	)

	severity := plog.SeverityNumberInfo
	body := fmt.Sprintf("Build finished: %s", finished.GetExitCode().GetName())
	if finished.GetExitCode().GetCode() != 0 {
		severity = plog.SeverityNumberError
	}
	var finishTS time.Time
	if finished.GetFinishTime() != nil {
		finishTS = finished.GetFinishTime().AsTime()
	}
	tb.emitLog(ctx, state, invocationID, "bazel.build.finished",
		body, severity, finishTS,
		map[string]string{
			"bazel.exit_code.name": finished.GetExitCode().GetName(),
		},
	)

	// Don't delete state yet — BuildMetrics may arrive after BuildFinished.
	// State cleanup is handled by handleBuildMetrics or the stale invocation reaper.
	return tb.consumeAndRecord(ctx, traces)
}

// handleAborted records the first abort reason + description on the invocation
// state. Abort attributes are stamped on the root span at finalize time (or
// flushOrphaned for reaped invocations). Subsequent aborts are logged at Warn
// but do not overwrite the original root cause.
func (tb *TraceBuilder) handleAborted(ctx context.Context, invocations map[string]*invocationState, invocationID string, aborted *bep.Aborted) error {
	state := invocations[invocationID]
	if state == nil {
		tb.logger.Debug("Aborted event dropped — no invocation state",
			zap.String("invocation_id", invocationID),
		)
		return nil
	}

	reason := abortReasonString(aborted.GetReason())
	description := aborted.GetDescription()
	first := state.recordAbort(aborted)

	if first {
		tb.logger.Debug("Build aborted",
			zap.String("invocation_id", invocationID),
			zap.String("reason", reason),
			zap.String("description", description),
		)
	} else {
		tb.logger.Warn("Additional abort event received; keeping first reason",
			zap.String("invocation_id", invocationID),
			zap.String("first_reason", state.abortReason),
			zap.String("new_reason", reason),
		)
	}

	tb.emitLog(ctx, state, invocationID, "bazel.build.aborted",
		fmt.Sprintf("Build aborted: %s: %s", reason, description),
		plog.SeverityNumberError, time.Time{},
		map[string]string{
			"bazel.abort.reason":      reason,
			"bazel.abort.description": description,
		},
	)

	return nil
}

// handleWorkspaceStatus buffers WorkspaceStatus items on the invocation state
// for emission as bazel.workspace.* attributes on the root span.
func (tb *TraceBuilder) handleWorkspaceStatus(_ context.Context, invocations map[string]*invocationState, invocationID string, ws *bep.WorkspaceStatus) error {
	state := invocations[invocationID]
	if state == nil {
		return nil
	}
	state.addWorkspaceItems(ws.GetItem())
	tb.logger.Debug("Workspace status received",
		zap.String("invocation_id", invocationID),
		zap.Int("items", len(ws.GetItem())),
	)
	return nil
}

// handleBuildMetadata buffers BuildMetadata entries on the invocation state
// for emission as bazel.metadata.* attributes on the root span.
func (tb *TraceBuilder) handleBuildMetadata(_ context.Context, invocations map[string]*invocationState, invocationID string, bm *bep.BuildMetadata) error {
	state := invocations[invocationID]
	if state == nil {
		return nil
	}
	state.addBuildMetadata(bm.GetMetadata())
	tb.logger.Debug("Build metadata received",
		zap.String("invocation_id", invocationID),
		zap.Int("entries", len(bm.GetMetadata())),
	)
	return nil
}

// handleConfiguration buffers a Configuration payload keyed by its id for
// later lookup when the owning TargetConfigured event arrives.
func (tb *TraceBuilder) handleConfiguration(_ context.Context, invocations map[string]*invocationState, invocationID string, eventID *bep.BuildEventId, cfg *bep.Configuration) error {
	state := invocations[invocationID]
	if state == nil {
		return nil
	}
	configID := eventID.GetConfiguration().GetId()
	state.storeConfiguration(configID, cfg)
	tb.logger.Debug("Configuration received",
		zap.String("invocation_id", invocationID),
		zap.String("config_id", configID),
		zap.String("mnemonic", cfg.GetMnemonic()),
	)
	return nil
}

// handleOptionsParsed buffers --tool_tag and explicit option counts onto the
// invocation state for emission on the root span at finalize.
func (tb *TraceBuilder) handleOptionsParsed(_ context.Context, invocations map[string]*invocationState, invocationID string, op *bep.OptionsParsed) error {
	state := invocations[invocationID]
	if state == nil {
		return nil
	}
	state.setOptionsParsed(op)
	return nil
}

// handleStructuredCommandLine reconstructs a human-readable command-line
// string from the "original" StructuredCommandLine payload and buffers it
// on the invocation state. Canonical and tool variants are ignored.
func (tb *TraceBuilder) handleStructuredCommandLine(_ context.Context, invocations map[string]*invocationState, invocationID string, cl *commandline.CommandLine) error {
	state := invocations[invocationID]
	if state == nil {
		return nil
	}
	if cl.GetCommandLineLabel() != "original" {
		return nil
	}
	if state.flushed {
		return nil
	}
	state.commandLine = reconstructCommandLine(cl)
	return nil
}

const commandLineMaxBytes = 1024

// reconstructCommandLine joins a CommandLine's sections into a single string
// (ChunkList chunks verbatim, Option lists via combined_form), truncating to
// commandLineMaxBytes and appending an ellipsis when truncated.
func reconstructCommandLine(cl *commandline.CommandLine) string {
	var parts []string
	for _, section := range cl.GetSections() {
		if cl := section.GetChunkList(); cl != nil {
			parts = append(parts, cl.GetChunk()...)
			continue
		}
		if ol := section.GetOptionList(); ol != nil {
			for _, opt := range ol.GetOption() {
				if cf := opt.GetCombinedForm(); cf != "" {
					parts = append(parts, cf)
				}
			}
		}
	}
	joined := strings.Join(parts, " ")
	if len(joined) <= commandLineMaxBytes {
		return joined
	}
	// Back off to rune boundary so we never emit invalid UTF-8.
	end := commandLineMaxBytes
	for end > 0 && !utf8.RuneStart(joined[end]) {
		end--
	}
	return joined[:end] + "…"
}

// handleTargetComplete enriches an existing bazel.target span with outcome
// attributes from TargetComplete. Arrives after TargetConfigured in practice;
// no-op when the target span is missing (aborted or out-of-order).
func (tb *TraceBuilder) handleTargetComplete(ctx context.Context, invocations map[string]*invocationState, invocationID string, eventID *bep.BuildEventId, tc *bep.TargetComplete) error {
	state := invocations[invocationID]
	if state == nil {
		return nil
	}
	label := eventID.GetTargetCompleted().GetLabel()
	if label == "" {
		return nil
	}
	state.completeTarget(label, tc)

	severity := plog.SeverityNumberInfo
	body := fmt.Sprintf("Target completed: %s", label)
	if !tc.GetSuccess() {
		severity = plog.SeverityNumberError
		body = fmt.Sprintf("Target failed: %s", label)
	}
	tb.emitLog(ctx, state, invocationID, "bazel.target.completed",
		body, severity, time.Time{},
		map[string]string{
			"bazel.target.label":   label,
			"bazel.target.success": strconv.FormatBool(tc.GetSuccess()),
		},
	)
	return nil
}

// handleTestSummary enriches an existing bazel.target span with aggregate
// test metrics from TestSummary. No-op for non-test targets (no summary
// arrives) or when the target span is missing.
func (tb *TraceBuilder) handleTestSummary(ctx context.Context, invocations map[string]*invocationState, invocationID string, eventID *bep.BuildEventId, ts *bep.TestSummary) error {
	state := invocations[invocationID]
	if state == nil {
		return nil
	}
	label := eventID.GetTestSummary().GetLabel()
	if label == "" {
		return nil
	}
	state.summarizeTarget(label, ts)

	tb.emitLog(ctx, state, invocationID, "bazel.target.test_summary",
		fmt.Sprintf("Test summary: %s", label),
		plog.SeverityNumberInfo, time.Time{},
		map[string]string{
			"bazel.target.label":               label,
			"bazel.target.test.overall_status": ts.GetOverallStatus().String(),
		},
	)
	return nil
}

func (tb *TraceBuilder) handleBuildMetrics(ctx context.Context, invocations map[string]*invocationState, invocationID string, metrics *bep.BuildMetrics) error {
	state := invocations[invocationID]
	if state == nil {
		return nil
	}

	traces, truncs := state.buildMetricsSpan(metrics)
	for _, tr := range truncs {
		tb.logger.Warn("Truncated high-cardinality bazel.metrics attribute",
			zap.String("invocation_id", invocationID),
			zap.String("attribute", tr.attribute),
			zap.Int("original_count", tr.original),
			zap.Int("kept", tr.kept),
		)
	}

	tb.emitLog(ctx, state, invocationID, "bazel.build.metrics",
		"Build metrics",
		plog.SeverityNumberInfo, time.Time{},
		nil,
	)

	ts := pcommon.NewTimestampFromTime(time.Now())
	md := buildInvocationGauges(invocationID, state, metrics, ts)

	// Update cumulative counters and merge into the same metrics payload.
	tb.counters.record(metrics)
	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	tb.counters.appendTo(sm, ts)

	// BuildMetrics is typically the last event in the BEP stream.
	// Clean up invocation state after consuming traces.
	tracesErr := tb.consumeAndRecord(ctx, traces)
	metricsErr := tb.consumeMetrics(ctx, md)
	delete(invocations, invocationID)
	tb.activeInvocations.Add(ctx, -1)
	return errors.Join(tracesErr, metricsErr)
}

// consumeAndRecord forwards traces to the next consumer and records an error metric on failure.
func (tb *TraceBuilder) consumeAndRecord(ctx context.Context, traces ptrace.Traces) error {
	if tb.tracesConsumer == nil {
		return nil
	}
	err := tb.tracesConsumer.ConsumeTraces(ctx, traces)
	if err != nil {
		errorType := "retryable"
		if consumererror.IsPermanent(err) {
			errorType = "permanent"
		}
		tb.consumerErrors.Add(ctx, 1, metric.WithAttributes(
			attribute.String("error_type", errorType),
		))
		return fmt.Errorf("consuming traces: %w", err)
	}
	return nil
}

// consumeMetrics forwards metrics to the next consumer. No-op if metricsConsumer is nil.
func (tb *TraceBuilder) consumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if tb.metricsConsumer == nil {
		return nil
	}
	if md.MetricCount() == 0 {
		return nil
	}
	if err := tb.metricsConsumer.ConsumeMetrics(ctx, md); err != nil {
		return fmt.Errorf("consuming metrics: %w", err)
	}
	return nil
}

// emitLog creates and sends a single log record for a BEP event.
// It is a no-op if logsConsumer is nil. Errors are logged but not propagated,
// since traces remain the primary signal.
func (tb *TraceBuilder) emitLog(ctx context.Context, state *invocationState, invocationID, eventName, body string, severity plog.SeverityNumber, ts time.Time, attrs map[string]string) {
	if tb.logsConsumer == nil {
		return
	}

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "bazel")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("besreceiver")

	lr := sl.LogRecords().AppendEmpty()
	if !ts.IsZero() {
		lr.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	}
	lr.SetSeverityNumber(severity)
	lr.SetSeverityText(severity.String())
	lr.Body().SetStr(body)

	lr.Attributes().PutStr("event.name", eventName)
	lr.Attributes().PutStr("bazel.invocation_id", invocationID)

	if state != nil {
		lr.SetTraceID(state.traceID)
	}

	for k, v := range attrs {
		lr.Attributes().PutStr(k, v)
	}

	if err := tb.logsConsumer.ConsumeLogs(ctx, logs); err != nil {
		tb.logger.Warn("Failed to emit log record",
			zap.String("event_name", eventName),
			zap.String("invocation_id", invocationID),
			zap.Error(err),
		)
	}
}

// reapStale scans all invocations and deletes any that exceed invocationTimeout,
// flushing pending spans before deletion.
func (tb *TraceBuilder) reapStale(invocations map[string]*invocationState, now time.Time) {
	for invID, state := range invocations {
		if now.Sub(state.createdAt) <= tb.invocationTimeout {
			continue
		}
		if traces, ok := state.flushOrphaned(); ok && tb.tracesConsumer != nil {
			if err := tb.tracesConsumer.ConsumeTraces(context.Background(), traces); err != nil {
				tb.logger.Error("Failed to flush orphaned spans",
					zap.String("invocation_id", invID),
					zap.Error(err),
				)
			}
		}
		delete(invocations, invID)
		tb.invocationsReaped.Add(context.Background(), 1)
		tb.activeInvocations.Add(context.Background(), -1)
		tb.logger.Warn("Reaped stale invocation",
			zap.String("invocation_id", invID),
			zap.Duration("age", now.Sub(state.createdAt)),
		)
	}
}

// traceIDFromUUID deterministically derives a TraceID from a Bazel invocation UUID.
func traceIDFromUUID(uuid string) pcommon.TraceID {
	h := sha256.Sum256([]byte(uuid))
	var tid pcommon.TraceID
	copy(tid[:], h[:16])
	return tid
}

// spanIDFromIdentity deterministically derives a SpanID from the given identity parts.
// Parts are joined with a NUL separator and SHA-256'd; bytes [16:24] are used so
// SpanIDs never overlap with TraceID bytes (which use [0:16]).
func spanIDFromIdentity(parts ...string) pcommon.SpanID {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	var sid pcommon.SpanID
	copy(sid[:], h[16:24])
	return sid
}

// targetKey builds the lookup key for the targets map.
func targetKey(label, configID string) string {
	return label + "\x00" + configID
}
