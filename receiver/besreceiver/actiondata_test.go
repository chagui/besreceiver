package besreceiver

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/types/known/durationpb"

	bep "github.com/chagui/besreceiver/internal/bep/buildeventstream"
)

// buildActionDataEntries is a tiny helper for constructing a list of N
// distinct ActionData entries with deterministic, inspectable values.
func buildActionDataEntries(mnemonics ...string) []*bep.BuildMetrics_ActionSummary_ActionData {
	out := make([]*bep.BuildMetrics_ActionSummary_ActionData, 0, len(mnemonics))
	for i, m := range mnemonics {
		i64 := int64(i + 1)
		out = append(out, &bep.BuildMetrics_ActionSummary_ActionData{
			Mnemonic:        m,
			ActionsExecuted: 10 * i64,
			ActionsCreated:  20 * i64,
			FirstStartedMs:  1000 * i64,
			LastEndedMs:     2000 * i64,
			SystemTime:      durationpb.New(time.Duration(i64) * 100 * time.Millisecond),
			UserTime:        durationpb.New(time.Duration(i64) * 200 * time.Millisecond),
		})
	}
	return out
}

func TestApplyActionDataAttrs_AllSevenFields(t *testing.T) {
	entries := []*bep.BuildMetrics_ActionSummary_ActionData{
		{
			Mnemonic:        "Javac",
			ActionsExecuted: 42,
			ActionsCreated:  50,
			FirstStartedMs:  1000,
			LastEndedMs:     2000,
			SystemTime:      durationpb.New(150 * time.Millisecond),
			UserTime:        durationpb.New(350 * time.Millisecond),
		},
	}
	attrs := pcommon.NewMap()

	applyActionDataAttrs(attrs, entries, actionDataOptions{maxEntries: 50, logger: zap.NewNop()})

	raw, ok := attrs.Get("bazel.metrics.action_data")
	if !ok {
		t.Fatal("expected bazel.metrics.action_data attribute")
	}
	slice := raw.Slice()
	if slice.Len() != 1 {
		t.Fatalf("expected 1 entry, got %d", slice.Len())
	}
	m := slice.At(0).Map()
	want := map[string]any{
		"mnemonic":         "Javac",
		"actions_executed": int64(42),
		"actions_created":  int64(50),
		"first_started_ms": int64(1000),
		"last_ended_ms":    int64(2000),
		"system_time_ms":   int64(150),
		"user_time_ms":     int64(350),
	}
	if m.Len() != len(want) {
		t.Errorf("expected %d fields per entry, got %d", len(want), m.Len())
	}
	for k, v := range want {
		got, ok := m.Get(k)
		if !ok {
			t.Errorf("missing field %q", k)
			continue
		}
		switch expected := v.(type) {
		case string:
			if got.Str() != expected {
				t.Errorf("%s: got %q, want %q", k, got.Str(), expected)
			}
		case int64:
			if got.Int() != expected {
				t.Errorf("%s: got %d, want %d", k, got.Int(), expected)
			}
		}
	}
}

func TestApplyActionDataAttrs_NilDurationsBecomeZero(t *testing.T) {
	entries := []*bep.BuildMetrics_ActionSummary_ActionData{
		{
			Mnemonic:        "GoCompile",
			ActionsExecuted: 1,
			// SystemTime and UserTime left nil — must surface as 0, not the
			// attribute being absent.
		},
	}
	attrs := pcommon.NewMap()

	applyActionDataAttrs(attrs, entries, actionDataOptions{maxEntries: 50, logger: zap.NewNop()})

	raw, ok := attrs.Get("bazel.metrics.action_data")
	if !ok {
		t.Fatal("expected bazel.metrics.action_data attribute")
	}
	m := raw.Slice().At(0).Map()
	sys, ok := m.Get("system_time_ms")
	if !ok || sys.Int() != 0 {
		t.Errorf("expected system_time_ms=0 for nil Duration, got %v", sys)
	}
	usr, ok := m.Get("user_time_ms")
	if !ok || usr.Int() != 0 {
		t.Errorf("expected user_time_ms=0 for nil Duration, got %v", usr)
	}
}

func TestApplyActionDataAttrs_EmptyListEmitsNothing(t *testing.T) {
	attrs := pcommon.NewMap()
	applyActionDataAttrs(attrs, nil, actionDataOptions{maxEntries: 50, logger: zap.NewNop()})
	if _, ok := attrs.Get("bazel.metrics.action_data"); ok {
		t.Error("expected no bazel.metrics.action_data attribute for empty list")
	}

	applyActionDataAttrs(attrs, []*bep.BuildMetrics_ActionSummary_ActionData{}, actionDataOptions{maxEntries: 50, logger: zap.NewNop()})
	if _, ok := attrs.Get("bazel.metrics.action_data"); ok {
		t.Error("expected no bazel.metrics.action_data attribute for zero-length slice")
	}
}

func TestApplyActionDataAttrs_TruncationWithWarning(t *testing.T) {
	entries := buildActionDataEntries("M1", "M2", "M3", "M4", "M5")

	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	attrs := pcommon.NewMap()
	applyActionDataAttrs(attrs, entries, actionDataOptions{maxEntries: 3, logger: logger, invocationID: "inv-trunc"})

	raw, ok := attrs.Get("bazel.metrics.action_data")
	if !ok {
		t.Fatal("expected bazel.metrics.action_data attribute")
	}
	if got := raw.Slice().Len(); got != 3 {
		t.Errorf("expected 3 entries after truncation, got %d", got)
	}
	// Keep the first N so queries match Bazel's top-N ranking.
	first := raw.Slice().At(0).Map()
	mnemonic, _ := first.Get("mnemonic")
	if mnemonic.Str() != "M1" {
		t.Errorf("expected first entry mnemonic=M1, got %s", mnemonic.Str())
	}

	if logs.Len() != 1 {
		t.Fatalf("expected 1 warning log, got %d", logs.Len())
	}
	entry := logs.All()[0]
	if entry.Level != zap.WarnLevel {
		t.Errorf("expected warn level, got %s", entry.Level)
	}
	ctx := entry.ContextMap()
	if ctx["invocation_id"] != "inv-trunc" {
		t.Errorf("expected invocation_id=inv-trunc in log context, got %v", ctx["invocation_id"])
	}
	if ctx["original_count"] != int64(5) {
		t.Errorf("expected original_count=5 in log context, got %v", ctx["original_count"])
	}
}

func TestApplyActionDataAttrs_NoTruncationNoWarning(t *testing.T) {
	entries := buildActionDataEntries("M1", "M2")

	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	attrs := pcommon.NewMap()
	applyActionDataAttrs(attrs, entries, actionDataOptions{maxEntries: 50, logger: logger, invocationID: "inv-quiet"})

	raw, _ := attrs.Get("bazel.metrics.action_data")
	if raw.Slice().Len() != 2 {
		t.Errorf("expected 2 entries (under cap), got %d", raw.Slice().Len())
	}
	if logs.Len() != 0 {
		t.Errorf("expected 0 warnings, got %d", logs.Len())
	}
}

func TestDurationToMs(t *testing.T) {
	cases := []struct {
		name string
		in   *durationpb.Duration
		want int64
	}{
		{"nil", nil, 0},
		{"zero", durationpb.New(0), 0},
		{"whole_ms", durationpb.New(250 * time.Millisecond), 250},
		{"seconds", durationpb.New(3 * time.Second), 3000},
		{"sub_ms_truncates", durationpb.New(999 * time.Microsecond), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := durationToMs(tc.in); got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestAddActionDataGauges_FourMetricsPerMnemonic(t *testing.T) {
	ts := pcommon.NewTimestampFromTime(time.Now())
	state := &invocationState{started: &bep.BuildStarted{Command: "build"}}
	entries := []*bep.BuildMetrics_ActionSummary_ActionData{
		{
			Mnemonic:        "Javac",
			ActionsExecuted: 42,
			ActionsCreated:  50,
			SystemTime:      durationpb.New(150 * time.Millisecond),
			UserTime:        durationpb.New(350 * time.Millisecond),
		},
		{
			Mnemonic:        "GoCompile",
			ActionsExecuted: 7,
			ActionsCreated:  9,
			// nil durations → 0
		},
	}
	metrics := &bep.BuildMetrics{
		ActionSummary: &bep.BuildMetrics_ActionSummary{ActionData: entries},
	}

	md := buildInvocationGauges("inv-gauges", state, metrics, ts, actionDataOptions{maxEntries: 50, logger: zap.NewNop(), invocationID: "inv-gauges"})

	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)

	// Expected: per mnemonic -> 4 gauges. Two mnemonics -> 8 ActionData
	// gauges. ActionSummary scalar gauges add 2 more (actions_created,
	// actions_executed) because ActionSummary is non-nil. Total 10.
	gaugesByName := make(map[string][]map[string]pcommon.Value)
	for i := range sm.Metrics().Len() {
		m := sm.Metrics().At(i)
		dps := m.Gauge().DataPoints()
		for j := range dps.Len() {
			dp := dps.At(j)
			attrs := make(map[string]pcommon.Value)
			dp.Attributes().Range(func(k string, v pcommon.Value) bool {
				attrs[k] = v
				return true
			})
			attrs["__value__"] = pcommon.NewValueInt(dp.IntValue())
			gaugesByName[m.Name()] = append(gaugesByName[m.Name()], attrs)
		}
	}

	// Verify the four per-mnemonic metric names are present, each with two
	// data points (one per mnemonic).
	wantNames := []string{
		"bazel.invocation.action.actions_executed",
		"bazel.invocation.action.actions_created",
		"bazel.invocation.action.system_time",
		"bazel.invocation.action.user_time",
	}
	for _, name := range wantNames {
		dps, ok := gaugesByName[name]
		if !ok {
			t.Errorf("missing gauge %q", name)
			continue
		}
		if len(dps) != 2 {
			t.Errorf("gauge %q: expected 2 data points (one per mnemonic), got %d", name, len(dps))
		}
		for _, dp := range dps {
			// Shared attributes from baseAttrs plus the mnemonic.
			invID, ok := dp["bazel.invocation_id"]
			if !ok || invID.Str() != "inv-gauges" {
				t.Errorf("gauge %q dp: expected bazel.invocation_id=inv-gauges, got %v", name, invID)
			}
			cmd, ok := dp["bazel.command"]
			if !ok || cmd.Str() != "build" {
				t.Errorf("gauge %q dp: expected bazel.command=build, got %v", name, cmd)
			}
			if _, ok := dp["bazel.action.mnemonic"]; !ok {
				t.Errorf("gauge %q dp: missing bazel.action.mnemonic", name)
			}
		}
	}

	// Verify values for a specific mnemonic (Javac).
	assertJavac := func(name string, want int64) {
		t.Helper()
		for _, dp := range gaugesByName[name] {
			mnemonic := dp["bazel.action.mnemonic"]
			if mnemonic.Str() == "Javac" {
				if dp["__value__"].Int() != want {
					t.Errorf("%s Javac: got %d, want %d", name, dp["__value__"].Int(), want)
				}
				return
			}
		}
		t.Errorf("%s: no Javac data point", name)
	}
	assertJavac("bazel.invocation.action.actions_executed", 42)
	assertJavac("bazel.invocation.action.actions_created", 50)
	assertJavac("bazel.invocation.action.system_time", 150)
	assertJavac("bazel.invocation.action.user_time", 350)

	// Verify nil durations in GoCompile surface as 0.
	assertGoCompile := func(name string, want int64) {
		t.Helper()
		for _, dp := range gaugesByName[name] {
			mnemonic := dp["bazel.action.mnemonic"]
			if mnemonic.Str() == "GoCompile" {
				if dp["__value__"].Int() != want {
					t.Errorf("%s GoCompile: got %d, want %d", name, dp["__value__"].Int(), want)
				}
				return
			}
		}
		t.Errorf("%s: no GoCompile data point", name)
	}
	assertGoCompile("bazel.invocation.action.system_time", 0)
	assertGoCompile("bazel.invocation.action.user_time", 0)
}

func TestAddActionDataGauges_EmptyListEmitsNothing(t *testing.T) {
	ts := pcommon.NewTimestampFromTime(time.Now())
	state := &invocationState{started: &bep.BuildStarted{Command: "build"}}
	metrics := &bep.BuildMetrics{
		// ActionSummary present but no ActionData.
		ActionSummary: &bep.BuildMetrics_ActionSummary{
			ActionsCreated:  10,
			ActionsExecuted: 5,
		},
	}

	md := buildInvocationGauges("inv-noad", state, metrics, ts, actionDataOptions{maxEntries: 50, logger: zap.NewNop()})

	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	for i := range sm.Metrics().Len() {
		name := sm.Metrics().At(i).Name()
		switch name {
		case "bazel.invocation.action.actions_executed",
			"bazel.invocation.action.actions_created",
			"bazel.invocation.action.system_time",
			"bazel.invocation.action.user_time":
			t.Errorf("did not expect per-mnemonic gauge %q for empty ActionData", name)
		}
	}
}

func TestAddActionDataGauges_Truncation(t *testing.T) {
	ts := pcommon.NewTimestampFromTime(time.Now())
	state := &invocationState{started: &bep.BuildStarted{Command: "build"}}
	entries := buildActionDataEntries("A", "B", "C", "D", "E")
	metrics := &bep.BuildMetrics{
		ActionSummary: &bep.BuildMetrics_ActionSummary{ActionData: entries},
	}

	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	md := buildInvocationGauges("inv-trunc-gauge", state, metrics, ts, actionDataOptions{maxEntries: 2, logger: logger, invocationID: "inv-trunc-gauge"})

	// Expect 2 mnemonics kept * 4 metric names = 8 data points, spread
	// across 4 named metrics. Count unique mnemonics.
	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	mnemonics := make(map[string]struct{})
	for i := range sm.Metrics().Len() {
		m := sm.Metrics().At(i)
		if m.Name() != "bazel.invocation.action.actions_executed" {
			continue
		}
		dps := m.Gauge().DataPoints()
		for j := range dps.Len() {
			mn, _ := dps.At(j).Attributes().Get("bazel.action.mnemonic")
			mnemonics[mn.Str()] = struct{}{}
		}
	}
	if len(mnemonics) != 2 {
		t.Errorf("expected 2 mnemonics after cap=2, got %d (%v)", len(mnemonics), mnemonics)
	}

	// Warning was logged with original count.
	if logs.Len() != 1 {
		t.Fatalf("expected 1 warning, got %d", logs.Len())
	}
	ctx := logs.All()[0].ContextMap()
	if ctx["invocation_id"] != "inv-trunc-gauge" {
		t.Errorf("expected invocation_id=inv-trunc-gauge, got %v", ctx["invocation_id"])
	}
	if ctx["original_count"] != int64(5) {
		t.Errorf("expected original_count=5, got %v", ctx["original_count"])
	}
}

func TestBuildMetricsSpan_ActionData(t *testing.T) {
	s := &invocationState{
		traceID:    pcommon.TraceID{1, 2, 3},
		rootSpanID: pcommon.SpanID{1, 2, 3},
		uuid:       "uuid-x",
	}
	entries := buildActionDataEntries("Javac", "GoCompile")
	metrics := &bep.BuildMetrics{
		ActionSummary: &bep.BuildMetrics_ActionSummary{
			ActionsCreated:  100,
			ActionsExecuted: 80,
			ActionData:      entries,
		},
	}

	traces, _ := s.buildMetricsSpan(metrics, actionDataOptions{maxEntries: 50, logger: zap.NewNop()})

	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	raw, ok := span.Attributes().Get("bazel.metrics.action_data")
	if !ok {
		t.Fatal("expected bazel.metrics.action_data on bazel.metrics span")
	}
	if raw.Slice().Len() != 2 {
		t.Errorf("expected 2 entries, got %d", raw.Slice().Len())
	}

	// Scalar ActionSummary fields still emitted.
	ac, ok := span.Attributes().Get("bazel.metrics.actions_created")
	if !ok || ac.Int() != 100 {
		t.Errorf("expected bazel.metrics.actions_created=100, got %v", ac)
	}
}

func TestBuildMetricsSpan_NoActionData(t *testing.T) {
	s := &invocationState{
		traceID:    pcommon.TraceID{1, 2, 3},
		rootSpanID: pcommon.SpanID{1, 2, 3},
		uuid:       "uuid-x",
	}
	metrics := &bep.BuildMetrics{
		ActionSummary: &bep.BuildMetrics_ActionSummary{
			ActionsCreated:  1,
			ActionsExecuted: 1,
		},
	}

	traces, _ := s.buildMetricsSpan(metrics, actionDataOptions{maxEntries: 50, logger: zap.NewNop()})

	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	if _, ok := span.Attributes().Get("bazel.metrics.action_data"); ok {
		t.Error("did not expect bazel.metrics.action_data with empty ActionData list")
	}
}
