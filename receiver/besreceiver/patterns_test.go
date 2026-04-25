package besreceiver

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	buildpb "google.golang.org/genproto/googleapis/devtools/build/v1"
)

// patternEntries extracts the bazel.patterns Slice[Map] attribute from a
// span into a stable map keyed by pattern. Fails the test if the attribute
// is present but shaped wrong.
func patternEntries(t *testing.T, span ptrace.Span) (map[string]int64, bool) {
	t.Helper()
	attr, ok := span.Attributes().Get("bazel.patterns")
	if !ok {
		return nil, false
	}
	require.Equal(t, pcommon.ValueTypeSlice, attr.Type(), "bazel.patterns must be a slice")
	out := make(map[string]int64, attr.Slice().Len())
	for i := range attr.Slice().Len() {
		e := attr.Slice().At(i)
		require.Equal(t, pcommon.ValueTypeMap, e.Type(), "entry %d must be a map", i)
		m := e.Map()
		pv, pok := m.Get("pattern")
		require.True(t, pok, "entry %d missing pattern", i)
		require.Equal(t, pcommon.ValueTypeStr, pv.Type(), "entry %d pattern must be string", i)
		tv, tok := m.Get("target_count")
		require.True(t, tok, "entry %d missing target_count", i)
		require.Equal(t, pcommon.ValueTypeInt, tv.Type(), "entry %d target_count must be int", i)
		out[pv.Str()] = tv.Int()
	}
	return out, true
}

func TestPatternExpanded_SingleEvent_ProducesOneEntry(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-pat-1", "uuid-pat-1", "test", 1),
		makePatternExpandedOBE(t, "inv-pat-1", 2, []string{"//foo/..."}, 3),
		makeBuildFinishedOBE(t, "inv-pat-1", 3, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)
	entries, ok := patternEntries(t, span)
	require.True(t, ok, "expected bazel.patterns attribute")
	require.Len(t, entries, 1)
	assert.Equal(t, int64(3), entries["//foo/..."])
}

func TestPatternExpanded_MultiplePatterns_EvenSplitsTargetCount(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-pat-multi", "uuid-pat-multi", "build", 1),
		// One event with two patterns and 4 TargetConfigured children. The
		// proto carries no per-pattern attribution, so the receiver divides
		// evenly: floor(4/2)=2 per pattern. Per-event sum equals the child
		// count (no N×M inflation across the slice).
		makePatternExpandedOBE(t, "inv-pat-multi", 2, []string{"//a/...", "//b/..."}, 4),
		// Single-pattern events keep the exact count.
		makePatternExpandedOBE(t, "inv-pat-multi", 3, []string{"//c:tgt"}, 1),
		makeBuildFinishedOBE(t, "inv-pat-multi", 4, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)
	entries, ok := patternEntries(t, span)
	require.True(t, ok)
	assert.Equal(t, map[string]int64{
		"//a/...": 2,
		"//b/...": 2,
		"//c:tgt": 1,
	}, entries)
}

// TestPatternExpanded_NonTargetConfiguredChildren_NotCounted verifies that
// only TargetConfigured ids contribute to target_count. PatternExpanded
// children of nested patterns and unconfigured-label children (failure
// markers) must not inflate the count of resolved targets.
func TestPatternExpanded_NonTargetConfiguredChildren_NotCounted(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-pat-mixed", "uuid-pat-mixed", "build", 1),
		// 3 TargetConfigured + 2 nested-pattern + 4 unconfigured-label children.
		// Only the 3 TargetConfigured ids count toward target_count.
		makePatternExpandedMixedChildrenOBE(t, "inv-pat-mixed", 2, []string{"//top/..."}, 3, 2, 4),
		makeBuildFinishedOBE(t, "inv-pat-mixed", 3, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)
	entries, ok := patternEntries(t, span)
	require.True(t, ok)
	assert.Equal(t, map[string]int64{"//top/...": 3}, entries)
}

// TestPatternExpanded_PatternSkipped_EmitsSkippedAttribute verifies that
// pattern_skipped events route to bazel.patterns_skipped (not bazel.patterns)
// — Bazel emits these under --keep_going for patterns whose expansion was
// skipped. They have no target_count by definition.
func TestPatternExpanded_PatternSkipped_EmitsSkippedAttribute(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-pat-skipped", "uuid-pat-skipped", "test", 1),
		makePatternExpandedOBE(t, "inv-pat-skipped", 2, []string{"//ok/..."}, 2),
		makePatternSkippedOBE(t, "inv-pat-skipped", 3, []string{"//missing/..."}),
		makePatternSkippedOBE(t, "inv-pat-skipped", 4, []string{"//also/missing"}),
		makeBuildFinishedOBE(t, "inv-pat-skipped", 5, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)
	entries, ok := patternEntries(t, span)
	require.True(t, ok, "expected bazel.patterns for the successful expansion")
	assert.Equal(t, map[string]int64{"//ok/...": 2}, entries)

	skipped, sok := span.Attributes().Get("bazel.patterns_skipped")
	require.True(t, sok, "expected bazel.patterns_skipped attribute")
	require.Equal(t, pcommon.ValueTypeSlice, skipped.Type())
	got := make([]string, 0, skipped.Slice().Len())
	for i := range skipped.Slice().Len() {
		got = append(got, skipped.Slice().At(i).Str())
	}
	assert.ElementsMatch(t, []string{"//missing/...", "//also/missing"}, got)
}

// TestPatternExpanded_PatternSkippedOnly_NoPatternsAttribute verifies that
// when only pattern_skipped events arrive (no successful expansions), the
// bazel.patterns attribute is absent — skipped patterns alone do not
// promise any targets ran.
func TestPatternExpanded_PatternSkippedOnly_NoPatternsAttribute(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-pat-skip-only", "uuid-pat-skip-only", "build", 1),
		makePatternSkippedOBE(t, "inv-pat-skip-only", 2, []string{"//gone/..."}),
		makeBuildFinishedOBE(t, "inv-pat-skip-only", 3, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)
	_, ok := span.Attributes().Get("bazel.patterns")
	assert.False(t, ok, "bazel.patterns must be absent when no successful expansions arrived")
	skipped, sok := span.Attributes().Get("bazel.patterns_skipped")
	require.True(t, sok)
	require.Equal(t, pcommon.ValueTypeSlice, skipped.Type())
	require.Equal(t, 1, skipped.Slice().Len())
	assert.Equal(t, "//gone/...", skipped.Slice().At(0).Str())
}

func TestPatternExpanded_RepeatedPattern_AggregatesTargetCount(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-pat-agg", "uuid-pat-agg", "test", 1),
		makePatternExpandedOBE(t, "inv-pat-agg", 2, []string{"//same/..."}, 4),
		makePatternExpandedOBE(t, "inv-pat-agg", 3, []string{"//same/..."}, 7),
		makeBuildFinishedOBE(t, "inv-pat-agg", 4, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)
	entries, ok := patternEntries(t, span)
	require.True(t, ok)
	require.Len(t, entries, 1)
	assert.Equal(t, int64(11), entries["//same/..."])
}

func TestPatternExpanded_EmptyPatternAndZeroTargets_Skipped(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-pat-skip", "uuid-pat-skip", "build", 1),
		// Empty pattern string — skipped.
		makePatternExpandedOBE(t, "inv-pat-skip", 2, []string{""}, 3),
		// Zero-target expansion — whole event skipped.
		makePatternExpandedOBE(t, "inv-pat-skip", 3, []string{"//never"}, 0),
		// Real pattern to verify the attr is still emitted.
		makePatternExpandedOBE(t, "inv-pat-skip", 4, []string{"//real"}, 2),
		makeBuildFinishedOBE(t, "inv-pat-skip", 5, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)
	entries, ok := patternEntries(t, span)
	require.True(t, ok)
	assert.Equal(t, map[string]int64{"//real": 2}, entries)
}

func TestPatternExpanded_NoEvents_NoAttribute(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-pat-none", "uuid-pat-none", "build", 1),
		makeBuildFinishedOBE(t, "inv-pat-none", 2, 0, "SUCCESS"),
	)

	span := findRootSpan(t, sink)
	_, ok := span.Attributes().Get("bazel.patterns")
	assert.False(t, ok, "bazel.patterns must be absent when no PatternExpanded events arrived")
}

func TestPatternExpanded_OverCap_TruncatedWithWarning(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, logger, TraceBuilderConfig{
		Caps: HighCardinalityCaps{Patterns: 3},
	})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	seq := int64(1)
	events := []*buildpb.OrderedBuildEvent{
		makeBuildStartedOBE(t, "inv-pat-trunc", "uuid-pat-trunc", "build", seq),
	}
	// Emit 5 distinct patterns; cap is 3. Keys sort lexicographically so
	// the surviving set is deterministic: "//p00", "//p01", "//p02".
	for i := range 5 {
		seq++
		events = append(events, makePatternExpandedOBE(t, "inv-pat-trunc", seq, []string{fmt.Sprintf("//p%02d", i)}, 1))
	}
	seq++
	events = append(events, makeBuildFinishedOBE(t, "inv-pat-trunc", seq, 0, "SUCCESS"))

	processEvents(ctx, t, tb, events...)

	span := findRootSpan(t, sink)
	entries, ok := patternEntries(t, span)
	require.True(t, ok)
	assert.Len(t, entries, 3, "cap should have truncated to 3 entries")
	assert.Contains(t, entries, "//p00")
	assert.Contains(t, entries, "//p01")
	assert.Contains(t, entries, "//p02")

	warnings := logs.FilterMessage("Truncated high-cardinality root-span attribute").All()
	require.NotEmpty(t, warnings, "expected a truncation warning log")
	fields := warnings[0].ContextMap()
	assert.Equal(t, "inv-pat-trunc", fields["invocation_id"])
	assert.Equal(t, "bazel.patterns", fields["attribute"])
	assert.Equal(t, int64(5), fields["original_count"])
	assert.Equal(t, int64(3), fields["kept"])
}

// TestPatternExpanded_PostFlush_Dropped verifies that a PatternExpanded
// event arriving after the root span has been flushed (between
// BuildFinished and BuildMetrics, when state.flushed is set) does not
// retroactively mutate bazel.patterns. The receiver follows the
// workspace_status / build_metadata buffering policy: pre-finalize-only.
func TestPatternExpanded_PostFlush_Dropped(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-pat-postflush", "uuid-pat-postflush", "build", 1),
		makePatternExpandedOBE(t, "inv-pat-postflush", 2, []string{"//pre"}, 2),
		makeBuildFinishedOBE(t, "inv-pat-postflush", 3, 0, "SUCCESS"),
		// Arrives after finalize; must not appear in bazel.patterns.
		makePatternExpandedOBE(t, "inv-pat-postflush", 4, []string{"//post"}, 5),
		makePatternSkippedOBE(t, "inv-pat-postflush", 5, []string{"//also-post"}),
	)

	span := findRootSpan(t, sink)
	entries, ok := patternEntries(t, span)
	require.True(t, ok)
	assert.Equal(t, map[string]int64{"//pre": 2}, entries)
	_, sok := span.Attributes().Get("bazel.patterns_skipped")
	assert.False(t, sok, "post-flush pattern_skipped must not retro-stamp the root span")
}

func TestPatternExpanded_DefaultCapIs50(t *testing.T) {
	caps := defaultHighCardinalityCaps()
	assert.Equal(t, 50, caps.Patterns)
}

func TestPatternExpanded_ConfigDefault_IncludesPatternsCap(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	assert.Equal(t, 50, cfg.HighCardinalityCaps.Patterns)
}
