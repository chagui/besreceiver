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

func TestPatternExpanded_MultiplePatterns_EmitsEntryPerPattern(t *testing.T) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	processEvents(ctx, t, tb,
		makeBuildStartedOBE(t, "inv-pat-multi", "uuid-pat-multi", "build", 1),
		// One event carrying two patterns — each gets target_count=2.
		makePatternExpandedOBE(t, "inv-pat-multi", 2, []string{"//a/...", "//b/..."}, 2),
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

func TestPatternExpanded_DefaultCapIs50(t *testing.T) {
	caps := defaultHighCardinalityCaps()
	assert.Equal(t, 50, caps.Patterns)
}

func TestPatternExpanded_ConfigDefault_IncludesPatternsCap(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	assert.Equal(t, 50, cfg.HighCardinalityCaps.Patterns)
}
