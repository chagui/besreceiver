package besreceiver

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.uber.org/zap"
	pb "google.golang.org/genproto/googleapis/devtools/build/v1"

	bep "github.com/chagui/besreceiver/internal/bep/buildeventstream"
)

// collectSpanKinds returns a map from span "kind" to count across all
// trace batches in the sink. "Kind" collapses name variants like
// "bazel.action Javac" to "bazel.action" and "bazel.build build" to
// "bazel.build" so tests can focus on which span types were emitted.
func collectSpanKinds(sink *consumertest.TracesSink) map[string]int {
	counts := make(map[string]int)
	for _, td := range sink.AllTraces() {
		rs := td.ResourceSpans()
		for i := range rs.Len() {
			sss := rs.At(i).ScopeSpans()
			for j := range sss.Len() {
				spans := sss.At(j).Spans()
				for k := range spans.Len() {
					counts[spanKind(spans.At(k).Name())]++
				}
			}
		}
	}
	return counts
}

// spanKind strips the trailing descriptor from a span name so tests can
// compare by kind. "bazel.action Javac" → "bazel.action". Unknown names
// pass through unchanged.
func spanKind(name string) string {
	if i := strings.IndexByte(name, ' '); i > 0 {
		return name[:i]
	}
	return name
}

// driveFilteredInvocation feeds a minimal but representative BEP stream
// through a TraceBuilder configured with the given FilterConfig. The
// stream covers every per-target span type (target, action, test) plus
// the always-on root and metrics spans, so a single invocation exercises
// every filter decision point.
func driveFilteredInvocation(t *testing.T, filter FilterConfig, targets ...string) *consumertest.TracesSink {
	t.Helper()
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, nil, zap.NewNop(), TraceBuilderConfig{Filter: filter})
	tb.Start()
	t.Cleanup(tb.Stop)
	ctx := context.Background()

	const invID = "inv-filter"
	var seq int64 = 1
	send := func(obe *pb.OrderedBuildEvent) {
		t.Helper()
		if err := tb.ProcessOrderedBuildEvent(ctx, obe); err != nil {
			t.Fatalf("process event seq=%d: %v", obe.GetSequenceNumber(), err)
		}
	}

	send(makeBuildStartedOBE(t, invID, "uuid-filter", "build", seq))
	seq++
	for _, label := range targets {
		send(makeTargetConfiguredOBE(t, invID, label, seq))
		seq++
		send(makeActionOBE(t, invID, label, "Javac", seq, true))
		seq++
		send(makeTestResultOBE(t, invID, label, seq, bep.TestStatus_PASSED))
		seq++
	}
	send(makeBuildFinishedOBE(t, invID, seq, 0, "SUCCESS"))
	seq++
	send(makeBuildMetricsOBE(t, invID, seq, 1000, 500))

	return sink
}

func TestFilter_NoBlock_PreservesVerboseBehavior(t *testing.T) {
	// FilterConfig zero value (== omitted YAML block) must not drop any span.
	sink := driveFilteredInvocation(t, FilterConfig{}, "//pkg:lib")

	got := collectSpanKinds(sink)
	for _, want := range []string{"bazel.build", "bazel.target", "bazel.action", "bazel.test", "bazel.metrics"} {
		if got[want] == 0 {
			t.Errorf("expected at least one %s span, got counts=%v", want, got)
		}
	}
}

func TestFilter_DefaultLevelDrop_EmitsOnlyRootAndMetrics(t *testing.T) {
	sink := driveFilteredInvocation(t, FilterConfig{DefaultLevel: DetailLevelDrop}, "//pkg:lib")
	got := collectSpanKinds(sink)

	if got["bazel.build"] != 1 {
		t.Errorf("expected 1 bazel.build, got %d (all=%v)", got["bazel.build"], got)
	}
	if got["bazel.metrics"] != 1 {
		t.Errorf("expected 1 bazel.metrics, got %d (all=%v)", got["bazel.metrics"], got)
	}
	for _, unwanted := range []string{"bazel.target", "bazel.action", "bazel.test"} {
		if got[unwanted] != 0 {
			t.Errorf("%s should be dropped at drop level, got %d", unwanted, got[unwanted])
		}
	}
}

func TestFilter_DefaultLevelBuildOnly_MatchesDrop(t *testing.T) {
	// build_only is a UX-level rename of drop for the whole invocation; the
	// per-target spans it suppresses are identical.
	sink := driveFilteredInvocation(t, FilterConfig{DefaultLevel: DetailLevelBuildOnly}, "//pkg:lib")
	got := collectSpanKinds(sink)

	if got["bazel.build"] != 1 || got["bazel.metrics"] != 1 {
		t.Errorf("build_only: expected root + metrics only, got %v", got)
	}
	if got["bazel.target"] != 0 || got["bazel.action"] != 0 || got["bazel.test"] != 0 {
		t.Errorf("build_only: expected zero per-target spans, got %v", got)
	}
}

func TestFilter_DefaultLevelTargets_SuppressesActionsAndTests(t *testing.T) {
	sink := driveFilteredInvocation(t, FilterConfig{DefaultLevel: DetailLevelTargets}, "//pkg:lib")
	got := collectSpanKinds(sink)

	if got["bazel.target"] != 1 {
		t.Errorf("expected 1 bazel.target, got %d (all=%v)", got["bazel.target"], got)
	}
	if got["bazel.action"] != 0 {
		t.Errorf("expected 0 bazel.action at targets level, got %d", got["bazel.action"])
	}
	if got["bazel.test"] != 0 {
		t.Errorf("expected 0 bazel.test at targets level, got %d", got["bazel.test"])
	}
	if got["bazel.build"] != 1 || got["bazel.metrics"] != 1 {
		t.Errorf("targets level must still emit root + metrics, got %v", got)
	}
}

func TestFilter_DefaultLevelVerbose_EmitsEverySpan(t *testing.T) {
	sink := driveFilteredInvocation(t, FilterConfig{DefaultLevel: DetailLevelVerbose}, "//pkg:lib")
	got := collectSpanKinds(sink)
	for _, want := range []string{"bazel.build", "bazel.target", "bazel.action", "bazel.test", "bazel.metrics"} {
		if got[want] == 0 {
			t.Errorf("verbose: expected %s span, got counts=%v", want, got)
		}
	}
}

func TestFilter_PerTargetRules_ApplyIndependently(t *testing.T) {
	// Three targets, one per detail level. Verify each gets exactly the
	// span set its rule permits.
	sink := driveFilteredInvocation(t, FilterConfig{
		DefaultLevel: DetailLevelVerbose,
		Rules: []FilterRule{
			{Pattern: "//drop:target", DetailLevel: DetailLevelDrop},
			{Pattern: "//targets:target", DetailLevel: DetailLevelTargets},
			// //verbose:target falls through to default=verbose.
		},
	}, "//drop:target", "//targets:target", "//verbose:target")

	// Gather attribute-tagged target labels to assert which targets survived.
	seen := make(map[string]map[string]bool) // kind -> set of labels
	for _, td := range sink.AllTraces() {
		rs := td.ResourceSpans()
		for i := range rs.Len() {
			sss := rs.At(i).ScopeSpans()
			for j := range sss.Len() {
				spans := sss.At(j).Spans()
				for k := range spans.Len() {
					sp := spans.At(k)
					kind := spanKind(sp.Name())
					lbl, ok := sp.Attributes().Get("bazel.target.label")
					if !ok {
						continue
					}
					if seen[kind] == nil {
						seen[kind] = make(map[string]bool)
					}
					seen[kind][lbl.Str()] = true
				}
			}
		}
	}

	// //drop:target — no spans should reference it.
	for kind, labels := range seen {
		if labels["//drop:target"] {
			t.Errorf("dropped target should produce no %s span, but saw one", kind)
		}
	}
	// //targets:target — target span yes, action/test no.
	if !seen["bazel.target"]["//targets:target"] {
		t.Error("//targets:target should emit its bazel.target span")
	}
	if seen["bazel.action"]["//targets:target"] {
		t.Error("//targets:target should NOT emit action spans at targets level")
	}
	if seen["bazel.test"]["//targets:target"] {
		t.Error("//targets:target should NOT emit test spans at targets level")
	}
	// //verbose:target — everything.
	for _, kind := range []string{"bazel.target", "bazel.action", "bazel.test"} {
		if !seen[kind]["//verbose:target"] {
			t.Errorf("//verbose:target should emit %s span, got seen=%v", kind, seen[kind])
		}
	}
}

func TestFilter_SubtreePatternInStream(t *testing.T) {
	// //third_party/... → drop; default verbose. The //third_party target is
	// silenced while //src gets every span.
	sink := driveFilteredInvocation(t, FilterConfig{
		DefaultLevel: DetailLevelVerbose,
		Rules: []FilterRule{
			{Pattern: "//third_party/...", DetailLevel: DetailLevelDrop},
		},
	}, "//third_party/dep:lib", "//src/main:app")

	got := collectSpanKinds(sink)
	// 1 target for //src/main:app only.
	if got["bazel.target"] != 1 {
		t.Errorf("expected exactly 1 bazel.target (only //src/main:app), got %d", got["bazel.target"])
	}
	if got["bazel.action"] != 1 {
		t.Errorf("expected exactly 1 bazel.action (only //src/main:app), got %d", got["bazel.action"])
	}
}

func TestFilter_InvalidDetailLevel_FailsConfigValidate(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Filter.DefaultLevel = "loud"
	if err := cfg.Validate(); err == nil {
		t.Fatal("Config.Validate should reject invalid filter.default_level")
	}
}
