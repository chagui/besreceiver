package besreceiver

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// spanInfo is a lightweight view of a span for assertion purposes.
type spanInfo struct {
	Name         string
	SpanID       pcommon.SpanID
	ParentSpanID pcommon.SpanID
	TraceID      pcommon.TraceID
	Attributes   pcommon.Map
	Status       ptrace.Status
	Kind         ptrace.SpanKind
}

// collectSpans flattens all spans from a TracesSink into a slice of spanInfo.
func collectSpans(t testing.TB, sink *consumertest.TracesSink) []spanInfo {
	t.Helper()
	var spans []spanInfo
	for _, td := range sink.AllTraces() {
		for i := range td.ResourceSpans().Len() {
			rs := td.ResourceSpans().At(i)
			for j := range rs.ScopeSpans().Len() {
				ss := rs.ScopeSpans().At(j)
				for k := range ss.Spans().Len() {
					s := ss.Spans().At(k)
					spans = append(spans, spanInfo{
						Name:         s.Name(),
						SpanID:       s.SpanID(),
						ParentSpanID: s.ParentSpanID(),
						TraceID:      s.TraceID(),
						Attributes:   s.Attributes(),
						Status:       s.Status(),
						Kind:         s.Kind(),
					})
				}
			}
		}
	}
	return spans
}

// assertAllSpansShareTraceID checks all spans belong to the same trace.
func assertAllSpansShareTraceID(t testing.TB, sink *consumertest.TracesSink) {
	t.Helper()
	spans := collectSpans(t, sink)
	require.NotEmpty(t, spans, "no spans found")

	traceID := spans[0].TraceID
	assert.False(t, traceID.IsEmpty(), "trace ID should not be zero")
	for _, s := range spans[1:] {
		assert.Equal(t, traceID, s.TraceID,
			"span %q has different trace ID", s.Name)
	}
}

// assertParentedCorrectly verifies the span tree structural invariants:
//   - Exactly one root span (zero ParentSpanID) named "bazel.build *"
//   - All bazel.target spans are children of the root
//   - All bazel.action spans are children of a bazel.target or the root
//   - All bazel.test spans are children of a bazel.target or the root
//   - The bazel.metrics span is a child of the root
//   - Every non-root span's ParentSpanID references an existing span
func assertParentedCorrectly(t testing.TB, spans []spanInfo) {
	t.Helper()
	require.NotEmpty(t, spans, "no spans to validate")

	// Build a set of known span IDs and find the root.
	knownIDs := make(map[pcommon.SpanID]spanInfo, len(spans))
	var roots []spanInfo
	var zeroID pcommon.SpanID
	for _, s := range spans {
		knownIDs[s.SpanID] = s
		if s.ParentSpanID == zeroID {
			roots = append(roots, s)
		}
	}

	// Exactly one root span.
	require.Len(t, roots, 1, "expected exactly 1 root span, got %d: %v",
		len(roots), spanNames(roots))
	root := roots[0]
	assert.True(t, strings.HasPrefix(root.Name, "bazel.build"),
		"root span name should start with 'bazel.build', got %q", root.Name)

	// Every non-root span's parent must exist in the span set.
	for _, s := range spans {
		if s.ParentSpanID == zeroID {
			continue
		}
		_, ok := knownIDs[s.ParentSpanID]
		assert.True(t, ok,
			"span %q (ID=%x) references unknown parent %x",
			s.Name, s.SpanID, s.ParentSpanID)
	}

	// Validate parent type constraints.
	for _, s := range spans {
		if s.ParentSpanID == zeroID {
			continue
		}
		parent := knownIDs[s.ParentSpanID]
		switch {
		case s.Name == "bazel.target":
			assert.Equal(t, root.SpanID, s.ParentSpanID,
				"bazel.target span should be child of root, got parent %q", parent.Name)
		case strings.HasPrefix(s.Name, "bazel.action"):
			assert.True(t,
				parent.Name == "bazel.target" || parent.SpanID == root.SpanID,
				"bazel.action span %q should be child of bazel.target or root, got parent %q",
				s.Name, parent.Name)
		case s.Name == "bazel.test":
			assert.True(t,
				parent.Name == "bazel.target" || parent.SpanID == root.SpanID,
				"bazel.test span should be child of bazel.target or root, got parent %q",
				parent.Name)
		case s.Name == "bazel.metrics":
			assert.Equal(t, root.SpanID, s.ParentSpanID,
				"bazel.metrics span should be child of root, got parent %q", parent.Name)
		}
	}
}

// assertSpanExists checks at least one span with the given name exists.
func assertSpanExists(t testing.TB, spans []spanInfo, name string) {
	t.Helper()
	for _, s := range spans {
		if s.Name == name {
			return
		}
	}
	t.Errorf("expected span named %q, found: %v", name, spanNames(spans))
}

// assertSpanAttribute checks that a span with the given name has the expected string attribute.
// If multiple spans share the name, at least one must match.
func assertSpanAttribute(t testing.TB, spans []spanInfo, spanName, attrKey, expected string) {
	t.Helper()
	for _, s := range spans {
		if s.Name != spanName {
			continue
		}
		v, ok := s.Attributes.Get(attrKey)
		if ok && v.Str() == expected {
			return
		}
	}
	t.Errorf("no span %q with %s=%q", spanName, attrKey, expected)
}

// spanNames returns the names of spans for diagnostic messages.
func spanNames(spans []spanInfo) []string {
	names := make([]string, len(spans))
	for i, s := range spans {
		names[i] = s.Name
	}
	return names
}
