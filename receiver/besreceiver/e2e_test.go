package besreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_BuildAndTest replays a real BES stream captured from:
//
//	cd examples/bazel-project && bazel test //... --bes_backend=grpc://localhost:8082
//
// The fixture contains 36 events from a single invocation including BuildStarted,
// TargetConfigured (x2: //:greeting, //:pass_test), ActionExecuted (BazelWorkspaceStatusAction),
// TestResult (PASSED), BuildFinished, and BuildMetrics, plus many event types the
// receiver silently ignores (Progress, Configuration, NamedSetOfFiles, etc.).
func TestE2E_BuildAndTest(t *testing.T) {
	requests := loadBESStream(t, "testdata/besstream/build_and_test.besstream")

	t.Run("mock_stream", func(t *testing.T) {
		traces, metrics, logs := replayViaMockStream(t, requests)

		// --- Structural invariants (fixture-agnostic) ---
		assertAllSpansShareTraceID(t, traces)
		spans := collectSpans(t, traces)
		assertParentedCorrectly(t, spans)

		// At minimum: root + metrics. Targets/actions/tests depend on the build.
		require.GreaterOrEqual(t, len(spans), 2,
			"expected at least root + metrics spans, got: %v", spanNames(spans))

		// Root span always exists and carries the command.
		assertSpanExists(t, spans, "bazel.build test")
		assertSpanAttribute(t, spans, "bazel.build test", "bazel.command", "test")

		// Metrics span and OTel metrics are always emitted.
		assertSpanExists(t, spans, "bazel.metrics")
		allMetrics := metrics.AllMetrics()
		require.NotEmpty(t, allMetrics, "expected metrics from BuildMetrics event")
		assert.True(t, hasMetricNamed(allMetrics[0], "bazel.invocation.wall_time"),
			"expected bazel.invocation.wall_time gauge")
		assert.True(t, hasMetricNamed(allMetrics[0], "bazel.invocation.count"),
			"expected bazel.invocation.count counter")

		// Logs are always emitted (at least BuildStarted and BuildFinished).
		assert.Greater(t, logs.LogRecordCount(), 0, "expected at least one log record")

		// --- Fixture-specific assertions ---
		// These depend on the exact captured build and will break if the fixture
		// is re-captured with different cache state or Bazel version.
		t.Run("fixture_contents", func(t *testing.T) {
			require.Len(t, spans, 6, "span names: %v", spanNames(spans))
			assertSpanAttribute(t, spans, "bazel.target", "bazel.target.label", "//:greeting")
			assertSpanAttribute(t, spans, "bazel.target", "bazel.target.label", "//:pass_test")
			assertSpanExists(t, spans, "bazel.action BazelWorkspaceStatusAction")
			assertSpanExists(t, spans, "bazel.test")
			assertSpanAttribute(t, spans, "bazel.test", "bazel.test.status", "PASSED")
		})
	})

	t.Run("grpc", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping gRPC e2e test in short mode")
		}
		traces, metrics, _ := replayViaGRPC(t, requests)

		// Structural invariants only — no fixture-specific checks.
		assertAllSpansShareTraceID(t, traces)
		spans := collectSpans(t, traces)
		assertParentedCorrectly(t, spans)

		require.GreaterOrEqual(t, len(spans), 2,
			"expected at least root + metrics spans, got: %v", spanNames(spans))
		assertSpanExists(t, spans, "bazel.build test")
		assertSpanExists(t, spans, "bazel.metrics")

		allMetrics := metrics.AllMetrics()
		require.NotEmpty(t, allMetrics, "expected metrics from BuildMetrics event")
	})
}
