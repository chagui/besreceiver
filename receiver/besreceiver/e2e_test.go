package besreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_BuildAndTest replays a real BES stream captured from the TaskForge
// multi-language example project:
//
//	cd examples/bazel-project && bazel test //... --bes_backend=grpc://localhost:8082
//
// The fixture contains events from a single invocation across Go, Java, C++,
// Python, and proto targets, plus shell tests (including sharded).
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
		// Current fixture: TaskForge monorepo (Go, Java, C++, Python, proto, shell).
		t.Run("fixture_contents", func(t *testing.T) {
			// The TaskForge monorepo produces targets across Go, Java, C++,
			// Python, proto, OCI, and shell — verify key representatives exist.
			assertSpanAttribute(t, spans, "bazel.target", "bazel.target.label", "//services/gateway:gateway")
			assertSpanAttribute(t, spans, "bazel.target", "bazel.target.label", "//services/engine:engine")
			assertSpanAttribute(t, spans, "bazel.target", "bazel.target.label", "//services/processor:processor")
			assertSpanAttribute(t, spans, "bazel.target", "bazel.target.label", "//services/reporter:reporter")
			assertSpanAttribute(t, spans, "bazel.target", "bazel.target.label", "//proto:task_proto")
			assertSpanAttribute(t, spans, "bazel.target", "bazel.target.label", "//libs/hash:hash")
			assertSpanAttribute(t, spans, "bazel.target", "bazel.target.label", "//libs/tasklib:tasklib")

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
