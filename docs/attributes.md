# Attribute reference

This page lists every span attribute the `besreceiver` emits, grouped by
span type, alongside the BEP field it's derived from. Downstream log and
metric attributes are summarized at the end. Keep this in sync with
`receiver/besreceiver/invocation.go` and `metricsbuilder.go` when adding or
renaming attributes.

## Span hierarchy

Every invocation produces one trace (TraceID = `SHA-256(uuid)[0:16]`) with
the following span tree. Every span's TraceID matches the root; SpanIDs are
deterministic per [ADR 0001](adr/0001-deterministic-span-ids.md).

```
bazel.build (root)
├── bazel.target
│   ├── bazel.action
│   └── bazel.action
├── bazel.target
│   ├── bazel.test
│   └── bazel.test
└── bazel.metrics
```

## `bazel.build` (root)

Emitted from `BuildStarted` (start timestamp, common attrs) and
`BuildFinished` (end timestamp, exit-code attrs) or, on an aborted build
without `BuildFinished`, from the reaper path.

| Attribute                  | Type   | Source                                                         |
|----------------------------|--------|----------------------------------------------------------------|
| `bazel.command`              | string | `BuildStarted.command`                                           |
| `bazel.uuid`                 | string | `BuildStarted.uuid`                                              |
| `bazel.build_tool_version`   | string | `BuildStarted.build_tool_version`                                |
| `bazel.workspace_directory`  | string | `BuildStarted.workspace_directory`                               |
| `bazel.exit_code.name`       | string | `BuildFinished.exit_code.name` (absent on abort-only finalize)   |
| `bazel.exit_code.code`       | int    | `BuildFinished.exit_code.code`                                   |
| `bazel.abort.reason`         | string | `Aborted.reason` — lowercased enum; first abort wins             |
| `bazel.abort.description`    | string | `Aborted.description`                                            |
| `bazel.workspace.<key>`      | string | `WorkspaceStatus.item[*]` — keys sanitized, capped at 30         |
| `bazel.metadata.<key>`       | string | `BuildMetadata.metadata` — keys sanitized, capped at 20          |
| `bazel.tool_tag`             | string | `OptionsParsed.tool_tag` (e.g. CI system identifier)             |
| `bazel.options.startup_count`| int    | `len(OptionsParsed.explicit_startup_options)`                    |
| `bazel.options.command_count`| int    | `len(OptionsParsed.explicit_cmd_line)`                           |
| `bazel.command_line`         | string | Reconstructed `StructuredCommandLine` ("original" label, 1024 B) |

Key sanitization for `bazel.workspace.*` and `bazel.metadata.*`: lowercase,
collapse whitespace to `_`, strip characters outside `[a-z0-9_.]`, trim
leading/trailing `_` and `.`. Values are truncated to 256 bytes at a UTF-8
rune boundary. Metadata keys are sorted alphabetically before the cap so
which entries survive truncation is deterministic.

Status is set to `ERROR` when `exit_code.code != 0`. An abort overrides the
status message to `"aborted: <reason>: <description>"` but does not remove
the exit-code attributes — abort is the root cause, exit code is the
consequence.

## `bazel.target`

One span per configured target, emitted from `TargetConfigured`. Structural
parent for `bazel.action` and `bazel.test` spans belonging to the target.
`TargetComplete` and `TestSummary` arrive after `TargetConfigured` and
enrich the existing span with outcome and aggregate-test attributes.

| Attribute                              | Type    | Source                                             |
|----------------------------------------|---------|----------------------------------------------------|
| `bazel.target.label`                      | string  | Target label from `TargetConfiguredId`             |
| `bazel.target.rule_kind`                  | string  | `TargetConfigured.target_kind`                     |
| `bazel.target.config.mnemonic`            | string  | `Configuration.mnemonic` (e.g. `k8-opt`)           |
| `bazel.target.config.platform`            | string  | `Configuration.platform_name`                      |
| `bazel.target.config.cpu`                 | string  | `Configuration.cpu`                                |
| `bazel.target.config.is_tool`             | bool    | `Configuration.is_tool` (host/exec vs target)      |
| `bazel.target.success`                    | bool    | `TargetComplete.success`                           |
| `bazel.target.test_timeout_s`             | float64 | `TargetComplete.test_timeout` (seconds)            |
| `bazel.target.failure_detail`             | string  | `TargetComplete.failure_detail.message`            |
| `bazel.target.output_group_count`         | int     | `len(TargetComplete.output_group)`                 |
| `bazel.target.test.overall_status`        | string  | `TestSummary.overall_status` (enum)                |
| `bazel.target.test.total_run_count`       | int     | `TestSummary.total_run_count`                      |
| `bazel.target.test.shard_count`           | int     | `TestSummary.shard_count`                          |
| `bazel.target.test.total_num_cached`      | int     | `TestSummary.total_num_cached`                     |
| `bazel.target.test.total_run_duration_ms` | int     | `TestSummary.total_run_duration` (ms)              |

`bazel.target.config.*` attributes require the `Configuration` event for
this target's config id to have arrived before `TargetConfigured`. Bazel
emits it in that order in practice; when the Configuration is missing or
has the sentinel id `"none"`, config attributes are skipped. The receiver
does not back-patch attributes retroactively.

Status is set to `ERROR` when `TargetComplete.success` is false — message
is the `failure_detail.message` when present, otherwise `"target failed"`.
`TargetComplete` and `TestSummary` are no-ops if no target span exists for
the label (aborted or out-of-order), matching the silent-degradation
pattern used elsewhere. Non-test targets receive no `bazel.target.test.*`
attributes.

## `bazel.action`

One span per executed action, emitted from `ActionExecuted`. Name is
`bazel.action <mnemonic>` when the mnemonic is present, otherwise
`bazel.action`. Timestamps come from `ActionExecuted.start_time` /
`end_time`.

| Attribute               | Type   | Source                           |
|-------------------------|--------|----------------------------------|
| `bazel.action.mnemonic` | string | `ActionExecuted.type`            |
| `bazel.action.exit_code`| int    | `ActionExecuted.exit_code`       |
| `bazel.action.success`  | bool   | `ActionExecuted.success`         |
| `bazel.target.label`    | string | Owning target label (when known) |

Status is `ERROR` with message `"action failed"` when
`ActionExecuted.success` is false.

## `bazel.test`

One span per test attempt, emitted from `TestResult`. Name is `bazel.test`.
Start timestamp from `TestResult.test_attempt_start`; end timestamp is
start plus `test_attempt_duration` when set.

| Attribute             | Type   | Source                      |
|-----------------------|--------|-----------------------------|
| `bazel.test.status`   | string | `TestResult.status` (enum)  |
| `bazel.test.run`      | int    | `TestResultId.run`          |
| `bazel.test.shard`    | int    | `TestResultId.shard`        |
| `bazel.test.attempt`  | int    | `TestResultId.attempt`      |
| `bazel.target.label`  | string | Owning target label         |

Status is `ERROR` with the enum name as the message when the status is
anything other than `PASSED`.

## `bazel.metrics`

One span per invocation, emitted from `BuildMetrics`. Carries timing and
action-summary aggregates for the whole build.

| Attribute                                | Type | Source                                    |
|------------------------------------------|------|-------------------------------------------|
| `bazel.metrics.wall_time_ms`             | int  | `TimingMetrics.wall_time_in_ms`           |
| `bazel.metrics.cpu_time_ms`              | int  | `TimingMetrics.cpu_time_in_ms`            |
| `bazel.metrics.analysis_phase_time_ms`   | int  | `TimingMetrics.analysis_phase_time_in_ms` |
| `bazel.metrics.execution_phase_time_ms`  | int  | `TimingMetrics.execution_phase_time_in_ms`|
| `bazel.metrics.actions_created`          | int  | `ActionSummary.actions_created`           |
| `bazel.metrics.actions_executed`         | int  | `ActionSummary.actions_executed`          |

## Use cases

A few queries you can run directly against the emitted attributes, assuming
a backend that accepts span-attribute filters (Datadog, Tempo, Jaeger,
Honeycomb, etc.):

**Which builds were OOM-killed in the last 24h?**
Filter root spans where `bazel.abort.reason = "out_of_memory"`. The
`bazel.abort.description` attribute carries Bazel's message for
additional context.

**Show every build on the `main` branch.**
Filter root spans where `bazel.workspace.stable_git_branch = "main"`.
Requires `--workspace_status_command` to emit `STABLE_GIT_BRANCH`.

**Traces for a specific CI pipeline run.**
Filter root spans where `bazel.metadata.ci_pipeline_id = "<id>"`.
Requires the CI system to pass `--build_metadata=ci_pipeline_id=<id>`.

**How often do developers interrupt a build vs. Bazel failing it?**
Group root spans by `bazel.abort.reason`; `user_interrupted` tells you
Ctrl-C, any of the other eleven enum values tells you Bazel aborted.

**Median wall time for successful `bazel test` runs this week.**
Filter where `bazel.command = "test"` and `bazel.exit_code.code = 0`,
aggregate `bazel.metrics.wall_time_ms` with a p50.

**Correlate a trace with its Bazel invocation ID.**
`bazel.uuid` on the root span matches the `--invocation_id` shown in
Bazel's terminal output, making it easy to pivot from a CI log line
("Invocation ID: abc-123") to the full trace.

**Compare build times across configurations.**
Group target spans by `bazel.target.config.mnemonic` (e.g. `k8-opt` vs
`k8-fastbuild`) and aggregate their duration. Separate host-tool builds
from user-target builds using `bazel.target.config.is_tool = false`.

**Identify builds launched by a specific tool.**
Filter root spans where `bazel.tool_tag = "gazelle-ci"` (or whatever
value CI passes via `--tool_tag=...`) to see every invocation from that
tool without pulling metadata from external systems.

## Logs

Each handled BEP event also emits a log record with common attributes
`bazel.invocation_id` and event-specific key/value pairs. The log body is
a short human-readable summary (e.g. `"Build finished: SUCCESS"`). Severity
is `Info` for successful events, `Error` for failures and aborts. Log
records carry the invocation's TraceID so they correlate with the trace
emitted for the same invocation.

## Metrics

Per-invocation gauges (`bazel.invocation.*`) and cross-invocation
cumulative counters are emitted from `BuildMetrics` with
`bazel.invocation_id` and `bazel.command` as attributes. See
`metricsbuilder.go` for the full set.
