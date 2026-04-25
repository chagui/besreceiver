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
| `bazel.workspace_directory`  | string | `BuildStarted.workspace_directory` — **PII**, requires `include_workspace_dir` |
| `bazel.working_directory`    | string | `BuildStarted.working_directory` — **PII**, requires `include_working_dir`    |
| `bazel.host`                 | string | `BuildStarted.host` — **PII**, requires `include_hostname`       |
| `bazel.user`                 | string | `BuildStarted.user` — **PII**, requires `include_username`       |
| `bazel.exit_code.name`       | string | `BuildFinished.exit_code.name` (absent on abort-only finalize)   |
| `bazel.exit_code.code`       | int    | `BuildFinished.exit_code.code`                                   |
| `bazel.abort.reason`         | string | `Aborted.reason` — lowercased enum; first abort wins             |
| `bazel.abort.description`    | string | `Aborted.description`                                            |
| `bazel.workspace.<key>`      | string | `WorkspaceStatus.item[*]` — **PII**, requires `include_workspace_status`; per-key filtered |
| `bazel.metadata.<key>`       | string | `BuildMetadata.metadata` — **PII**, requires `include_build_metadata`; per-key filtered    |
| `bazel.tool_tag`             | string | `OptionsParsed.tool_tag` (e.g. CI system identifier)             |
| `bazel.options.startup_count`| int    | `len(OptionsParsed.explicit_startup_options)`                    |
| `bazel.options.command_count`| int    | `len(OptionsParsed.explicit_cmd_line)`                           |
| `bazel.command_line`         | string | Reconstructed `StructuredCommandLine` — **PII**, requires `include_command_line` |
| `bazel.run.argv`                       | Slice[string]      | `ExecRequestConstructed.argv` (only on `bazel run`) — **PII**, requires `include_command_args` |
| `bazel.run.working_directory`          | string             | `ExecRequestConstructed.working_directory` (only on `bazel run`) — **PII**, requires `include_working_dir` |
| `bazel.run.environment_variable_count` | int                | `len(ExecRequestConstructed.environment_variable)` (only on `bazel run`) |
| `bazel.run.environment`                | Slice[Map]         | `ExecRequestConstructed.environment_variable` (only on `bazel run`) — `{name, value}` entries; **PII**, requires `include_command_args` |
| `bazel.run.should_exec`                | bool               | `ExecRequestConstructed.should_exec` (only on `bazel run`) |

The `bazel.run.*` group is populated only for `bazel run` invocations, where
Bazel emits `ExecRequestConstructed` after the build succeeds and before the
target is exec'd. `bazel build` / `bazel test` invocations carry none of
these attributes. `bazel.run.environment_variable_count` and
`bazel.run.should_exec` are always emitted when the event arrives (counts
and bools carry no PII); argv, environment, and working_directory are
PII-gated per the table above.

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
| `bazel.action.mnemonic`       | string       | `ActionExecuted.type`                                          |
| `bazel.action.exit_code`      | int          | `ActionExecuted.exit_code`                                     |
| `bazel.action.success`        | bool         | `ActionExecuted.success`                                       |
| `bazel.target.label`          | string       | Owning target label (when known)                               |
| `bazel.action.command_line`   | string slice | `ActionExecuted.command_line` — **PII**, requires `include_command_args` |
| `bazel.action.primary_output` | string       | `ActionExecuted.primary_output.name` — **PII**, requires `include_action_output_paths` |

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

## Summary counters

Aggregate per-invocation counters stamped on the root `bazel.build` span. Counts
reflect the full build — targets dropped by [detail-level filtering](filter.md)
still contribute to the totals so dashboards and alerts see a stable view of
build health regardless of trace verbosity. Enabled by default; set
`summary.enabled: false` to suppress the entire block.

| Attribute                         | Type | Source                                         |
|-----------------------------------|------|------------------------------------------------|
| `bazel.summary.total_targets`     | int  | count of `TargetConfigured` events             |
| `bazel.summary.total_actions`     | int  | count of `ActionExecuted` events               |
| `bazel.summary.success_actions`   | int  | `ActionExecuted` with `success=true`           |
| `bazel.summary.failed_actions`    | int  | `ActionExecuted` with `success=false`          |
| `bazel.summary.total_tests`       | int  | count of `TestSummary` events (one per target) |
| `bazel.summary.passed_tests`      | int  | `TestSummary.overall_status = PASSED`          |
| `bazel.summary.failed_tests`      | int  | `TestSummary.overall_status = FAILED/TIMEOUT/...` |
| `bazel.summary.flaky_tests`       | int  | `TestSummary.overall_status = FLAKY`           |

Test tallies come from `TestSummary` (post-retry verdict, emitted once per test
target) rather than `TestResult` (one per attempt), so flaky retries don't
double-count. Zero-valued counters are still emitted when the feature is
enabled so downstream queries can rely on attribute presence.

## PII controls

Attributes marked **PII** above are gated by the `pii:` config block on the
receiver. Every flag defaults to `false`, so by default none of those
attributes are emitted. Enable them selectively based on your compliance
posture:

```yaml
receivers:
  bes:
    pii:
      include_hostname: false              # bazel.host + host-like workspace/metadata keys
      include_username: false              # bazel.user + user-like workspace/metadata keys
      include_workspace_dir: false         # bazel.workspace_directory + matching workspace/metadata keys
      include_working_dir: false           # bazel.working_directory + bazel.run.working_directory + matching workspace/metadata keys
      include_command_args: false          # bazel.action.command_line + bazel.run.argv + bazel.run.environment
      include_action_output_paths: false   # bazel.action.primary_output
      include_workspace_status: false      # bazel.workspace.* pathway (per-key filtered)
      include_build_metadata: false        # bazel.metadata.* pathway (per-key filtered)
      include_command_line: false          # bazel.command_line (coarse only)
```

### Composition

Flags gate by *content*, not by *pathway*. If `include_hostname: false`, no
hostname-like attribute is emitted regardless of which BEP event carries
it. This means the coarse gates (`include_workspace_status`,
`include_build_metadata`) open a pathway but per-key filters still apply
inside it.

Concretely, with `include_workspace_status: true` and `include_hostname:
false`: `bazel.workspace.stable_git_branch` is emitted (safe key), but
`bazel.workspace.build_host` is suppressed (hostname is off).

The sanitized keys matched as PII-sensitive:

| Flag (when false)          | Suppresses workspace/metadata keys       |
|----------------------------|------------------------------------------|
| `include_hostname`         | `host`, `hostname`, `build_host`         |
| `include_username`         | `user`, `username`, `build_user`         |
| `include_working_dir`      | `working_dir`, `working_directory`       |
| `include_workspace_dir`    | `workspace_dir`, `workspace_directory`   |

`include_command_line` is coarse-only: the attribute is a free-form
reconstructed string, so per-key filtering doesn't apply. Operators
needing finer redaction of the command line should layer a
`redactionprocessor` on the pipeline.

Note that `bazel.workspace_directory` is gated — earlier receiver versions
emitted it unconditionally. Operators who relied on it must set
`include_workspace_dir: true` to restore the prior behavior.

Non-PII attributes (`bazel.command`, `bazel.uuid`, `bazel.tool_tag`, etc.)
are always emitted regardless of `pii:` settings.

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

## Progress logs

Bazel's `Progress` BEP events carry streaming stdout/stderr chunks. They
are opt-in because payloads can be large and may contain anything Bazel
prints — command lines, file paths, env vars, or secrets echoed by
actions. Enable routing to the logs pipeline with:

```yaml
receivers:
  bes:
    progress:
      enabled: true          # default: false
      max_chunk_size: 65536  # bytes per record; 0 = unlimited
```

When enabled, each non-empty stream on a `Progress` event emits one log
record with `event.name=bazel.progress`. Empty streams are skipped. A
`Progress` with empty stdout AND empty stderr emits nothing.

| Attribute                        | Type   | Source                                          |
|----------------------------------|--------|-------------------------------------------------|
| `bazel.invocation_id`            | string | BES stream invocation id                        |
| `bazel.progress.stream`          | string | `stdout` or `stderr`                            |
| `bazel.progress.bytes`           | int    | Original byte length of the stream chunk        |
| `bazel.progress.truncated`       | bool   | `true` when `bytes > max_chunk_size`            |
| `bazel.progress.opaque_count`    | int    | `BuildEventId.Progress.opaque_count` — same on the stderr and stdout records of one event so consumers can pair them |

Both `bazel.progress.bytes` and `bazel.progress.truncated` are stamped on
every record so downstream queries can rely on attribute presence rather
than guarding against absent keys.

Severity is `INFO` on both streams — Bazel routes normal-build chatter
(`Loading:`, `Analyzing:`, `Build completed successfully`) through stderr,
so mapping stderr to `ERROR` would page on every green build. Operators
who need elevation can stack a `transformprocessor`. When both streams
carry data on the same `Progress` event, the receiver emits stderr first
to match the BEP spec ordering (`build_event_stream.proto:303-304`).

The record body is the (possibly truncated) stream content. Truncation
clips at a UTF-8 rune boundary so consumers never see invalid encoding.
Records carry the invocation's TraceID, matching the correlation scheme
used for other log records.

Memory note: this feature truncates the *log record body*, not the
inbound gRPC payload. The full Progress message must still be unmarshalled
by the receiver before truncation runs. See `docs/adr/0002-large-payload-handling.md`
for the heap-bounding story (issues #64/#65/#66).

Since stream contents can contain PII, the feature defaults off. When
enabled, stack a `redactionprocessor` (or similar) downstream to scrub
sensitive substrings before export — per-key PII controls do not apply
to free-form output.

## Metrics

Per-invocation gauges (`bazel.invocation.*`) and cross-invocation
cumulative counters are emitted from `BuildMetrics` with
`bazel.invocation_id` and `bazel.command` as attributes. See
`metricsbuilder.go` for the full set.
