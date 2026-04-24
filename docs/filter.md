# Detail level filtering

Large Bazel builds produce dense traces — a full monorepo invocation can emit
tens of thousands of spans, most of them uninteresting third-party compilations.
The receiver's `filter` block lets operators dial span verbosity per-target so
the trace view stays focused on the code under active investigation while still
preserving top-level build metrics for everything else.

Filtering runs at event-processing time, before a span is allocated. Dropped
targets cost nothing in memory or export bandwidth.

## Configuration

```yaml
receivers:
  bes:
    filter:
      default_level: verbose
      rules:
        - pattern: "//third_party/..."
          detail_level: build_only
        - pattern: "//src/main/..."
          detail_level: verbose
```

A missing or empty `filter` block preserves the pre-filter behaviour (`verbose`
everywhere). `default_level` applies when no rule matches the target label.
Rules are evaluated in declaration order and the first match wins.

## Detail levels

| Level        | Spans emitted for matching targets          |
|--------------|---------------------------------------------|
| `drop`       | none (root `bazel.build` + `bazel.metrics` only, which bypass the filter) |
| `build_only` | same as `drop` — operator-friendly alias when used as `default_level` |
| `targets`    | `bazel.target` only — no `bazel.action`, no `bazel.test` |
| `verbose`    | everything (default) — `bazel.target`, `bazel.action`, `bazel.test` |

The root `bazel.build` and aggregate `bazel.metrics` spans are always emitted
regardless of filter configuration so invocation-level metrics and the build
summary on the root span remain intact.

## Pattern syntax

Patterns match the canonical target label Bazel emits in BEP
(`//pkg:target`). Three forms are accepted:

| Pattern            | Matches                                               |
|--------------------|-------------------------------------------------------|
| `//pkg:target`     | exact label only                                      |
| `//pkg:*`          | every target inside package `//pkg`                   |
| `//pkg/...`        | every target under `//pkg` and any descendant package |

Relative labels (`pkg:t`) and external-repo labels (`@repo//...`) are rejected
at config load time. Malformed patterns fail fast via `Config.Validate` with a
clear error pointing at the offending rule.

## Interaction with other features

- **PII controls** apply per-attribute and are orthogonal. A span dropped by
  the filter never reaches PII gating, but a span that survives the filter is
  still subject to PII-configured attribute redaction.
- **Cardinality caps** (`high_cardinality_caps`, `max_action_data_entries`)
  apply on the `bazel.metrics` span which the filter does not touch; they
  remain effective under every detail level.
- **Reparenting** (`bes.events.reparented` counter) keys on target label. If
  both an action and its owning target are dropped by the same rule, no
  reparenting happens and the counter is not incremented.
