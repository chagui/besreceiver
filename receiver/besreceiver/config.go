package besreceiver

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/config/configgrpc"
)

// Config defines the configuration for the BES receiver.
type Config struct {
	configgrpc.ServerConfig `mapstructure:",squash"`

	// InvocationTimeout is the maximum age of an invocation before it is
	// reaped by the stale-invocation reaper. Default: 1h.
	InvocationTimeout time.Duration `mapstructure:"invocation_timeout"`

	// ReaperInterval is how often the stale-invocation reaper runs.
	// Default: 5m.
	ReaperInterval time.Duration `mapstructure:"reaper_interval"`

	// PII opts specific BEP fields into span emission. All flags default to
	// false; enabling a flag surfaces the matching attribute on the root or
	// action span. See PIIConfig.
	PII PIIConfig `mapstructure:"pii"`

	// HighCardinalityCaps bounds the size of Slice[Map] attributes emitted on
	// the bazel.metrics span from high-cardinality BuildMetrics sub-messages
	// (GarbageMetrics, PackageLoadMetrics, EvaluationStat lists). Zero-valued
	// fields fall back to defaults (see defaultHighCardinalityCaps). Exceeding
	// a cap truncates the list and emits a Warn-level log.
	HighCardinalityCaps HighCardinalityCaps `mapstructure:"high_cardinality_caps"`

	// MaxActionDataEntries caps the number of per-mnemonic ActionData entries
	// emitted on the bazel.metrics span attribute and as standalone gauges.
	// Bazel already ranks the list by execution count (top-N at source); this
	// cap is a cardinality guard for downstream backends. Default: 50. When
	// Bazel emits more entries than the cap, the first N (source order) are
	// kept and a warning is logged with the invocation ID and original count.
	MaxActionDataEntries int `mapstructure:"max_action_data_entries"`

	// Filter configures per-target detail level filtering. When absent or
	// empty, the receiver emits every span (pre-filter behaviour). See
	// FilterConfig.
	Filter FilterConfig `mapstructure:"filter"`

	// Summary controls aggregate build counters stamped on the root span.
	// See SummaryConfig.
	Summary SummaryConfig `mapstructure:"summary"`

	// Progress gates routing of Bazel Progress BEP events (streaming
	// stdout/stderr chunks) through the OTel logs pipeline. Defaults to
	// disabled to preserve pre-feature behaviour — Progress payloads can be
	// large and contain anything Bazel prints, so operators must opt in. See
	// ProgressConfig.
	Progress ProgressConfig `mapstructure:"progress"`
}

// ProgressConfig controls emission of Progress BEP events as log records.
// When Enabled is false (default), Progress events are silently dropped; when
// true, each non-empty stream (stdout/stderr) on a Progress event produces
// one log record with severity INFO (stdout) or ERROR (stderr).
//
// MaxChunkSize caps the body size per record: chunks exceeding it are
// truncated and stamped with bazel.progress.truncated=true plus
// bazel.progress.original_bytes=N. A value of 0 disables the cap.
type ProgressConfig struct {
	// Enabled is the master switch. Defaults to false — Progress payloads
	// may contain command lines, file paths, env vars, or secrets echoed by
	// actions, and they are the largest events on the wire in practice.
	Enabled bool `mapstructure:"enabled"`
	// MaxChunkSize is the maximum body size (bytes) per emitted log record.
	// Chunks larger than this are truncated at the byte boundary and
	// annotated with bazel.progress.truncated=true. Zero means unlimited.
	// Negative values are rejected at Validate.
	MaxChunkSize int `mapstructure:"max_chunk_size"`
}

// defaultProgressMaxChunkSize bounds Progress log bodies to 64 KiB by default.
// A single Bazel Progress payload can span hundreds of MiB on large builds;
// routing the full content untrimmed into the logs pipeline would blow the
// default OTLP batch size and inflate backend storage cost. 64 KiB keeps a
// useful amount of context (~1000 lines of typical build output) while leaving
// the operator room to raise or lower the cap explicitly.
const defaultProgressMaxChunkSize = 64 * 1024

// HighCardinalityCaps configures per-attribute limits on the Slice[Map]
// attributes emitted on the bazel.metrics span for high-cardinality
// BuildMetrics sub-messages, plus the bazel.patterns Slice[Map] stamped on
// the root span. Emission is span-attribute-only (no standalone metrics) to
// avoid series explosion in metrics backends. A value of 0 is treated as
// "use default".
type HighCardinalityCaps struct {
	// Garbage caps the number of entries in bazel.metrics.garbage derived from
	// MemoryMetrics.garbage_metrics. Default: 20.
	Garbage int `mapstructure:"garbage"`
	// PackageLoad caps the number of entries in bazel.metrics.package_load
	// derived from PackageMetrics.package_load_metrics. Default: 100.
	PackageLoad int `mapstructure:"package_load"`
	// GraphValues caps the number of entries in each of the five
	// bazel.metrics.graph.{dirtied,changed,built,cleaned,evaluated}_values
	// attributes derived from BuildGraphMetrics. Default: 50.
	GraphValues int `mapstructure:"graph_values"`
	// Patterns caps the number of entries in bazel.patterns on the root span,
	// derived from PatternExpanded events. Default: 50.
	Patterns int `mapstructure:"patterns"`
}

// Default caps chosen for typical Bazel invocations; see HighCardinalityCaps
// for rationale.
const (
	defaultCapGarbage     = 20
	defaultCapPackageLoad = 100
	defaultCapGraphValues = 50
	defaultCapPatterns    = 50
)

// defaultHighCardinalityCaps returns the documented default caps. Mirrored on
// Config via createDefaultConfig so operator-supplied configs see the defaults
// without having to opt in explicitly.
func defaultHighCardinalityCaps() HighCardinalityCaps {
	return HighCardinalityCaps{
		Garbage:     defaultCapGarbage,
		PackageLoad: defaultCapPackageLoad,
		GraphValues: defaultCapGraphValues,
		Patterns:    defaultCapPatterns,
	}
}

// withDefaults returns a copy of c where any zero-valued field is replaced
// with the corresponding default.
func (c HighCardinalityCaps) withDefaults() HighCardinalityCaps {
	d := defaultHighCardinalityCaps()
	if c.Garbage <= 0 {
		c.Garbage = d.Garbage
	}
	if c.PackageLoad <= 0 {
		c.PackageLoad = d.PackageLoad
	}
	if c.GraphValues <= 0 {
		c.GraphValues = d.GraphValues
	}
	if c.Patterns <= 0 {
		c.Patterns = d.Patterns
	}
	return c
}

// SummaryConfig controls whether per-invocation aggregate counters are emitted
// as bazel.summary.* attributes on the root bazel.build span. Default enabled.
//
// Summary counts reflect the full build — they are recorded before any
// detail-level filter would drop the underlying target/action/test span,
// so a dropped span still contributes to the totals.
type SummaryConfig struct {
	// Enabled toggles emission of all bazel.summary.* attributes. When true
	// (default), every counter is emitted as Int64 including zero values so
	// downstream queries can rely on attribute presence.
	Enabled bool `mapstructure:"enabled"`
}

// PIIConfig controls which potentially-sensitive fields from BEP events are
// emitted as span attributes. All fields default to false (redacted).
//
// Flags compose by content, not by pathway. If IncludeHostname is false, no
// hostname-like attribute is emitted regardless of which BEP event carries
// it — so operator-controlled pathways like workspace status and build
// metadata are also filtered per-key when their coarse gate is open.
type PIIConfig struct {
	// IncludeHostname gates hostname-like attributes anywhere they appear:
	// bazel.host on the root span (from BuildStarted.host), plus any
	// bazel.workspace.<k> or bazel.metadata.<k> whose sanitized key is
	// "host", "hostname", or "build_host".
	IncludeHostname bool `mapstructure:"include_hostname"`
	// IncludeUsername gates username-like attributes: bazel.user (from
	// BuildStarted.user), plus bazel.workspace.<k> / bazel.metadata.<k>
	// whose sanitized key is "user", "username", or "build_user".
	IncludeUsername bool `mapstructure:"include_username"`
	// IncludeWorkspaceDir gates bazel.workspace_directory on the root span
	// and any bazel.workspace/metadata.<k> where k is "workspace_dir" or
	// "workspace_directory". Breaking: previously the root attribute was
	// always emitted.
	IncludeWorkspaceDir bool `mapstructure:"include_workspace_dir"`
	// IncludeWorkingDir gates bazel.working_directory on the root span and
	// any bazel.workspace/metadata.<k> where k is "working_dir" or
	// "working_directory".
	IncludeWorkingDir bool `mapstructure:"include_working_dir"`
	// IncludeCommandArgs gates bazel.action.command_line on action spans.
	// For large monorepos this can materially inflate span payload size.
	IncludeCommandArgs bool `mapstructure:"include_command_args"`
	// IncludeActionOutputPaths gates bazel.action.primary_output on action spans.
	IncludeActionOutputPaths bool `mapstructure:"include_action_output_paths"`
	// IncludeWorkspaceStatus opens the bazel.workspace.* pathway (items from
	// --workspace_status_command). When true, workspace items are still
	// filtered per-key against the specific PII flags above — e.g. a
	// BUILD_HOST item is suppressed unless IncludeHostname is also true.
	IncludeWorkspaceStatus bool `mapstructure:"include_workspace_status"`
	// IncludeBuildMetadata opens the bazel.metadata.* pathway (items from
	// --build_metadata=k=v). Same per-key filtering as workspace status.
	IncludeBuildMetadata bool `mapstructure:"include_build_metadata"`
	// IncludeCommandLine gates bazel.command_line on the root span (the
	// reconstructed "original" StructuredCommandLine). Coarse-only: the
	// string is free-form so per-field filtering is not attempted —
	// operators needing finer control should layer a redactionprocessor.
	IncludeCommandLine bool `mapstructure:"include_command_line"`
	// IncludeFetchQueryString controls whether `bazel.fetch.url` (and the
	// matching log body) preserves the URL query string. Default false:
	// fetch URLs frequently carry pre-signed credentials in query params
	// (S3, GCS) so the receiver strips them. Operators who deliberately
	// want the full URL — e.g. to debug a custom mirror — opt in here.
	// Userinfo (`user:pass@host`) is always stripped regardless.
	IncludeFetchQueryString bool `mapstructure:"include_fetch_query_string"`
}

// Validate checks that the receiver configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.NetAddr.Endpoint == "" {
		return errors.New("endpoint must not be empty")
	}
	if cfg.InvocationTimeout < 0 {
		return fmt.Errorf("invocation_timeout must not be negative, got %s", cfg.InvocationTimeout)
	}
	if cfg.ReaperInterval < 0 {
		return fmt.Errorf("reaper_interval must not be negative, got %s", cfg.ReaperInterval)
	}
	if cfg.InvocationTimeout > 0 && cfg.ReaperInterval > 0 && cfg.ReaperInterval >= cfg.InvocationTimeout {
		return fmt.Errorf("reaper_interval (%s) must be less than invocation_timeout (%s)", cfg.ReaperInterval, cfg.InvocationTimeout)
	}
	if cfg.HighCardinalityCaps.Garbage < 0 {
		return fmt.Errorf("high_cardinality_caps.garbage must not be negative, got %d", cfg.HighCardinalityCaps.Garbage)
	}
	if cfg.HighCardinalityCaps.PackageLoad < 0 {
		return fmt.Errorf("high_cardinality_caps.package_load must not be negative, got %d", cfg.HighCardinalityCaps.PackageLoad)
	}
	if cfg.HighCardinalityCaps.GraphValues < 0 {
		return fmt.Errorf("high_cardinality_caps.graph_values must not be negative, got %d", cfg.HighCardinalityCaps.GraphValues)
	}
	if cfg.HighCardinalityCaps.Patterns < 0 {
		return fmt.Errorf("high_cardinality_caps.patterns must not be negative, got %d", cfg.HighCardinalityCaps.Patterns)
	}
	if cfg.MaxActionDataEntries < 0 {
		return fmt.Errorf("max_action_data_entries must not be negative, got %d", cfg.MaxActionDataEntries)
	}
	if cfg.Progress.MaxChunkSize < 0 {
		return fmt.Errorf("progress.max_chunk_size must not be negative, got %d", cfg.Progress.MaxChunkSize)
	}
	if err := cfg.Filter.Validate(); err != nil {
		return err
	}
	return nil
}
