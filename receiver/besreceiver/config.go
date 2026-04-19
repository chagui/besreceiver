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
	return nil
}
