package besreceiver

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/confignet"
)

func TestConfigDefaults(t *testing.T) {
	cfg := createDefaultConfig().(*Config)

	if cfg.NetAddr.Endpoint != "localhost:8082" {
		t.Errorf("expected default endpoint localhost:8082, got %s", cfg.NetAddr.Endpoint)
	}
	if cfg.NetAddr.Transport != confignet.TransportTypeTCP {
		t.Errorf("expected default transport TCP, got %s", cfg.NetAddr.Transport)
	}
	if cfg.InvocationTimeout != defaultInvocationTimeout {
		t.Errorf("expected default invocation_timeout %s, got %s", defaultInvocationTimeout, cfg.InvocationTimeout)
	}
	if cfg.ReaperInterval != defaultReaperInterval {
		t.Errorf("expected default reaper_interval %s, got %s", defaultReaperInterval, cfg.ReaperInterval)
	}
}

func TestConfigDefaults_PII(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	// All PII flags must default to false — the breaking change is load-bearing.
	if cfg.PII.IncludeHostname {
		t.Error("IncludeHostname must default to false")
	}
	if cfg.PII.IncludeUsername {
		t.Error("IncludeUsername must default to false")
	}
	if cfg.PII.IncludeWorkspaceDir {
		t.Error("IncludeWorkspaceDir must default to false")
	}
	if cfg.PII.IncludeWorkingDir {
		t.Error("IncludeWorkingDir must default to false")
	}
	if cfg.PII.IncludeCommandArgs {
		t.Error("IncludeCommandArgs must default to false")
	}
	if cfg.PII.IncludeActionOutputPaths {
		t.Error("IncludeActionOutputPaths must default to false")
	}
	if cfg.PII.IncludeWorkspaceStatus {
		t.Error("IncludeWorkspaceStatus must default to false")
	}
	if cfg.PII.IncludeBuildMetadata {
		t.Error("IncludeBuildMetadata must default to false")
	}
	if cfg.PII.IncludeCommandLine {
		t.Error("IncludeCommandLine must default to false")
	}
}

func TestConfigCustom(t *testing.T) {
	cfg := &Config{
		ServerConfig: configgrpc.ServerConfig{
			NetAddr: confignet.AddrConfig{
				Endpoint:  "127.0.0.1:9090",
				Transport: confignet.TransportTypeTCP,
			},
		},
		InvocationTimeout: 2 * time.Hour,
		ReaperInterval:    10 * time.Minute,
	}

	if cfg.NetAddr.Endpoint != "127.0.0.1:9090" {
		t.Errorf("expected endpoint 127.0.0.1:9090, got %s", cfg.NetAddr.Endpoint)
	}
	if cfg.InvocationTimeout != 2*time.Hour {
		t.Errorf("expected invocation_timeout 2h, got %s", cfg.InvocationTimeout)
	}
	if cfg.ReaperInterval != 10*time.Minute {
		t.Errorf("expected reaper_interval 10m, got %s", cfg.ReaperInterval)
	}
}

func TestConfigValidate(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if err := cfg.Validate(); err != nil {
		t.Errorf("expected default config to be valid, got: %v", err)
	}
}

func TestConfigValidate_EmptyEndpoint(t *testing.T) {
	cfg := &Config{
		ServerConfig: configgrpc.ServerConfig{
			NetAddr: confignet.AddrConfig{
				Endpoint: "",
			},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for empty endpoint")
	}
}

func TestConfigValidate_NegativeInvocationTimeout(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.InvocationTimeout = -1 * time.Second
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for negative invocation_timeout")
	}
}

func TestConfigValidate_NegativeReaperInterval(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.ReaperInterval = -1 * time.Second
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for negative reaper_interval")
	}
}

func TestConfigValidate_ReaperIntervalExceedsTimeout(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.InvocationTimeout = 5 * time.Minute
	cfg.ReaperInterval = 10 * time.Minute
	if err := cfg.Validate(); err == nil {
		t.Error("expected error when reaper_interval >= invocation_timeout")
	}
}

func TestConfigDefaults_MaxActionDataEntries(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if cfg.MaxActionDataEntries != defaultMaxActionDataEntries {
		t.Errorf("expected default max_action_data_entries=%d, got %d", defaultMaxActionDataEntries, cfg.MaxActionDataEntries)
	}
}

func TestConfigValidate_NegativeMaxActionDataEntries(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.MaxActionDataEntries = -1
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for negative max_action_data_entries")
	}
}
