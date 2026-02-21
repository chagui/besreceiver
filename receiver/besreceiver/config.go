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
