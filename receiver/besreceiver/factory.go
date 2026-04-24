package besreceiver

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/chagui/besreceiver/receiver/besreceiver/internal/metadata"
)

var (
	sharedReceivers   = make(map[string]*besReceiver)
	sharedReceiversMu sync.Mutex
)

// NewFactory creates a factory for the BES receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithTraces(createTracesReceiver, metadata.TracesStability),
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
		receiver.WithMetrics(createMetricsReceiver, metadata.MetricsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		ServerConfig: configgrpc.ServerConfig{
			NetAddr: confignet.AddrConfig{
				Endpoint:  "localhost:8082",
				Transport: confignet.TransportTypeTCP,
			},
		},
		InvocationTimeout:    defaultInvocationTimeout,
		ReaperInterval:       defaultReaperInterval,
		PII:                  PIIConfig{},
		HighCardinalityCaps:  defaultHighCardinalityCaps(),
		MaxActionDataEntries: defaultMaxActionDataEntries,
		Filter: FilterConfig{
			// Default preserves pre-filter behaviour: every span is emitted
			// unless the operator opts into a stricter level or adds rules.
			DefaultLevel: DetailLevelVerbose,
		},
		Summary: SummaryConfig{Enabled: true},
	}
}

// getOrCreateReceiver returns a shared besReceiver for the given config,
// creating one if it doesn't exist yet. This allows traces and logs pipelines
// to share a single gRPC server.
func getOrCreateReceiver(cfg *Config, settings receiver.Settings) *besReceiver {
	sharedReceiversMu.Lock()
	defer sharedReceiversMu.Unlock()

	key := cfg.NetAddr.Endpoint
	r, ok := sharedReceivers[key]
	if ok {
		return r
	}

	r = &besReceiver{
		config:   cfg,
		logger:   settings.Logger,
		settings: settings,
	}
	sharedReceivers[key] = r
	return r
}

func createTracesReceiver(
	_ context.Context,
	settings receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (receiver.Traces, error) {
	rCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", cfg)
	}
	r := getOrCreateReceiver(rCfg, settings)
	r.tracesConsumer = nextConsumer
	return r, nil
}

func createLogsReceiver(
	_ context.Context,
	settings receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (receiver.Logs, error) {
	rCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", cfg)
	}
	r := getOrCreateReceiver(rCfg, settings)
	r.logsConsumer = nextConsumer
	return r, nil
}

func createMetricsReceiver(
	_ context.Context,
	settings receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (receiver.Metrics, error) {
	rCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", cfg)
	}
	r := getOrCreateReceiver(rCfg, settings)
	r.metricsConsumer = nextConsumer
	return r, nil
}
