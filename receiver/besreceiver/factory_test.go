package besreceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/chagui/besreceiver/receiver/besreceiver/internal/metadata"
)

func TestNewFactory(t *testing.T) {
	f := NewFactory()
	if f == nil {
		t.Fatal("expected non-nil factory")
	}
	if f.Type().String() != "bes" {
		t.Errorf("expected type 'bes', got %q", f.Type().String())
	}
}

func TestCreateDefaultConfig(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig()
	if cfg == nil {
		t.Fatal("expected non-nil default config")
	}
	rCfg := cfg.(*Config)
	if rCfg.NetAddr.Endpoint != "localhost:8082" {
		t.Errorf("expected default endpoint localhost:8082, got %s", rCfg.NetAddr.Endpoint)
	}
}

func TestCreateTracesReceiver(t *testing.T) {
	t.Cleanup(func() {
		sharedReceiversMu.Lock()
		sharedReceivers = make(map[string]*besReceiver)
		sharedReceiversMu.Unlock()
	})

	f := NewFactory()
	cfg := f.CreateDefaultConfig()
	sink := new(consumertest.TracesSink)
	settings := receivertest.NewNopSettings(metadata.Type)

	recv, err := f.CreateTraces(context.Background(), settings, cfg, sink)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recv == nil {
		t.Fatal("expected non-nil receiver")
	}
}

func TestCreateLogsReceiver(t *testing.T) {
	t.Cleanup(func() {
		sharedReceiversMu.Lock()
		sharedReceivers = make(map[string]*besReceiver)
		sharedReceiversMu.Unlock()
	})

	f := NewFactory()
	cfg := f.CreateDefaultConfig()
	sink := new(consumertest.LogsSink)
	settings := receivertest.NewNopSettings(metadata.Type)

	recv, err := f.CreateLogs(context.Background(), settings, cfg, sink)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recv == nil {
		t.Fatal("expected non-nil receiver")
	}
}

func TestCreateMetricsReceiver(t *testing.T) {
	t.Cleanup(func() {
		sharedReceiversMu.Lock()
		sharedReceivers = make(map[string]*besReceiver)
		sharedReceiversMu.Unlock()
	})

	f := NewFactory()
	cfg := f.CreateDefaultConfig()
	sink := new(consumertest.MetricsSink)
	settings := receivertest.NewNopSettings(metadata.Type)

	recv, err := f.CreateMetrics(context.Background(), settings, cfg, sink)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recv == nil {
		t.Fatal("expected non-nil receiver")
	}
}

func TestSharedReceiverInstance(t *testing.T) {
	t.Cleanup(func() {
		sharedReceiversMu.Lock()
		sharedReceivers = make(map[string]*besReceiver)
		sharedReceiversMu.Unlock()
	})

	f := NewFactory()
	cfg := f.CreateDefaultConfig()
	settings := receivertest.NewNopSettings(metadata.Type)

	tracesSink := new(consumertest.TracesSink)
	tracesRecv, err := f.CreateTraces(context.Background(), settings, cfg, tracesSink)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	logsSink := new(consumertest.LogsSink)
	logsRecv, err := f.CreateLogs(context.Background(), settings, cfg, logsSink)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	metricsSink := new(consumertest.MetricsSink)
	metricsRecv, err := f.CreateMetrics(context.Background(), settings, cfg, metricsSink)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// All receivers should be the same underlying instance.
	if tracesRecv != logsRecv {
		t.Error("expected traces and logs receivers to share the same instance")
	}
	if tracesRecv != metricsRecv {
		t.Error("expected traces and metrics receivers to share the same instance")
	}

	// Verify all three consumers are set.
	r := tracesRecv.(*besReceiver)
	if r.tracesConsumer == nil {
		t.Error("expected tracesConsumer to be set")
	}
	if r.logsConsumer == nil {
		t.Error("expected logsConsumer to be set")
	}
	if r.metricsConsumer == nil {
		t.Error("expected metricsConsumer to be set")
	}
}

// TestReceiver_ThreadsPIIConfig is a regression test for a wiring bug where
// r.config.PII was parsed from YAML into r.config but never forwarded to the
// TraceBuilder. Without this, all PII flags stayed at their zero-value
// defaults regardless of what the operator set in the receiver config.
func TestReceiver_ThreadsPIIConfig(t *testing.T) {
	t.Cleanup(func() {
		sharedReceiversMu.Lock()
		sharedReceivers = make(map[string]*besReceiver)
		sharedReceiversMu.Unlock()
	})

	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.NetAddr.Endpoint = "localhost:0"
	cfg.PII.IncludeHostname = true
	cfg.PII.IncludeCommandArgs = true

	sink := new(consumertest.TracesSink)
	settings := receivertest.NewNopSettings(metadata.Type)
	recv, err := f.CreateTraces(context.Background(), settings, cfg, sink)
	require.NoError(t, err)

	require.NoError(t, recv.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { _ = recv.Shutdown(context.Background()) })

	r := recv.(*besReceiver)
	assert.True(t, r.traceBuilder.pii.IncludeHostname,
		"IncludeHostname should propagate from Config.PII through TraceBuilder")
	assert.True(t, r.traceBuilder.pii.IncludeCommandArgs,
		"IncludeCommandArgs should propagate from Config.PII through TraceBuilder")
	assert.False(t, r.traceBuilder.pii.IncludeUsername,
		"unset flags should remain at zero value")
}
