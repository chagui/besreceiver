package besreceiver

import (
	"context"
	"testing"

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

	// Both receivers should be the same underlying instance.
	if tracesRecv != logsRecv {
		t.Error("expected traces and logs receivers to share the same instance")
	}

	// Verify both consumers are set.
	r := tracesRecv.(*besReceiver)
	if r.tracesConsumer == nil {
		t.Error("expected tracesConsumer to be set")
	}
	if r.logsConsumer == nil {
		t.Error("expected logsConsumer to be set")
	}
}
