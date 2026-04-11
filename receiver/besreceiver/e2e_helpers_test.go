package besreceiver

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "google.golang.org/genproto/googleapis/devtools/build/v1"

	"github.com/chagui/besreceiver/internal/besio"
	"github.com/chagui/besreceiver/receiver/besreceiver/internal/metadata"
)

// loadBESStream reads a .besstream fixture file and returns the deserialized requests.
func loadBESStream(t testing.TB, path string) []*pb.PublishBuildToolEventStreamRequest {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err, "opening fixture %s", path)
	defer f.Close()

	requests, err := besio.ReadStream(f)
	require.NoError(t, err, "reading fixture %s", path)
	require.NotEmpty(t, requests, "fixture %s is empty", path)
	return requests
}

// replayViaMockStream feeds a BES stream through the receiver's
// PublishBuildToolEventStream using a mockBESStream. Returns populated
// consumer sinks for traces, metrics, and logs.
func replayViaMockStream(t testing.TB, requests []*pb.PublishBuildToolEventStreamRequest) (
	*consumertest.TracesSink, *consumertest.MetricsSink, *consumertest.LogsSink,
) {
	t.Helper()

	tracesSink := new(consumertest.TracesSink)
	metricsSink := new(consumertest.MetricsSink)
	logsSink := new(consumertest.LogsSink)

	tb := NewTraceBuilder(tracesSink, logsSink, metricsSink, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()

	r := &besReceiver{
		logger:       zap.NewNop(),
		traceBuilder: tb,
	}

	stream := &mockBESStream{
		ctx:    context.Background(),
		events: requests,
	}

	err := r.PublishBuildToolEventStream(stream)
	require.NoError(t, err)

	return tracesSink, metricsSink, logsSink
}

// replayViaGRPC replays a BES stream through a real gRPC server.
// Returns populated consumer sinks for traces, metrics, and logs.
func replayViaGRPC(t testing.TB, requests []*pb.PublishBuildToolEventStreamRequest) (
	*consumertest.TracesSink, *consumertest.MetricsSink, *consumertest.LogsSink,
) {
	t.Helper()

	tracesSink := new(consumertest.TracesSink)
	metricsSink := new(consumertest.MetricsSink)
	logsSink := new(consumertest.LogsSink)

	settings := receivertest.NewNopSettings(metadata.Type)
	cfg := createDefaultConfig().(*Config)
	cfg.NetAddr = confignet.AddrConfig{
		Endpoint:  "localhost:0",
		Transport: confignet.TransportTypeTCP,
	}

	recv := &besReceiver{
		config:          cfg,
		logger:          settings.Logger,
		settings:        settings,
		tracesConsumer:  tracesSink,
		logsConsumer:    logsSink,
		metricsConsumer: metricsSink,
	}

	ctx := context.Background()
	host := componenttest.NewNopHost()
	require.NoError(t, recv.Start(ctx, host))
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, recv.Shutdown(shutdownCtx))
	}()

	conn, err := grpc.NewClient(
		recv.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := pb.NewPublishBuildEventClient(conn)
	stream, err := client.PublishBuildToolEventStream(ctx)
	require.NoError(t, err)

	for _, req := range requests {
		obe := req.GetOrderedBuildEvent()
		sendAndACK(t, stream, req, obe.GetSequenceNumber())
	}

	require.NoError(t, stream.CloseSend())

	return tracesSink, metricsSink, logsSink
}
