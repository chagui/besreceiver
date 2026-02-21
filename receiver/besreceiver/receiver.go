package besreceiver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "google.golang.org/genproto/googleapis/devtools/build/v1"
)

type besReceiver struct {
	config         *Config
	logger         *zap.Logger
	settings       receiver.Settings
	grpcServer     *grpc.Server
	traceBuilder   *TraceBuilder
	listener       net.Listener
	tracesConsumer consumer.Traces
	logsConsumer   consumer.Logs
	startOnce      sync.Once
	refCount       atomic.Int32
}

func (r *besReceiver) Start(ctx context.Context, host component.Host) error {
	r.refCount.Add(1)

	var startErr error
	r.startOnce.Do(func() {
		r.traceBuilder = NewTraceBuilder(r.tracesConsumer, r.logsConsumer, r.logger, TraceBuilderConfig{
			InvocationTimeout: r.config.InvocationTimeout,
			ReaperInterval:    r.config.ReaperInterval,
			MeterProvider:     r.settings.MeterProvider,
		})
		r.traceBuilder.Start()

		var err error
		r.grpcServer, err = r.config.ToServer(ctx, host.GetExtensions(), r.settings.TelemetrySettings)
		if err != nil {
			r.traceBuilder.Stop()
			startErr = fmt.Errorf("failed to create gRPC server: %w", err)
			return
		}

		pb.RegisterPublishBuildEventServer(r.grpcServer, r)

		r.listener, err = r.config.NetAddr.Listen(ctx)
		if err != nil {
			r.traceBuilder.Stop()
			startErr = fmt.Errorf("failed to listen on %s: %w", r.config.NetAddr.Endpoint, err)
			return
		}

		r.logger.Info("Starting BES gRPC server", zap.String("endpoint", r.listener.Addr().String()))
		go func() {
			if serveErr := r.grpcServer.Serve(r.listener); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
				r.logger.Error("gRPC server error", zap.Error(serveErr))
			}
		}()
	})

	return startErr
}

// Addr returns the network address the receiver is listening on.
// Only valid after Start() has returned successfully.
func (r *besReceiver) Addr() net.Addr {
	return r.listener.Addr()
}

func (r *besReceiver) Shutdown(ctx context.Context) error {
	if r.refCount.Add(-1) > 0 {
		return nil
	}
	// Reset to zero in case Shutdown is called without a prior Start.
	r.refCount.Store(0)

	// Clean up shared receiver entry.
	if r.config != nil {
		sharedReceiversMu.Lock()
		delete(sharedReceivers, r.config.NetAddr.Endpoint)
		sharedReceiversMu.Unlock()
	}

	if r.traceBuilder != nil {
		r.traceBuilder.Stop()
	}

	if r.grpcServer != nil {
		done := make(chan struct{})
		go func() {
			r.grpcServer.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			r.grpcServer.Stop()
			return fmt.Errorf("shutdown interrupted: %w", ctx.Err())
		}
	}
	return nil
}

// PublishLifecycleEvent handles lifecycle events (build enqueued, build finished lifecycle).
func (r *besReceiver) PublishLifecycleEvent(_ context.Context, req *pb.PublishLifecycleEventRequest) (*emptypb.Empty, error) {
	r.logger.Debug("Received lifecycle event",
		zap.String("build_id", req.GetBuildEvent().GetStreamId().GetBuildId()),
	)
	return &emptypb.Empty{}, nil
}

// PublishBuildToolEventStream handles the bidirectional BEP stream from Bazel.
func (r *besReceiver) PublishBuildToolEventStream(stream pb.PublishBuildEvent_PublishBuildToolEventStreamServer) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("receiving build event: %w", err)
		}

		obe := req.GetOrderedBuildEvent()
		invocationID := obe.GetStreamId().GetInvocationId()
		seqNum := obe.GetSequenceNumber()

		if processErr := r.traceBuilder.ProcessOrderedBuildEvent(stream.Context(), obe); processErr != nil {
			if !consumererror.IsPermanent(processErr) {
				// Retryable errors (e.g. pipeline back-pressure) — close the
				// stream so Bazel can retry the upload.
				r.logger.Error("Retryable error processing build event, closing stream",
					zap.String("invocation_id", invocationID),
					zap.Int64("sequence_number", seqNum),
					zap.Error(processErr),
				)
				return fmt.Errorf("failed to process build event: %w", processErr)
			}
			// Permanent errors (e.g. data serialization failures) won't be
			// resolved by retrying. Log and continue processing the stream.
			r.logger.Error("Permanent error processing build event",
				zap.String("invocation_id", invocationID),
				zap.Int64("sequence_number", seqNum),
				zap.Error(processErr),
			)
		}

		// ACK the event
		resp := &pb.PublishBuildToolEventStreamResponse{
			StreamId:       obe.GetStreamId(),
			SequenceNumber: seqNum,
		}
		if sendErr := stream.Send(resp); sendErr != nil {
			return fmt.Errorf("sending ACK: %w", sendErr)
		}
	}
}
