// Command besrecord is a lightweight BES gRPC server that records incoming
// PublishBuildToolEventStream requests to a length-delimited protobuf file.
//
// All requests from all streams are collected and written to a single file
// on shutdown. This avoids clobbering when Bazel opens multiple streams.
//
// Usage:
//
//	besrecord -addr localhost:8082 -output path/to/capture.besstream
//
// Then point Bazel at it:
//
//	bazel build //... --bes_backend=grpc://localhost:8082 --build_event_publish_all_actions
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	pb "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/chagui/besreceiver/internal/besio"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	addr := flag.String("addr", "localhost:0", "gRPC listen address (use port 0 for ephemeral)")
	output := flag.String("output", "", "output .besstream file path (required)")
	portFile := flag.String("port-file", "", "write the assigned port to this file (useful with -addr localhost:0)")
	flag.Parse()

	if *output == "" {
		flag.Usage()
		os.Exit(1)
	}

	srv := grpc.NewServer()
	recorder := &besRecorder{}
	pb.RegisterPublishBuildEventServer(srv, recorder)

	lc := net.ListenConfig{}
	lis, err := lc.Listen(context.Background(), "tcp", *addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", *addr, err)
	}

	// Write the assigned port to a file so callers can discover it.
	if *portFile != "" {
		_, port, splitErr := net.SplitHostPort(lis.Addr().String())
		if splitErr != nil {
			return fmt.Errorf("parsing listen address: %w", splitErr)
		}
		if writeErr := os.WriteFile(*portFile, []byte(port), 0o600); writeErr != nil {
			return fmt.Errorf("writing port file: %w", writeErr)
		}
	}

	log.Printf("Recording BES streams on %s -> %s", lis.Addr(), *output)

	// Graceful shutdown on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		log.Println("Shutting down...")
		srv.GracefulStop()
	}()

	if serveErr := srv.Serve(lis); serveErr != nil {
		return fmt.Errorf("gRPC server error: %w", serveErr)
	}

	// Write all collected requests to the output file after shutdown.
	recorder.mu.Lock()
	allRequests := recorder.requests
	recorder.mu.Unlock()

	if len(allRequests) == 0 {
		log.Println("No requests recorded.")
		return nil
	}

	f, createErr := os.Create(*output)
	if createErr != nil {
		return fmt.Errorf("creating output file: %w", createErr)
	}
	defer f.Close()

	if writeErr := besio.WriteStream(f, allRequests); writeErr != nil {
		return fmt.Errorf("writing stream: %w", writeErr)
	}

	log.Printf("Done. Recorded %d requests across %d streams to %s.",
		len(allRequests), recorder.streamCount, *output)
	return nil
}

type besRecorder struct {
	pb.UnimplementedPublishBuildEventServer

	mu          sync.Mutex
	requests    []*pb.PublishBuildToolEventStreamRequest
	streamCount int
}

func (r *besRecorder) PublishBuildToolEventStream(stream pb.PublishBuildEvent_PublishBuildToolEventStreamServer) error {
	var streamRequests []*pb.PublishBuildToolEventStreamRequest

	for {
		req, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return fmt.Errorf("receiving event: %w", recvErr)
		}
		streamRequests = append(streamRequests, req)

		// ACK the event so Bazel continues sending.
		obe := req.GetOrderedBuildEvent()
		if sendErr := stream.Send(&pb.PublishBuildToolEventStreamResponse{
			StreamId:       obe.GetStreamId(),
			SequenceNumber: obe.GetSequenceNumber(),
		}); sendErr != nil {
			return fmt.Errorf("sending ACK: %w", sendErr)
		}
	}

	if len(streamRequests) == 0 {
		return nil
	}

	r.mu.Lock()
	r.requests = append(r.requests, streamRequests...)
	r.streamCount++
	r.mu.Unlock()

	log.Printf("Stream closed: collected %d requests", len(streamRequests))
	return nil
}

func (r *besRecorder) PublishLifecycleEvent(_ context.Context, _ *pb.PublishLifecycleEventRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
