package besio

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestRoundTrip(t *testing.T) {
	requests := []*pb.PublishBuildToolEventStreamRequest{
		{
			OrderedBuildEvent: &pb.OrderedBuildEvent{
				StreamId: &pb.StreamId{
					BuildId:      "build-1",
					InvocationId: "inv-1",
				},
				SequenceNumber: 1,
				Event: &pb.BuildEvent{
					Event: &pb.BuildEvent_BazelEvent{
						BazelEvent: &anypb.Any{
							TypeUrl: "type.googleapis.com/build_event_stream.BuildEvent",
							Value:   []byte("fake-payload-1"),
						},
					},
				},
			},
		},
		{
			OrderedBuildEvent: &pb.OrderedBuildEvent{
				StreamId: &pb.StreamId{
					BuildId:      "build-1",
					InvocationId: "inv-1",
				},
				SequenceNumber: 2,
				Event: &pb.BuildEvent{
					Event: &pb.BuildEvent_BazelEvent{
						BazelEvent: &anypb.Any{
							TypeUrl: "type.googleapis.com/build_event_stream.BuildEvent",
							Value:   []byte("fake-payload-2"),
						},
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	require.NoError(t, WriteStream(&buf, requests))

	got, err := ReadStream(&buf)
	require.NoError(t, err)
	require.Len(t, got, len(requests))

	for i := range requests {
		assert.True(t, proto.Equal(requests[i], got[i]),
			"request %d: got %v, want %v", i, got[i], requests[i])
	}
}

func TestReadStream_Empty(t *testing.T) {
	got, err := ReadStream(strings.NewReader(""))
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestReadStream_Truncated(t *testing.T) {
	req := &pb.PublishBuildToolEventStreamRequest{
		OrderedBuildEvent: &pb.OrderedBuildEvent{
			StreamId:       &pb.StreamId{BuildId: "b"},
			SequenceNumber: 1,
		},
	}
	var buf bytes.Buffer
	require.NoError(t, WriteStream(&buf, []*pb.PublishBuildToolEventStreamRequest{req}))

	// Truncate the buffer mid-message.
	truncated := buf.Bytes()[:buf.Len()/2]
	_, err := ReadStream(bytes.NewReader(truncated))
	assert.Error(t, err)
}

func TestReadStream_OversizedMessage(t *testing.T) {
	// Write a varint claiming a message bigger than maxMessageSize.
	var buf bytes.Buffer
	// Manually write a varint for a size exceeding the limit.
	size := uint64(maxMessageSize + 1)
	varintBuf := make([]byte, 10)
	n := 0
	for size >= 0x80 {
		varintBuf[n] = byte(size) | 0x80
		size >>= 7
		n++
	}
	varintBuf[n] = byte(size)
	n++
	buf.Write(varintBuf[:n])

	_, err := ReadStream(&buf)
	assert.ErrorContains(t, err, "exceeds limit")
}

func TestWriteStream_Empty(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteStream(&buf, nil))
	assert.Empty(t, buf.Bytes())
}

func TestWriteStream_WriterError(t *testing.T) {
	req := &pb.PublishBuildToolEventStreamRequest{
		OrderedBuildEvent: &pb.OrderedBuildEvent{
			StreamId:       &pb.StreamId{BuildId: "b"},
			SequenceNumber: 1,
		},
	}
	err := WriteStream(&failWriter{}, []*pb.PublishBuildToolEventStreamRequest{req})
	assert.Error(t, err)
}

// failWriter is an io.Writer that always returns an error.
type failWriter struct{}

func (failWriter) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}
