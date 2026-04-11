// Package besio provides utilities for reading and writing length-delimited
// BES (Build Event Service) stream files. Each message in the file is a
// PublishBuildToolEventStreamRequest preceded by its varint-encoded byte length.
package besio

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	pb "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/protobuf/proto"
)

// maxMessageSize is the upper bound on a single serialized message (10 MB).
// Matches the limit in bepparser.go.
const maxMessageSize = 10 * 1024 * 1024

// WriteStream writes requests as length-delimited protobuf: for each message,
// a varint-encoded byte length followed by the marshaled bytes.
func WriteStream(w io.Writer, requests []*pb.PublishBuildToolEventStreamRequest) error {
	buf := make([]byte, binary.MaxVarintLen64)
	for i, req := range requests {
		data, marshalErr := proto.Marshal(req)
		if marshalErr != nil {
			return fmt.Errorf("marshaling request %d: %w", i, marshalErr)
		}
		n := binary.PutUvarint(buf, uint64(len(data)))
		if _, writeErr := w.Write(buf[:n]); writeErr != nil {
			return fmt.Errorf("writing length prefix for request %d: %w", i, writeErr)
		}
		if _, writeErr := w.Write(data); writeErr != nil {
			return fmt.Errorf("writing request %d: %w", i, writeErr)
		}
	}
	return nil
}

// ReadStream reads all PublishBuildToolEventStreamRequest messages from a
// length-delimited protobuf stream. Returns io.ErrUnexpectedEOF if the
// stream is truncated mid-message.
func ReadStream(r io.Reader) ([]*pb.PublishBuildToolEventStreamRequest, error) {
	br := bufio.NewReader(r)
	requests := make([]*pb.PublishBuildToolEventStreamRequest, 0)
	for {
		size, err := binary.ReadUvarint(br)
		if errors.Is(err, io.EOF) {
			return requests, nil
		}
		if err != nil {
			return requests, fmt.Errorf("reading length prefix: %w", err)
		}
		if size > maxMessageSize {
			return requests, fmt.Errorf("message size %d exceeds limit %d", size, maxMessageSize)
		}
		data := make([]byte, size)
		if _, readErr := io.ReadFull(br, data); readErr != nil {
			return requests, fmt.Errorf("reading message body: %w", readErr)
		}
		var req pb.PublishBuildToolEventStreamRequest
		if unmarshalErr := proto.Unmarshal(data, &req); unmarshalErr != nil {
			return requests, fmt.Errorf("unmarshaling request: %w", unmarshalErr)
		}
		requests = append(requests, &req)
	}
}
