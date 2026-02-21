package besreceiver

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	bep "github.com/chagui/besreceiver/internal/bep/buildeventstream"
)

const (
	// maxEventSize is the maximum allowed size for a single BEP event payload (10 MB).
	maxEventSize = 10 * 1024 * 1024
)

var (
	// ErrNilEvent is returned when a nil event is passed to ParseBazelEvent.
	ErrNilEvent = errors.New("nil bazel event")

	// ErrOversizedEvent is returned when the event payload exceeds maxEventSize.
	ErrOversizedEvent = errors.New("bazel event exceeds maximum allowed size")
)

// ParseBazelEvent unmarshals an Any-wrapped BEP event into a typed BuildEvent.
// Bazel wraps each BEP event as an Any in the BES OrderedBuildEvent.event.bazel_event field.
func ParseBazelEvent(anyEvent *anypb.Any) (*bep.BuildEvent, error) {
	if anyEvent == nil {
		return nil, ErrNilEvent
	}

	if len(anyEvent.GetValue()) > maxEventSize {
		return nil, fmt.Errorf("%w: %d bytes (max %d)", ErrOversizedEvent, len(anyEvent.GetValue()), maxEventSize)
	}

	var event bep.BuildEvent
	// Use direct proto.Unmarshal on the value bytes. The Any type URL from Bazel
	// may not match the registered type, so UnmarshalTo can fail.
	if err := proto.Unmarshal(anyEvent.GetValue(), &event); err != nil {
		return nil, fmt.Errorf("failed to unmarshal bazel event: %w", err)
	}
	return &event, nil
}
