package besreceiver

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	bep "github.com/chagui/besreceiver/internal/bep/buildeventstream"
)

func marshalBuildEvent(t *testing.T, event *bep.BuildEvent) *anypb.Any {
	t.Helper()
	data, err := proto.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}
	return &anypb.Any{
		TypeUrl: "type.googleapis.com/build_event_stream.BuildEvent",
		Value:   data,
	}
}

func TestParseBazelEvent(t *testing.T) {
	tests := []struct {
		name      string
		input     *anypb.Any
		wantError bool
		wantErrIs error // optional sentinel error check
		validate  func(t *testing.T, event *bep.BuildEvent)
	}{
		{
			name: "BuildStarted",
			input: marshalBuildEvent(t, &bep.BuildEvent{
				Id: &bep.BuildEventId{
					Id: &bep.BuildEventId_Started{
						Started: &bep.BuildEventId_BuildStartedId{},
					},
				},
				Payload: &bep.BuildEvent_Started{
					Started: &bep.BuildStarted{
						Uuid:    "test-uuid-123",
						Command: "build",
						StartTime: &timestamppb.Timestamp{
							Seconds: 1700000000,
						},
					},
				},
			}),
			validate: func(t *testing.T, event *bep.BuildEvent) {
				t.Helper()
				started := event.GetStarted()
				if started == nil {
					t.Fatal("expected BuildStarted payload")
				}
				if started.GetUuid() != "test-uuid-123" {
					t.Errorf("expected uuid test-uuid-123, got %s", started.GetUuid())
				}
				if started.GetCommand() != "build" {
					t.Errorf("expected command build, got %s", started.GetCommand())
				}
			},
		},
		{
			name: "ActionExecuted",
			input: marshalBuildEvent(t, &bep.BuildEvent{
				Id: &bep.BuildEventId{
					Id: &bep.BuildEventId_ActionCompleted{
						ActionCompleted: &bep.BuildEventId_ActionCompletedId{
							Label:         "//pkg:target",
							PrimaryOutput: "bazel-out/k8-fastbuild/bin/pkg/target.jar",
						},
					},
				},
				Payload: &bep.BuildEvent_Action{
					Action: &bep.ActionExecuted{
						Success:  true,
						Type:     "Javac",
						ExitCode: 0,
					},
				},
			}),
			validate: func(t *testing.T, event *bep.BuildEvent) {
				t.Helper()
				action := event.GetAction()
				if action == nil {
					t.Fatal("expected ActionExecuted payload")
				}
				if action.GetType() != "Javac" {
					t.Errorf("expected mnemonic Javac, got %s", action.GetType())
				}
				if !action.GetSuccess() {
					t.Error("expected success=true")
				}
			},
		},
		{
			name:      "NilInput",
			input:     nil,
			wantError: true,
			wantErrIs: ErrNilEvent,
		},
		{
			name:      "InvalidData",
			input:     &anypb.Any{Value: []byte("not a protobuf")},
			wantError: true,
		},
		{
			name:      "OversizedEvent",
			input:     &anypb.Any{Value: make([]byte, maxEventSize+1)},
			wantError: true,
			wantErrIs: ErrOversizedEvent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := ParseBazelEvent(tt.input)
			if tt.wantError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.wantErrIs != nil && !errors.Is(err, tt.wantErrIs) {
					t.Errorf("expected error wrapping %v, got %v", tt.wantErrIs, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.validate != nil {
				tt.validate(t, event)
			}
		})
	}
}

func FuzzParseBazelEvent(f *testing.F) {
	// Seed with a valid protobuf-encoded BuildEvent.
	valid := &bep.BuildEvent{
		Payload: &bep.BuildEvent_Started{
			Started: &bep.BuildStarted{Uuid: "seed", Command: "build"},
		},
	}
	data, _ := proto.Marshal(valid)
	f.Add(data)

	// Seed with empty and garbage inputs.
	f.Add([]byte{})
	f.Add([]byte("not a protobuf"))

	f.Fuzz(func(_ *testing.T, data []byte) {
		// ParseBazelEvent must not panic on any input.
		wrapped := &anypb.Any{Value: data}
		_, _ = ParseBazelEvent(wrapped)
	})
}
