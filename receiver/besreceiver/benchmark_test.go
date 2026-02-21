package besreceiver

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	bep "github.com/chagui/besreceiver/internal/bep/buildeventstream"
)

func BenchmarkProcessOrderedBuildEvent(b *testing.B) {
	sink := new(consumertest.TracesSink)
	tb := NewTraceBuilder(sink, nil, zap.NewNop(), TraceBuilderConfig{})
	tb.Start()
	defer tb.Stop()
	ctx := context.Background()

	// Pre-create invocation state.
	startOBE := makeOrderedBuildEvent(b, "inv-bench", 1, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_Started{Started: &bep.BuildEventId_BuildStartedId{}},
		},
		Payload: &bep.BuildEvent_Started{
			Started: &bep.BuildStarted{
				Uuid:      "uuid-bench",
				Command:   "build",
				StartTime: &timestamppb.Timestamp{Seconds: 1700000000},
			},
		},
	})
	if err := tb.ProcessOrderedBuildEvent(ctx, startOBE); err != nil {
		b.Fatal(err)
	}

	// Pre-build the action event to benchmark.
	actionOBE := makeOrderedBuildEvent(b, "inv-bench", 2, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_ActionCompleted{
				ActionCompleted: &bep.BuildEventId_ActionCompletedId{Label: "//pkg:lib"},
			},
		},
		Payload: &bep.BuildEvent_Action{
			Action: &bep.ActionExecuted{
				Success:   true,
				Type:      "Javac",
				StartTime: &timestamppb.Timestamp{Seconds: 1700000001},
				EndTime:   &timestamppb.Timestamp{Seconds: 1700000005},
			},
		},
	})

	b.ResetTimer()
	for range b.N {
		_ = tb.ProcessOrderedBuildEvent(ctx, actionOBE)
	}
}

func BenchmarkParseBazelEvent(b *testing.B) {
	data, _ := proto.Marshal(&bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_ActionCompleted{
				ActionCompleted: &bep.BuildEventId_ActionCompletedId{Label: "//pkg:lib"},
			},
		},
		Payload: &bep.BuildEvent_Action{
			Action: &bep.ActionExecuted{
				Success:  true,
				Type:     "Javac",
				ExitCode: 0,
			},
		},
	})
	wrapped := &anypb.Any{
		TypeUrl: "type.googleapis.com/build_event_stream.BuildEvent",
		Value:   data,
	}

	b.ResetTimer()
	for range b.N {
		_, _ = ParseBazelEvent(wrapped)
	}
}

func BenchmarkTraceIDFromUUID(b *testing.B) {
	for range b.N {
		_ = traceIDFromUUID("550e8400-e29b-41d4-a716-446655440000")
	}
}

func BenchmarkResolveTargetSpan(b *testing.B) {
	state := &invocationState{
		rootSpanID: pcommon.SpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 1}),
		targets:    make(map[string]pcommon.SpanID, 100),
	}
	for i := range 100 {
		label := fmt.Sprintf("//pkg:target-%d", i)
		state.targets[targetKey(label, fmt.Sprintf("cfg-%d", i))] = newSpanID()
	}

	b.Run("exact_match", func(b *testing.B) {
		for range b.N {
			_ = state.resolveTargetSpan("//pkg:target-50", "cfg-50")
		}
	})

	b.Run("root_fallback", func(b *testing.B) {
		for range b.N {
			_ = state.resolveTargetSpan("//pkg:nonexistent", "cfg-0")
		}
	})
}
