package besreceiver

import (
	"context"
	"fmt"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	pb "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	bep "github.com/chagui/besreceiver/internal/bep/buildeventstream"
	"github.com/chagui/besreceiver/internal/bep/commandline"
	"github.com/chagui/besreceiver/internal/bep/failuredetails"
)

// --- Mock gRPC stream ---

// mockBESStream implements pb.PublishBuildEvent_PublishBuildToolEventStreamServer
// for unit testing the stream handler without a real gRPC connection.
type mockBESStream struct {
	grpc.ServerStream

	ctx     context.Context
	events  []*pb.PublishBuildToolEventStreamRequest
	idx     int
	acks    []*pb.PublishBuildToolEventStreamResponse
	sendErr error // injected Send error
}

func (m *mockBESStream) Context() context.Context { return m.ctx }

func (m *mockBESStream) Send(resp *pb.PublishBuildToolEventStreamResponse) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.acks = append(m.acks, resp)
	return nil
}

func (m *mockBESStream) Recv() (*pb.PublishBuildToolEventStreamRequest, error) {
	if m.idx >= len(m.events) {
		return nil, io.EOF
	}
	req := m.events[m.idx]
	m.idx++
	return req, nil
}

func (m *mockBESStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockBESStream) SendHeader(metadata.MD) error { return nil }
func (m *mockBESStream) SetTrailer(metadata.MD)       {}
func (m *mockBESStream) SendMsg(any) error            { return nil }
func (m *mockBESStream) RecvMsg(any) error            { return nil }

// --- OrderedBuildEvent helpers (used by tracebuilder tests) ---

func makeOrderedBuildEvent(t testing.TB, invocationID string, seqNum int64, bepEvent *bep.BuildEvent) *pb.OrderedBuildEvent {
	t.Helper()
	data, err := proto.Marshal(bepEvent)
	if err != nil {
		t.Fatalf("failed to marshal BEP event: %v", err)
	}
	return &pb.OrderedBuildEvent{
		StreamId: &pb.StreamId{
			BuildId:      "build-1",
			InvocationId: invocationID,
		},
		SequenceNumber: seqNum,
		Event: &pb.BuildEvent{
			Event: &pb.BuildEvent_BazelEvent{
				BazelEvent: &anypb.Any{
					TypeUrl: "type.googleapis.com/build_event_stream.BuildEvent",
					Value:   data,
				},
			},
		},
	}
}

func makeBuildStartedOBE(t testing.TB, invID, uuid, command string, seqNum int64) *pb.OrderedBuildEvent {
	t.Helper()
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_Started{Started: &bep.BuildEventId_BuildStartedId{}},
		},
		Payload: &bep.BuildEvent_Started{
			Started: &bep.BuildStarted{
				Uuid:      uuid,
				Command:   command,
				StartTime: &timestamppb.Timestamp{Seconds: 1700000000},
			},
		},
	})
}

func makeBuildFinishedOBE(t testing.TB, invID string, seqNum int64, code int32, name string) *pb.OrderedBuildEvent {
	t.Helper()
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_BuildFinished{BuildFinished: &bep.BuildEventId_BuildFinishedId{}},
		},
		Payload: &bep.BuildEvent_Finished{
			Finished: &bep.BuildFinished{
				ExitCode:   &bep.BuildFinished_ExitCode{Name: name, Code: code},
				FinishTime: &timestamppb.Timestamp{Seconds: 1700000010},
			},
		},
	})
}

func makeWorkspaceStatusOBE(t testing.TB, invID string, seqNum int64, items map[string]string) *pb.OrderedBuildEvent {
	t.Helper()
	wsItems := make([]*bep.WorkspaceStatus_Item, 0, len(items))
	// Sort by key for deterministic wire-level order (BEP carries a repeated field).
	keys := make([]string, 0, len(items))
	for k := range items {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		wsItems = append(wsItems, &bep.WorkspaceStatus_Item{Key: k, Value: items[k]})
	}
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_WorkspaceStatus{
				WorkspaceStatus: &bep.BuildEventId_WorkspaceStatusId{},
			},
		},
		Payload: &bep.BuildEvent_WorkspaceStatus{
			WorkspaceStatus: &bep.WorkspaceStatus{Item: wsItems},
		},
	})
}

func makeBuildMetadataOBE(t testing.TB, invID string, seqNum int64, entries map[string]string) *pb.OrderedBuildEvent {
	t.Helper()
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_BuildMetadata{
				BuildMetadata: &bep.BuildEventId_BuildMetadataId{},
			},
		},
		Payload: &bep.BuildEvent_BuildMetadata{
			BuildMetadata: &bep.BuildMetadata{Metadata: entries},
		},
	})
}

// makePatternExpandedOBE builds an OrderedBuildEvent for a PatternExpanded
// payload. patterns are the expanded pattern strings carried on the event
// id (BuildEventId.pattern.pattern); targetCount is the number of synthetic
// TargetConfigured children used to represent how many targets the
// expansion resolved to. Child labels are irrelevant — the receiver only
// counts them.
func makePatternExpandedOBE(t testing.TB, invID string, seqNum int64, patterns []string, targetCount int) *pb.OrderedBuildEvent {
	t.Helper()
	children := make([]*bep.BuildEventId, 0, targetCount)
	for i := range targetCount {
		children = append(children, &bep.BuildEventId{
			Id: &bep.BuildEventId_TargetConfigured{
				TargetConfigured: &bep.BuildEventId_TargetConfiguredId{
					Label: fmt.Sprintf("//pattern_child:t%d", i),
				},
			},
		})
	}
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_Pattern{
				Pattern: &bep.BuildEventId_PatternExpandedId{Pattern: patterns},
			},
		},
		Children: children,
		Payload: &bep.BuildEvent_Expanded{
			Expanded: &bep.PatternExpanded{},
		},
	})
}

// makeAbortedOBE builds an OrderedBuildEvent for an Aborted payload. Aborted
// events can ride on any BuildEventId; this helper uses BuildFinished by
// default to exercise the "aborts replace another event ID" path. Pass a
// non-nil overrideID to use a different event id shape.
func makeAbortedOBE(t testing.TB, invID string, seqNum int64, reason bep.Aborted_AbortReason, description string, overrideID *bep.BuildEventId) *pb.OrderedBuildEvent {
	t.Helper()
	id := overrideID
	if id == nil {
		id = &bep.BuildEventId{
			Id: &bep.BuildEventId_BuildFinished{BuildFinished: &bep.BuildEventId_BuildFinishedId{}},
		}
	}
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: id,
		Payload: &bep.BuildEvent_Aborted{
			Aborted: &bep.Aborted{
				Reason:      reason,
				Description: description,
			},
		},
	})
}

func makeActionOBE(t testing.TB, invID, label, mnemonic string, seqNum int64, success bool) *pb.OrderedBuildEvent {
	t.Helper()
	exitCode := int32(0)
	if !success {
		exitCode = 1
	}
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_ActionCompleted{
				ActionCompleted: &bep.BuildEventId_ActionCompletedId{
					Label: label,
				},
			},
		},
		Payload: &bep.BuildEvent_Action{
			Action: &bep.ActionExecuted{
				Success:   success,
				Type:      mnemonic,
				ExitCode:  exitCode,
				StartTime: &timestamppb.Timestamp{Seconds: 1700000001},
				EndTime:   &timestamppb.Timestamp{Seconds: 1700000005},
			},
		},
	})
}

func makeTestResultOBE(t testing.TB, invID, label string, seqNum int64, status bep.TestStatus) *pb.OrderedBuildEvent {
	t.Helper()
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_TestResult{
				TestResult: &bep.BuildEventId_TestResultId{
					Label:   label,
					Run:     1,
					Shard:   0,
					Attempt: 1,
				},
			},
		},
		Payload: &bep.BuildEvent_TestResult{
			TestResult: &bep.TestResult{
				Status:           status,
				TestAttemptStart: &timestamppb.Timestamp{Seconds: 1700000002},
			},
		},
	})
}

func makeTargetConfiguredOBE(t testing.TB, invID, label string, seqNum int64) *pb.OrderedBuildEvent {
	t.Helper()
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_TargetConfigured{
				TargetConfigured: &bep.BuildEventId_TargetConfiguredId{Label: label},
			},
		},
		Payload: &bep.BuildEvent_Configured{
			Configured: &bep.TargetConfigured{TargetKind: "java_library rule"},
		},
	})
}

func makeTargetCompleteOBE(t testing.TB, invID, label string, seqNum int64, success bool, testTimeout time.Duration, failureMsg string, outputGroupCount int) *pb.OrderedBuildEvent {
	t.Helper()
	tc := &bep.TargetComplete{Success: success}
	if testTimeout > 0 {
		tc.TestTimeout = durationpb.New(testTimeout)
	}
	if failureMsg != "" {
		tc.FailureDetail = &failuredetails.FailureDetail{Message: failureMsg}
	}
	for i := range outputGroupCount {
		tc.OutputGroup = append(tc.OutputGroup, &bep.OutputGroup{Name: fmt.Sprintf("group-%d", i)})
	}
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_TargetCompleted{
				TargetCompleted: &bep.BuildEventId_TargetCompletedId{Label: label},
			},
		},
		Payload: &bep.BuildEvent_Completed{Completed: tc},
	})
}

// makeTargetConfiguredWithConfigOBE is like makeTargetConfiguredOBE but
// attaches a TargetCompleted child event id carrying a configuration id so
// handleTargetConfigured can resolve the matching Configuration.
func makeTargetConfiguredWithConfigOBE(t testing.TB, invID, label, configID string, seqNum int64) *pb.OrderedBuildEvent {
	t.Helper()
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_TargetConfigured{
				TargetConfigured: &bep.BuildEventId_TargetConfiguredId{Label: label},
			},
		},
		Children: []*bep.BuildEventId{{
			Id: &bep.BuildEventId_TargetCompleted{
				TargetCompleted: &bep.BuildEventId_TargetCompletedId{
					Label:         label,
					Configuration: &bep.BuildEventId_ConfigurationId{Id: configID},
				},
			},
		}},
		Payload: &bep.BuildEvent_Configured{
			Configured: &bep.TargetConfigured{TargetKind: "java_library rule"},
		},
	})
}

func makeConfigurationOBE(t testing.TB, invID, configID, mnemonic, platform, cpu string, isTool bool, seqNum int64) *pb.OrderedBuildEvent {
	t.Helper()
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_Configuration{
				Configuration: &bep.BuildEventId_ConfigurationId{Id: configID},
			},
		},
		Payload: &bep.BuildEvent_Configuration{
			Configuration: &bep.Configuration{
				Mnemonic:     mnemonic,
				PlatformName: platform,
				Cpu:          cpu,
				IsTool:       isTool,
			},
		},
	})
}

func makeOptionsParsedOBE(t testing.TB, invID, toolTag string, startup, cmd []string, seqNum int64) *pb.OrderedBuildEvent {
	t.Helper()
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_OptionsParsed{
				OptionsParsed: &bep.BuildEventId_OptionsParsedId{},
			},
		},
		Payload: &bep.BuildEvent_OptionsParsed{
			OptionsParsed: &bep.OptionsParsed{
				ToolTag:                toolTag,
				ExplicitStartupOptions: startup,
				ExplicitCmdLine:        cmd,
			},
		},
	})
}

func makeStructuredCommandLineOBE(t testing.TB, invID, label string, sections []*commandline.CommandLineSection, seqNum int64) *pb.OrderedBuildEvent {
	t.Helper()
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_StructuredCommandLine{
				StructuredCommandLine: &bep.BuildEventId_StructuredCommandLineId{
					CommandLineLabel: label,
				},
			},
		},
		Payload: &bep.BuildEvent_StructuredCommandLine{
			StructuredCommandLine: &commandline.CommandLine{
				CommandLineLabel: label,
				Sections:         sections,
			},
		},
	})
}

// chunkSection wraps a ChunkList section factory for tests.
func chunkSection(chunks ...string) *commandline.CommandLineSection {
	return &commandline.CommandLineSection{
		SectionType: &commandline.CommandLineSection_ChunkList{
			ChunkList: &commandline.ChunkList{Chunk: chunks},
		},
	}
}

// optionSection wraps an OptionList section factory for tests. Each option
// is represented by its combined_form.
func optionSection(options ...string) *commandline.CommandLineSection {
	opts := make([]*commandline.Option, 0, len(options))
	for _, o := range options {
		opts = append(opts, &commandline.Option{CombinedForm: o})
	}
	return &commandline.CommandLineSection{
		SectionType: &commandline.CommandLineSection_OptionList{
			OptionList: &commandline.OptionList{Option: opts},
		},
	}
}

func makeTestSummaryOBE(t testing.TB, invID, label string, seqNum int64, status bep.TestStatus, totalRun, shards, cached int32, dur time.Duration) *pb.OrderedBuildEvent {
	t.Helper()
	ts := &bep.TestSummary{
		OverallStatus:  status,
		TotalRunCount:  totalRun,
		ShardCount:     shards,
		TotalNumCached: cached,
	}
	if dur > 0 {
		ts.TotalRunDuration = durationpb.New(dur)
	}
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_TestSummary{
				TestSummary: &bep.BuildEventId_TestSummaryId{Label: label},
			},
		},
		Payload: &bep.BuildEvent_TestSummary{TestSummary: ts},
	})
}

func makeProgressOBE(t testing.TB, invID string, seqNum int64, stdout, stderr string) *pb.OrderedBuildEvent {
	t.Helper()
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_Progress{
				Progress: &bep.BuildEventId_ProgressId{OpaqueCount: int32(seqNum)},
			},
		},
		Payload: &bep.BuildEvent_Progress{
			Progress: &bep.Progress{Stdout: stdout, Stderr: stderr},
		},
	})
}

// makeFetchOBE builds a BuildEvent_Fetch ordered build event with the url
// carried on the BuildEventId (where Bazel actually puts it) and success
// on the payload. Defaults the downloader enum to UNKNOWN; use
// makeFetchOBEWithDownloader to exercise the HTTP / GRPC variants.
func makeFetchOBE(t testing.TB, invID, url string, seqNum int64, success bool) *pb.OrderedBuildEvent {
	t.Helper()
	return makeFetchOBEWithDownloader(t, invID, url, seqNum, success, bep.BuildEventId_FetchId_UNKNOWN)
}

// makeFetchOBEWithDownloader is the full-arity helper exposing
// FetchId.Downloader.
func makeFetchOBEWithDownloader(t testing.TB, invID, url string, seqNum int64, success bool, downloader bep.BuildEventId_FetchId_Downloader) *pb.OrderedBuildEvent {
	t.Helper()
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_Fetch{
				Fetch: &bep.BuildEventId_FetchId{Url: url, Downloader: downloader},
			},
		},
		Payload: &bep.BuildEvent_Fetch{
			Fetch: &bep.Fetch{Success: success},
		},
	})
}

func makeBuildMetricsOBE(t testing.TB, invID string, seqNum, wallMs, cpuMs int64) *pb.OrderedBuildEvent {
	t.Helper()
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_BuildMetrics{BuildMetrics: &bep.BuildEventId_BuildMetricsId{}},
		},
		Payload: &bep.BuildEvent_BuildMetrics{
			BuildMetrics: &bep.BuildMetrics{
				TimingMetrics: &bep.BuildMetrics_TimingMetrics{
					WallTimeInMs: wallMs,
					CpuTimeInMs:  cpuMs,
				},
				ActionSummary: &bep.BuildMetrics_ActionSummary{
					ActionsCreated:  100,
					ActionsExecuted: 80,
				},
			},
		},
	})
}

// makeExecRequestConstructedOBE builds an ExecRequestConstructed event for
// `bazel run` tests. argv and envVars are emitted as-is (argv as a list of
// strings, env as a list of name=value pairs). workingDir and shouldExec are
// set directly on the payload. The BEP proto types these fields as bytes, so
// we cast through []byte to match the generated Go shape.
func makeExecRequestConstructedOBE(t testing.TB, invID string, seqNum int64, argv []string, workingDir string, env map[string]string, shouldExec bool) *pb.OrderedBuildEvent {
	t.Helper()
	argvBytes := make([][]byte, 0, len(argv))
	for _, a := range argv {
		argvBytes = append(argvBytes, []byte(a))
	}
	// Sort env by key for deterministic emission — BEP map iteration order is
	// non-deterministic and assertions on the environment slice compare by
	// index.
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	envVars := make([]*bep.EnvironmentVariable, 0, len(env))
	for _, k := range keys {
		envVars = append(envVars, &bep.EnvironmentVariable{
			Name:  []byte(k),
			Value: []byte(env[k]),
		})
	}
	return makeOrderedBuildEvent(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_ExecRequest{
				ExecRequest: &bep.BuildEventId_ExecRequestId{},
			},
		},
		Payload: &bep.BuildEvent_ExecRequest{
			ExecRequest: &bep.ExecRequestConstructed{
				Argv:                argvBytes,
				WorkingDirectory:    []byte(workingDir),
				EnvironmentVariable: envVars,
				ShouldExec:          shouldExec,
			},
		},
	})
}

// sendAndACK sends a request on the stream, receives the ACK, and asserts the sequence number matches.
func sendAndACK(t testing.TB, stream pb.PublishBuildEvent_PublishBuildToolEventStreamClient, req *pb.PublishBuildToolEventStreamRequest, expectedSeq int64) {
	t.Helper()
	if err := stream.Send(req); err != nil {
		t.Fatalf("failed to send seq %d: %v", expectedSeq, err)
	}
	ack, err := stream.Recv()
	if err != nil {
		t.Fatalf("failed to receive ACK %d: %v", expectedSeq, err)
	}
	if ack.GetSequenceNumber() != expectedSeq {
		t.Errorf("expected ACK seq %d, got %d", expectedSeq, ack.GetSequenceNumber())
	}
}

// hasMetricNamed returns true if any metric in md has the given name.
func hasMetricNamed(md pmetric.Metrics, name string) bool {
	for i := range md.ResourceMetrics().Len() {
		rm := md.ResourceMetrics().At(i)
		for j := range rm.ScopeMetrics().Len() {
			sm := rm.ScopeMetrics().At(j)
			for k := range sm.Metrics().Len() {
				if sm.Metrics().At(k).Name() == name {
					return true
				}
			}
		}
	}
	return false
}

// processEvents feeds a sequence of ordered build events into the trace builder,
// failing the test on the first error.
func processEvents(ctx context.Context, t testing.TB, tb *TraceBuilder, events ...*pb.OrderedBuildEvent) {
	t.Helper()
	for _, e := range events {
		require.NoError(t, tb.ProcessOrderedBuildEvent(ctx, e))
	}
}

// findMetricIntValue searches a Metrics payload for a metric by name and returns
// the first data point's int value. Returns (0, false) if the metric is not found.
func findMetricIntValue(md pmetric.Metrics, name string) (int64, bool) {
	for i := range md.ResourceMetrics().Len() {
		rm := md.ResourceMetrics().At(i)
		for j := range rm.ScopeMetrics().Len() {
			sm := rm.ScopeMetrics().At(j)
			for k := range sm.Metrics().Len() {
				m := sm.Metrics().At(k)
				if m.Name() == name && m.Sum().DataPoints().Len() > 0 {
					return m.Sum().DataPoints().At(0).IntValue(), true
				}
			}
		}
	}
	return 0, false
}

// --- BES request helpers (used by receiver stream tests) ---

func makeBESRequest(t testing.TB, invocationID string, seqNum int64, bepEvent *bep.BuildEvent) *pb.PublishBuildToolEventStreamRequest {
	t.Helper()
	data, err := proto.Marshal(bepEvent)
	if err != nil {
		t.Fatalf("failed to marshal BEP event: %v", err)
	}
	return &pb.PublishBuildToolEventStreamRequest{
		OrderedBuildEvent: &pb.OrderedBuildEvent{
			StreamId: &pb.StreamId{
				BuildId:      "build-1",
				InvocationId: invocationID,
			},
			SequenceNumber: seqNum,
			Event: &pb.BuildEvent{
				Event: &pb.BuildEvent_BazelEvent{
					BazelEvent: &anypb.Any{
						TypeUrl: "type.googleapis.com/build_event_stream.BuildEvent",
						Value:   data,
					},
				},
			},
		},
	}
}

func makeBuildStartedReq(t testing.TB, invID, uuid, command string, seqNum int64) *pb.PublishBuildToolEventStreamRequest {
	t.Helper()
	return makeBESRequest(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_Started{Started: &bep.BuildEventId_BuildStartedId{}},
		},
		Payload: &bep.BuildEvent_Started{
			Started: &bep.BuildStarted{
				Uuid:      uuid,
				Command:   command,
				StartTime: &timestamppb.Timestamp{Seconds: 1700000000},
			},
		},
	})
}

func makeBuildFinishedReq(t testing.TB, invID string, seqNum int64, code int32, name string) *pb.PublishBuildToolEventStreamRequest {
	t.Helper()
	return makeBESRequest(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_BuildFinished{BuildFinished: &bep.BuildEventId_BuildFinishedId{}},
		},
		Payload: &bep.BuildEvent_Finished{
			Finished: &bep.BuildFinished{
				ExitCode:   &bep.BuildFinished_ExitCode{Name: name, Code: code},
				FinishTime: &timestamppb.Timestamp{Seconds: 1700000010},
			},
		},
	})
}

func makeBuildMetricsReq(t testing.TB, invID string, seqNum, wallMs, cpuMs int64) *pb.PublishBuildToolEventStreamRequest {
	t.Helper()
	return makeBESRequest(t, invID, seqNum, &bep.BuildEvent{
		Id: &bep.BuildEventId{
			Id: &bep.BuildEventId_BuildMetrics{BuildMetrics: &bep.BuildEventId_BuildMetricsId{}},
		},
		Payload: &bep.BuildEvent_BuildMetrics{
			BuildMetrics: &bep.BuildMetrics{
				TimingMetrics: &bep.BuildMetrics_TimingMetrics{
					WallTimeInMs: wallMs,
					CpuTimeInMs:  cpuMs,
				},
				ActionSummary: &bep.BuildMetrics_ActionSummary{
					ActionsCreated:  100,
					ActionsExecuted: 80,
				},
			},
		},
	})
}
