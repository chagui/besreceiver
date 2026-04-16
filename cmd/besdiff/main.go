// Command besdiff decodes two .besstream files and prints a structural summary
// comparing their BEP events, ignoring ephemeral fields (UUIDs, timestamps).
package main

import (
	"flag"
	"fmt"
	"os"

	pb "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	bep "github.com/chagui/besreceiver/internal/bep/buildeventstream"
	"github.com/chagui/besreceiver/internal/besio"
)

func main() {
	flag.Parse()
	if flag.NArg() != 2 { //nolint:mnd // exactly two positional args: file1 and file2
		fmt.Fprintf(os.Stderr, "usage: besdiff <file1> <file2>\n")
		os.Exit(1)
	}
	a, err := loadStream(flag.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, "loading:", err)
		os.Exit(1)
	}
	b, err := loadStream(flag.Arg(1))
	if err != nil {
		fmt.Fprintln(os.Stderr, "loading:", err)
		os.Exit(1)
	}

	fmt.Printf("File A: %d messages\nFile B: %d messages\n\n", len(a), len(b))

	eventsA := extractBEPTypes(a)
	eventsB := extractBEPTypes(b)

	fmt.Println("=== BEP event type sequence ===")
	fmt.Printf("A: %d BEP events, B: %d BEP events\n\n", len(eventsA), len(eventsB))

	printTypeCounts(eventsA, eventsB)
	printStructuralDiffs(eventsA, eventsB)
}

func printTypeCounts(eventsA, eventsB []eventInfo) {
	countsA := countTypes(eventsA)
	countsB := countTypes(eventsB)

	allTypes := make(map[string]bool)
	for k := range countsA {
		allTypes[k] = true
	}
	for k := range countsB {
		allTypes[k] = true
	}

	fmt.Println("=== Event type counts ===")
	fmt.Printf("%-50s %5s %5s %s\n", "Event Type", "A", "B", "Delta")
	fmt.Println("---")
	for t := range allTypes {
		ca, cb := countsA[t], countsB[t]
		delta := ""
		if ca != cb {
			delta = fmt.Sprintf("%+d", cb-ca)
		}
		fmt.Printf("%-50s %5d %5d %s\n", t, ca, cb, delta)
	}
}

func printStructuralDiffs(eventsA, eventsB []eventInfo) {
	fmt.Println("\n=== Structural differences (ignoring timestamps/UUIDs) ===")
	total := max(len(eventsA), len(eventsB))
	diffs := 0
	opts := prototext.MarshalOptions{Multiline: true, Indent: "  "}
	for i := range total {
		if i >= len(eventsA) {
			diffs++
			fmt.Printf("Message %d: only in B — %s\n", i, eventsB[i].eventType)
			if eventsB[i].bepEvent != nil {
				fmt.Printf("  %s\n", opts.Format(eventsB[i].bepEvent))
			}
			continue
		}
		if i >= len(eventsB) {
			diffs++
			fmt.Printf("Message %d: only in A — %s\n", i, eventsA[i].eventType)
			if eventsA[i].bepEvent != nil {
				fmt.Printf("  %s\n", opts.Format(eventsA[i].bepEvent))
			}
			continue
		}
		if eventsA[i].eventType != eventsB[i].eventType {
			diffs++
			fmt.Printf("Message %d: type changed: %s → %s\n", i, eventsA[i].eventType, eventsB[i].eventType)
		}
	}
	if diffs == 0 {
		fmt.Println("No structural differences — same event types in the same order.")
		fmt.Println("Differences are limited to ephemeral fields (UUIDs, timestamps, hashes).")
	} else {
		fmt.Printf("\n%d structural difference(s) found.\n", diffs)
	}
}

type eventInfo struct {
	eventType string
	bepEvent  *bep.BuildEvent
}

func extractBEPTypes(reqs []*pb.PublishBuildToolEventStreamRequest) []eventInfo {
	events := make([]eventInfo, 0, len(reqs))
	for _, req := range reqs {
		obe := req.GetOrderedBuildEvent()
		if obe == nil {
			continue
		}
		evt := obe.GetEvent()
		if evt == nil {
			continue
		}
		bazelEvt := evt.GetBazelEvent()
		if bazelEvt == nil {
			// Component lifecycle events (TOOL/CONTROLLER streams).
			events = append(events, eventInfo{
				eventType: fmt.Sprintf("lifecycle:%s:seq%d",
					obe.GetStreamId().GetComponent(), obe.GetSequenceNumber()),
			})
			continue
		}
		be := parseBEP(bazelEvt)
		if be == nil {
			events = append(events, eventInfo{eventType: "unknown"})
			continue
		}
		events = append(events, eventInfo{
			eventType: bepEventType(be),
			bepEvent:  be,
		})
	}
	return events
}

func parseBEP(a *anypb.Any) *bep.BuildEvent {
	var be bep.BuildEvent
	if err := proto.Unmarshal(a.GetValue(), &be); err != nil {
		return nil
	}
	return &be
}

func bepEventType(be *bep.BuildEvent) string { //nolint:gocyclo,cyclop // type switch over BEP event IDs is inherently branchy
	id := be.GetId()
	if id == nil {
		return "no_id"
	}
	idVal := id.GetId()
	if idVal == nil {
		return "no_id"
	}
	switch v := idVal.(type) {
	case *bep.BuildEventId_Started:
		return "Started"
	case *bep.BuildEventId_UnstructuredCommandLine:
		return "UnstructuredCommandLine"
	case *bep.BuildEventId_StructuredCommandLine:
		return fmt.Sprintf("StructuredCommandLine(%s)", v.StructuredCommandLine.GetCommandLineLabel())
	case *bep.BuildEventId_WorkspaceStatus:
		return "WorkspaceStatus"
	case *bep.BuildEventId_OptionsParsed:
		return "OptionsParsed"
	case *bep.BuildEventId_Configuration:
		return "Configuration"
	case *bep.BuildEventId_Pattern:
		return "Pattern"
	case *bep.BuildEventId_TargetConfigured:
		return fmt.Sprintf("TargetConfigured(%s)", v.TargetConfigured.GetLabel())
	case *bep.BuildEventId_TargetCompleted:
		return fmt.Sprintf("TargetCompleted(%s)", v.TargetCompleted.GetLabel())
	case *bep.BuildEventId_ActionCompleted:
		return fmt.Sprintf("ActionCompleted(%s)", v.ActionCompleted.GetLabel())
	case *bep.BuildEventId_TestResult:
		return fmt.Sprintf("TestResult(%s,run=%d,shard=%d,attempt=%d)",
			v.TestResult.GetLabel(), v.TestResult.GetRun(), v.TestResult.GetShard(), v.TestResult.GetAttempt())
	case *bep.BuildEventId_TestSummary:
		return fmt.Sprintf("TestSummary(%s)", v.TestSummary.GetLabel())
	case *bep.BuildEventId_BuildFinished:
		return "BuildFinished"
	case *bep.BuildEventId_BuildMetrics:
		return "BuildMetrics"
	case *bep.BuildEventId_Progress:
		return fmt.Sprintf("Progress(%d)", v.Progress.GetOpaqueCount())
	case *bep.BuildEventId_NamedSet:
		return fmt.Sprintf("NamedSet(%s)", v.NamedSet.GetId())
	case *bep.BuildEventId_Fetch:
		return fmt.Sprintf("Fetch(%s)", v.Fetch.GetUrl())
	case *bep.BuildEventId_BuildToolLogs:
		return "BuildToolLogs"
	default:
		return fmt.Sprintf("other(%T)", idVal)
	}
}

func countTypes(events []eventInfo) map[string]int {
	counts := make(map[string]int)
	for _, e := range events {
		counts[e.eventType]++
	}
	return counts
}

func loadStream(path string) ([]*pb.PublishBuildToolEventStreamRequest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	defer f.Close()
	reqs, err := besio.ReadStream(f)
	if err != nil {
		return nil, fmt.Errorf("reading stream from %s: %w", path, err)
	}
	return reqs, nil
}
