//go:generate mdatagen metadata.yaml

// Package besreceiver implements an OpenTelemetry Collector receiver that
// accepts Bazel Build Event Protocol (BEP) streams via the Build Event Service
// (BES) gRPC API and converts them into distributed traces and log records.
package besreceiver // import "github.com/chagui/besreceiver/receiver/besreceiver"
