// This module exists solely to exclude the Bazel example project from the
// root module's Go tooling (go vet, go test, golangci-lint). All Go code
// in this directory is built by Bazel, not by the Go toolchain.
module taskforge

go 1.23.4
