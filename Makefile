.PHONY: all build test test-short test-cover test-bench lint vet clean fuzz fix generate ocb ci docker

# Use absolute path: GNU Make 3.81 (macOS default) doesn't propagate export PATH to recipe shells.
GOTESTSUM := $(shell go env GOPATH)/bin/gotestsum

all: test build

## build: Build the receiver (verify compilation)
build:
	go build ./...

## test: Run all tests with race detector (no cache)
test:
	$(GOTESTSUM) -- -count 1 -race ./...

## test-short: Run fast tests only (skip slow/timing-dependent tests)
test-short:
	$(GOTESTSUM) -- -short -race ./...

## test-cover: Run tests with coverage report
test-cover:
	$(GOTESTSUM) -- -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

## test-bench: Run benchmarks with memory allocation stats
test-bench:
	go test -bench=. -benchmem -run=^$$ ./...

## fuzz: Run fuzz tests for 30 seconds
fuzz:
	go test -fuzz=Fuzz -fuzztime=30s ./receiver/besreceiver/

## vet: Run go vet
vet:
	go vet ./...

## lint: Run golangci-lint
lint:
	golangci-lint run ./...

## fix: Modernize code using go fix (run until stable)
fix:
	go fix ./...
	go fix ./...

## tidy: Run go mod tidy
tidy:
	go mod tidy

## generate: Regenerate Go code from proto files and mdatagen output
generate:
	buf generate third_party/bazel/protobuf
	PATH="$$(go env GOPATH)/bin:$$PATH" go generate ./...

## clean: Remove build artifacts
clean:
	rm -rf build/ coverage.out

## ocb: Build the custom collector binary with OCB
ocb:
	builder --config builder-config.yaml

## ci: Run the full CI chain locally
ci: vet lint test build

## docker: Build the Docker image
docker:
	docker build -t otelcol-bazel:latest .
