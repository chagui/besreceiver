.PHONY: all build test test-short test-cover test-bench test-e2e lint vet clean fuzz fix generate ocb ci docker \
       compose-build compose-up compose-down compose-clean example record record-fixtures agent-status nilaway deadcode changelog \
       install uninstall pre-commit-install pre-commit

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

## test-e2e: Run end-to-end tests with real BES fixtures
test-e2e:
	$(GOTESTSUM) -- -count 1 -race -run TestE2E ./receiver/besreceiver/

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

## nilaway: Run Uber's nil-pointer flow analysis
nilaway:
	go run go.uber.org/nilaway/cmd/nilaway@latest ./...

## deadcode: Find unreachable functions via call-graph analysis
deadcode:
	go run golang.org/x/tools/cmd/deadcode@latest -test ./...

## ci: Run the full CI chain locally
ci: vet lint test build

## docker: Build the Docker image
docker:
	docker build -t otelcol-bazel:latest .

COMPOSE := docker compose -f examples/datadog/docker-compose.yaml

## compose-build: Build the dev Compose images
compose-build:
	$(COMPOSE) build

## compose-up: Start the Datadog example stack (with live reload)
compose-up:
	$(COMPOSE) up --build -d

## compose-down: Stop the Datadog example stack
compose-down:
	$(COMPOSE) down

## compose-clean: Stop the stack and remove volumes
compose-clean:
	$(COMPOSE) down -v

## example: Run the full Datadog example end-to-end
example: compose-up
	@echo "Waiting for collector to be ready..."
	@until wget -q --spider http://localhost:13133 2>/dev/null; do sleep 2; done
	cd examples/bazel-project && bazel test //... --bes_backend=grpc://localhost:8082

BESSTREAM_DIR := receiver/besreceiver/testdata/besstream

## record: Record a BES stream (usage: make record OUTPUT=path/to/file.besstream)
record:
	go run ./cmd/besrecord -output $(OUTPUT)

## record-fixtures: Re-capture all E2E test fixtures from the example Bazel project
record-fixtures:
	@mkdir -p $(BESSTREAM_DIR)
	@PORT_FILE=$$(mktemp); \
		echo "Starting BES recorder on ephemeral port..."; \
		go run ./cmd/besrecord \
			-output $(BESSTREAM_DIR)/build_and_test.besstream \
			-port-file $$PORT_FILE &\
		RECORD_PID=$$!; \
		while [ ! -s $$PORT_FILE ]; do sleep 0.1; done; \
		PORT=$$(cat $$PORT_FILE); \
		rm -f $$PORT_FILE; \
		echo "Recorder ready on port $$PORT"; \
		cd examples/bazel-project && bazel test //... \
			--bes_backend=grpc://localhost:$$PORT 2>&1; \
		kill $$RECORD_PID 2>/dev/null; \
		wait $$RECORD_PID 2>/dev/null; \
		echo "Fixtures captured."

## agent-status: Show Datadog Agent status
agent-status:
	$(COMPOSE) exec dd-agent agent status

## changelog: Generate CHANGELOG.md from conventional commits
changelog:
	git-cliff -o CHANGELOG.md

## install: Install besreceiver as a system service (systemd on Linux, launchd on macOS); pass BINARY=path/to/besreceiver
install:
	sudo BESRECEIVER_BINARY="$(BINARY)" BESRECEIVER_CONFIG="$(BESRECEIVER_CONFIG)" \
		packaging/install.sh $(INSTALL_ARGS)

## uninstall: Remove the besreceiver service, binary, and config (pass CONFIRM=1 to actually remove)
uninstall:
	sudo packaging/uninstall.sh $(if $(CONFIRM),--yes,) $(UNINSTALL_ARGS)

## pre-commit-install: Install prek and wire up the git pre-commit hook
pre-commit-install:
	@command -v prek >/dev/null || cargo binstall --locked prek
	prek install

## pre-commit: Run pre-commit hooks against all files (CI-equivalent)
pre-commit:
	prek run --all-files
