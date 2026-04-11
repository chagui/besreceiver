# TaskForge -- Multi-Language Bazel Monorepo Example

A multi-language Bazel monorepo demonstrating a task processing platform.
Its primary purpose is to produce **rich, diverse Build Event Stream (BES)
events** when built, making it ideal for testing BES consumers like
[besreceiver](../../README.md).

## What it demonstrates

- **4 languages**: Go, Java, C++, Python -- each with library, binary, and test targets
- **Shared protobuf contract**: `task.proto` compiled to Java and C++ via native `java_proto_library` / `cc_proto_library`
- **Cross-component deps**: Go binary depends on Go library; C++ binary depends on C++ library + proto; Java binary depends on Java proto
- **OCI images**: container images for Go and C++ services using distroless base images (`rules_oci` + `rules_pkg`)
- **Shell tests**: passing, failing (manual), and sharded tests exercise different BES event shapes
- **Bazel 9 + bzlmod**: `MODULE.bazel` only, all rules loaded explicitly

## Directory layout

```
.
├── MODULE.bazel          # bzlmod deps: rules_go, rules_cc, rules_java,
│                         #   rules_python, protobuf, rules_oci, rules_pkg
├── .bazelrc              # --build_event_publish_all_actions
├── proto/                # Shared protobuf contract
│   ├── task.proto        #   Task, TaskResult, Priority, Status
│   └── BUILD.bazel       #   proto_library + java_proto + cc_proto
├── libs/
│   ├── hash/             # C++ FNV-1a hash library
│   └── tasklib/          # Go task model + validation
├── services/
│   ├── gateway/          # Go HTTP server (uses libs/tasklib)
│   ├── processor/        # Java batch processor (uses proto)
│   ├── reporter/         # Python CLI report generator
│   └── engine/           # C++ execution engine (uses libs/hash + proto)
├── images/               # OCI image targets (distroless)
└── tests/                # Shell integration tests
```

## Components

| Service | Language | Role | Dependencies |
|---------|----------|------|-------------|
| **gateway** | Go | HTTP API server for task submission and listing | `libs/tasklib` |
| **processor** | Java | Batch task processor with priority-based routing | `proto/task_java_proto` |
| **reporter** | Python | CLI that generates task summary reports | standalone (stdlib) |
| **engine** | C++ | High-performance task execution with fingerprinting | `libs/hash`, `proto/task_cc_proto` |

## Dependency graph

```
proto/task.proto
├── services/processor (Java)
└── services/engine   (C++)

libs/hash/ (C++)
└── services/engine

libs/tasklib/ (Go)
└── services/gateway
```

## Build commands

```bash
# Build everything
bazel build //...

# Run all tests (excludes manual fail_test)
bazel test //...

# Run individual services
bazel run //services/gateway
bazel run //services/processor
bazel run //services/reporter
bazel run //services/engine
```

## OCI images

```bash
# Build all images
bazel build //images/...

# Load into Docker
bazel run //images:gateway_load
bazel run //images:engine_load
```

Images use [distroless](https://github.com/GoogleContainerTools/distroless)
base images for minimal attack surface:
- `gcr.io/distroless/cc-debian12` for Go and C++ binaries
- `gcr.io/distroless/java21-debian12` for Java (available but not wired up yet)

## BES usage

```bash
# Send BES events to the besreceiver
bazel build //... --bes_backend=grpc://localhost:8082

# Or from the repo root, use the record-fixtures target
make record-fixtures
```

The `.bazelrc` enables `--build_event_publish_all_actions` so all action
events are emitted, including cache hits.

## BES event diversity

A full `bazel build //... && bazel test //...` produces events for:
- Proto compilation (protoc)
- C++ compilation and linking (CppCompile, CppLink)
- Go compilation (GoCompilePkg, GoLink)
- Java compilation (Javac, Turbine)
- Python target analysis
- OCI image assembly
- Shell test execution (including sharded tests)
- Unit tests in all four languages

## Requirements

- Bazel 9.0.2+ (all toolchains are hermetic via bzlmod)
