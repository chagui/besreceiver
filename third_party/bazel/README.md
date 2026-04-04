## Purpose

There is no official generated Go code for the Build Event Stream protobuf definition.
This directory contains the original protobuf definitions extracted from the
[Bazel source tree](https://github.com/bazelbuild/bazel/tree/master/src/main/protobuf).
Go code is generated with `buf generate` and committed to `internal/bep/`.

## Version

The vendored protos are pinned to a specific Bazel commit recorded in the
[VERSION](VERSION) file. See that file for the exact commit hash and nearest
release tag.

Proto3 wire compatibility means this receiver works with a range of Bazel
versions — older Bazel simply won't emit newer fields (zero-valued), and newer
Bazel's unknown fields are silently preserved on the wire but ignored by the
generated Go code. Update the protos when a new Bazel version adds events or
fields you want to handle.

**Tested with**: Bazel 7.x through 9.x.

## Updating

1. Identify the target Bazel commit or release tag.
2. Copy the proto files listed below from the Bazel source tree.
3. Flatten import paths (e.g., `src/main/protobuf/foo.proto` → `foo.proto`).
4. Replace `java_package`/`java_outer_classname` options with the appropriate
   `go_package` option for each file.
5. Update `VERSION` with the new commit hash, date, and nearest release.
6. Run `make generate` to regenerate the Go code in `internal/bep/`.
7. Run `make test` to verify nothing broke.

## Source files

Proto files in this directory and their Bazel source locations:

| Local file | Bazel source path |
|---|---|
| `build_event_stream.proto` | `src/main/java/com/google/devtools/build/lib/buildeventstream/proto/` |
| `package_load_metrics.proto` | `src/main/java/com/google/devtools/build/lib/packages/metrics/` |
| `analysis_cache_service_metadata_status.proto` | `src/main/java/com/google/devtools/build/lib/skyframe/serialization/analysis/proto/` |
| `action_cache.proto` | `src/main/protobuf/` |
| `command_line.proto` | `src/main/protobuf/` |
| `failure_details.proto` | `src/main/protobuf/` |
| `invocation_policy.proto` | `src/main/protobuf/` |
| `option_filters.proto` | `src/main/protobuf/` |
| `strategy_policy.proto` | `src/main/protobuf/` |
