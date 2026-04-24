# otelcol-bazel image

Source-of-truth scaffolding for the `otelcol-bazel` container image. The
artifacts in this directory are consumed by an external image monorepo that
owns CI and publication; this directory is what changes when the receiver or
its collector distribution is bumped.

## Contents

| File | Purpose |
|---|---|
| `Dockerfile` | Multi-stage build: `golang` + OCB → `distroless/static-debian12:nonroot`. |
| `builder-config.yaml` | OCB manifest. Pins `besreceiver` and all collector modules to remote versions — no `path: "."`. |
| `collector-config.yaml` | Default runtime config. Datadog Agent egress, loopback-only internal endpoints, operator-supplied BES bind. |
| `.dockerignore` | Restricts the build context to only the two YAML files. |

## Runtime configuration override

**The default config is baked in at `/etc/otelcol-bazel/config.yaml` and can
be overridden at runtime via `--config`.**

Rationale: a baked-in default keeps the image runnable out-of-the-box (CI
smoke tests, local `docker run`) and documents the intended shape. Runtime
override preserves the flexibility operators need without requiring a new
image build per environment-specific tweak (endpoints, TLS, auth tokens,
queue sizes).

Two override paths are supported:

1. **Bind-mount replacement**: mount a different file at the default path.
   ```bash
   docker run --rm \
     -v $(pwd)/my-config.yaml:/etc/otelcol-bazel/config.yaml:ro \
     -e BES_LISTEN_ADDR=0.0.0.0 \
     -e DD_AGENT_HOST=dd-agent \
     -p 8082:8082 \
     otelcol-bazel:ci-latest
   ```
2. **Alternative `--config` path**: mount at a different path and override the
   command. Supports multiple configs (OTel Collector merges them):
   ```bash
   docker run --rm \
     -v $(pwd)/overrides.yaml:/etc/otelcol-bazel/overrides.yaml:ro \
     -e BES_LISTEN_ADDR=0.0.0.0 \
     -e DD_AGENT_HOST=dd-agent \
     otelcol-bazel:ci-latest \
     --config=/etc/otelcol-bazel/config.yaml \
     --config=/etc/otelcol-bazel/overrides.yaml
   ```

### Required environment variables

| Name | Purpose |
|---|---|
| `BES_LISTEN_ADDR` | Interface for BES gRPC ingress. No safe default — the collector fails to start if unset. Use `POD_IP` (downward API) in Kubernetes, the container IP in Docker, or `127.0.0.1` for local testing. |
| `DD_AGENT_HOST` | Datadog Agent host for OTLP egress. In Kubernetes set from the downward API to the node IP; in Docker use the agent's service name. |

## Version-update procedure

Four versions MUST stay in sync when bumping. Changing one without the
others produces a broken OCB build, a Go-module resolution failure, or a
silently wrong binary.

1. **`besreceiver` gomod version** in `builder-config.yaml`
   ```yaml
   receivers:
     - gomod: "github.com/chagui/besreceiver vX.Y.Z"
   ```
   Point at a tagged release of this repository.

2. **`go.opentelemetry.io/collector/*` modules** in `builder-config.yaml`
   All `go.opentelemetry.io/collector/...` entries must share one release
   (e.g. all `v0.146.1`). Collector core modules refuse to cross-link across
   versions.

3. **Contrib release** in `builder-config.yaml`
   All `github.com/open-telemetry/opentelemetry-collector-contrib/...`
   entries must share one version (e.g. `v0.146.0`). The contrib release
   number tracks the core release, usually one patch behind or at parity.

4. **`OCB_VERSION` build arg** in `Dockerfile`
   Must equal the beta (`v0.x.y`) version used for collector modules. OCB
   refuses to build when its own version disagrees with the modules it
   emits.

After updating all four, rebuild locally to verify:

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t otelcol-bazel:dev \
  .
```

If OCB errors with "version mismatch" or Go fails to resolve a module, one
of the four is out of sync.

## Local build

```bash
# Single-arch (native)
docker build -t otelcol-bazel:dev .

# Multi-arch (requires buildx + QEMU for cross-build)
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t otelcol-bazel:dev \
  .
```

### `GOPRIVATE` for newly tagged releases

If the pinned `besreceiver` tag was just pushed, `sum.golang.org` may report a
checksum mismatch until the proxy indexes it (or the mismatch may persist if a
tag was force-moved). In that case build with `GOPRIVATE` so the Go toolchain
skips the public sum database for this module:

```bash
docker build --build-arg GOPRIVATE='github.com/chagui/*' -t otelcol-bazel:dev .
```

Once `sum.golang.org` reflects the current tag, drop the flag. Never ship an
image whose build process permanently bypasses sum checks for a dependency.

## Image properties

- **Base**: `gcr.io/distroless/static-debian12:nonroot` — no shell, no
  package manager, runs as uid/gid 65532.
- **Binary**: statically linked, `CGO_ENABLED=0`, built inside the Dockerfile
  so `GOARCH` follows the target platform (multi-arch).
- **Exposed port**: `8082` (BES gRPC). Health check (`13133`) and internal
  prometheus (`8888`) bind to loopback and are not exposed.
- **Entrypoint**: `/otelcol-bazel --config=/etc/otelcol-bazel/config.yaml`.
