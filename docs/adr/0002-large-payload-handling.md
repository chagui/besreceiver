# 0002. Large-payload handling (300 MB Progress events)

## Status

Proposed — 2026-04-24

## Context

A single Bazel BEP `Progress` event arrived at the besreceiver at ~299 MB on
a production build. It exhausted the default 4 MiB gRPC
`max_recv_msg_size`; `stream.Recv()` returned `RESOURCE_EXHAUSTED`, the
server closed the BES stream, and Bazel's upload aborted. The immediate
workaround is to raise `max_recv_msg_size_mib` on the receiver
(`configgrpc.ServerConfig` exposes it; see `receiver/besreceiver/config.go:13`)
and, on the Bazel side, to ensure `--bes_outerr_chunk_size` is not overridden
away from its 1 MiB default. That unblocks one build. It does not scale: gRPC
must allocate the full message into a single Go buffer before
`stream.Recv()` returns, and we copy the `anypb.Any` payload into a
`bep.BuildEvent` before dispatch (`receiver/besreceiver/tracebuilder.go:164-195`).
A 300 MB message therefore costs at minimum ~300 MB of heap per concurrent
invocation during the gRPC decode, plus another ~300 MB transiently while
the proto is unmarshaled. Ten concurrent large builds would blow a 4 GB
collector without warning.

### Why was it 300 MB? — Bazel-side chunking semantics

Bazel bounds Progress payload size with two flags:

| Flag | Default | Role |
|---|---|---|
| `--bes_outerr_buffer_size` | 10 240 B | Flush threshold: when buffered stdout/stderr reaches this size, a Progress event is emitted. |
| `--bes_outerr_chunk_size` | 1 048 576 B (1 MiB) | Maximum size of the *buffered* flush. |

The Bazel command-line reference documents an important caveat:

> Individual writes are still reported in a single event, even if larger than
> the specified value up to `--bes_outerr_chunk_size`.

So a single action that emits 300 MB of stdout/stderr in one `write(2)` can
still produce a single 300 MB Progress event. The 1 MiB chunk cap is an
upper bound on *buffered* content; it does not fragment an individual oversize
write. This matches the failure mode we observed: a code-generation step or
verbose compiler was the likely culprit. (Bazel also exposes
`--experimental_ui_max_stdouterr_bytes`, default 1 MiB, which caps the
*console* view but does not affect what BEP emits.)

### What the receiver currently does

`receiver/besreceiver/receiver.go:136-179` runs a tight
`stream.Recv()` / dispatch loop — one message in, processed synchronously on
the builder goroutine via `ProcessOrderedBuildEvent`, then ACK. The gRPC
unmarshal is done for us by `pb.PublishBuildEvent_…StreamServer`, so the
entire 300 MB lands on the heap before we see it. We do not currently handle
Progress events at all (`tracebuilder.processEvent` has no
`BuildEvent_Progress` arm — grep `tracebuilder.go:206-277`). Issue #60
introduces a handler that truncates the body to a configurable
`max_chunk_size` before emitting a log record. **Truncation happens after
gRPC has already allocated the full message.**

### Blast radius

Per concurrent invocation, peak memory is ≈ 2 × (largest Progress message) +
(event-builder state). At `max_recv_msg_size_mib: 512`, a worst-case
invocation consumes ~1 GiB transiently. For a collector sized at 1 GiB heap
with `memory_limiter.limit_mib: 512`, a single 300 MB Progress event is
enough to breach `spike_limit_mib` and begin dropping data pipeline-wide.

## Options considered

### A. Fix it at the source: keep `--bes_outerr_chunk_size` at 1 MiB and document it

**Idea.** The 1 MiB default is already sane. Operators who hit the 300 MB
case have either an action that emits a single giant write, or someone set
`--bes_outerr_chunk_size` to an enormous value. Document the behavior and
recommend keeping the default. Add runbook guidance to identify actions
producing giant writes (typically verbose logging in tests, compiler
`-v`/`-Wall`, codegen).

**Pros.** Zero receiver code change. Pushes the problem where it can actually
be bounded.

**Cons.** We cannot enforce Bazel flags on our operators' behalf. The
"individual write" caveat means even a correctly-configured Bazel can still
produce oversize Progress events; a misbehaving action defeats the flag.

**Cost.** ~1 doc PR. Low risk.

### B. Raise `max_recv_msg_size_mib` + rely on `memory_limiter`

**Idea.** Set a generous server-side cap (e.g. 64 MiB or 256 MiB) and let
`memory_limiter` reject new data when heap pressure rises. The
`memory_limiter` processor applies back-pressure at the pipeline boundary:
when memory exceeds `limit_mib`, it starts returning
`consumererror.NewPermanent(...)` from `ConsumeTraces`, which the BES
stream sees as a retryable error — the receiver closes the stream
(`receiver.go:152-159`) and Bazel's BES client retries.

**Pros.** Uses existing knobs. No new code in the receiver.

**Cons.** `memory_limiter` reacts *after* allocation, not before. The 300 MB
allocation still happens; only subsequent messages are refused. Under a
burst of concurrent oversize Progress events, we can OOM between the 1 s
check interval (`collector-config.yaml:45`). Also, Bazel's retry brings the
same oversize payload back with it — we loop until the action finishes
streaming or the client times out.

**Cost.** ~0 code. Operational risk: we cannot bound the cliff edge.

### C. Cap inbound via `max_recv_msg_size_mib` and reject oversize with a clear status

**Idea.** Set `max_recv_msg_size_mib` deliberately (say 16 MiB — well above
the 1 MiB default Progress chunk, well below the point where a single
message threatens heap budget). When Bazel exceeds it, gRPC returns
`ResourceExhausted` to the client. Document the tradeoff: the build's BEP
upload fails, but the build itself succeeds, and the collector stays up.

**Pros.** Deterministic. Single-knob story for capacity planning: peak RSS
budget per concurrent invocation ≈ 2 × `max_recv_msg_size_mib`.

**Cons.** Bazel's BES client treats `ResourceExhausted` as retryable, so
we'd see the same request re-arriving until
`--build_event_upload_max_retries` (default 4) is exhausted. The user sees a
failed BES upload and opaque `ERROR: The Build Event Protocol upload
failed` in their console. No automatic fallback.

**Cost.** ~1 config PR + doc. Low risk.

### D. Proto-aware inbound truncation

**Idea.** Intercept at the gRPC level. Register a custom
`grpc.ForceServerCodec` or custom `protobuf` unmarshaller that inspects the
length-prefixed fields of `BuildEvent` on the wire and skips (or
length-caps) the `Progress.stdout` / `Progress.stderr` fields without
allocating them in full. The rest of the `BuildEvent` (fields like
`event.id`, `children`) is tiny.

**Pros.** Bounds memory at the source without refusing the upload. A 300 MB
Progress becomes a ~64 KiB Progress with `bazel.progress.truncated=true`
— exactly what #60 wants, except it happens *before* allocation.

**Cons.** Significant engineering cost. Requires a hand-rolled proto
decoder for the `BuildEvent` wrapper or a fork of
`google.golang.org/protobuf`'s unmarshaller. Fragile against future BEP
field additions. gRPC-Go does not expose a streaming length-prefixed decoder
for individual fields; we'd have to read the framed message bytes ourselves
with `grpc.ForceServerCodec` and run our own parser over them. Likely
6-12 weeks of work including tests and maintenance-friendly
design.

**Cost.** High. Justified only if B/C prove insufficient in practice.

### E. Relax back-pressure by pushing decode off the hot path

**Idea.** Keep the gRPC 16 MiB cap (C), but also move the `proto.Unmarshal`
→ trace-builder dispatch onto a bounded worker pool so the receive loop
doesn't block. Today the loop is strictly serial per stream
(`receiver.go:136-179`, `tracebuilder.go:164-195`), which means
back-pressure is already in place by construction. No change here.

**Pros.** Verifies the serial model is correct. Not an option so much as a
confirmation.

**Cons.** n/a — we already have per-stream serialization. Parallelism
across invocations is naturally bounded by Bazel's one-stream-per-invocation
model plus `max_concurrent_streams` in `configgrpc`.

**Cost.** n/a.

## Does #60 solve this?

**No.** #60 truncates the *log record body* to `max_chunk_size` before
handing it to `logsConsumer`. By the time the `handleProgress` handler runs,
the `bep.BuildEvent` is already fully unmarshaled in memory. The 300 MB has
already been allocated twice: once as the gRPC frame buffer, once as the
`Progress` struct with Go string fields for stdout/stderr. #60 is the right
user-facing behavior for logs pipelines but does not change the memory
envelope.

Solving the *memory* problem requires either refusing oversize payloads at
the gRPC layer (options B/C) or intercepting before unmarshal (option D).

## configgrpc — what else is exposed

Reviewed `go.opentelemetry.io/collector/config/configgrpc.ServerConfig`
(pkg.go.dev/go.opentelemetry.io/collector/config/configgrpc):

| Field | Default | Useful here? |
|---|---|---|
| `MaxRecvMsgSizeMiB` | 0 → gRPC default 4 MiB | Yes — the primary knob. |
| `MaxConcurrentStreams` | unlimited | Secondary. Caps concurrent invocations per connection; helps bound aggregate memory. |
| `ReadBufferSize` | 512 KiB | Low-level TCP buffer. No effect on per-message memory. |
| `WriteBufferSize` | 512 KiB | Outbound only. |
| `Keepalive.ServerParameters.MaxConnectionAge` | off | Useful to recycle connections but orthogonal. |

There is no "streaming" or "fragmented decode" mode in configgrpc. Whatever
we do must be built on top of `MaxRecvMsgSizeMiB` and application-level
policy.

## Capacity planning back-of-envelope

At `max_recv_msg_size_mib: 16`:

- Peak per invocation ≈ 2 × 16 MiB = 32 MiB during decode.
- Invocation state (spans + buffered workspace/metadata) ≈ 5-50 MiB for a
  typical build; call it 50 MiB ceiling.
- Budget per invocation ≈ 80 MiB.
- 1 GiB collector heap, leaving ~400 MiB for gRPC/memory_limiter overhead
  → ~8 concurrent invocations.

At `max_recv_msg_size_mib: 64`: ~3 concurrent invocations per GiB.

At `max_recv_msg_size_mib: 512` (the workaround): ~1 concurrent invocation
per GiB, and any single build is one spike away from OOM.

**Implication.** 16 MiB is the right default recommendation for docs. 64
MiB is the ceiling we should advertise as "safe with 4 GiB heap". Above
that, operators are on their own.

## Recommendation

**Do first:**

1. **Set a sane receiver default.** Override `configgrpc`'s 0-is-4-MiB
   default in `createDefaultConfig` (`factory.go:33-46`) to
   `MaxRecvMsgSizeMiB: 16`. This is a 4× headroom over Bazel's 1 MiB chunk
   default; it absorbs the "individual write larger than chunk_size" caveat
   for reasonable cases while keeping per-invocation memory bounded.
   Document the change as a behavior change in release notes.

2. **Document Bazel-side flags.** README and `docs/` should cover:
   - Keep `--bes_outerr_chunk_size` at its 1 MiB default; do not raise it.
   - `--bes_outerr_buffer_size` controls flush cadence, not upper bound.
   - Actions that emit giant single writes defeat both flags — identify and
     fix them.
   - `--build_event_upload_max_retries` behavior on `ResourceExhausted`.

3. **Document memory planning.** Add `docs/memory-planning.md` (or extend
   `docs/architecture.md`) with the capacity math above and a worked
   example showing `max_recv_msg_size_mib` × concurrent invocations × heap.

4. **Land #60 as-is.** It is the correct user-facing log shape even though
   it does not address the memory problem. Truncating the log body is
   independently valuable.

**Defer until justified by data:**

5. **Proto-aware inbound truncation (option D).** Only worth the
   engineering cost if operators report OOMs despite the 16 MiB cap — i.e.
   if the single-write-larger-than-chunk-size pattern is common in practice.
   Track adoption via an internal metric `bes.grpc.oversize_rejections` so
   we know whether 16 MiB is too tight or if D is needed.

6. **Dedicated `progress.max_inbound_size` knob.** Today we lean on the
   global `max_recv_msg_size_mib`. A Progress-specific cap would need D to
   implement meaningfully.

## Open questions

1. **Does Bazel retry a `ResourceExhausted` BES failure forever, or does it
   give up after `--build_event_upload_max_retries`?** If the latter, option
   C is well-behaved. If the former (or if retries happen too fast), we'll
   amplify the problem under our own cap. Needs a reproduction test.

2. **What is the real distribution of Progress sizes in production?** We
   should add a histogram (`bes.events.bytes_received` bucketed by event
   type) to the receiver's internal metrics to validate the 16 MiB
   recommendation.

3. **Do we want symmetric limits on outbound (ACK) size?** Currently ACKs
   are tiny (just `stream_id` + `sequence_number`), so no. Flagged for
   revisit if we ever grow the ACK shape.

4. **Interaction with `bes_upload_mode: fully_async`.** When Bazel uploads
   asynchronously after the build completes, receiver failures are
   non-blocking from the user's perspective but still drop data. Worth
   calling out in docs.

5. **Should we raise a gRPC status code hint?** When gRPC returns
   `ResourceExhausted`, the wire status includes a `message` field; we
   could populate it with actionable text like
   `"Progress event exceeded 16 MiB; check --bes_outerr_chunk_size and
   inspect actions emitting large single writes"`. Requires confirming
   Bazel surfaces server-provided detail to the user console.
