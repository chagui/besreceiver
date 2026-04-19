# 0001. Deterministic SpanIDs

## Status

Accepted — 2026-04-19

## Context

TraceID is derived deterministically from the Bazel invocation UUID
(`SHA-256(uuid)[0:16]`), so replaying the same build through the receiver
always yields the same TraceID. SpanIDs, however, were generated from
`crypto/rand`, so the same event sequence produced a different span tree
on every run.

This makes trace-diffing between builds infeasible. Questions like "what
new span appeared between this green build and yesterday's red one?" require
stable IDs on both sides. They also make the emitted trace non-reproducible,
which hurts debugging when the same BEP fixture is replayed through tests
or the E2E harness.

## Decision

Derive each SpanID from a per-span identity tuple rather than randomness:

    spanID = SHA-256(strings.Join(parts, "\x00"))[16:24]

The identity tuple is the invocation UUID plus a role discriminator and the
stable fields from the owning BEP event:

| Span            | Identity parts                                     |
|-----------------|----------------------------------------------------|
| `bazel.build`   | uuid, "root"                                       |
| `bazel.target`  | uuid, "target", label                              |
| `bazel.action`  | uuid, "action", label, mnemonic, primary_output    |
| `bazel.test`    | uuid, "test", label, run, shard, attempt           |
| `bazel.metrics` | uuid, "metrics"                                    |

Bytes [16:24] are used so SpanIDs never overlap with the TraceID prefix
at [0:16] — a defensive choice against accidental collisions across the
two IDs.

## Consequences

The same BEP event stream replayed with the same invocation UUID now yields
a byte-identical span tree. Trace-diffing across runs becomes a deep equality
check on the (name, spanID, parentSpanID) triples. The E2E replay tests can
assert exact span-ID equality, catching regressions in parent-child
resolution that previously were invisible.

SpanIDs are 64 bits; the birthday-bound collision probability hits ~50%
around 2^32 ≈ 4.3B spans. Within a single invocation (thousands to low
millions of spans), collisions are statistically negligible; cross-invocation
collisions are harmless because TraceIDs differ.

Identity stability depends on BEP fields that Bazel treats as stable:
rename a mnemonic (`Javac` → `JavaCompile`) or change how primary outputs
are spelled, and the same build across Bazel versions produces different
SpanIDs. This is acceptable — trace-diffing across Bazel upgrades is not a
supported use case, and TraceID already carries the same implicit contract.
