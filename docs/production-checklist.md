# Production Checklist

This library is a production-quality in-memory component. It is not a
production distributed messaging system. The checklist below applies to the
former. Items that only a distributed system could satisfy are listed at the end
as explicit non-goals.

Legend: [x] done and verified in this repository, [~] provided but the caller
must apply it, [ ] not provided by design.

## Concurrency safety

- [x] All public methods safe for concurrent use.
- [x] Shared state inventory and protection documented (`concurrency.md`).
- [x] No send on closed channel by construction (send/close gate).
- [x] Idempotent, concurrent-safe Close for broker and subscription.

## Race detection

- [x] `go test -race -count=1 ./...` passes; also run repeatedly and under
      `GOMAXPROCS` 1, 2 and 8.
- [~] Run `-race` in your CI (`make check`). The repository ships a workflow.

## Deadlock avoidance

- [x] Two locks, never nested; no lock held across user code.
- [x] The only lock held across a blocking operation is per subscription, and
      the operation is interruptible.
- [~] Publish with a deadline whenever any subscriber uses Block.

## Goroutine lifecycle

- [x] No background goroutines. AfterFunc goroutine exits when terminate returns.
- [x] Leak checks in tests.
- [~] Consumers must exit on channel closure and callers must close
  subscriptions or cancel contexts.

## Shutdown behavior

- [x] Close rejects new work, terminates all subscriptions, releases blocked
      publishers, waits for completion.
- [~] Stop publishers before Close if you need a clean cut.

## Memory bounds

- [x] Every buffer is bounded; `MaxBufferSize` enforced.
- [x] Empty topics are removed.
- [~] No global limit on subscriptions or total buffered messages.

## Backpressure

- [x] Explicit per-subscription policy; default Block; four policies tested.
- [~] Choose the policy per consumer deliberately.

## Message loss

- [x] Loss only by explicit policy, subscription end, cancel mid fan-out, or
      process exit; policy loss counted.
- [ ] No durability, replay or redelivery.

## Observability

- [x] `Stats()` and `Subscription.Stats()` backed by atomics.
- [~] Export to your metrics system by polling `Stats()`. No Prometheus
  dependency is imposed.
- [ ] No tracing or logging hooks (deliberately; they would run on the hot path).

## Error handling

- [x] Sentinel errors, `errors.Is` compatible, documented per operation.

## Testing

- [x] Unit, concurrency, stress, invariant and fuzz tests.
- [x] Deterministic synchronization (no sleep-based assertions for positive
      outcomes).
- [~] Add tests for your own consumer logic; the library cannot verify it.

## Benchmarks

- [x] Provided, with documented meaning. Numbers in `performance.md` are samples
      from a 1 vCPU sandbox; re-measure on target hardware.

## Documentation

- [x] Architecture, concurrency, backpressure, semantics, performance, failure
      modes, engineering review guide.

## API stability

- [~] The API is small on purpose. It is not yet versioned; pin a version
  before relying on it. Planned-compatible: new Options, new Stats fields.

## Resource ownership

- [x] Channel closed only by the library; payload ownership is documented as
      shared and immutable-by-convention.

## Failure handling

- [x] All failure modes enumerated (`failure-modes.md`).
- [~] Handle `Err()` on subscription closure (resubscribe or alert).

## Explicit non-goals (distributed system territory)

Durability, replication, consumer offsets, replay, partitioning, leader
election, cross-process delivery, at-least-once or exactly-once semantics,
authentication and authorization, multi-tenant quotas. If you need any of
these, use a system built for it (see the engineering review guide's extension
section) rather than growing this library toward one.
