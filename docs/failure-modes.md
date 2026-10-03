# Failure Modes

## Concept

Every concurrent system fails at its boundaries: slow parties, departing
parties, and shutdown. This document enumerates the failure modes of this
broker and states, for each, the trigger, what happens, how the implementation
handles it, what an operator can observe, and the tradeoff.

## Mental model

Failures here are almost never crashes. They are stalls (a publisher waits),
gaps (a subscriber misses messages), or leaks (something is never released).
The design aims to make each of the three explicit, bounded and countable.

## 1. Slow subscriber

- Trigger: a consumer reads slower than the publish rate.
- What happens: its buffer fills (see 2).
- Handling: per-subscription policy. Other subscribers have their own buffers.
- Observable: `Subscription.Stats().Buffered` approaches `Capacity`;
  `SlowConsumerEvents` increases.
- Tradeoff: isolation costs one buffer per subscriber. Under Block, isolation is
  incomplete (see 12).

## 2. Full subscriber buffer

- Trigger: `len(out) == cap(out)` when a message arrives.
- What happens: depends on policy. Block waits; DropNewest discards the new
  message; DropOldest evicts the head; Disconnect closes the subscription.
- Handling: `Subscription.deliver`, tests `TestDropNewestPolicy`,
  `TestDropOldestPolicy`, `TestDisconnectPolicy`,
  `TestBlockPolicyBlocksUntilConsumerReads`.
- Observable: `SlowConsumerEvents`, `Dropped`, `Disconnected`, `Err()`.
- Tradeoff: loss versus latency versus explicit failure; see `backpressure.md`.

## 3. Subscriber cancellation

- Trigger: the subscription context is cancelled or times out.
- What happens: `context.AfterFunc` runs `terminate(ctx.Err())` in its own
  goroutine; the channel closes; the registry entry is removed.
- Handling: asynchronous, idempotent, first reason wins.
- Observable: `Err()` is `context.Canceled` or `DeadlineExceeded`;
  `SubscriberCount` drops shortly after (eventually, not instantaneously).
- Tradeoff: a Publish racing with cancellation can still enqueue one more
  message that is then readable from the closed channel's buffer. Cancellation
  does not mean "no more messages" until `Err()` is non-nil.

## 4. Subscriber closure during publish

- Trigger: `Close()` called while Publish is delivering, possibly while it is
  blocked on that subscriber.
- What happens: `close(done)` wakes the blocked sender; terminate waits for the
  sender to leave; then closes the channel.
- Handling: the send/close gate; the sender observes `resClosed` and continues
  with the next subscriber.
- Observable: Publish returns nil; no panic.
- Tests: `TestPublishDuringSubscriberClose`, `TestPublishWhileSubscribersChurn`.
- Tradeoff: the gate holds a read lock across a blocking send (justified in
  `architecture.md`).

## 5. Broker closure during publish

- Trigger: `Broker.Close()` while Publish is in flight.
- What happens: Publish either already passed the closed check (delivers to
  whatever is still open, returns nil) or sees `ErrClosed`. Blocked publishers
  are released because every subscription's `done` is closed.
- Observable: `PublishErrors` counts the `ErrClosed` returns. A nil return
  does not imply delivery to anyone.
- Tests: `TestBrokerCloseDuringPublish`, `TestBrokerCloseRacingManyPublishers`.
- Tradeoff: no linearization between a Publish and a Close. Callers who need a
  clean cut must coordinate (stop publishers, then Close).

## 6. Concurrent shutdown

- Trigger: several goroutines call `Close` (broker or subscription) at once.
- What happens: `sync.Once` runs shutdown once; others block until complete.
- Handling: every caller returns only when shutdown has finished.
- Tests: `TestBrokerConcurrentClose`, `TestSubscriberConcurrentClose`.
- Tradeoff: a caller of Close can block for as long as the winner takes, which
  is bounded by in-flight senders leaving.

## 7. Publisher cancellation

- Trigger: the publish context ends while waiting on a Block subscriber.
- What happens: Publish returns `ctx.Err()`; subscribers earlier in the
  snapshot already hold the message, later ones do not.
- Observable: `PublishErrors` increments; `Published` was already counted.
- Test: `TestBlockedPublishHonoursContext`.
- Tradeoff: no atomic fan-out. Making it atomic would require buffering the
  message separately per subscriber with a rollback, which is not worth it for
  at-most-once.

## 8. Goroutine leak

- Trigger: forgetting to close a subscription, or a consumer that never
  returns.
- What happens: the library itself starts no goroutines to leak. With a
  cancellable context, the `AfterFunc` registration holds the subscription
  until cancel or Close. The consumer goroutine, which is the caller's, blocks
  forever on `range sub.Messages()`.
- Handling: Broker.Close closes everything, ending consumer range loops.
  `terminate` stops the AfterFunc registration. `testutil.LeakCheck` verifies in
  tests.
- Observable: `ActiveSubscribers` that never returns to expected levels; the
  goroutine profile.
- Tradeoff: ownership is the caller's. Always pair Subscribe with Close or a
  context that ends, and always `defer broker.Close()`.

## 9. Memory growth

- Trigger: many subscribers times large buffers times large payloads; leaked
  subscriptions; unbounded topic names.
- What happens: memory grows linearly with `S * B * (64 + payload)`.
- Handling: `MaxBufferSize` bounds one subscription; empty topics are deleted
  from the map; buffers are bounded by construction (there is no unbounded
  queue anywhere).
- Observable: process memory; `Buffered/Capacity`.
- Tradeoff: no global memory limit. Callers control S and B.

## 10. Lock contention

- Trigger: many cores publishing, or Subscribe/unsubscribe churn on a large
  topic.
- What happens: throughput stops scaling; publish latency has spikes during
  O(N) registry writes.
- Handling: copy-on-write keeps reads cheap; no lock is held while blocking
  on the registry.
- Observable: mutex profile (`-mutexprofile`), benchmark scaling.
- Tradeoff and next steps: see `performance.md`.

## 11. Message loss

- Trigger: overflow policy, subscription ended before delivery, publisher
  cancellation mid fan-out, process exit.
- Handling: policy losses are counted; others are inherent to at-most-once.
- Observable: `Dropped`, `Disconnected`, `PublishErrors`.
- Tradeoff: the library gives visibility, not recovery.

## 12. Subscriber starvation

- Trigger: under Block, a stalled subscriber early in the delivery order
  delays all later subscribers on that topic.
- What happens: later subscribers receive each message only after the
  stalled one accepted it, or the publish context expires, in which case they
  may never receive it.
- Handling: documented and tested (`TestBlockPolicyCouplesSubscribersOnSameTopic`).
  Use a drop policy or Disconnect for subscribers that must not hold others
  back.
- Observable: rising publish latency; `SlowConsumerEvents`.
- Tradeoff: sequential delivery keeps the library goroutine-free and ordered,
  at the cost of this coupling. Parallel fan-out would fix it at the cost of
  goroutines and complexity.

## 13. Self-deadlock (caller error)

- Trigger: a consumer goroutine publishes, with a context that never ends, to
  a Block subscription that only that goroutine would drain.
- What happens: it waits for itself.
- Handling: cannot be detected by the library. Document; use deadlines.
