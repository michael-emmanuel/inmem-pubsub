# Concurrency

## Concept

The broker is called from arbitrary goroutines: publishers, subscribers,
closers and the `context` package. Every operation must be safe against every
other, including Close against everything. This document lists the shared
state, who protects it, and the arguments for freedom from races and
deadlocks.

## Shared state inventory

| State | Writers | Readers | Protection |
|---|---|---|---|
| `Broker.topics` (map) | Subscribe, removeSubscription, Close | Publish, SubscriberCount | `Broker.mu` (RWMutex) |
| Slice values inside `topics` | never mutated; replaced | Publish, after releasing `mu` | Immutability (copy-on-write) |
| `Broker.closed` | Close | Publish, Subscribe, removeSubscription | `Broker.mu` |
| `Subscription.closed` | terminate | deliver | `Subscription.mu` |
| `Subscription.out` (channel) | Publish (send, evict), terminate (close) | consumer (receive) | Channel ops are internally synchronized; close is gated by `Subscription.mu` |
| `Subscription.reason` | terminate (once) | Err | Written before `close(done)`; read only after `<-done` (happens-before via channel close) |
| `Subscription.stop` | Subscribe | terminate | `atomic.Pointer` |
| All counters, ID generators | many | many | `sync/atomic` |

## Lock inventory and order

There are two locks, and they are never held together:

```
Broker.mu          held for: map lookup, one slice copy, flag read/write
Subscription.mu    held (read) for: one delivery attempt, possibly blocking
                   held (write) for: set closed + close(out)
```

Acquisition order: there is none, because there is no nesting.

- Publish: `Broker.mu.RLock` ... `RUnlock`, then later `Subscription.mu.RLock`.
- terminate: `Broker.mu.Lock` (inside `removeSubscription`) ... `Unlock`, then
  `Subscription.mu.Lock`.
- Close: takes `Broker.mu`, releases it, then calls terminate.

No code path takes a `Subscription.mu` while holding `Broker.mu` or vice versa,
so a lock-order inversion cannot exist. If you ever need to nest them, the
order would have to be declared and enforced; this design avoids needing it.

## Why a mutex and not atomics or lock-free structures

The registry invariant spans several words: "the map, the slices it points to,
and the `closed` flag must agree". Atomics protect single words. Compare-and-swap
on a pointer to an immutable registry snapshot (an `atomic.Pointer` to a struct)
is a valid alternative that makes Publish fully lock-free, but then Subscribe
and Close become CAS retry loops and the shutdown argument ("after `closed`
is set no new subscription can be added") needs a second mechanism. For a
library whose point is to be understandable, an RWMutex with copy-on-write
slices captures nearly all of the benefit with simple proofs.

Atomics are used for statistics and ID generation only: independent counters
that are written often, read rarely, and whose individual values carry no
cross-field invariant. They must not replace the mutexes for registry state.

## Why RWMutex on the broker

Publish is the hot, frequent operation and needs shared access. Subscribe and
unsubscribe are rare writers. `sync.RWMutex` allows concurrent publishers.
Caveat: a pending writer blocks new readers (to prevent writer starvation), so
a long write section stalls all publishers. Our write sections are one slice
copy, O(N) in subscribers of that topic. See `performance.md`.

## Race analysis

### Publish vs unsubscribe

Publish snapshots the slice and releases the lock. Unsubscribe replaces the
slice in the map with a new one; it never edits the snapshot. The old slice is
garbage-collected when the last publisher drops it. Memory-safe.

Logically, the unsubscribed subscription may still be in a publisher's
snapshot. `deliver` handles it: it checks `closed` and `done` under the
subscription read lock and returns `resClosed`. After `Close()` returns,
`closed` is true, so nothing new is enqueued (`TestClosedSubscriberNeverReceivesNewMessage`).

### Publish vs broker Close

Close sets `closed` under the registry write lock. A Publish that read
`closed == false` before that point proceeds with a snapshot and delivers to
subscriptions, which Close is terminating concurrently. Each delivery either
completes before the subscription's write lock is taken, or sees `closed` or
`done` and is skipped. Such a Publish returns nil. A Publish that starts after
Close returns `ErrClosed`.

### Send vs close of the channel

Covered by the send/close gate in `architecture.md`.

### Concurrent Close of the same subscription

`sync.Once`. Losers block until the winner finishes and then return. Test:
`TestSubscriberConcurrentClose`.

### Cancellation vs Close vs Disconnect vs broker Close

All four race to `terminate`. Whichever wins sets `reason`. The rest are no-ops
that wait for completion. `Err()` therefore reports the first cause. Tests:
`TestCloseThenCancelContext`, `TestSubscriptionCloseAfterBrokerClose`.

### Subscribe vs broker Close

Subscribe checks `closed` and inserts under the same write-lock acquisition
that Close uses to set `closed` and take the registry snapshot. Either Subscribe
wins (and is in the snapshot, so Close terminates it), or Close wins (and
Subscribe returns `ErrClosed`). There is no third outcome.

### DropOldest vs consumer

Under DropOldest the publisher receives from the channel. A channel can have
many concurrent receivers, so this is legal. The consumer may take the message
the publisher intended to evict; the loop retries until the new message fits.
If several publishers evict concurrently, they may evict messages another
publisher just enqueued. All evictions are counted. Per-publisher ordering of
surviving messages is preserved because the channel is FIFO.

## Deadlock analysis

Publish can block only in the Block policy `select`, which is interruptible by
the subscription closing, the publish context ending, or buffer space. With
`context.Background()` and a consumer that never reads and never closes, a
Block publisher blocks forever. That is intended backpressure, not a library
deadlock, and it is bounded by passing a context with a deadline.

Self-deadlock hazard that the library cannot prevent: a consumer that publishes
to its own full Block subscription from the goroutine that is supposed to drain
it. Use a deadline, a drop policy, or a separate goroutine.

Close can wait only for in-flight senders (bounded because `done` is closed
first) and for the registry write lock (held for a slice copy). It cannot wait
on consumers, because it never waits for the channel to be drained.

Subscribe waits only for the registry write lock.

No lock is held while calling user code. The library invokes no user callbacks
under any lock (there are no callbacks).

## Lock contention points

1. `Broker.mu` read side: every Publish takes it briefly. Many cores hitting the
   same RWMutex cause cache-line bouncing on its reader count. Sharding the
   registry by topic hash would remove it.
2. `Broker.mu` write side: Subscribe and unsubscribe block all publishers for
   the duration of an O(N) slice copy.
3. `Subscription.mu` read side: every delivery takes it; contended only among
   publishers to the same subscription.
4. The channel's own internal lock: concurrent publishers to one subscription
   serialize on it; this is inherent to a channel.
5. Atomic counters: shared cache lines. Per-P sharded counters would help at
   very high publish rates; not implemented because the added complexity is not
   justified without a measured problem.

## Verification

- `go test -race ./...` is required. The race detector finds data races that
  were actually exercised; it does not prove their absence, so the tests are
  designed to maximize interleavings (stress tests, GOMAXPROCS variation,
  repeated runs with `-count`).
- Tests that wait use `testutil.WaitTimeout` so a deadlock fails fast with a
  goroutine dump instead of hanging until the global timeout.
- `testutil.LeakCheck` fails a test if goroutines remain after shutdown.
- Block-policy scenarios are made deterministic by polling the
  `SlowConsumerEvents` counter to know that a publisher has reached the full
  buffer, rather than sleeping and hoping.

## Alternatives considered

- Single-owner goroutine per broker (actor model) with all operations as
  messages: simplest to reason about but turns every Publish into a channel
  round trip and re-creates the backpressure problem at the actor's mailbox.
- Per-topic sharded registries: reduces contention; adds a shard-count
  parameter and complicates Close (must lock all shards in order).
- Atomic registry snapshot (`atomic.Pointer`): lock-free reads. A good next step
  if profiling shows `Broker.mu` read contention.
