# Architecture

This document describes how the broker is built and, more importantly, why.
Read it alongside `pubsub/broker.go` and `pubsub/subscription.go`; the whole
implementation is about 400 lines.

## Concept

A publisher sends a message to a topic. Every subscriber currently registered
on that topic receives its own independent delivery. This is fan-out.

```
Publisher(s)
    |
    v
+---------+
| Broker  |   topics: map[string][]*Subscription
+---------+
    |  topic "orders"
    +----------+----------+
    |          |          |
    v          v          v
  Sub A      Sub B      Sub C
  [buf 100]  [buf 10]   [buf 64]
  Block      DropNewest DropOldest
    |          |          |
    v          v          v
 Consumer   Consumer   Consumer
```

## Mental model

Think of each subscription as a small private mailbox with a bounded number of
slots and a rule for what to do when the mailbox is full. The broker is only a
directory that maps topic names to mailboxes. Publishing is: look up the
mailboxes, then offer the message to each one according to its own rule.

The broker owns no delivery goroutines. Delivery runs on the publisher's
goroutine; consumption runs on the consumer's goroutine. The channel is the
only hand-off point between them.

## Data structures

### Broker

| Field | Type | Protected by | Purpose |
|---|---|---|---|
| `topics` | `map[string][]*Subscription` | `mu` | Registry: topic to subscribers |
| `closed` | `bool` | `mu` | Shutdown flag, checked by Publish and Subscribe |
| `closeOnce` | `sync.Once` | itself | Makes Close idempotent and makes concurrent callers wait |
| `stats` | atomic counters | atomics | Observability |
| `nextMsgID`, `nextSubID` | `atomic.Uint64` | atomics | ID generation |
| `cfg` | `config` | immutable after `New` | Default buffer size, clock |

There is no separate `Topic` type. A topic is just a key whose value is an
immutable slice of subscriptions. A topic type would add an indirection and a
second lock to reason about without adding behavior. The slice is replaced, never
mutated (copy-on-write), which is what allows Publish to iterate it without
holding a lock.

When the last subscriber of a topic leaves, the map entry is deleted, so a
workload with many short-lived topic names does not leak registry memory.

### Subscription

| Field | Type | Protected by | Purpose |
|---|---|---|---|
| `out` | `chan Message` (buffered) | channel semantics + `mu` gate | The consumer-facing buffer |
| `done` | `chan struct{}` | closed once by `terminate` | Wakes blocked senders |
| `mu` | `sync.RWMutex` | n/a | Send/close gate (see below) |
| `closed` | `bool` | `mu` | True once `out` has been closed |
| `once` | `sync.Once` | itself | Ensures one termination |
| `reason` | `error` | `once` and `done` ordering | Why it ended, returned by `Err()` |
| `stop` | `atomic.Pointer[func() bool]` | atomic | Cancels the `context.AfterFunc` |
| `delivered`, `dropped` | atomic | atomics | Per-subscription stats |

## Ownership

Channels:

- Who creates subscriber channels? `Subscribe`, once, with the requested
  capacity.
- Who may close them? Only `Subscription.terminate`, exactly once.
- Can publishers close them? No. Publishers only send (and, under DropOldest,
  receive).
- Can subscribers close their own channel? No. `Messages()` returns
  `<-chan Message`, so the type system forbids it. A consumer ends its
  subscription with `Close()`, which routes through `terminate`.
- Why not let the consumer close it, as in many tutorials? Because the sender
  side (the publisher) is the one that would panic. The rule of thumb in Go is
  that the party that sends, or a party that can prove no sender is active,
  closes. `terminate` proves it with the send/close gate below.

Mutexes:

- `Broker.mu` protects the registry and shutdown flag.
- `Subscription.mu` protects the invariant "no goroutine sends on `out` while or
  after it is closed".

Goroutines:

- The package starts no long-lived goroutines.
- Publish runs on the caller's goroutine. All delivery, including blocking, is
  on that goroutine.
- `context.AfterFunc`, used only when a subscription context can be cancelled,
  runs `terminate` in a goroutine owned by the `context` package. It exits when
  `terminate` returns. If the subscription ends first, `terminate` calls the
  stop function, which removes the registration, so nothing is left pinned.
- Consumer goroutines belong to the caller. They exit when `Messages()` closes.

## The send/close gate (preventing send-on-closed-channel)

Sending on a closed channel panics, and "check a flag then send" is a race. The
fix is to make check-and-send atomic with respect to close:

```go
// deliver (sender side)
s.mu.RLock()
defer s.mu.RUnlock()
if s.closed { return resClosed }
... select { case s.out <- m: ... }   // may block (Block policy)

// terminate (closer side)
close(s.done)          // 1. wake blocked senders
removeFromRegistry(s)  // 2. no new publishers will find s
s.mu.Lock()            // 3. wait for in-flight senders to leave
s.closed = true
close(s.out)           // 4. now provably no sender is in the critical section
s.mu.Unlock()
```

Many senders can hold the read lock concurrently, so publishers do not
serialize on one subscription. The write lock is taken once per subscription
lifetime.

Why this cannot deadlock: step 3 waits only for senders. A sender is either
doing a non-blocking operation, or blocked in a `select` that includes
`<-s.done`. Step 1 has already closed `done`, so every blocked sender wakes,
returns, and releases its read lock. Senders never wait on `terminate`.

The lock is held across a potentially blocking send on purpose. This is the one
deliberate exception to "never hold a lock across a blocking operation", and it
is safe only because (a) the lock is per subscription, so it cannot block
unrelated subscribers or topics, (b) the blocking send is interruptible by
`done` and by the publish context, and (c) only the closer wants the write
side. The registry lock `Broker.mu` is never held while sending.

## Flows

### Publish

```
Publish(ctx, topic, payload)
  1. validate topic, ctx.Err()
  2. Broker.mu.RLock; if closed -> ErrClosed; subs = topics[topic]; RUnlock
  3. published++; build Message (ID, timestamp)
  4. for each s in subs (no broker lock held):
       result = s.deliver(ctx, msg)       // holds s.mu.RLock only
       canceled   -> return ctx.Err()
       disconnect -> s.terminate(ErrSlowConsumer)   // after RUnlock
  5. return nil
```

Can a subscriber disappear while Publish iterates? Yes, and it is handled. The
slice `subs` is an immutable snapshot, so iteration is memory safe. If a
subscription terminates after the snapshot, `deliver` returns `resClosed` and
Publish moves on. A subscription that registers after the snapshot does not
receive this message. That matches the documented semantics (no replay, no
atomic membership across a publish).

### Subscribe

```
Subscribe(ctx, topic, opts)
  1. validate topic, buffer size, policy, ctx.Err()
  2. allocate Subscription and channel (outside any lock)
  3. Broker.mu.Lock; if closed -> ErrClosed
       topics[topic] = append(copy(old), s); active++
     Unlock
  4. if ctx can be cancelled: register context.AfterFunc(terminate)
```

Allocation happens before taking the lock to keep the critical section to one
slice copy. `active` is incremented under the lock so it can never be observed
negative.

### Unsubscribe (Close, context cancel, Disconnect, broker shutdown)

All four routes call `terminate(reason)`. `sync.Once` makes the first reason
win, and makes every concurrent caller block until termination is finished, so a
caller of `Close()` can rely on the channel being closed when it returns.

### Broker shutdown

```
Close()
  closeOnce.Do:
    Broker.mu.Lock; closed = true; collect all subs; reset map; Unlock
    for each sub: terminate(ErrClosed)      // no broker lock held
```

Setting `closed` and clearing the registry in one critical section means no
`Subscribe` can succeed after the snapshot of subscriptions was taken, so no
subscription can escape shutdown. This is tested by
`TestSubscribeDuringBrokerClose`.

`terminate` calls `removeSubscription`, which takes `Broker.mu`. That is why
`Close` must not hold `mu` while terminating, and why `removeSubscription`
returns early when `closed` is set (the map is already empty, so the O(N)
removal for each of N subscriptions is skipped).

## Context cancellation

| Context | Meaning |
|---|---|
| `Publish(ctx, ...)` | Bounds time spent waiting on Block-policy subscribers. Checked before work starts and while blocked. Does not undo deliveries already made. |
| `Subscribe(ctx, ...)` | Scopes the subscription's lifetime, it is not a timeout on Subscribe. Cancelling it closes the subscription (asynchronously) with `Err() == ctx.Err()`. |

Cancellation does not interrupt a non-blocking enqueue; there is nothing to
interrupt. It does interrupt a blocked send, via the `ctx.Done()` case.
Subscriber removal on cancellation is eventual, not immediate: it happens in
the `AfterFunc` goroutine, so a publish racing with cancellation can still
enqueue a message that the consumer will find in the buffer.

## Slow consumer and backpressure

See `backpressure.md`. In short: each subscription has a bounded buffer and an
explicit policy (Block, DropNewest, DropOldest, Disconnect). Block is the
default so that loss is never the implicit outcome.

## Alternatives considered

| Alternative | Why not |
|---|---|
| One goroutine per subscriber, fed by an unbounded queue | Hidden goroutines and unbounded memory: exactly what the spec warns against. Needs its own shutdown protocol. |
| One goroutine per topic doing fan-out | Serializes all publishers for a topic through one goroutine and one mailbox, adds a latency hop, and a slow subscriber still stalls the topic goroutine under Block. Reasonable for ordering guarantees, but unneeded here. |
| `sync.Map` for the registry | Optimized for write-once, read-many with disjoint keys. Our hot map is read-mostly, but we also need a consistent "closed" flag with the map, and the per-topic slice value needs atomic replacement. A mutex gives us both with simpler reasoning. |
| Holding the registry lock during delivery | Lets one blocked send stall Subscribe, Close and every publisher. |
| Closing channels from publishers | Needs coordination among N publishers; panics on races. |
| `reflect.Select` fan-in | Slow, opaque, unnecessary. |
