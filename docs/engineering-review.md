# In-Memory Pub/Sub Engineering Review Guide

## 1. Project Overview

An in-memory Pub/Sub library in Go. Publishers send to topics and each
topic fans out to subscribers that each own a bounded buffer and an explicit
overflow policy: block, drop newest, drop oldest, or disconnect. The basic API
is straight forward; the interesting part was the concurrency: making subscriber
lifecycle safe against concurrent publish, close, context cancellation and
broker shutdown without send-on-closed-channel panics, without holding locks
across blocking sends, and with slow consumers handled by policy instead of by
accident. Tested with the race detector, stress tests, fuzzing and leak
checks, and documented the delivery semantics at-most-once

## 2. Architecture Explanation

```
Publisher(s)
    |
    v
 Broker  --- mu (RWMutex) protects topics map + closed flag
    |
    v
 topics["orders"] = [ S1, S2, S3 ]     immutable slice (copy-on-write)
                      |   |   |
                      v   v   v
                    [buf][buf][buf]    one bounded channel + policy each
                      |   |   |
                  consumer goroutines (owned by the caller)
```

Talk track:

- Broker: a registry from topic to subscribers, one RWMutex, an atomic stats
  block. No goroutines.
- Topic registry: a map to an immutable slice. Subscribe and unsubscribe build a
  new slice. Publish takes a read lock, grabs the slice, releases the lock.
- Subscriber: a struct with a buffered channel, a policy, a `done` channel and a
  small lock that gates sends against close.
- Buffers and channels: the buffer is the channel's capacity. Created by
  Subscribe, closed only by the subscription's terminate routine.
- Locks: two kinds, never nested. Registry lock for membership; per-subscription
  lock for "no send during close".
- Lifecycle: Created, Active, then Closed by one of four causes (user Close,
  context, slow-consumer disconnect, broker shutdown). One `sync.Once` makes it
  single-shot and the first cause wins.

## 3. Deep Dive

### Publish flow

Validate. RLock, read `closed`, take the slice, RUnlock. Build the message.
For each subscriber, call `deliver`, which takes that subscriber's read lock,
tries a non-blocking send, and on a full buffer applies the policy. No broker
lock is held during delivery.

### Subscribe flow

Validate, allocate the channel outside the lock, then take the write lock only
to check `closed`, copy-append the slice and bump the active counter.

### Unsubscribe flow

All paths go to `terminate`: close `done` (wakes blocked senders), stop the
context hook, remove from registry, take the subscription write lock (waits for
in-flight senders), set `closed`, close the channel.

### Broker shutdown

Under the write lock set `closed` and take every subscription; release the lock;
terminate each. Doing the flag and the snapshot together is what guarantees no
Subscribe slips through.

### Context cancellation

Publish's context bounds waiting on Block subscribers. Subscribe's context
scopes the subscription via `context.AfterFunc`, which only costs a goroutine
when it fires.

### Slow consumers and backpressure

Per-subscription bounded buffer and explicit policy. Default Block gives
lossless backpressure; the others trade completeness for non-blocking publish.
Counters make every loss visible.

### Locking strategy

Snapshot-then-deliver. Never block under the registry lock. One deliberate
exception, the per-subscription read lock held across a blocking send, is safe
because it is interruptible by `done` and the context, and only the closer ever
wants the write side.

## 4. Concurrency Questions

**Where are the races?**
The candidates are: Publish iterating while membership changes (solved by
immutable snapshots), a send racing a channel close (solved by the send/close
gate), two closers racing (solved by `sync.Once`), Subscribe racing broker Close
(solved by checking and inserting under the same lock Close uses to set `closed`
and snapshot), and context cancel racing user Close (same `Once`). Counters are
atomics. I verified with `-race` and stress tests that deliberately create these
interleavings.

**How do you prevent send-on-closed-channel?**
Only `terminate` closes the channel, and it does so under the subscription's
write lock. Every send happens under the read lock after checking `closed`.
Read and write are mutually exclusive, so a sender is either fully before the
close (channel open) or fully after (sees `closed` and returns). Blocked
senders are woken first by closing `done`, so the write lock is acquired
promptly.

**What happens if Publish and Close happen simultaneously?**
Either Publish observes `closed` and returns `ErrClosed`, or it already has a
snapshot and attempts delivery. Each delivery either wins the race with that
subscription's close (message enqueued, remains readable) or sees it closed and
skips. A blocked Publish is released by `done`. The Publish may return nil with
zero deliveries; that is documented.

**What happens if Unsubscribe happens while Publish is iterating?**
Publish iterates a private immutable slice, so the iteration is memory-safe.
The unsubscribed subscription may still be in that slice, but `deliver` checks
`closed` and `done` under its lock and skips it. After `Close()` returns,
nothing new is enqueued (`TestClosedSubscriberNeverReceivesNewMessage`).

**Why did you choose a mutex?**
The invariant I need to protect, map plus slices plus `closed`, spans several
words, and I want readers (Publish) to share access. RWMutex expresses that. The
critical sections are tiny and never block on anything else.

**Why not sync.Map?**
`sync.Map` shines for keys written once and read many times, or goroutines
touching disjoint keys. My map has frequent reads, but I also need an atomic
relationship between the map and the `closed` flag, and the value (a subscriber
list) is replaced on each membership change. With `sync.Map` I would need a
second mechanism for shutdown correctness. It also gives up type safety.

**Why not one goroutine per topic?**
It serializes all publishers of a topic through one goroutine and adds a
hand-off per message. It does not remove the slow-consumer problem: if that
goroutine blocks on a subscriber, the whole topic stalls, so I would still need
buffers and policies. It does give strict total order per topic, which I
chose not to promise.

**Why not one goroutine per subscriber?**
Each needs a queue (bounded, so I still need a policy; unbounded, so OOM), a
shutdown protocol, and an extra scheduler hop per message. A buffered channel
is already the queue. No hidden goroutines keeps ownership clear.

**Where can lock contention occur?**
Registry read lock under many-core publishing (reader-count cache line);
registry write lock during Subscribe/unsubscribe of a large topic (O(N) copy
blocks all publishers); the channel lock when many publishers target one
subscriber; atomic counters' cache line. Fixes if measured: shard by topic,
atomic snapshot pointer, sharded counters.

**Can this deadlock?**
Library-internal: no. Two locks, never nested, no user code under lock; the
only blocking-under-lock case is interruptible by `done`, and `terminate`
closes `done` before it asks for the write lock. Caller-induced: a consumer
that publishes to its own full Block subscription with an unbounded context
waits on itself. That is a usage error that I document and mitigate with
deadlines.

**How did you verify race safety?**
`go test -race` across unit tests and stress tests (random subscribe, close,
cancel, publish across all policies), repeated with `-count` and different
`GOMAXPROCS`, plus fuzzed validation, goroutine leak checks, and invariants:
no negative active count, no cross-topic delivery, no enqueue after Close,
every successful Subscribe is closed by broker Close.

**Why is go test -race important?**
Races are non-deterministic and often invisible in normal runs. The detector
instruments memory accesses and reports unsynchronized conflicting access that
actually occurred in the run. It cannot prove absence, so the tests are
designed to provoke interleavings. It is also why some fields are atomics: the context-stop pointer is written by
Subscribe and may be read by the AfterFunc goroutine immediately, and an atomic
pointer gives the happens-before edge that a plain field would lack.

## 5. Backpressure Questions

**What happens when a subscriber is slow?**
Its buffer fills. Then its policy decides. Other subscribers are unaffected
except through Block's sequential delivery.

**What happens when its buffer fills?**
First message to find it full counts a slow-consumer event. Block waits;
DropNewest discards the new message; DropOldest evicts the head then enqueues;
Disconnect discards the message and closes the subscription with
`ErrSlowConsumer`.

**Should Publish block?**
By default yes, because silent loss is worse than visible latency, and the publish
context bounds it. If the publisher is a request handler that must not stall,
choose a drop policy for that subscriber.

**When would you drop messages?**
When freshness beats completeness (telemetry, UI updates), when consumers can
resync from a source of truth, or when one slow consumer must not hold the
rest.

**Why drop newest vs oldest?**
DropNewest keeps what the consumer would have seen first; cheap; right when
history matters. DropOldest keeps latest state; right when only the current
value matters (price, position).

**When would you disconnect a subscriber?**
When a gap would be dangerous if unnoticed: disconnect turns it into an
explicit event the consumer must handle, by resubscribing and resyncing.

**Does buffering solve backpressure?**
No. Buffering absorbs bursts. If the sustained consumer rate is below the
sustained producer rate, any finite buffer fills; it only buys
`capacity / (produce - consume)` seconds. Backpressure (or dropping) is the
steady-state answer.

Distinction to say aloud: buffering stores, blocking waits, backpressure is
slowness propagating to the source, dropping discards. Block is the mechanism
that yields backpressure; dropping is the refusal to propagate it.

## 6. Delivery Semantics Questions

**At-most-once or at-least-once?** At-most-once. No acks, no retries.

**Can messages be lost?** Yes: by overflow policy (counted), if the subscriber
ended before enqueue, if Publish was cancelled mid fan-out, or process exit.

**Can messages be duplicated?** Not by the library; nothing retries.

**Are messages ordered?** Per publisher goroutine, per subscriber, yes
(subsequence if drops). Across publishers, no; different subscribers can see
different interleavings. `Message.ID` is for correlation, not ordering.

**Does each subscriber see the same messages?** Same set only if nobody
drops and all were subscribed at the time; each has its own buffer and policy.

**What happens during shutdown?** New operations get `ErrClosed`; all
subscriptions close; blocked publishers are released; buffered messages remain
readable; in-flight Publish may return nil.

**Would you use this for financial transactions?** No. No durability,
no replay, no acknowledgement; a crash loses everything in flight. I would
use a transactional outbox with a durable log (Kafka, or SQS/SNS with
idempotent consumers). This library is for in-process notification where loss
on crash is acceptable.

## 7. Scaling Questions

| Dimension               | Effect                                                                                  | Why                                                    |
| ----------------------- | --------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| More subscribers        | Publish latency grows linearly (O(N)); memory grows with `S * B`                        | One enqueue per subscriber                             |
| More publishers         | Throughput scales until registry lock and channel contention                            | Shared RLock cache line; per-subscription channel lock |
| More topics             | Registry grows O(T); contention can drop if load is spread, but one global lock remains | Sharding would remove it                               |
| Larger messages         | Copy cost is constant (payload shared by reference), but retained memory grows          | `Message` is 64 bytes; payload pointer shared          |
| Larger buffers          | Better burst absorption, more memory, more GC scan, later detection of slowness         | Buffer holds pointers                                  |
| Higher consumer latency | Buffers fill; policy triggers; under Block, publish latency rises                       | Core backpressure behavior                             |

There is no network, so no serialization or partial failure, this is not a substitute for a distributed
broker. GC pressure comes from retained payloads in buffers, not from the
library's own allocation (Publish allocates nothing in the hot path). CPU is
dominated by channel operations and wake-ups, about 120 to 200 ns per
subscriber in my sandbox measurement (single vCPU; say that, do not oversell).

## 8. System Design Extensions

```
Single process        ->   Multiple processes      ->   Distributed Pub/Sub
in-memory channels         network transport             durable replicated log
per-sub buffers            per-subscriber sessions       offsets, partitions, groups
```

What changes, concept by concept:

| Capability                  | What it requires                                                                                                                                                      |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Durable storage             | Append-only log on disk; fsync policy; the log becomes the source of truth and "buffer" becomes "position in the log"                                                 |
| Consumer offsets            | Per-consumer cursor stored durably; backpressure becomes lag                                                                                                          |
| Replay                      | Retention of old data; subscribe from an offset instead of "from now"                                                                                                 |
| Partitioning                | Hash keys to partitions; order is per partition; parallelism = partitions                                                                                             |
| Replication                 | Leader plus followers, ISR or quorum; acknowledgement levels                                                                                                          |
| Leader election             | Consensus (Raft/ZooKeeper-style) for partition ownership                                                                                                              |
| Horizontal scaling          | Many brokers; partition placement and rebalancing                                                                                                                     |
| Load balancing              | Consumer groups: each partition to one consumer in a group                                                                                                            |
| Persistent queues           | Queue semantics with ack and visibility timeout                                                                                                                       |
| At-least-once               | Ack after processing; redeliver on timeout; consumers must be idempotent                                                                                              |
| Exactly-once processing     | At-least-once plus idempotent or transactional consumers (dedupe keys, transactional outbox); true exactly-once delivery is not achievable over an unreliable network |
| Cross-process communication | Wire protocol, framing, flow control, reconnect, authentication                                                                                                       |

Technology map: Kafka (partitioned replicated log, offsets, replay, consumer
groups); NATS (fast core with at-most-once, JetStream for persistence);
Redis Streams (log with consumer groups, simple ops); RabbitMQ (queues,
routing exchanges, acks); AWS SNS/SQS (SNS fan-out to durable SQS queues, at-least-once).

Slow-consumer policies are the in-process analogues of
real concepts (Block ~ flow control, Disconnect ~ NATS slow-consumer
disconnect, DropOldest ~ lossy ring buffer), but the in-memory implementation
provides none of the durability or distribution capabilities above.

## 9. Follow-Up Q&As

1. **Why use channels?** They give a bounded FIFO, `select` for cancellation
   and race-detector awareness. I do not reimplement a queue.
2. **Why not use a queue (slice plus mutex plus cond)?** More code, no
   `select` composition with `ctx.Done()` and `done`, and I would reimplement
   blocking and wakeup. Channels also let DropOldest work by receiving.
3. **How does fan-out work?** Registry maps topic to subscriber slice; Publish
   offers the message to each subscriber's own channel.
4. **What is backpressure?** The consumer's inability to keep up
   propagating to the producer, slowing it. Block implements it.
5. **How do you handle slow consumers?** Bounded buffer plus an explicit policy
   per subscription, with counters.
6. **What happens when a buffer is full?** Policy-dependent; see section 5.
7. **How do you prevent races?** Immutable snapshots, a send/close gate,
   `sync.Once`, atomics for counters, and `-race` stress testing.
8. **How do you prevent deadlocks?** No nested locks, no registry lock across
   blocking, blocking sends interruptible by `done` and the context, and
   `done` closed before the write lock is requested.
9. **What is the lock granularity?** One RWMutex for the whole registry, one
   RWMutex per subscription. Registry is coarse; subscription is fine-grained.
10. **Could you reduce lock contention?** Shard registry by topic hash; publish
    via `atomic.Pointer` to an immutable registry; shard counters per P. I
    would only do it after a mutex profile showed it.
11. **Why not sync.Map?** See section 4.
12. **How do you handle shutdown?** `sync.Once` Close: set closed and snapshot
    under the lock, terminate each outside the lock, wake blocked senders.
13. **How do you detect goroutine leaks?** `testutil.LeakCheck` compares
    goroutine counts after cleanup; `WaitTimeout` dumps stacks on a hang; in
    production, goroutine pprof and `ActiveSubscribers`.
14. **What happens if a subscriber closes during Publish?** Blocked sender is
    woken by `done`; terminate waits for it; the delivery returns `resClosed`;
    Publish continues.
15. **What are your delivery guarantees?** At-most-once, per-publisher order,
    no persistence, loss only by policy or lifecycle, counted for policy.
16. **How would you persist messages?** Add an append-only log; subscribers
    read by offset; a flush policy; the in-memory channel becomes a cache in
    front of the log. That is a different system.
17. **How would you scale horizontally?** Partition topics by key across
    nodes, with a router; each node runs this kind of broker per partition
    and replicates the log for durability.
18. **How would you implement replay?** Retain messages with sequence
    numbers; a new subscription takes a start offset; needs retention and
    compaction policy.
19. **How would you implement consumer groups?** Members of a group share a
    subscription: each message goes to one member (round-robin or by
    partition). In this design: a group subscription type with a shared
    channel read by several consumers.
20. **How would you partition topics?** Hash a key to one of P partitions, each
    with its own registry/lock (this also cuts contention); order is per
    partition.
21. **How would you implement metrics?** Already have atomic counters and
    `Stats()`. Add an exporter that polls and publishes to Prometheus/OTel;
    avoid per-message hooks on the hot path.
22. **How would you handle millions of subscribers?** Copy-on-write
    subscribe is O(N) and fan-out is O(N) on one goroutine: shard subscribers,
    fan out in parallel with workers per shard, use a ring buffer design to
    cut memory, and probably move to a networked tier with batching. Each
    buffer of 64 slots is 4 KB, so a million subscribers is gigabytes.
23. **What is the memory complexity?** O(T + S) registry plus
    O(S _ B _ 64 bytes) buffers plus retained payloads.
24. **What is the publish complexity?** O(N) in subscribers of the topic,
    O(1) extra space, because each subscriber needs its own enqueue.
25. **What would you change for production distributed use?** Replace channels
    with a durable log; add acks and offsets; partition and replicate; add
    network flow control; or just adopt Kafka/NATS/SQS.
26. **Why does Publish return nil if Close raced with it?** Because no
    linearization point is promised between them and returning an error for a
    publish that partly delivered would be misleading. Callers needing a clean
    cut stop publishers before Close.
27. **Why is Block the default?** Lossless and visible; changing the default
    to a drop policy would make loss the implicit behavior.
28. **Why delivered counts enqueue, not consumption?** The broker cannot know
    when the consumer processes; measuring enqueue is honest. Use `Buffered`
    for the gap.
29. **Why copy-on-write slices instead of copying per Publish?** Publish is the
    hot path; allocating a snapshot per call costs allocation and GC. COW moves
    cost to rare Subscribe/unsubscribe.
30. **Why AfterFunc instead of a goroutine per subscription?** A goroutine per
    subscription waiting on `ctx.Done()` is a hidden goroutine that exists for
    the whole subscription; AfterFunc has no goroutine until the context fires.
31. **Is Message.ID a sequence number?** No: assigned at admission, not
    enqueue; concurrent publishers can enqueue out of ID order.
32. **How would you test a deadlock?** Wrap waits in `WaitTimeout`; make
    blocking states observable (the `SlowConsumerEvents` poll) instead of sleeping.

## 10. Why This Design?

| Decision                                  | Reason                                        | Alternative                               | Tradeoff                                      |
| ----------------------------------------- | --------------------------------------------- | ----------------------------------------- | --------------------------------------------- |
| Mutex (RWMutex) instead of sync.Map       | Multi-word invariant, shared readers          | sync.Map, atomic snapshot                 | Reader-count contention at high core counts   |
| Buffered channels as queues               | Bounded, FIFO, select-friendly                | Slice+cond, ring buffer                   | Per-op channel overhead                       |
| Per-subscriber buffers                    | Isolation and per-consumer policy             | Shared ring with cursors (Disruptor-like) | More memory; simpler semantics                |
| Four overflow policies, Block default     | Make backpressure intentional; no silent loss | Single fixed policy                       | More surface; every policy needs tests        |
| context.Context for publish and subscribe | Standard cancellation, deadlines              | Custom timeout options                    | Cancel is not atomic across fan-out           |
| Atomic counters                           | Independent hot counters                      | Mutex-protected stats                     | No cross-field consistency                    |
| Snapshot subscribers before delivery      | Never hold registry lock while blocking       | Hold RLock during delivery                | Needs the send/close gate for lifecycle races |
| Copy-on-write registry                    | Allocation-free Publish                       | Copy per Publish                          | O(N) Subscribe                                |
| Idempotent Close via sync.Once            | Many concurrent closers (user, ctx, broker)   | Flags with CAS                            | Callers may block briefly for the winner      |
| No persistence                            | Honest scope; fast                            | Write-ahead log                           | Everything lost on crash                      |
| No distributed coordination               | Scope; single address space                   | Raft/gossip                               | Not a replacement for Kafka/NATS              |
| Max buffer cap                            | Bound memory per Subscribe                    | Unlimited                                 | Rejects legitimately huge buffers             |

## 11. Pitfalls

- Block couples subscribers sequentially on a topic.
- Registry lock is global; contention at scale would call for sharding.
- Publish is not atomic across subscribers under cancellation.
- Cancellation of a subscription is asynchronous.
- Benchmarks were run on a single vCPU, so they say nothing about parallel
  scaling.
- The send/close gate holds a read lock across a blocking send; it is safe
  but it is the first thing a reviewer should challenge, and now you can
  defend it.
