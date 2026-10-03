# Buffering, Backpressure and Slow Consumers

## Concept

Four words are often confused. Keep them separate:

| Term | Meaning | In this library |
|---|---|---|
| Buffering | Temporarily storing messages between producer and consumer | The subscription's buffered channel (`BufferSize`) |
| Backpressure | The consumer's slowness propagating back to slow the producer | The Block policy: Publish waits |
| Dropping | Discarding messages to protect the system | DropNewest, DropOldest, and the message that triggers Disconnect |
| Blocking | A goroutine waiting for something | What Publish does under Block when the buffer is full |

Blocking is a mechanism. Backpressure is the effect you get when blocking
propagates to the source. Dropping is the alternative to propagating.
Buffering delays the decision but does not make it.

## Mental model

A bathtub with a faucet (publisher), a drain (consumer) and a finite tub
(buffer). If the faucet is faster than the drain, the tub fills, and no matter
how large the tub, it eventually overflows. You have exactly four choices when
it does: turn the faucet down (Block), let new water spill (DropNewest), pull
the plug on the oldest water to make room (DropOldest), or remove the tub
(Disconnect).

Worked example: publisher 1000 msg/s, subscriber 100 msg/s, buffer 100.

```
net fill rate = 1000 - 100 = 900 msg/s
time to fill  = 100 / 900  ~ 0.11 s
```

A buffer of 100 absorbs about a tenth of a second of this imbalance. A buffer
of 10,000 absorbs about 11 seconds. Neither changes the long-run outcome; the
buffer only buys time. Buffers are for bursts, where the average consumer rate
exceeds the average producer rate but the instantaneous rate does not.

## Implementation

Every subscription has `out chan Message` with capacity `BufferSize` and an
`OverflowPolicy`. The delivery path first attempts a non-blocking send. Only if
the buffer is full does it consult the policy, and it counts a slow-consumer
event at that point.

```
              deliver()
                 |
          non-blocking send
           /            \
        ok               full  --> slowEvents++
         |                         |
    delivered++      +-------------+-------------+--------------+
                     |             |             |              |
                   Block       DropNewest    DropOldest     Disconnect
                     |             |             |              |
             wait: space /    discard new   evict head,    discard new,
             done / ctx       dropped++     enqueue new     terminate(
                                            dropped++       ErrSlowConsumer)
```

### Block (default)

```
Publisher -> [ full buffer ] -x-> Publisher waits (until space, close, or ctx)
```

- Lossless. Faithful backpressure.
- The publisher inherits the latency of the slowest Block subscriber.
- Delivery is sequential, so subscribers registered after the slow one also
  wait (`TestBlockPolicyCouplesSubscribersOnSameTopic`). Subscribers on other
  topics are unaffected (`TestSlowConsumerDoesNotBlockUnrelatedTopicsOrControlPlane`).
- Always pass a context with a deadline when consumers can stall.

Why this is the default: the library should never lose data unless the caller
asked it to. Blocking is visible (publish latency rises, `SlowConsumerEvents`
climbs) and bounded by the context, whereas loss can go unnoticed.

### DropNewest

- Keeps the oldest messages. The consumer sees a gap at the tail of a burst.
- O(1), no interaction with the consumer.
- Appropriate when old data is still valuable, or when the consumer will
  re-sync by other means.

### DropOldest

- Keeps the freshest messages; the consumer sees a gap in the middle (newest
  data wins).
- Appropriate for state streams: price ticks, sensor readings, "latest config".
- Slightly more expensive: the publisher performs a channel receive.

### Disconnect

- Converts a silent gap into an explicit event: the consumer's channel closes
  and `Err()` is `ErrSlowConsumer`. The consumer decides whether to resubscribe,
  re-sync from a source of truth, or alert.
- Messages already buffered stay readable, so the consumer can drain, then
  notice the closure.
- NATS uses the same idea for slow consumers.
- Resubscribing loses everything published in between, as with any gap.

## Memory implications of buffer size

Worst-case buffered memory is approximately:

```
subscribers * BufferSize * (64 bytes per Message + retained payload size)
```

`MaxBufferSize` (1,048,576) caps one subscription at about 64 MiB of Message
slots before payloads. Large buffers also increase GC scan work because the
channel buffer holds pointers. A large buffer also hides a slow consumer for
longer; it delays the symptom, not the cause. Prefer a modest buffer plus an
explicit policy and alerting on `SlowConsumerEvents` and per-subscription
`Buffered/Capacity`.

## Observability

- `Stats().SlowConsumerEvents`: deliveries that found a full buffer.
- `Stats().Dropped`, `Subscription.Stats().Dropped`: discarded messages.
- `Stats().Disconnected`: subscriptions removed by Disconnect.
- `Subscription.Stats().Buffered / Capacity`: how close a subscriber is to
  overflowing right now.

Nothing is dropped without being counted.

## Failure modes

- Block with a stalled consumer and no deadline: publisher stalls
  indefinitely. Mitigation: deadlines, drop policy, or Disconnect.
- DropOldest with several publishers: evictions may remove another
  publisher's message. Acceptable under at-most-once semantics.
- Disconnect with a briefly slow consumer (GC pause): the burst that
  overflows disconnects it. Size the buffer for burst, not for steady state.

## Alternatives considered

- Unbounded queue per subscriber: removes the policy question and replaces it
  with out-of-memory. Rejected.
- Per-subscriber goroutine with an internal unbounded queue: same problem plus a
  hidden goroutine.
- Rate limiting at publish: orthogonal; can be layered by the caller.
- Per-subscription timeout on Block ("block for up to X, then drop"): possible
  with a publish context today, but the timeout applies to the whole fan-out
  rather than one subscriber.
