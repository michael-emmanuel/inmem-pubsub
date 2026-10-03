# Delivery Semantics

## Concept

What a system promises about messages is its delivery semantics. This library
promises as little as it can honestly deliver, and the guarantees below are the
only ones intended.

## Summary

| Property | Guarantee |
|---|---|
| Delivery | At-most-once per subscriber |
| Persistence | None. Everything is in process memory |
| Replay | None. A subscriber sees only messages published after it registered |
| Duplication | The library never delivers a message to the same subscription twice |
| Loss | Possible, only through the configured overflow policy, subscriber closure or broker shutdown; always counted when caused by a policy |
| Ordering, single publisher goroutine | Preserved per subscriber (subsequence if drops occur) |
| Ordering across publishers | None. Two subscribers may see concurrent publishers' messages interleaved differently |
| Fan-out | Each active subscriber is offered each message independently |
| Isolation | Subscribers have independent buffers; a subscriber's slowness affects others only through the Block policy's sequential delivery |
| Atomicity of fan-out | None. Publish can be cancelled part-way, leaving some subscribers with the message |

## Ordering

Publish on one goroutine does not return until the message has been offered
to every subscriber in the snapshot, so a later Publish from that goroutine is
enqueued after an earlier one in every subscriber's channel. The channel is
FIFO, so the consumer reads them in that order.

With drops, the surviving messages keep their relative order. DropNewest keeps a
prefix of the burst, DropOldest a suffix, Disconnect a prefix then closure.

With several publisher goroutines, no total order exists. `Message.ID` is
assigned at admission and is strictly increasing, but a publisher admitted
earlier may be delayed before enqueueing (descheduled, or blocked behind a
slow Block subscriber), so a subscriber can read IDs out of order. Do not use
`ID` as a sequence number for ordering; it is for correlation.

Per-publisher order is tested in `TestConcurrentPublish`.

## Duplication

None from the library. There is no retry path. At-most-once is only meaningful
relative to a consumer that does not itself re-publish.

## Loss

A message copy for a subscriber is lost when:

1. Overflow policy discards it (counted in Stats).
2. The subscription ended before enqueue (never subscribed, closed, cancelled,
   disconnected, broker closed). Not counted: the subscriber no longer exists.
3. The publisher's context ended before the broker reached that subscriber.
   Publish returned an error.
4. The process exits or crashes. All buffers vanish.

A message already in a subscriber's buffer when the subscription closes is
not lost: it stays readable until drained. If the consumer stops reading, it is
discarded with the channel by the garbage collector.

## Concurrency

Publish, Subscribe, Close and all consumers may run concurrently. The
registry snapshot decides which subscribers a particular Publish serves.
Subscribing concurrently with a Publish may or may not receive that message;
there is no defined order between a Subscribe and a concurrent Publish unless
the caller establishes one (for example, wait for Subscribe to return before
publishing).

## Subscriber isolation

- Buffers are per subscription.
- Overflow handling is per subscription.
- The payload value is shared, not copied. Mutating a pointer-typed payload
  from one subscriber is visible to all. Treat payloads as immutable.
- Under Block, delivery is sequential in registration order, so a stalled Block
  subscriber delays those registered after it. This is the one coupling.

## Shutdown behavior

`Broker.Close`: new Publish and Subscribe return `ErrClosed`; every active
subscription is closed with `Err() == ErrClosed`; blocked publishers are
released; buffered messages remain readable. A Publish that was already
past the closed check may return nil with zero deliveries.

`Subscription.Close`: after it returns, nothing new is enqueued; buffered
messages remain readable; `Err() == ErrSubscriberClosed`.

Context cancellation of a subscription: asynchronous. Removal happens shortly
after cancel. A message published in between may still be enqueued.

## What this is not

| System | What it adds that this library does not have |
|---|---|
| Kafka | Durable replicated log, consumer offsets, replay, partitions, consumer groups |
| NATS (core) | Network transport, subject wildcards, queue groups; JetStream adds persistence |
| Redis Pub/Sub | Network transport; also at-most-once and fire-and-forget; Redis Streams add persistence |
| Google Cloud Pub/Sub | Managed durable service, at-least-once with acks, retries, dead letters |
| RabbitMQ | Broker process, durable queues, acknowledgements, routing exchanges |

It is closest in spirit to Redis Pub/Sub or NATS core, minus the network and
the process boundary, and it shares their at-most-once semantics. It is
appropriate for in-process event distribution: UI-style event buses, cache
invalidation signals, internal notifications, plugin hooks. It is not
appropriate for anything that must survive a crash or cross a process boundary:
financial transactions, orders, billing events.
