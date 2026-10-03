# In-Memory Pub/Sub in Go

A small, in-process publish/subscribe library with per-subscriber buffers,
explicit slow-consumer policies and a lifecycle that is safe under concurrent
publish, subscribe, close, cancellation and shutdown.

Requires Go 1.22 or newer (declared in `go.mod`; `context.AfterFunc` needs 1.21,
the tests use 1.22 loop-variable semantics). No third-party dependencies.

## Overview

```go
broker := pubsub.New()
defer broker.Close()

sub, err := broker.Subscribe(ctx, "orders", pubsub.SubscribeOptions{
    BufferSize: 100,
    Overflow:   pubsub.DropOldest,
})
if err != nil { /* ... */ }

go func() {
    for msg := range sub.Messages() {
        handle(msg)
    }
    log.Println("subscription ended:", sub.Err())
}()

err = broker.Publish(ctx, "orders", OrderCreated{ID: 42})
```

Public API (intentionally small):

| Symbol                                                                  | Purpose                                                |
| ----------------------------------------------------------------------- | ------------------------------------------------------ |
| `New(opts ...Option) *Broker`                                           | Create a broker (`WithDefaultBufferSize`, `WithClock`) |
| `Broker.Publish(ctx, topic, payload) error`                             | Fan a message out to the topic's subscribers           |
| `Broker.Subscribe(ctx, topic, SubscribeOptions) (*Subscription, error)` | Register a consumer                                    |
| `Broker.SubscriberCount(topic) int`                                     | Live subscribers on a topic                            |
| `Broker.Stats() Stats`                                                  | Counters                                               |
| `Broker.Close() error`                                                  | Idempotent shutdown                                    |
| `Subscription.Messages() <-chan Message`                                | Receive messages                                       |
| `Subscription.Err() error`                                              | Why it ended (nil while active)                        |
| `Subscription.Stats()`, `Topic()`, `Close()`                            | Per-subscription view and teardown                     |

## Why this project exists

Writing `ch <- msg` in a loop is easy. Getting it right when subscribers come
and go, consumers are slow, contexts are cancelled and the process shuts down
is the actual engineering problem. This repository is a compact, tested
answer to that problem, with the design decisions written down.

## Core concepts

- Topic: a name. Publishing to a topic with no subscribers is not an error.
- Subscription: one consumer's registration, with its own bounded buffer and
  overflow policy.
- Fan-out: each active subscriber is offered each message independently.
- Message: `ID`, `Topic`, `Payload` (shared by reference), `Timestamp`.

## Architecture

```
Publisher --> Broker --> topics["orders"] = [S1, S2, S3]   (immutable slice)
                              |    |    |
                           [buf] [buf] [buf]               (channel + policy)
                              |    |    |
                          consumers (caller-owned goroutines)
```

The broker has no goroutines of its own. Publish runs delivery on the caller's
goroutine. See `docs/architecture.md`.

## Concurrency model

- One `RWMutex` protects the registry and the shutdown flag.
- Publish snapshots the subscriber slice under the read lock and releases it
  before delivering; the slice is copy-on-write so the snapshot is immutable.
- Each subscription has a small `RWMutex` that gates sends against the channel
  close, so a send on a closed channel cannot happen.
- The two locks are never held together. Atomics are used for counters only.

Details, race analysis and deadlock reasoning: `docs/concurrency.md`.

## Fan-out model

Every subscriber has its own channel. One slow subscriber's buffer filling does
not change another's buffer. The payload value is shared, so treat it as
immutable. Under the Block policy delivery is sequential, which is the one way
a slow subscriber can delay others on the same topic.

## Buffering

`SubscribeOptions.BufferSize` sets the channel capacity (default 64, maximum
1,048,576). Buffers absorb bursts. They do not remove the need for a policy:
a sustained producer/consumer rate mismatch fills any finite buffer. Memory is
about `subscribers * BufferSize * 64 bytes` plus retained payloads.

## Backpressure

| Policy            | On full buffer                                | Loses data             | Publisher can wait |
| ----------------- | --------------------------------------------- | ---------------------- | ------------------ |
| `Block` (default) | Publish waits for room, close, or ctx         | No                     | Yes                |
| `DropNewest`      | Discard the incoming message                  | Yes, counted           | No                 |
| `DropOldest`      | Evict oldest, enqueue incoming                | Yes, counted           | No                 |
| `Disconnect`      | Close the subscription with `ErrSlowConsumer` | Yes, counted, explicit | No                 |

Buffering stores, blocking waits, backpressure is slowness propagating to the
source, dropping discards. See `docs/backpressure.md`.

## Slow consumer behavior

Run `go run ./examples/slow-consumer` to see all four policies on the same
workload. Overflow is observable through `Stats().SlowConsumerEvents`,
`Stats().Dropped`, `Stats().Disconnected`, and
`Subscription.Stats()`.

## Subscriber lifecycle

```
Created --> Active --+--> Closed (Close)            Err = ErrSubscriberClosed
                     +--> Closed (context done)     Err = ctx.Err()
                     +--> Closed (slow consumer)    Err = ErrSlowConsumer
                     +--> Closed (broker Close)     Err = ErrClosed
```

The first cause wins. Close is idempotent and concurrency-safe. Messages
buffered before closure stay readable, so `range` drains them before ending.

## Delivery semantics

At-most-once. No persistence, no replay, no redelivery. Per-publisher-goroutine
order is preserved per subscriber; there is no ordering across publishers.
Publish is not atomic across subscribers. See `docs/delivery-semantics.md`.

## Failure modes

Twelve modes plus one caller-error case, each with trigger, behavior,
handling, observability and tradeoff: `docs/failure-modes.md`.

## Observability

`Broker.Stats()` returns `Published`, `Delivered`, `Dropped`,
`SlowConsumerEvents`, `Disconnected`, `PublishErrors`, `ActiveSubscribers`,
all backed by `sync/atomic`. Fields are read independently, so a snapshot is not
cross-field consistent while traffic is flowing. No metrics library is imposed;
poll `Stats()` from your own exporter.

## Performance characteristics

Publish is O(N) in the topic's subscribers. Subscribe and unsubscribe are O(N)
(copy-on-write). Publish does not allocate in the library. Sample numbers from
a 1 vCPU sandbox are in `docs/performance.md`, along with how to read them and
why they are not general claims.

## Testing

```
go test ./...
go test -race ./...
go test -bench=. -benchmem ./...
```

The suite covers basic behavior, fan-out, buffering, every overflow policy,
lifecycle, shutdown, and races: concurrent publish, subscribe, unsubscribe,
publish-versus-close, subscribe-versus-broker-close, a randomized lifecycle
stress test with invariants, a fuzz test of input validation, and goroutine leak
checks. Waiting is bounded by `testutil.WaitTimeout`, so a deadlock fails with a
stack dump instead of hanging.

## Race detection

`go test -race -count=1 ./...` is the required validation command. The race
detector reports only races that execute, so the tests are written to force
interleavings and are worth repeating:

```
go test -race -count=20 ./pubsub/
GOMAXPROCS=1 go test -race -count=5 ./pubsub/
```

## Benchmarks

```
go test -run '^$' -bench=. -benchmem ./pubsub/
```

Benchmarks cover single and many subscribers, concurrent publishers, buffer
sizes, overflow policies, a 10,000-subscriber topic and registry mutation cost.
Each is described in `docs/performance.md`.

## Project structure

```
.
├── README.md
├── Makefile
├── go.mod
├── LICENSE
├── pubsub/                 library: broker, subscription, options, stats, errors, tests, benchmarks
├── internal/testutil/      Eventually, WaitTimeout, LeakCheck
├── examples/               basic, fanout, slow-consumer
└── docs/                   architecture, concurrency, backpressure, delivery-semantics,
                            performance, failure-modes, production-checklist, engineering-review
```

## Design tradeoffs

- Snapshot-then-deliver instead of locking during delivery: no registry lock
  across blocking sends, at the cost of needing a per-subscription send/close
  gate.
- Copy-on-write subscriber slices: allocation-free Publish, O(N) Subscribe.
- Sequential delivery on the publisher's goroutine: no hidden goroutines and
  per-publisher ordering, at the cost of coupling subscribers under Block.
- Block as default: loss is never implicit.
- No `Topic` type and no `ErrTopicNotFound`: publishing to an empty topic is
  normal in pub/sub.

## Limitations

- In-process only, no persistence, no replay, no acknowledgements.
- One global registry lock; no sharding.
- No wildcard topics, no consumer groups, no filtering.
- No atomic multi-subscriber delivery under cancellation.
- No global memory limit.

## Production considerations

Publish with deadlines if any subscriber uses Block. Choose a policy per
consumer. Alert on `SlowConsumerEvents` and `Buffered/Capacity`. Close
subscriptions or cancel their contexts. Stop publishers before `Close` for a
clean cut. Checklist: `docs/production-checklist.md`.

## Comparison with distributed messaging systems

| System         | Relationship                                                              |
| -------------- | ------------------------------------------------------------------------- |
| Kafka          | Durable replicated log with offsets and replay; this has none of that     |
| NATS core      | Similar at-most-once model and slow-consumer idea; adds network, subjects |
| Redis Pub/Sub  | Similar at-most-once fan-out; adds network and shared server              |
| Google Pub/Sub | Managed, durable, at-least-once with acks; this is none of those          |
| RabbitMQ       | Durable queues, routing, acks; this has none of those                     |

Use this library for in-process event distribution. Use one of those systems
when messages must survive a crash or cross processes.

## Engineer Review along with Q&As

`docs/engineering-review.md` contains architectural walkthrough,
concurrency, backpressure, semantics and scaling Q&A, the path from this design to a
distributed system, 32 follow-up discussions, and a decision table.

## Commands

```
make fmt        # gofmt -w .
make vet        # go vet ./...
make test       # go test ./...
make race       # go test -race -count=1 ./...
make fuzz       # 30s fuzz of validation
make benchmark  # go test -bench
make examples   # run all examples
make check      # vet + gofmt check + race tests (what CI runs)
```
