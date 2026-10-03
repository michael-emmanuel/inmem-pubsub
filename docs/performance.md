# Performance

## Concept

Publish must reach every subscriber of the topic, so its cost scales with the
number of subscribers. The goal of the design is not asymptotic cleverness; it is
to keep the constant small, avoid allocation on the hot path, and never let one
slow party hold a lock that others need.

## Complexity

Let N be the number of subscribers on the topic being used, T the number of
topics, B the buffer size, M the payload size.

| Operation | Time | Notes |
|---|---|---|
| Publish | O(N) | One map lookup, then one delivery attempt per subscriber. Cannot be O(1): each subscriber needs its own enqueue. Block can add unbounded wait. |
| Subscribe | O(N) | Copy-on-write copies the topic's subscriber slice. Allocates `B` message slots. |
| Unsubscribe | O(N) | Find and rebuild the slice. |
| Broker.Close | O(S) | S is total subscriptions. |
| SubscriberCount | O(1) | |
| Stats | O(1) | A few atomic loads. |

| Resource | Space |
|---|---|
| Registry | O(T + S) |
| Buffers | O(S * B) Message slots (64 bytes each), plus retained payloads |
| Per Publish | O(1) extra: the subscriber slice is a shared snapshot, not a copy |

Copy-on-write moves the O(N) cost from the frequent operation (Publish: no
allocation, no copy) to the rare ones (Subscribe and unsubscribe). If your
workload churns subscribers heavily on huge topics, this trade reverses.

## Benchmarks

Run:

```
go test -run '^$' -bench=. -benchmem ./pubsub/
```

What each measures:

| Benchmark | Measures |
|---|---|
| PublishNoSubscribers | Fixed overhead of Publish with an empty topic |
| Publish | One publisher, one draining subscriber |
| FanOut/subs=N | One publisher, N draining subscribers, scaling with N |
| PublishManySubscribers | Per-subscriber iteration cost with 10,000 non-draining subscribers on DropNewest (the full-buffer path) |
| ConcurrentPublish | Parallel publishers, 8 draining subscribers |
| BufferSize/buf=B | Effect of buffer size on hand-off cost with one draining subscriber |
| OverflowPolicy | Cost of the full-buffer path for each policy |
| SubscribeUnsubscribe/existing=N | Registry mutation cost versus topic size |

### Sample results (not a claim about your hardware)

Environment: Go 1.22.2, linux/amd64, Intel Xeon @ 2.10GHz, 1 vCPU
(`nproc` = 1), run in a shared sandbox, `-benchtime=1s`. A single CPU means
publishers and consumers time-slice on one core, so the concurrent numbers
below say nothing about multi-core scaling. Treat them as shape, not
performance claims. Re-run on your own hardware.

```
BenchmarkPublishNoSubscribers              121.5 ns/op   0 allocs/op
BenchmarkPublish                           212.2 ns/op   0 allocs/op
BenchmarkFanOut/subs=1                     224.2 ns/op   0 allocs/op
BenchmarkFanOut/subs=10                   1167   ns/op   0 allocs/op
BenchmarkFanOut/subs=100                 12211   ns/op   0 allocs/op
BenchmarkFanOut/subs=1000               195544   ns/op   1 allocs/op
BenchmarkPublishManySubscribers         521615   ns/op   0 allocs/op   (10,000 subs)
BenchmarkConcurrentPublish                 869   ns/op   0 allocs/op   (8 subs)
BenchmarkBufferSize/buf=1                  393   ns/op
BenchmarkBufferSize/buf=16                 244   ns/op
BenchmarkBufferSize/buf=256                209   ns/op
BenchmarkBufferSize/buf=4096               218   ns/op
BenchmarkOverflowPolicy/Block/drained      252   ns/op
BenchmarkOverflowPolicy/DropNewest/full    173   ns/op
BenchmarkOverflowPolicy/DropOldest/full    240   ns/op
BenchmarkSubscribeUnsubscribe/existing=10      670 ns/op    576 B/op
BenchmarkSubscribeUnsubscribe/existing=1000  12056 ns/op  16784 B/op
BenchmarkSubscribeUnsubscribe/existing=10000 121809 ns/op 164240 B/op
```

The "7 B/op" the tool reports in most rows is the benchmark boxing its loop
integer into `any`, not library allocation. Publish itself does not allocate
on the hot path.

### Reading the numbers

- Fan-out is linear: about 1.2 microseconds for 10 subscribers, 12 for 100, 195
  for 1,000. Roughly 120 to 200 ns per subscriber here, which includes channel
  send, the per-subscription lock, atomic counters, and waking consumer
  goroutines. Do not extrapolate; GC, cache effects and scheduler behavior
  change with scale.
- The fixed cost (about 120 ns) is validation, lock, map lookup, ID, clock
  and atomics.
- Buffer size matters most at the small end. With a buffer of 1, publisher and
  consumer must alternate (more goroutine hand-offs). From about 16 slots up the
  difference vanishes in this measurement, because a draining consumer is
  keeping up. Larger buffers matter for burst absorption and jitter, which this
  steady-state benchmark does not model.
- DropNewest is cheapest on overflow: no channel receive and no wake-up.
  DropOldest pays for a receive plus a send.
- Subscribe cost grows linearly with topic size as predicted, with 6
  allocations of which the slice copy dominates bytes.

## Lock contention

Where it can happen, in order of likelihood:

1. `Broker.mu` read side under many cores: the RWMutex reader count is a
   shared cache line. Only visible with many parallel publishers on a
   multi-core machine.
2. `Broker.mu` write side: Subscribe/unsubscribe on a very large topic holds
   the write lock for the duration of an O(N) copy, stalling publishers
   everywhere (the registry is shared across topics).
3. Many publishers to the same subscription, serializing on the channel lock.

Mitigations, not implemented because no measurement motivates them: shard the
registry by topic hash; publish through an `atomic.Pointer` registry snapshot;
per-P sharded counters.

## Channel overhead and allocation

A Go channel send/receive costs on the order of tens of nanoseconds uncontended,
more when it wakes a parked goroutine (a scheduler hand-off). This is the floor
per delivery and why the per-subscriber cost is not a few nanoseconds.

Allocation: Publish allocates nothing in the library. The `Message` is copied
by value into the channel buffer. Per-subscription allocation happens once: the
channel buffer (`B * 64` bytes), the struct, and the done channel. Payload
allocation belongs to the caller.

GC: large buffers of messages with pointer payloads increase the work of each
GC cycle, because the buffer is scanned. Drained buffers are cheap; full ones
are not.

## Subscribers versus throughput

Throughput per publisher is approximately `1 / (fixed + N * per_subscriber)`
for steady-state draining consumers. Aggregate message deliveries per second
stay roughly flat as N grows (each delivery costs about the same) while
publish rate drops as 1/N. Under Block, tail latency is bounded by the slowest
subscriber, so adding one slow subscriber changes throughput more than
adding a hundred fast ones.

## Why the design is shaped this way

- Snapshot-then-deliver: correctness first (no lock across blocking sends),
  and, via copy-on-write, no allocation per Publish.
- Per-subscription lock rather than one global send lock: delivery to one
  subscription never contends with delivery to another.
- No goroutine per subscriber: avoids 2KB+ stacks, scheduler load and an
  extra hand-off per message. The cost is that Publish does delivery work on
  the caller's goroutine.

## Alternatives

- Parallel fan-out (publish to N subscribers with a worker pool): lower latency
  for large N under Block, at the cost of goroutines, a join, and weaker ordering.
- Batching: amortize lock and wake-up costs; changes the API.
- Lock-free ring buffers: faster hand-off; much harder to make correct, and
  channels give `select`, cancellation and race-detector support for free.
