package pubsub

import "sync/atomic"

// Stats is a point-in-time view of broker counters.
//
// The fields are read with independent atomic loads, so a Stats value is not a
// consistent cross-field snapshot: while publishers are running,
// Delivered+Dropped may momentarily disagree with Published.
// Once the broker is quiescent the values are exact.
type Stats struct {
	// Published counts Publish calls that were admitted (valid topic, live
	// context, open broker), including ones that later returned a context
	// error part-way through fan-out.
	Published uint64

	// Delivered counts successful enqueues into a subscriber buffer. It does
	// not mean the consumer has read the message. Under DropOldest an evicted
	// message was previously counted as Delivered and is additionally counted
	// in Dropped.
	Delivered uint64

	// Dropped counts messages discarded by an overflow policy (DropNewest,
	// DropOldest eviction, or the message that triggered Disconnect).
	Dropped uint64

	// SlowConsumerEvents counts deliveries that found the subscriber's buffer
	// full, regardless of the policy applied afterwards.
	SlowConsumerEvents uint64

	// Disconnected counts subscriptions removed by the Disconnect policy.
	Disconnected uint64

	// PublishErrors counts Publish calls that returned a non-nil error.
	PublishErrors uint64

	// ActiveSubscribers is the number of live subscriptions across all topics.
	ActiveSubscribers int64
}

// SubscriptionStats is a point-in-time view of one subscription.
type SubscriptionStats struct {
	Delivered uint64 // successful enqueues into this subscription's buffer
	Dropped   uint64 // messages discarded for this subscription
	Buffered  int    // messages currently waiting in the buffer
	Capacity  int    // buffer capacity
}

// counters holds broker-wide statistics. Each field is independent, written on
// the hot path and read rarely, which is the case atomics are for. They never
// guard the subscriber registry: that is a multi-word invariant and needs a
// mutex (see docs/concurrency.md).
type counters struct {
	published     atomic.Uint64
	delivered     atomic.Uint64
	dropped       atomic.Uint64
	slowEvents    atomic.Uint64
	disconnected  atomic.Uint64
	publishErrors atomic.Uint64
	active        atomic.Int64
}
