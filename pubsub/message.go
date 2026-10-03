package pubsub

import "time"

// Message is the unit delivered to subscribers.
//
// Ownership: Message is passed by value, but Payload is an interface value and
// is shared, not copied. Every subscriber of a topic receives the same
// Payload value. If the payload is a pointer, map, slice or any other
// reference type, all subscribers (and the publisher) alias the same memory.
// Payloads must therefore be treated as immutable after Publish. The library
// does not enforce this; it is a convention, as with values sent on a Go
// channel.
//
// Lifetime: a Message sitting in a subscriber buffer keeps its Payload
// reachable until the subscriber receives it or the subscription is garbage
// collected. Large payloads multiplied by large buffers dominate memory use.
type Message struct {
	// ID is a broker-wide, strictly increasing identifier assigned at Publish.
	// IDs reflect the order in which Publish calls were admitted, not the
	// order in which any particular subscriber enqueued them.
	ID uint64

	// Topic is the topic the message was published to.
	Topic string

	// Payload is the shared, caller-supplied value. See the type comment.
	Payload any

	// Timestamp is when Publish admitted the message (broker clock).
	Timestamp time.Time
}
