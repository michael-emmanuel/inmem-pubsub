package pubsub

import "time"

const (
	// DefaultBufferSize is used when neither SubscribeOptions.BufferSize nor
	// WithDefaultBufferSize is set.
	DefaultBufferSize = 64

	// MaxBufferSize bounds the per-subscription buffer so that a single
	// Subscribe call cannot allocate unbounded memory. A Message is 64 bytes
	// on 64-bit platforms, so the maximum is roughly 64 MiB per subscription,
	// before counting payload memory.
	MaxBufferSize = 1 << 20
)

// OverflowPolicy decides what Publish does when a subscriber's buffer is full.
type OverflowPolicy int

const (
	// Block makes Publish wait until the subscriber has room, the subscription
	// closes, or the publish context ends. This is the zero value and the
	// default. It is lossless but couples the publisher (and the subscribers
	// that come after the slow one in the delivery order) to the slowest
	// consumer. Always publish with a context that has a deadline if
	// consumers can stall.
	Block OverflowPolicy = iota

	// DropNewest discards the message being published for this subscriber.
	// The buffer content is untouched. Cheap, never blocks, preserves the
	// oldest data.
	DropNewest

	// DropOldest evicts the oldest buffered message to make room for the new
	// one. Never blocks, preserves the freshest data (appropriate for
	// "latest state wins" streams such as price ticks).
	DropOldest

	// Disconnect closes the subscription with ErrSlowConsumer when its buffer
	// overflows. The message that overflowed is counted as dropped. The
	// consumer sees its channel close and can resubscribe, making loss
	// explicit at the consumer rather than silent inside the stream.
	Disconnect
)

func (p OverflowPolicy) valid() bool { return p >= Block && p <= Disconnect }

// String implements fmt.Stringer.
func (p OverflowPolicy) String() string {
	switch p {
	case Block:
		return "Block"
	case DropNewest:
		return "DropNewest"
	case DropOldest:
		return "DropOldest"
	case Disconnect:
		return "Disconnect"
	default:
		return "OverflowPolicy(invalid)"
	}
}

// SubscribeOptions configures a single subscription.
type SubscribeOptions struct {
	// BufferSize is the capacity of the subscription's channel. Zero selects
	// the broker default. Negative values and values above MaxBufferSize are
	// rejected with ErrInvalidBufferSize. There is deliberately no unbuffered
	// mode: an unbuffered subscriber turns every Publish into a rendezvous.
	BufferSize int

	// Overflow is the slow-consumer policy. The zero value is Block.
	Overflow OverflowPolicy
}

type config struct {
	defaultBuffer int
	now           func() time.Time
}

// Option configures a Broker.
type Option func(*config)

// WithDefaultBufferSize sets the buffer size used by subscriptions that do not
// specify one. Values less than 1 or greater than MaxBufferSize are ignored.
func WithDefaultBufferSize(n int) Option {
	return func(c *config) {
		if n >= 1 && n <= MaxBufferSize {
			c.defaultBuffer = n
		}
	}
}

// WithClock overrides the clock used for Message.Timestamp. It exists so tests
// and callers needing deterministic timestamps do not have to sleep. A nil
// function is ignored.
func WithClock(now func() time.Time) Option {
	return func(c *config) {
		if now != nil {
			c.now = now
		}
	}
}
