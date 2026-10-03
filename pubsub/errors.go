package pubsub

import "errors"

// Sentinel errors. Compare with errors.Is.
var (
	// ErrClosed is returned by Publish and Subscribe after Broker.Close, and is
	// the Subscription.Err value of subscriptions closed by broker shutdown.
	ErrClosed = errors.New("pubsub: broker closed")

	// ErrTopicRequired is returned when the topic name is empty.
	ErrTopicRequired = errors.New("pubsub: topic is required")

	// ErrInvalidBufferSize is returned when SubscribeOptions.BufferSize is
	// negative or larger than MaxBufferSize.
	ErrInvalidBufferSize = errors.New("pubsub: invalid buffer size")

	// ErrInvalidOverflowPolicy is returned for an unknown OverflowPolicy.
	ErrInvalidOverflowPolicy = errors.New("pubsub: invalid overflow policy")

	// ErrSubscriberClosed is the Subscription.Err value after the owner called
	// Subscription.Close.
	ErrSubscriberClosed = errors.New("pubsub: subscription closed")

	// ErrSlowConsumer is the Subscription.Err value of a subscription that was
	// removed by the Disconnect overflow policy.
	ErrSlowConsumer = errors.New("pubsub: subscription disconnected: slow consumer")
)
