// Package pubsub is an in-process, in-memory publish/subscribe broker.
//
// Publishers send a payload to a named topic. The broker fans the message out
// to every active subscription on that topic. Each subscription owns an
// independent bounded buffer (a buffered channel) and an explicit overflow
// policy that decides what happens when the buffer is full.
//
// # Guarantees
//
//   - At-most-once delivery. There is no persistence, replay or redelivery.
//   - Each active subscriber is offered each message independently.
//   - Messages published by a single goroutine are enqueued to each
//     subscriber in publish order. There is no ordering across publishers.
//   - Messages are never dropped silently: every drop is counted in Stats
//     and in SubscriptionStats, and the policy that causes it is chosen by
//     the caller.
//   - No send on a closed channel can occur, and the package starts no
//     background goroutines of its own. The only goroutine it ever causes is
//     the context.AfterFunc callback that runs once when a subscription
//     context is cancelled.
//
// # Non-goals
//
// This package is not Kafka, NATS, Redis Pub/Sub, Google Pub/Sub or RabbitMQ.
// It does not cross process boundaries, survive a crash, or deliver
// messages to consumers that were not subscribed at publish time.
//
// See the docs/ directory of the repository for the design.
package pubsub
