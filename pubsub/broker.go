package pubsub

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Broker routes published messages to subscriptions by topic name.
//
// A Broker is safe for concurrent use by multiple goroutines. The zero value
// is not usable; construct one with New. Close it when done.
//
// Shared state and its protection (full discussion in docs/concurrency.md):
//
//	mu protects topics and closed.
//	    The slices stored in topics are immutable once published into the map
//	    (copy-on-write), so a reader that obtained one under mu.RLock may keep
//	    iterating after releasing the lock.
//	stats and the ID counters are lock-free atomics.
//	Per-subscription state is protected by the subscription's own lock.
//
// Lock order: Broker.mu and Subscription.mu are never held at the same time.
// This removes any possibility of lock-order inversion between them.
type Broker struct {
	cfg config

	nextMsgID atomic.Uint64
	nextSubID atomic.Uint64
	stats     counters

	mu     sync.RWMutex
	topics map[string][]*Subscription
	closed bool

	closeOnce sync.Once
}

// New creates a broker.
func New(opts ...Option) *Broker {
	cfg := config{defaultBuffer: DefaultBufferSize, now: time.Now}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Broker{
		cfg:    cfg,
		topics: make(map[string][]*Subscription),
	}
}

// Subscribe registers a new subscription on topic.
//
// The returned Subscription's channel receives every message published to the
// topic after Subscribe returns (and possibly a few concurrently in flight
// that were published during registration; there is no replay of earlier
// messages).
//
// ctx scopes the subscription's lifetime: when ctx is done the subscription is
// closed asynchronously with Err() == ctx.Err(). ctx does not bound the
// Subscribe call itself, which never blocks beyond a short registry lock.
// An already-cancelled ctx makes Subscribe fail without registering.
//
// Errors: ErrTopicRequired, ErrInvalidBufferSize, ErrInvalidOverflowPolicy,
// ErrClosed, or ctx.Err().
func (b *Broker) Subscribe(ctx context.Context, topic string, opts SubscribeOptions) (*Subscription, error) {
	if topic == "" {
		return nil, ErrTopicRequired
	}
	if opts.BufferSize < 0 || opts.BufferSize > MaxBufferSize {
		return nil, ErrInvalidBufferSize
	}
	if !opts.Overflow.valid() {
		return nil, ErrInvalidOverflowPolicy
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	size := opts.BufferSize
	if size == 0 {
		size = b.cfg.defaultBuffer
	}
	s := &Subscription{
		id:     b.nextSubID.Add(1),
		topic:  topic,
		broker: b,
		policy: opts.Overflow,
		out:    make(chan Message, size),
		done:   make(chan struct{}),
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrClosed
	}
	// Copy-on-write: never mutate a slice a concurrent Publish may be reading.
	old := b.topics[topic]
	next := make([]*Subscription, len(old)+1)
	copy(next, old)
	next[len(old)] = s
	b.topics[topic] = next
	// Incremented under mu, before the subscription is visible to anything
	// that could terminate it, so ActiveSubscribers is never negative.
	b.stats.active.Add(1)
	b.mu.Unlock()

	if ctx.Done() != nil {
		stop := context.AfterFunc(ctx, func() { s.terminate(ctx.Err()) })
		s.stop.Store(&stop)
		// If the subscription ended before stop was published, terminate could
		// not have called it. Do it here so the AfterFunc registration does not
		// pin s until ctx ends.
		select {
		case <-s.done:
			stop()
		default:
		}
	}
	return s, nil
}

// Publish delivers payload to every active subscription on topic.
//
// Publish returns after the message has been offered to each subscriber that
// was registered when Publish took its registry snapshot. What "offered"
// means depends on each subscriber's OverflowPolicy; only Block can make
// Publish wait, and it waits only for ctx, subscription closure, or buffer
// space. Publishing to a topic with no subscribers is not an error.
//
// Publish is not atomic across subscribers: if ctx ends mid fan-out, a prefix
// of the subscribers may already hold the message and Publish returns
// ctx.Err().
//
// Errors: ErrTopicRequired, ErrClosed, or ctx.Err(). A Publish racing with
// Close may return nil without delivering to anyone.
func (b *Broker) Publish(ctx context.Context, topic string, payload any) error {
	if topic == "" {
		return b.fail(ErrTopicRequired)
	}
	if err := ctx.Err(); err != nil {
		return b.fail(err)
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return b.fail(ErrClosed)
	}
	subs := b.topics[topic] // immutable snapshot; safe to use after RUnlock
	b.mu.RUnlock()

	b.stats.published.Add(1)
	msg := Message{
		ID:        b.nextMsgID.Add(1),
		Topic:     topic,
		Payload:   payload,
		Timestamp: b.cfg.now(),
	}

	// No broker lock is held below. Delivery may block (Block policy), and a
	// blocked publisher must never stop Subscribe, Close or other publishers.
	for _, s := range subs {
		switch s.deliver(ctx, msg) {
		case resCanceled:
			return b.fail(ctx.Err())
		case resDisconnect:
			// Must run after deliver has released the subscription read lock.
			s.terminate(ErrSlowConsumer)
		}
	}
	return nil
}

// SubscriberCount returns the number of live subscriptions on topic.
func (b *Broker) SubscriberCount(topic string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.topics[topic])
}

// Stats returns a snapshot of broker counters. See Stats for consistency
// caveats.
func (b *Broker) Stats() Stats {
	return Stats{
		Published:          b.stats.published.Load(),
		Delivered:          b.stats.delivered.Load(),
		Dropped:            b.stats.dropped.Load(),
		SlowConsumerEvents: b.stats.slowEvents.Load(),
		Disconnected:       b.stats.disconnected.Load(),
		PublishErrors:      b.stats.publishErrors.Load(),
		ActiveSubscribers:  b.stats.active.Load(),
	}
}

// Close shuts the broker down. It is idempotent and safe to call
// concurrently: every call returns only after shutdown has completed.
//
// Close rejects new Publish and Subscribe calls, then closes every active
// subscription with Err() == ErrClosed. Blocked publishers are released.
// Messages already buffered in a subscription remain readable until drained.
// Close always returns nil; the error result keeps the signature compatible
// with io.Closer.
func (b *Broker) Close() error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		var all []*Subscription
		for _, subs := range b.topics {
			all = append(all, subs...)
		}
		b.topics = make(map[string][]*Subscription)
		b.mu.Unlock()

		// Terminate outside mu: terminate may wait for in-flight senders, and
		// we must not hold the registry lock while waiting on anything.
		for _, s := range all {
			s.terminate(ErrClosed)
		}
	})
	return nil
}

func (b *Broker) fail(err error) error {
	b.stats.publishErrors.Add(1)
	return err
}

// removeSubscription removes s from the registry. It is a no-op if the
// registry was already cleared by Close or s was already removed.
func (b *Broker) removeSubscription(s *Subscription) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	old := b.topics[s.topic]
	for i, cur := range old {
		if cur != s {
			continue
		}
		if len(old) == 1 {
			delete(b.topics, s.topic) // do not leak empty topic entries
			return
		}
		next := make([]*Subscription, 0, len(old)-1)
		next = append(next, old[:i]...)
		next = append(next, old[i+1:]...)
		b.topics[s.topic] = next
		return
	}
}
