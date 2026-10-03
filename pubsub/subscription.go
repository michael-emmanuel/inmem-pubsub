package pubsub

import (
	"context"
	"sync"
	"sync/atomic"
)

type deliveryResult int

const (
	resDelivered deliveryResult = iota
	resDropped
	resClosed
	resCanceled
	resDisconnect
)

// Subscription is one consumer's registration on a topic.
//
// Lifecycle:
//
//	Active --Close()------------> Closed (Err() == ErrSubscriberClosed)
//	Active --ctx done------------> Closed (Err() == ctx.Err())
//	Active --Disconnect policy---> Closed (Err() == ErrSlowConsumer)
//	Active --Broker.Close()------> Closed (Err() == ErrClosed)
//
// Channel ownership: the Messages channel is created by Subscribe and closed
// exclusively by terminate, exactly once, after every in-flight sender has
// left. Publishers never close it; consumers cannot (it is receive-only).
type Subscription struct {
	id     uint64
	topic  string
	broker *Broker
	policy OverflowPolicy

	out  chan Message
	done chan struct{} // closed first by terminate; wakes blocked senders

	// mu is a send/close gate, not a data lock. Senders hold RLock for the
	// duration of one delivery attempt. terminate takes Lock to set closed
	// and close(out), which therefore cannot happen while any sender is
	// between its closed check and its channel operation.
	mu     sync.RWMutex
	closed bool

	once   sync.Once
	reason error // written once, before close(done); read only after <-done

	stop atomic.Pointer[func() bool] // context.AfterFunc stop, if ctx had a Done channel

	delivered atomic.Uint64
	dropped   atomic.Uint64
}

// Messages returns the channel on which messages arrive. It is closed when
// the subscription ends for any reason; use Err to learn why. Messages that
// were buffered before the end remain readable, so range loops drain them
// before terminating.
func (s *Subscription) Messages() <-chan Message { return s.out }

// Topic returns the subscribed topic.
func (s *Subscription) Topic() string { return s.topic }

// Err returns nil while the subscription is active and, after it ends, the
// reason: ErrSubscriberClosed, ErrSlowConsumer, ErrClosed, or the
// subscription context's error.
func (s *Subscription) Err() error {
	select {
	case <-s.done:
		return s.reason
	default:
		return nil
	}
}

// Stats returns per-subscription counters.
func (s *Subscription) Stats() SubscriptionStats {
	return SubscriptionStats{
		Delivered: s.delivered.Load(),
		Dropped:   s.dropped.Load(),
		Buffered:  len(s.out),
		Capacity:  cap(s.out),
	}
}

// Close ends the subscription. It is idempotent and safe to call concurrently
// and from within a consumer loop. It returns after the Messages channel has
// been closed and the subscription has been removed from the broker. It
// always returns nil.
//
// After Close returns, no new message is enqueued to this subscription.
func (s *Subscription) Close() error {
	s.terminate(ErrSubscriberClosed)
	return nil
}

// terminate ends the subscription exactly once. Concurrent callers block until
// the winner has finished, so every caller observes a fully closed state.
//
// Order matters:
//  1. record reason, close(done): wakes senders blocked in Block policy so
//     they release their read lock promptly;
//  2. cancel the context callback;
//  3. remove from the registry so new Publish calls stop seeing s;
//  4. take the write lock (waits for the bounded number of in-flight senders),
//     set closed, close(out). Senders that arrive later see closed == true.
//
// Deadlock freedom: step 4 can wait only for senders, and every sender either
// completes a non-blocking operation or waits in a select that includes done,
// which step 1 has already closed. Senders never wait for terminate.
func (s *Subscription) terminate(reason error) {
	s.once.Do(func() {
		s.reason = reason
		close(s.done)

		if p := s.stop.Load(); p != nil {
			(*p)()
		}
		if reason == ErrSlowConsumer {
			s.broker.stats.disconnected.Add(1)
		}
		s.broker.removeSubscription(s)

		s.mu.Lock()
		s.closed = true
		close(s.out)
		s.mu.Unlock()

		s.broker.stats.active.Add(-1)
	})
}

// deliver offers m to this subscription according to its overflow policy.
// It holds s.mu.RLock throughout, which is what makes the send safe against
// close(out). It must not call terminate (that needs the write lock).
func (s *Subscription) deliver(ctx context.Context, m Message) deliveryResult {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return resClosed
	}
	select {
	case <-s.done:
		return resClosed // closing in progress; do not enqueue
	default:
	}

	// Fast path: room in the buffer.
	select {
	case s.out <- m:
		s.recordDelivered()
		return resDelivered
	default:
	}

	// Buffer full: this is the slow-consumer event.
	s.broker.stats.slowEvents.Add(1)

	switch s.policy {
	case DropNewest:
		s.recordDropped()
		return resDropped

	case DropOldest:
		return s.deliverDropOldest(m)

	case Disconnect:
		s.recordDropped()
		return resDisconnect

	default: // Block
		select {
		case s.out <- m:
			s.recordDelivered()
			return resDelivered
		case <-s.done:
			return resClosed
		case <-ctx.Done():
			return resCanceled
		}
	}
}

// deliverDropOldest evicts from the head of the channel until the new message
// fits. The consumer may receive concurrently, so each failed attempt either
// evicts one message (progress) or finds the buffer drained (next send
// succeeds). Concurrent publishers evicting each other's messages is
// possible and acceptable: all evictions are counted.
func (s *Subscription) deliverDropOldest(m Message) deliveryResult {
	for {
		select {
		case s.out <- m:
			s.recordDelivered()
			return resDelivered
		default:
		}
		select {
		case <-s.out:
			s.recordDropped()
		default:
		}
		select {
		case <-s.done:
			return resClosed
		default:
		}
	}
}

func (s *Subscription) recordDelivered() {
	s.delivered.Add(1)
	s.broker.stats.delivered.Add(1)
}

func (s *Subscription) recordDropped() {
	s.dropped.Add(1)
	s.broker.stats.dropped.Add(1)
}
