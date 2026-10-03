package pubsub_test

import (
	"context"
	"testing"
	"time"

	"github.com/example/inmem-pubsub/internal/testutil"
	"github.com/example/inmem-pubsub/pubsub"
)

const wait = 2 * time.Second

// newBroker returns a broker that is closed, and leak-checked, at test end.
// LeakCheck is registered first so that (cleanups run LIFO) the broker is
// closed before goroutines are counted.
func newBroker(t *testing.T, opts ...pubsub.Option) *pubsub.Broker {
	t.Helper()
	testutil.LeakCheck(t)
	b := pubsub.New(opts...)
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func subscribe(t *testing.T, b *pubsub.Broker, topic string, opts pubsub.SubscribeOptions) *pubsub.Subscription {
	t.Helper()
	s, err := b.Subscribe(context.Background(), topic, opts)
	if err != nil {
		t.Fatalf("Subscribe(%q): %v", topic, err)
	}
	return s
}

func publish(t *testing.T, b *pubsub.Broker, topic string, payload any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	if err := b.Publish(ctx, topic, payload); err != nil {
		t.Fatalf("Publish(%q, %v): %v", topic, payload, err)
	}
}

func recv(t *testing.T, s *pubsub.Subscription) pubsub.Message {
	t.Helper()
	select {
	case m, ok := <-s.Messages():
		if !ok {
			t.Fatal("subscription channel closed unexpectedly")
		}
		return m
	case <-time.After(wait):
		t.Fatal("timed out waiting for message")
	}
	return pubsub.Message{}
}

func expectNoMessage(t *testing.T, s *pubsub.Subscription, d time.Duration) {
	t.Helper()
	select {
	case m, ok := <-s.Messages():
		if ok {
			t.Fatalf("unexpected message %+v", m)
		}
		t.Fatal("unexpected channel close")
	case <-time.After(d):
	}
}

// drain reads until the channel is closed and returns the payloads read.
func drain(t *testing.T, s *pubsub.Subscription) []any {
	t.Helper()
	var got []any
	timeout := time.After(wait)
	for {
		select {
		case m, ok := <-s.Messages():
			if !ok {
				return got
			}
			got = append(got, m.Payload)
		case <-timeout:
			t.Fatal("timed out waiting for subscription channel to close")
		}
	}
}

func expectClosed(t *testing.T, s *pubsub.Subscription) { t.Helper(); _ = drain(t, s) }

func payloads(t *testing.T, s *pubsub.Subscription, n int) []any {
	t.Helper()
	out := make([]any, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, recv(t, s).Payload)
	}
	return out
}
