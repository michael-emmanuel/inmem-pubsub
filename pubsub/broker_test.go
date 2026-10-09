package pubsub_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/michael-emmanuel/inmem-pubsub/internal/testutil"
	"github.com/michael-emmanuel/inmem-pubsub/pubsub"
)

func TestPublishSubscribeBasic(t *testing.T) {
	b := newBroker(t)
	s := subscribe(t, b, "orders", pubsub.SubscribeOptions{BufferSize: 4})

	publish(t, b, "orders", "hello")
	m := recv(t, s)

	if m.Topic != "orders" || m.Payload != "hello" || m.ID == 0 || m.Timestamp.IsZero() {
		t.Fatalf("unexpected message: %+v", m)
	}
}

func TestPublishNoSubscribers(t *testing.T) {
	b := newBroker(t)
	publish(t, b, "nobody", 1)

	st := b.Stats()
	if st.Published != 1 || st.Delivered != 0 || st.Dropped != 0 || st.PublishErrors != 0 {
		t.Fatalf("unexpected stats: %+v", st)
	}
}

func TestSubscriberOnlyReceivesItsTopic(t *testing.T) {
	b := newBroker(t)
	orders := subscribe(t, b, "orders", pubsub.SubscribeOptions{})
	users := subscribe(t, b, "users", pubsub.SubscribeOptions{})

	publish(t, b, "orders", "o1")
	publish(t, b, "users", "u1")

	if got := recv(t, orders); got.Payload != "o1" {
		t.Fatalf("orders got %+v", got)
	}
	if got := recv(t, users); got.Payload != "u1" {
		t.Fatalf("users got %+v", got)
	}
	expectNoMessage(t, orders, 20*time.Millisecond)
	expectNoMessage(t, users, 20*time.Millisecond)
}

func TestPublishFanOut(t *testing.T) {
	b := newBroker(t)
	const n = 5
	subs := make([]*pubsub.Subscription, n)
	for i := range subs {
		subs[i] = subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 8})
	}

	for i := 0; i < 3; i++ {
		publish(t, b, "t", i)
	}

	want := []any{0, 1, 2}
	for i, s := range subs {
		if got := payloads(t, s, 3); !reflect.DeepEqual(got, want) {
			t.Fatalf("subscriber %d got %v, want %v", i, got, want)
		}
	}
	if st := b.Stats(); st.Delivered != 3*n {
		t.Fatalf("Delivered = %d, want %d", st.Delivered, 3*n)
	}
}

// Subscribers have independent buffers: a subscriber that never reads and
// overflows must not change what another subscriber receives.
func TestFanOutSubscribersAreIsolated(t *testing.T) {
	b := newBroker(t)
	stuck := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 1, Overflow: pubsub.DropNewest})
	fast := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 50})

	for i := 0; i < 20; i++ {
		publish(t, b, "t", i)
	}

	got := payloads(t, fast, 20)
	for i, p := range got {
		if p != i {
			t.Fatalf("fast subscriber saw %v at %d", p, i)
		}
	}
	if d := stuck.Stats().Dropped; d != 19 {
		t.Fatalf("stuck subscriber dropped %d, want 19", d)
	}
}

type mutablePayload struct{ n int }

// Documents the sharing semantics: the payload is not copied.
func TestPayloadIsSharedByReference(t *testing.T) {
	b := newBroker(t)
	s1 := subscribe(t, b, "t", pubsub.SubscribeOptions{})
	s2 := subscribe(t, b, "t", pubsub.SubscribeOptions{})

	p := &mutablePayload{n: 1}
	publish(t, b, "t", p)

	m1, m2 := recv(t, s1), recv(t, s2)
	if m1.Payload.(*mutablePayload) != p || m2.Payload.(*mutablePayload) != p {
		t.Fatal("expected all subscribers to share the same payload pointer")
	}
}

func TestMessageIDsAndOrderingSinglePublisher(t *testing.T) {
	b := newBroker(t)
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 100})
	for i := 0; i < 100; i++ {
		publish(t, b, "t", i)
	}
	var last uint64
	for i := 0; i < 100; i++ {
		m := recv(t, s)
		if m.Payload != i {
			t.Fatalf("payload %v at position %d", m.Payload, i)
		}
		if m.ID <= last {
			t.Fatalf("IDs not increasing: %d after %d", m.ID, last)
		}
		last = m.ID
	}
}

func TestClockOption(t *testing.T) {
	fixed := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	b := newBroker(t, pubsub.WithClock(func() time.Time { return fixed }))
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{})
	publish(t, b, "t", nil)
	if got := recv(t, s).Timestamp; !got.Equal(fixed) {
		t.Fatalf("Timestamp = %v, want %v", got, fixed)
	}
}

func TestDefaultBufferSizeOption(t *testing.T) {
	b := newBroker(t, pubsub.WithDefaultBufferSize(7))
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{})
	if c := s.Stats().Capacity; c != 7 {
		t.Fatalf("capacity = %d, want 7", c)
	}
	s2 := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 3})
	if c := s2.Stats().Capacity; c != 3 {
		t.Fatalf("capacity = %d, want 3", c)
	}
}

func TestValidationErrors(t *testing.T) {
	b := newBroker(t)
	ctx := context.Background()

	if err := b.Publish(ctx, "", 1); !errors.Is(err, pubsub.ErrTopicRequired) {
		t.Fatalf("Publish empty topic: %v", err)
	}
	if _, err := b.Subscribe(ctx, "", pubsub.SubscribeOptions{}); !errors.Is(err, pubsub.ErrTopicRequired) {
		t.Fatalf("Subscribe empty topic: %v", err)
	}
	if _, err := b.Subscribe(ctx, "t", pubsub.SubscribeOptions{BufferSize: -1}); !errors.Is(err, pubsub.ErrInvalidBufferSize) {
		t.Fatalf("negative buffer: %v", err)
	}
	if _, err := b.Subscribe(ctx, "t", pubsub.SubscribeOptions{BufferSize: pubsub.MaxBufferSize + 1}); !errors.Is(err, pubsub.ErrInvalidBufferSize) {
		t.Fatalf("huge buffer: %v", err)
	}
	if _, err := b.Subscribe(ctx, "t", pubsub.SubscribeOptions{Overflow: pubsub.OverflowPolicy(99)}); !errors.Is(err, pubsub.ErrInvalidOverflowPolicy) {
		t.Fatalf("bad policy: %v", err)
	}
	if n := b.SubscriberCount("t"); n != 0 {
		t.Fatalf("failed Subscribe registered %d subscribers", n)
	}
}

func TestPublishWithCancelledContext(t *testing.T) {
	b := newBroker(t)
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := b.Publish(ctx, "t", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	expectNoMessage(t, s, 20*time.Millisecond)
	if st := b.Stats(); st.Published != 0 || st.PublishErrors != 1 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestSubscriberCountAndTopicCleanup(t *testing.T) {
	b := newBroker(t)
	a := subscribe(t, b, "t", pubsub.SubscribeOptions{})
	c := subscribe(t, b, "t", pubsub.SubscribeOptions{})
	if n := b.SubscriberCount("t"); n != 2 {
		t.Fatalf("count = %d", n)
	}
	_ = a.Close()
	if n := b.SubscriberCount("t"); n != 1 {
		t.Fatalf("count = %d", n)
	}
	_ = c.Close()
	if n := b.SubscriberCount("t"); n != 0 {
		t.Fatalf("count = %d", n)
	}
	if st := b.Stats(); st.ActiveSubscribers != 0 {
		t.Fatalf("active = %d", st.ActiveSubscribers)
	}
}

// ---- subscriber lifecycle ----

func TestSubscriberClose(t *testing.T) {
	b := newBroker(t)
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{})
	other := subscribe(t, b, "t", pubsub.SubscribeOptions{})
	if s.Err() != nil {
		t.Fatalf("active subscription Err = %v", s.Err())
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	expectClosed(t, s)
	if !errors.Is(s.Err(), pubsub.ErrSubscriberClosed) {
		t.Fatalf("Err = %v", s.Err())
	}

	publish(t, b, "t", 1) // must not panic or reach the closed subscriber
	if got := recv(t, other); got.Payload != 1 {
		t.Fatalf("other got %+v", got)
	}
}

func TestSubscriberDoubleClose(t *testing.T) {
	b := newBroker(t)
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{})
	for i := 0; i < 3; i++ {
		if err := s.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i, err)
		}
	}
	if st := b.Stats(); st.ActiveSubscribers != 0 {
		t.Fatalf("active = %d, want 0 (decrement must happen once)", st.ActiveSubscribers)
	}
}

func TestSubscriberConcurrentClose(t *testing.T) {
	b := newBroker(t)
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Close()
		}()
	}
	testutil.WaitTimeout(t, &wg, wait, "concurrent Close")
	if st := b.Stats(); st.ActiveSubscribers != 0 {
		t.Fatalf("active = %d", st.ActiveSubscribers)
	}
	expectClosed(t, s)
}

func TestSubscriberContextCancellation(t *testing.T) {
	b := newBroker(t)
	ctx, cancel := context.WithCancel(context.Background())
	s, err := b.Subscribe(ctx, "t", pubsub.SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}

	cancel()
	expectClosed(t, s)
	if !errors.Is(s.Err(), context.Canceled) {
		t.Fatalf("Err = %v", s.Err())
	}
	testutil.Eventually(t, wait, func() bool { return b.SubscriberCount("t") == 0 }, "subscriber removal")
}

func TestSubscribeWithAlreadyCancelledContext(t *testing.T) {
	b := newBroker(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Subscribe(ctx, "t", pubsub.SubscribeOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if n := b.SubscriberCount("t"); n != 0 {
		t.Fatalf("count = %d", n)
	}
}

func TestSubscriberContextDeadline(t *testing.T) {
	b := newBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	s, err := b.Subscribe(ctx, "t", pubsub.SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	expectClosed(t, s)
	if !errors.Is(s.Err(), context.DeadlineExceeded) {
		t.Fatalf("Err = %v", s.Err())
	}
}

// Close before the context fires must stop the AfterFunc registration; the
// leak check in newBroker plus a later cancel must not panic or double count.
func TestCloseThenCancelContext(t *testing.T) {
	b := newBroker(t)
	ctx, cancel := context.WithCancel(context.Background())
	s, _ := b.Subscribe(ctx, "t", pubsub.SubscribeOptions{})
	_ = s.Close()
	cancel()
	time.Sleep(10 * time.Millisecond)
	if st := b.Stats(); st.ActiveSubscribers != 0 {
		t.Fatalf("active = %d", st.ActiveSubscribers)
	}
	if !errors.Is(s.Err(), pubsub.ErrSubscriberClosed) {
		t.Fatalf("first reason must win, got %v", s.Err())
	}
}

// ---- broker shutdown ----

func TestBrokerCloseClosesSubscriptions(t *testing.T) {
	b := newBroker(t)
	var subs []*pubsub.Subscription
	for _, topic := range []string{"a", "b", "a"} {
		subs = append(subs, subscribe(t, b, topic, pubsub.SubscribeOptions{}))
	}
	publish(t, b, "a", "buffered")

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if got := drain(t, subs[0]); len(got) != 1 || got[0] != "buffered" {
		t.Fatalf("buffered message must remain readable after close, got %v", got)
	}
	for _, s := range subs {
		expectClosed(t, s)
		if !errors.Is(s.Err(), pubsub.ErrClosed) {
			t.Fatalf("Err = %v", s.Err())
		}
	}
	if st := b.Stats(); st.ActiveSubscribers != 0 {
		t.Fatalf("active = %d", st.ActiveSubscribers)
	}
}

func TestOperationsAfterBrokerClose(t *testing.T) {
	b := newBroker(t)
	_ = b.Close()

	if err := b.Publish(context.Background(), "t", 1); !errors.Is(err, pubsub.ErrClosed) {
		t.Fatalf("Publish: %v", err)
	}
	if _, err := b.Subscribe(context.Background(), "t", pubsub.SubscribeOptions{}); !errors.Is(err, pubsub.ErrClosed) {
		t.Fatalf("Subscribe: %v", err)
	}
	if n := b.SubscriberCount("t"); n != 0 {
		t.Fatalf("count = %d", n)
	}
}

func TestBrokerDoubleClose(t *testing.T) {
	b := newBroker(t)
	for i := 0; i < 3; i++ {
		if err := b.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i, err)
		}
	}
}

func TestBrokerConcurrentClose(t *testing.T) {
	b := newBroker(t)
	subs := make([]*pubsub.Subscription, 20)
	for i := range subs {
		subs[i] = subscribe(t, b, "t", pubsub.SubscribeOptions{})
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = b.Close()
			// Close blocks until shutdown completed, so this must hold for
			// every caller, not just the one that ran the shutdown.
			if st := b.Stats(); st.ActiveSubscribers != 0 {
				t.Errorf("active = %d after Close returned", st.ActiveSubscribers)
			}
		}()
	}
	testutil.WaitTimeout(t, &wg, wait, "concurrent broker Close")
	for _, s := range subs {
		expectClosed(t, s)
	}
}

// Subscription Close after the broker already closed it must be a no-op.
func TestSubscriptionCloseAfterBrokerClose(t *testing.T) {
	b := newBroker(t)
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{})
	_ = b.Close()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(s.Err(), pubsub.ErrClosed) {
		t.Fatalf("first reason must win, got %v", s.Err())
	}
}

// Invariant: once Close has returned, nothing new is enqueued.
func TestClosedSubscriberNeverReceivesNewMessage(t *testing.T) {
	b := newBroker(t)
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 64})
	for i := 0; i < 10; i++ {
		publish(t, b, "t", "before")
	}
	_ = s.Close()
	for i := 0; i < 10; i++ {
		publish(t, b, "t", "after")
	}
	got := drain(t, s)
	if len(got) != 10 {
		t.Fatalf("got %d messages, want 10", len(got))
	}
	for _, p := range got {
		if p != "before" {
			t.Fatalf("received %v enqueued after Close returned", p)
		}
	}
}

func TestStatsAccountingDropNewest(t *testing.T) {
	b := newBroker(t)
	const subs, msgs, buf = 3, 10, 4
	for i := 0; i < subs; i++ {
		subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: buf, Overflow: pubsub.DropNewest})
	}
	for i := 0; i < msgs; i++ {
		publish(t, b, "t", i)
	}
	st := b.Stats()
	if st.Published != msgs || st.Delivered+st.Dropped != msgs*subs {
		t.Fatalf("stats do not add up: %+v", st)
	}
	if st.Delivered != buf*subs || st.SlowConsumerEvents != (msgs-buf)*subs {
		t.Fatalf("unexpected stats: %+v", st)
	}
}
