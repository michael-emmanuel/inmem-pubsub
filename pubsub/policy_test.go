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

func TestBufferAbsorbsBurst(t *testing.T) {
	b := newBroker(t)
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 100})

	// No consumer is running: a burst up to the buffer size must not block.
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	for i := 0; i < 100; i++ {
		if err := b.Publish(ctx, "t", i); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if st := b.Stats(); st.SlowConsumerEvents != 0 {
		t.Fatalf("burst within capacity must not be a slow-consumer event: %+v", st)
	}
	if got := s.Stats(); got.Buffered != 100 || got.Capacity != 100 {
		t.Fatalf("stats: %+v", got)
	}
	for i := 0; i < 100; i++ {
		if p := recv(t, s).Payload; p != i {
			t.Fatalf("got %v at %d", p, i)
		}
	}
}

func TestBufferCapacityRespected(t *testing.T) {
	b := newBroker(t)
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 3, Overflow: pubsub.DropNewest})
	for i := 0; i < 5; i++ {
		publish(t, b, "t", i)
	}
	if got := s.Stats(); got.Buffered != 3 || got.Dropped != 2 || got.Delivered != 3 {
		t.Fatalf("stats: %+v", got)
	}
}

func TestDropNewestPolicy(t *testing.T) {
	b := newBroker(t)
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 3, Overflow: pubsub.DropNewest})
	for i := 0; i < 6; i++ {
		publish(t, b, "t", i)
	}
	if got := payloads(t, s, 3); !reflect.DeepEqual(got, []any{0, 1, 2}) {
		t.Fatalf("got %v, want oldest messages [0 1 2]", got)
	}
	st := b.Stats()
	if st.Dropped != 3 || st.SlowConsumerEvents != 3 || st.Delivered != 3 {
		t.Fatalf("stats: %+v", st)
	}
	if s.Err() != nil {
		t.Fatal("DropNewest must not end the subscription")
	}
}

func TestDropOldestPolicy(t *testing.T) {
	b := newBroker(t)
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 3, Overflow: pubsub.DropOldest})
	for i := 0; i < 6; i++ {
		publish(t, b, "t", i)
	}
	if got := payloads(t, s, 3); !reflect.DeepEqual(got, []any{3, 4, 5}) {
		t.Fatalf("got %v, want freshest messages [3 4 5]", got)
	}
	st := b.Stats()
	// Every message was enqueued once; three were later evicted.
	if st.Delivered != 6 || st.Dropped != 3 || st.SlowConsumerEvents != 3 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestDisconnectPolicy(t *testing.T) {
	b := newBroker(t)
	slow := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 2, Overflow: pubsub.Disconnect})
	ok := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 10, Overflow: pubsub.Disconnect})

	for i := 0; i < 3; i++ {
		publish(t, b, "t", i)
	}

	// Publish returns only after the disconnect has completed.
	if !errors.Is(slow.Err(), pubsub.ErrSlowConsumer) {
		t.Fatalf("Err = %v, want ErrSlowConsumer", slow.Err())
	}
	if got := drain(t, slow); !reflect.DeepEqual(got, []any{0, 1}) {
		t.Fatalf("slow subscriber drained %v, want [0 1]", got)
	}
	if n := b.SubscriberCount("t"); n != 1 {
		t.Fatalf("SubscriberCount = %d, want 1", n)
	}
	if got := payloads(t, ok, 3); !reflect.DeepEqual(got, []any{0, 1, 2}) {
		t.Fatalf("healthy subscriber got %v", got)
	}
	st := b.Stats()
	if st.Disconnected != 1 || st.Dropped != 1 || st.ActiveSubscribers != 1 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestBlockPolicyBlocksUntilConsumerReads(t *testing.T) {
	b := newBroker(t)
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 1})
	publish(t, b, "t", 0) // fills the buffer

	done := make(chan error, 1)
	go func() { done <- b.Publish(context.Background(), "t", 1) }()

	// Deterministic: wait until the publisher has actually hit the full buffer.
	testutil.Eventually(t, wait, func() bool { return b.Stats().SlowConsumerEvents == 1 }, "publisher reaches full buffer")
	select {
	case err := <-done:
		t.Fatalf("Publish returned (%v) while buffer was full", err)
	case <-time.After(30 * time.Millisecond):
	}

	if p := recv(t, s).Payload; p != 0 {
		t.Fatalf("got %v", p)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(wait):
		t.Fatal("Publish did not unblock after consumer read")
	}
	if p := recv(t, s).Payload; p != 1 {
		t.Fatalf("got %v", p)
	}
	if st := b.Stats(); st.Dropped != 0 {
		t.Fatalf("Block must be lossless: %+v", st)
	}
}

func TestBlockedPublishHonoursContext(t *testing.T) {
	b := newBroker(t)
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 1})
	publish(t, b, "t", 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Publish(ctx, "t", 1) }()
	testutil.Eventually(t, wait, func() bool { return b.Stats().SlowConsumerEvents == 1 }, "publisher blocked")

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(wait):
		t.Fatal("cancelled Publish did not return")
	}
	if s.Err() != nil {
		t.Fatal("publisher cancellation must not end the subscription")
	}
	if st := b.Stats(); st.PublishErrors != 1 || st.Delivered != 1 {
		t.Fatalf("stats: %+v", st)
	}
}

// Documents the cost of Block: subscribers are served sequentially, so a
// stalled subscriber delays every subscriber registered after it.
func TestBlockPolicyCouplesSubscribersOnSameTopic(t *testing.T) {
	b := newBroker(t)
	slow := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 1})
	fast := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 10})
	publish(t, b, "t", 0)
	if p := recv(t, fast).Payload; p != 0 {
		t.Fatal(p)
	}

	done := make(chan error, 1)
	go func() { done <- b.Publish(context.Background(), "t", 1) }()
	testutil.Eventually(t, wait, func() bool { return b.Stats().SlowConsumerEvents == 1 }, "publisher blocked on slow")

	expectNoMessage(t, fast, 30*time.Millisecond) // starved behind slow

	_ = recv(t, slow) // free one slot
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if p := recv(t, fast).Payload; p != 1 {
		t.Fatalf("got %v", p)
	}
}

// A blocked publisher on one topic must not stall other topics, Subscribe,
// SubscriberCount, Stats or Close: no registry lock is held while blocked.
func TestSlowConsumerDoesNotBlockUnrelatedTopicsOrControlPlane(t *testing.T) {
	b := newBroker(t)
	subscribe(t, b, "slow", pubsub.SubscribeOptions{BufferSize: 1})
	other := subscribe(t, b, "other", pubsub.SubscribeOptions{})
	publish(t, b, "slow", 0)

	go func() { _ = b.Publish(context.Background(), "slow", 1) }() // blocks until broker Close
	testutil.Eventually(t, wait, func() bool { return b.Stats().SlowConsumerEvents == 1 }, "publisher blocked")

	publish(t, b, "other", "x")
	if p := recv(t, other).Payload; p != "x" {
		t.Fatal(p)
	}
	late := subscribe(t, b, "slow", pubsub.SubscribeOptions{}) // control plane still live
	if n := b.SubscriberCount("slow"); n != 2 {
		t.Fatalf("count = %d", n)
	}
	_ = late.Close()
	// newBroker's cleanup closes the broker, which must release the blocked publisher.
}

func TestSlowConsumerDoesNotBlockFastSubscriberWithDropNewest(t *testing.T) {
	b := newBroker(t)
	slow := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 1, Overflow: pubsub.DropNewest})
	fast := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 200, Overflow: pubsub.DropNewest})

	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	for i := 0; i < 200; i++ {
		if err := b.Publish(ctx, "t", i); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	got := payloads(t, fast, 200)
	for i, p := range got {
		if p != i {
			t.Fatalf("fast got %v at %d", p, i)
		}
	}
	if d := slow.Stats().Dropped; d != 199 {
		t.Fatalf("slow dropped %d, want 199", d)
	}
}

// A fast consumer plus a slow consumer: the slow one drops, the fast one does not.
func TestSlowConsumerRateMismatch(t *testing.T) {
	b := newBroker(t)
	slow := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 8, Overflow: pubsub.DropNewest})
	fast := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 8, Overflow: pubsub.DropNewest})

	const total = 500
	var fastGot, slowGot int
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range fast.Messages() {
			fastGot++
		}
	}()
	go func() {
		defer wg.Done()
		for range slow.Messages() {
			slowGot++
			time.Sleep(time.Millisecond)
		}
	}()

	for i := 0; i < total; i++ {
		publish(t, b, "t", i)
		if i%4 == 0 {
			time.Sleep(50 * time.Microsecond) // let the fast consumer keep up
		}
	}
	_ = b.Close()
	testutil.WaitTimeout(t, &wg, wait, "consumers")

	sd := slow.Stats().Dropped
	if sd == 0 {
		t.Fatal("expected the slow consumer to drop messages")
	}
	if uint64(slowGot)+sd != total {
		t.Fatalf("slow: received %d + dropped %d != %d", slowGot, sd, total)
	}
	if uint64(fastGot)+fast.Stats().Dropped != total {
		t.Fatalf("fast: received %d + dropped %d != %d", fastGot, fast.Stats().Dropped, total)
	}
}

func TestOverflowPolicyString(t *testing.T) {
	for p, want := range map[pubsub.OverflowPolicy]string{
		pubsub.Block: "Block", pubsub.DropNewest: "DropNewest",
		pubsub.DropOldest: "DropOldest", pubsub.Disconnect: "Disconnect",
	} {
		if p.String() != want {
			t.Fatalf("%d -> %q", p, p.String())
		}
	}
}
