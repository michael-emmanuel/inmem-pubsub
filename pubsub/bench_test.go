package pubsub_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/michael-emmanuel/inmem-pubsub/pubsub"
)

// setup creates a broker with n subscriptions on topic "t". If drain is true,
// each subscription gets a consumer goroutine that discards messages; this
// measures publish cost with consumers keeping up. If false, nothing reads,
// which with a drop policy measures the buffer-full path.
func setup(b *testing.B, n, buf int, policy pubsub.OverflowPolicy, drain bool) *pubsub.Broker {
	b.Helper()
	br := pubsub.New()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		s, err := br.Subscribe(context.Background(), "t", pubsub.SubscribeOptions{BufferSize: buf, Overflow: policy})
		if err != nil {
			b.Fatal(err)
		}
		if drain {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range s.Messages() {
				}
			}()
		}
	}
	b.Cleanup(func() { _ = br.Close(); wg.Wait() })
	return br
}

// BenchmarkPublishNoSubscribers measures the fixed cost of Publish: argument
// checks, RLock, map lookup, ID allocation, clock read, atomics.
func BenchmarkPublishNoSubscribers(b *testing.B) {
	br := setup(b, 0, 0, pubsub.Block, false)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = br.Publish(ctx, "t", i)
	}
}

// BenchmarkPublish is single publisher, single draining subscriber.
func BenchmarkPublish(b *testing.B) {
	br := setup(b, 1, 1024, pubsub.Block, true)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = br.Publish(ctx, "t", i)
	}
}

// BenchmarkFanOut is one publisher, N draining subscribers. ns/op should grow
// roughly linearly with N (Publish is O(N)).
func BenchmarkFanOut(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("subs=%d", n), func(b *testing.B) {
			br := setup(b, n, 1024, pubsub.Block, true)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = br.Publish(ctx, "t", i)
			}
		})
	}
}

// BenchmarkPublishManySubscribers isolates the per-subscriber iteration cost
// with 10,000 subscribers that never read and drop on overflow, so there are
// no consumer goroutines competing for CPU.
func BenchmarkPublishManySubscribers(b *testing.B) {
	br := setup(b, 10000, 1, pubsub.DropNewest, false)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = br.Publish(ctx, "t", i)
	}
}

// BenchmarkConcurrentPublish is many publishers (one per GOMAXPROCS slot) and
// 8 draining subscribers: it exposes contention on the registry RLock and on
// each subscription's channel.
func BenchmarkConcurrentPublish(b *testing.B) {
	br := setup(b, 8, 1024, pubsub.Block, true)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = br.Publish(ctx, "t", i)
			i++
		}
	})
}

// BenchmarkBufferSize varies the buffer with one draining subscriber. Small
// buffers force more publisher/consumer hand-offs; large ones absorb jitter.
func BenchmarkBufferSize(b *testing.B) {
	for _, size := range []int{1, 16, 256, 4096} {
		b.Run(fmt.Sprintf("buf=%d", size), func(b *testing.B) {
			br := setup(b, 1, size, pubsub.Block, true)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = br.Publish(ctx, "t", i)
			}
		})
	}
}

// BenchmarkOverflowPolicy compares the cost of the buffer-full path. The
// subscriber never reads, so after the first message every publish overflows
// (the Block case uses a draining subscriber, since a stalled one would
// block the benchmark forever).
func BenchmarkOverflowPolicy(b *testing.B) {
	cases := []struct {
		name   string
		policy pubsub.OverflowPolicy
		drain  bool
	}{
		{"Block/drained", pubsub.Block, true},
		{"DropNewest/full", pubsub.DropNewest, false},
		{"DropOldest/full", pubsub.DropOldest, false},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			br := setup(b, 1, 16, c.policy, c.drain)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = br.Publish(ctx, "t", i)
			}
		})
	}
}

// BenchmarkSubscribeUnsubscribe measures the cost of registry mutation with N
// existing subscribers on the topic. It is O(N) because the subscriber slice
// is copy-on-write; that is the price of an allocation-free Publish.
func BenchmarkSubscribeUnsubscribe(b *testing.B) {
	for _, n := range []int{10, 1000, 10000} {
		b.Run(fmt.Sprintf("existing=%d", n), func(b *testing.B) {
			br := setup(b, n, 1, pubsub.DropNewest, false)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s, _ := br.Subscribe(ctx, "t", pubsub.SubscribeOptions{BufferSize: 1})
				_ = s.Close()
			}
		})
	}
}
