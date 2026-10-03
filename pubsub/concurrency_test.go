package pubsub_test

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/inmem-pubsub/internal/testutil"
	"github.com/example/inmem-pubsub/pubsub"
)

type tagged struct{ pub, seq int }

// Many publishers, one subscriber: nothing is lost under Block, and each
// publisher's own messages arrive in the order that publisher sent them.
func TestConcurrentPublish(t *testing.T) {
	b := newBroker(t)
	const pubs, per = 8, 500
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 16})

	var consumer sync.WaitGroup
	consumer.Add(1)
	go func() {
		defer consumer.Done()
		next := make([]int, pubs)
		for n := 0; n < pubs*per; n++ {
			m, ok := <-s.Messages()
			if !ok {
				t.Errorf("channel closed after %d messages", n)
				return
			}
			p := m.Payload.(tagged)
			if p.seq != next[p.pub] {
				t.Errorf("publisher %d: got seq %d, want %d", p.pub, p.seq, next[p.pub])
				return
			}
			next[p.pub]++
		}
	}()

	var wg sync.WaitGroup
	for p := 0; p < pubs; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if err := b.Publish(context.Background(), "t", tagged{p, i}); err != nil {
					t.Errorf("publish: %v", err)
					return
				}
			}
		}()
	}
	testutil.WaitTimeout(t, &wg, 10*time.Second, "publishers")
	testutil.WaitTimeout(t, &consumer, 10*time.Second, "consumer")
	if st := b.Stats(); st.Published != pubs*per || st.Delivered != pubs*per || st.Dropped != 0 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestConcurrentSubscribeAndUnsubscribe(t *testing.T) {
	b := newBroker(t)
	const n = 64
	subs := make([]*pubsub.Subscription, n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := b.Subscribe(context.Background(), "t", pubsub.SubscribeOptions{})
			if err != nil {
				t.Errorf("subscribe: %v", err)
				return
			}
			subs[i] = s
		}()
	}
	testutil.WaitTimeout(t, &wg, wait, "subscribers")
	if got := b.SubscriberCount("t"); got != n {
		t.Fatalf("count = %d, want %d", got, n)
	}
	if st := b.Stats(); st.ActiveSubscribers != n {
		t.Fatalf("active = %d", st.ActiveSubscribers)
	}

	for _, s := range subs {
		wg.Add(1)
		go func() { defer wg.Done(); _ = s.Close() }()
	}
	testutil.WaitTimeout(t, &wg, wait, "unsubscribers")
	if got := b.SubscriberCount("t"); got != 0 {
		t.Fatalf("count = %d", got)
	}
}

// A publisher blocked on a full buffer must be released when that subscriber
// closes, and must not panic with send-on-closed-channel.
func TestPublishDuringSubscriberClose(t *testing.T) {
	b := newBroker(t)
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 1})
	publish(t, b, "t", 0)

	done := make(chan error, 1)
	go func() { done <- b.Publish(context.Background(), "t", 1) }()
	testutil.Eventually(t, wait, func() bool { return b.Stats().SlowConsumerEvents == 1 }, "publisher blocked")

	_ = s.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
	case <-time.After(wait):
		t.Fatal("publisher still blocked after subscriber Close")
	}
}

// Stress version: publishers run flat out while subscribers come and go.
func TestPublishWhileSubscribersChurn(t *testing.T) {
	for _, policy := range []pubsub.OverflowPolicy{pubsub.Block, pubsub.DropNewest, pubsub.DropOldest, pubsub.Disconnect} {
		t.Run(policy.String(), func(t *testing.T) {
			b := newBroker(t)
			stop := make(chan struct{})
			var pubWG, churnWG, consWG sync.WaitGroup

			for p := 0; p < 4; p++ {
				pubWG.Add(1)
				go func() {
					defer pubWG.Done()
					for i := 0; ; i++ {
						select {
						case <-stop:
							return
						default:
						}
						ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
						err := b.Publish(ctx, "t", i)
						cancel()
						if err != nil && !errors.Is(err, context.DeadlineExceeded) {
							t.Errorf("publish: %v", err)
							return
						}
					}
				}()
			}
			for c := 0; c < 4; c++ {
				churnWG.Add(1)
				go func() {
					defer churnWG.Done()
					for i := 0; i < 200; i++ {
						s, err := b.Subscribe(context.Background(), "t", pubsub.SubscribeOptions{BufferSize: 1 + i%4, Overflow: policy})
						if err != nil {
							t.Errorf("subscribe: %v", err)
							return
						}
						consWG.Add(1)
						go func() {
							defer consWG.Done()
							n := 0
							for range s.Messages() {
								if n++; n == 3 {
									_ = s.Close() // close from inside the consumer loop
								}
							}
						}()
						if i%2 == 0 {
							_ = s.Close()
						}
					}
				}()
			}

			testutil.WaitTimeout(t, &churnWG, 20*time.Second, "churn")
			close(stop)
			testutil.WaitTimeout(t, &pubWG, 20*time.Second, "publishers")
			_ = b.Close()
			testutil.WaitTimeout(t, &consWG, 20*time.Second, "consumers")
			if st := b.Stats(); st.ActiveSubscribers != 0 {
				t.Fatalf("active = %d", st.ActiveSubscribers)
			}
		})
	}
}

func TestBrokerCloseDuringPublish(t *testing.T) {
	b := newBroker(t)
	s := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 1})
	publish(t, b, "t", 0)

	done := make(chan error, 1)
	go func() { done <- b.Publish(context.Background(), "t", 1) }()
	testutil.Eventually(t, wait, func() bool { return b.Stats().SlowConsumerEvents == 1 }, "publisher blocked")

	_ = b.Close()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, pubsub.ErrClosed) {
			t.Fatalf("publish: %v", err)
		}
	case <-time.After(wait):
		t.Fatal("publisher still blocked after broker Close")
	}
	expectClosed(t, s)
}

func TestBrokerCloseRacingManyPublishers(t *testing.T) {
	b := newBroker(t)
	var consWG, pubWG sync.WaitGroup
	for i := 0; i < 8; i++ {
		s := subscribe(t, b, "t", pubsub.SubscribeOptions{BufferSize: 4})
		consWG.Add(1)
		go func() {
			defer consWG.Done()
			for range s.Messages() {
			}
		}()
	}
	for p := 0; p < 8; p++ {
		pubWG.Add(1)
		go func() {
			defer pubWG.Done()
			for {
				if err := b.Publish(context.Background(), "t", 1); err != nil {
					if !errors.Is(err, pubsub.ErrClosed) {
						t.Errorf("publish: %v", err)
					}
					return
				}
			}
		}()
	}
	time.Sleep(20 * time.Millisecond)
	_ = b.Close()
	testutil.WaitTimeout(t, &pubWG, 10*time.Second, "publishers to observe ErrClosed")
	testutil.WaitTimeout(t, &consWG, 10*time.Second, "consumers")
}

func TestSubscribeDuringBrokerClose(t *testing.T) {
	b := newBroker(t)
	var mu sync.Mutex
	var subs []*pubsub.Subscription
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				s, err := b.Subscribe(context.Background(), "t", pubsub.SubscribeOptions{BufferSize: 1})
				if err != nil {
					if !errors.Is(err, pubsub.ErrClosed) {
						t.Errorf("subscribe: %v", err)
					}
					return
				}
				mu.Lock()
				subs = append(subs, s)
				mu.Unlock()
			}
		}()
	}
	time.Sleep(time.Millisecond)
	_ = b.Close()
	testutil.WaitTimeout(t, &wg, wait, "subscribers")

	// Invariant: every subscription that Subscribe returned is closed by Close.
	for _, s := range subs {
		expectClosed(t, s)
	}
	if st := b.Stats(); st.ActiveSubscribers != 0 {
		t.Fatalf("active = %d", st.ActiveSubscribers)
	}
	if n := b.SubscriberCount("t"); n != 0 {
		t.Fatalf("registry leak: %d", n)
	}
}

// Randomised lifecycle stress. Invariants checked:
//   - ActiveSubscribers is never negative while running;
//   - a subscriber only ever receives messages for its own topic;
//   - after Close, every consumer goroutine terminates and ActiveSubscribers == 0.
func TestRandomizedLifecycleStress(t *testing.T) {
	b := newBroker(t)
	topics := []string{"a", "b", "c"}

	var (
		mu       sync.Mutex
		live     []*pubsub.Subscription
		cancels  []context.CancelFunc
		workers  sync.WaitGroup
		consumer sync.WaitGroup
		negative atomic.Bool
		stop     = make(chan struct{})
	)

	go func() { // monitor; exits on stop
		for {
			select {
			case <-stop:
				return
			default:
				if b.Stats().ActiveSubscribers < 0 {
					negative.Store(true)
				}
				time.Sleep(100 * time.Microsecond)
			}
		}
	}()

	for w := 0; w < 6; w++ {
		seed := int64(w + 1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 600; i++ {
				switch rng.Intn(5) {
				case 0, 1: // subscribe
					topic := topics[rng.Intn(len(topics))]
					ctx, cancel := context.WithCancel(context.Background())
					s, err := b.Subscribe(ctx, topic, pubsub.SubscribeOptions{
						BufferSize: 1 + rng.Intn(4),
						Overflow:   pubsub.OverflowPolicy(rng.Intn(4)),
					})
					if err != nil {
						cancel()
						t.Errorf("subscribe: %v", err)
						return
					}
					consumer.Add(1)
					go func() {
						defer consumer.Done()
						for m := range s.Messages() {
							if m.Topic != s.Topic() {
								t.Errorf("topic %q delivered to %q subscriber", m.Topic, s.Topic())
								return
							}
						}
					}()
					mu.Lock()
					live = append(live, s)
					cancels = append(cancels, cancel)
					mu.Unlock()
				case 2: // close a random subscription (possibly already closed)
					mu.Lock()
					var s *pubsub.Subscription
					if len(live) > 0 {
						s = live[rng.Intn(len(live))]
					}
					mu.Unlock()
					if s != nil {
						_ = s.Close()
					}
				case 3: // cancel a random context
					mu.Lock()
					var c context.CancelFunc
					if len(cancels) > 0 {
						c = cancels[rng.Intn(len(cancels))]
					}
					mu.Unlock()
					if c != nil {
						c()
					}
				default: // publish
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
					err := b.Publish(ctx, topics[rng.Intn(len(topics))], i)
					cancel()
					if err != nil && !errors.Is(err, context.DeadlineExceeded) {
						t.Errorf("publish: %v", err)
						return
					}
				}
			}
		}()
	}

	testutil.WaitTimeout(t, &workers, 30*time.Second, "workers")
	_ = b.Close()
	testutil.WaitTimeout(t, &consumer, 10*time.Second, "consumers")
	close(stop)
	mu.Lock()
	for _, c := range cancels {
		c()
	}
	mu.Unlock()

	if negative.Load() {
		t.Fatal("ActiveSubscribers went negative")
	}
	if st := b.Stats(); st.ActiveSubscribers != 0 {
		t.Fatalf("active = %d after Close", st.ActiveSubscribers)
	}
}

// FuzzSubscribeValidation checks that validation is total: every input either
// yields the documented error or a working subscription, and never panics.
func FuzzSubscribeValidation(f *testing.F) {
	f.Add("orders", 10, 0)
	f.Add("", 0, 0)
	f.Add("t", -1, 1)
	f.Add("t", 1<<40, 2)
	f.Add("t", 5, 99)
	f.Fuzz(func(t *testing.T, topic string, size, policy int) {
		b := pubsub.New()
		defer b.Close()
		s, err := b.Subscribe(context.Background(), topic, pubsub.SubscribeOptions{
			BufferSize: size, Overflow: pubsub.OverflowPolicy(policy),
		})
		switch {
		case topic == "":
			if !errors.Is(err, pubsub.ErrTopicRequired) {
				t.Fatalf("err = %v", err)
			}
		case size < 0 || size > pubsub.MaxBufferSize:
			if !errors.Is(err, pubsub.ErrInvalidBufferSize) {
				t.Fatalf("err = %v", err)
			}
		case policy < 0 || policy > int(pubsub.Disconnect):
			if !errors.Is(err, pubsub.ErrInvalidOverflowPolicy) {
				t.Fatalf("err = %v", err)
			}
		default:
			if err != nil {
				t.Fatalf("valid subscribe failed: %v", err)
			}
			if err := b.Publish(context.Background(), topic, 1); err != nil {
				t.Fatal(err)
			}
			if m := <-s.Messages(); m.Topic != topic {
				t.Fatalf("topic mismatch %q", m.Topic)
			}
		}
	})
}
