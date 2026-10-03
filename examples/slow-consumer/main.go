// Command slow-consumer runs the same workload against each overflow policy:
// a publisher emitting 40 messages as fast as it can, and a consumer that
// needs 5ms per message, behind a 4-slot buffer. The output shows what each
// policy does with the excess.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/example/inmem-pubsub/pubsub"
)

const (
	messages = 40
	bufSize  = 4
	workTime = 5 * time.Millisecond
)

func main() {
	for _, policy := range []pubsub.OverflowPolicy{pubsub.Block, pubsub.DropNewest, pubsub.DropOldest, pubsub.Disconnect} {
		run(policy)
	}
}

func run(policy pubsub.OverflowPolicy) {
	broker := pubsub.New()
	sub, err := broker.Subscribe(context.Background(), "ticks", pubsub.SubscribeOptions{
		BufferSize: bufSize,
		Overflow:   policy,
	})
	if err != nil {
		log.Fatal(err)
	}

	var (
		wg       sync.WaitGroup
		received []int
	)
	wg.Add(1)
	go func() { // slow consumer
		defer wg.Done()
		for msg := range sub.Messages() {
			received = append(received, msg.Payload.(int))
			time.Sleep(workTime)
		}
	}()

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < messages; i++ {
		if err := broker.Publish(ctx, "ticks", i); err != nil {
			log.Fatal(err)
		}
	}
	publishTook := time.Since(start)

	// Let the consumer finish what is buffered, then shut down.
	time.Sleep(workTime * (bufSize + 2))
	_ = broker.Close()
	wg.Wait()

	st := broker.Stats()
	fmt.Printf("policy=%-10s publish took %-6v received=%-2d dropped=%-2d slow-events=%-2d disconnected=%d sub.Err=%v\n",
		policy, publishTook.Round(time.Millisecond), len(received), st.Dropped, st.SlowConsumerEvents, st.Disconnected, sub.Err())
	fmt.Printf("    received: %v\n\n", received)
}
