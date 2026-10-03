// Command basic shows the complete lifecycle: create a broker, subscribe,
// publish, consume, close.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/example/inmem-pubsub/pubsub"
)

type OrderCreated struct {
	ID    int
	Total float64
}

func main() {
	broker := pubsub.New()
	defer broker.Close() // idempotent; also closes every subscription

	ctx := context.Background()
	sub, err := broker.Subscribe(ctx, "orders", pubsub.SubscribeOptions{BufferSize: 16})
	if err != nil {
		log.Fatal(err)
	}

	// The consumer goroutine ends when the Messages channel is closed, which
	// happens on sub.Close(), context cancellation or broker.Close().
	done := make(chan struct{})
	go func() {
		defer close(done)
		for msg := range sub.Messages() {
			order := msg.Payload.(OrderCreated) // payloads are shared: treat as read-only
			fmt.Printf("received order %d total=%.2f (message id %d)\n", order.ID, order.Total, msg.ID)
		}
		fmt.Println("subscription ended:", sub.Err())
	}()

	pubCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	for i := 1; i <= 3; i++ {
		if err := broker.Publish(pubCtx, "orders", OrderCreated{ID: i, Total: float64(i) * 9.99}); err != nil {
			log.Fatal(err)
		}
	}

	_ = sub.Close()
	<-done
	fmt.Printf("stats: %+v\n", broker.Stats())
}
