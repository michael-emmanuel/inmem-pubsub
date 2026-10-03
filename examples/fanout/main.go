// Command fanout shows one publisher feeding several independent subscribers.
// Each subscriber has its own buffer and sees every message.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/example/inmem-pubsub/pubsub"
)

func main() {
	broker := pubsub.New()
	defer broker.Close()

	names := []string{"audit-log", "email", "analytics"}
	var wg sync.WaitGroup
	results := make([][]int, len(names))

	for i := range names {
		sub, err := broker.Subscribe(context.Background(), "orders", pubsub.SubscribeOptions{BufferSize: 8})
		if err != nil {
			log.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range sub.Messages() {
				results[i] = append(results[i], msg.Payload.(int))
			}
		}()
	}

	for n := 1; n <= 5; n++ {
		if err := broker.Publish(context.Background(), "orders", n); err != nil {
			log.Fatal(err)
		}
	}

	// Closing the broker closes every subscription; consumers drain what was
	// already buffered, then exit.
	_ = broker.Close()
	wg.Wait()

	for i, name := range names {
		fmt.Printf("%-10s received %v\n", name, results[i])
	}
	fmt.Printf("stats: %+v\n", broker.Stats())
}
