package websocket

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestHubConcurrentStress hammers Publish, register, unregister and
// updateSubscription from many goroutines at once, with concurrently
// connecting/disconnecting clients, to give `go test -race` (CLAUDE.md's
// test command) a much larger concurrency surface than the sequential
// existing tests exercise - proving Hub's single-goroutine-owns-state
// design (see hub.go's doc comment) actually holds up under load.
func TestHubConcurrentStress(t *testing.T) {
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	const numClients = 50
	const numPublishers = 20
	const publishesPerGoroutine = 200

	var wg sync.WaitGroup

	// Concurrently register/unregister/resubscribe clients while publishers
	// hammer the broadcast channel - drives register, unregister and
	// updateSubscription concurrently with dispatch.
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := &Client{
				hub:    hub,
				send:   make(chan []byte, 4),
				topics: map[string]struct{}{fmt.Sprintf("topic-%d", i%5): {}},
			}
			hub.register <- c

			// Drain concurrently so dispatch's non-blocking send doesn't
			// just always hit the "buffer full, drop" path.
			drainDone := make(chan struct{})
			go func() {
				defer close(drainDone)
				for range c.send {
				}
			}()

			hub.updateSubscription <- subscriptionUpdate{
				client: c,
				sub:    subscriptionMessage{Subscribe: []string{fmt.Sprintf("topic-%d", (i+1)%5)}},
			}

			time.Sleep(time.Millisecond)
			hub.unregister <- c
			<-drainDone
		}(i)
	}

	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < publishesPerGoroutine; j++ {
				hub.Publish(fmt.Sprintf("topic-%d", j%5), []byte(`[{"n":1}]`), 1)
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("stress test did not complete in time")
	}
}
