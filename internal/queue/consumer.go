// Package queue provides a pluggable abstraction over the queue backends
// (Gearman, RabbitMQ, ...) that deliver statusengine events.
package queue

import "context"

// Message is a raw payload received from a single queue, tagged with the
// queue name it came from (e.g. "statusngin_hoststatus") so the pipeline can
// route it to the correct decoder.
type Message struct {
	Queue   string
	Payload []byte
}

// Consumer is implemented by every pluggable queue backend. Implementations
// must be safe to Stop concurrently with Start's delivery loop, so that
// graceful shutdown can flush in-flight work before exiting.
type Consumer interface {
	// Start begins consuming messages and returns a channel that receives
	// them until ctx is cancelled or Stop is called, at which point the
	// channel is closed.
	Start(ctx context.Context) (<-chan Message, error)

	// Stop signals the consumer to stop pulling new messages and releases
	// its underlying connection(s).
	Stop() error
}
