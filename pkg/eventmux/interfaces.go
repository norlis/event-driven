package eventmux

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/pkg/event"
)

// Filter decides whether a message should be routed to a given handler.
type Filter interface {
	Match(msg *event.Message) bool
}

// Publisher sends a CloudEvent to a downstream transport (Pub/Sub, SNS, HTTP, …).
// Implementations must honor ctx for cancellation/deadline of the outbound I/O
// and read the W3C trace context from it for propagation.
type Publisher interface {
	Publish(ctx context.Context, ce cloudevents.Event) error
}

// Subscription is a long-running source of CloudEvents. Start must block
// until ctx is cancelled and invoke handler for every received message.
type Subscription interface {
	Start(ctx context.Context, handler func(*event.Message)) error
}
