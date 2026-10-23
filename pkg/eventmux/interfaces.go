package eventmux

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/event"
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

// Subscription is a long-running source of CloudEvents.
type Subscription interface {
	// Start establishes the subscription and returns once messages can flow;
	// a setup failure is returned here, synchronously. ctx bounds the setup
	// calls only, not the subscription's lifetime. Afterwards every received
	// message goes to handler and a failure that stops delivery for good
	// (not a Stop) goes to onError, at most once. onError must not block;
	// nil is tolerated.
	Start(ctx context.Context, handler func(*event.Message), onError func(error)) error
	// Stop ends delivery and waits for in-flight handlers until ctx expires.
	Stop(ctx context.Context) error
}
