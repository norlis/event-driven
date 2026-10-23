package jetstream

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/norlis/event-driven/v2/pkg/event"
)

// termReasonDiscarded surfaces in the server's MSG_TERMINATED advisory.
const termReasonDiscarded = "discarded by eventmux"

// newDelivery maps a JetStream message to event.Delivery. Discard uses Term
// rather than Ack so the server records it and MaxDeliver is not consumed.
func newDelivery(msg jetstream.Msg, nakDelay time.Duration) event.Delivery {
	var count uint64
	if md, err := msg.Metadata(); err == nil {
		count = md.NumDelivered
	}
	return event.Delivery{
		Ack: func() error { return wrap("ack", msg.Ack()) },
		Nack: func() error {
			if nakDelay > 0 {
				return wrap("nak", msg.NakWithDelay(nakDelay))
			}
			return wrap("nak", msg.Nak())
		},
		Discard: func() error { return wrap("term", msg.TermWithReason(termReasonDiscarded)) },
		Count:   count,
	}
}

func wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("jetstream: %s: %w", op, err)
}
