package pubsub

import (
	"fmt"

	gpubsub "cloud.google.com/go/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/transport/internal/envelope"
)

// marshal produces a binary-mode message: "ce-*" attributes (extensions
// included) plus Content-Type, and the event data as Message.Data.
func marshal(ce cloudevents.Event) (*gpubsub.Message, error) {
	attrs, err := envelope.Encode(ce)
	if err != nil {
		return nil, fmt.Errorf("pubsub: marshal: %w", err)
	}
	attrs[contentTypeAttribute] = envelope.ContentType(ce)
	return &gpubsub.Message{Data: ce.Data(), Attributes: attrs}, nil
}
