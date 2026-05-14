package pubsub

import (
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

// Marshaler maps a CloudEvent into a Pub/Sub *Message ready to publish. Swap
// to customize attribute placement, body encoding, ordering keys, or to
// produce structured-mode messages (Content-Type=application/cloudevents+json
// with the full CloudEvent JSON in Data).
type Marshaler interface {
	Marshal(ce cloudevents.Event) (*gpubsub.Message, error)
}

// DefaultMarshaler produces binary-mode messages: CE attributes go in the
// Pub/Sub message Attributes with a "ce-" prefix, and the event data goes
// in Message.Data.
//
// String-typed CloudEvent extensions are propagated as-is (without the
// "ce-" prefix) to match the convention DefaultUnmarshaler expects.
type DefaultMarshaler struct{}

// Marshal implements Marshaler.
func (DefaultMarshaler) Marshal(ce cloudevents.Event) (*gpubsub.Message, error) {
	attrs := map[string]string{
		"ce-id":          ce.ID(),
		"ce-source":      ce.Source(),
		"ce-type":        ce.Type(),
		"ce-specversion": ce.SpecVersion(),
	}
	if !ce.Time().IsZero() {
		attrs["ce-time"] = ce.Time().Format(time.RFC3339)
	}
	if subj := ce.Subject(); subj != "" {
		attrs["ce-subject"] = subj
	}
	for k, v := range ce.Extensions() {
		if s, ok := v.(string); ok {
			attrs[k] = s
		}
	}
	return &gpubsub.Message{
		Data:       ce.Data(),
		Attributes: attrs,
	}, nil
}
