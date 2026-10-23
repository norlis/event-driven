package pubsub

import (
	"fmt"
	"maps"

	gpubsub "cloud.google.com/go/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/transport/internal/envelope"
)

// contentTypeAttribute carries the payload media type; the value
// application/cloudevents+json selects structured mode.
const contentTypeAttribute = "Content-Type"

var defaults = envelope.Defaults{Type: "com.google.cloud.pubsub.message", Source: "//pubsub.googleapis.com"}

// unmarshal reads binary mode ("ce-*" attributes, data in Message.Data) or
// structured mode (Content-Type application/cloudevents+json, the whole event
// in Message.Data). An invalid envelope is an event.ErrDecode: the subscriber
// hands it to the router as an undecodable message.
func unmarshal(m *gpubsub.Message) (cloudevents.Event, error) {
	def := defaults
	def.ID = m.ID
	ce, err := envelope.Decode(m.Attributes[contentTypeAttribute], maps.All(m.Attributes), m.Data, def)
	if err != nil {
		return cloudevents.Event{}, fmt.Errorf("pubsub: unmarshal: %w", err)
	}
	return ce, nil
}
