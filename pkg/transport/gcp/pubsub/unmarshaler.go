package pubsub

import (
	"encoding/json"
	"strings"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

// Unmarshaler maps a raw Pub/Sub *Message into a CloudEvent. Swap to
// customize attribute parsing, body decoding, or to bridge legacy formats.
type Unmarshaler interface {
	Unmarshal(m *gpubsub.Message) (cloudevents.Event, error)
}

// DefaultUnmarshaler reads CloudEvents from Pub/Sub messages in either:
//
//   - Structured content mode: Content-Type is "application/cloudevents+json"
//     and the entire CloudEvent is JSON-encoded in Message.Data.
//   - Binary content mode: CE context attributes are in Message.Attributes
//     with the "ce-" prefix; event data is in Message.Data.
//
// Structured-mode parse failures fall back to binary mode silently. Wrap
// DefaultUnmarshaler if you need visibility into that fallback.
type DefaultUnmarshaler struct{}

// Unmarshal implements Unmarshaler.
func (DefaultUnmarshaler) Unmarshal(m *gpubsub.Message) (cloudevents.Event, error) {
	// Structured content mode.
	if m.Attributes["Content-Type"] == "application/cloudevents+json" {
		var ce cloudevents.Event
		if err := json.Unmarshal(m.Data, &ce); err == nil {
			return ce, nil
		}
		// fall through to binary mode
	}

	// Binary content mode.
	ce := cloudevents.New()
	ce.SetSpecVersion("1.0")

	if id := m.Attributes["ce-id"]; id != "" {
		ce.SetID(id)
	} else {
		ce.SetID(m.ID)
	}
	if t := m.Attributes["ce-type"]; t != "" {
		ce.SetType(t)
	} else {
		ce.SetType("com.google.cloud.pubsub.message")
	}
	if src := m.Attributes["ce-source"]; src != "" {
		ce.SetSource(src)
	} else {
		ce.SetSource("//pubsub.googleapis.com")
	}
	if subj := m.Attributes["ce-subject"]; subj != "" {
		ce.SetSubject(subj)
	}
	if t := m.Attributes["ce-time"]; t != "" {
		if parsed, parseErr := time.Parse(time.RFC3339, t); parseErr == nil {
			ce.SetTime(parsed)
		}
	} else {
		ce.SetTime(time.Now())
	}

	_ = ce.SetData(cloudevents.ApplicationJSON, m.Data)

	// Forward non-ce attributes as extensions (preserving the convention
	// DefaultMarshaler uses: extensions are stored without the ce- prefix).
	for k, v := range m.Attributes {
		if strings.HasPrefix(k, "ce-") || k == "Content-Type" {
			continue
		}
		ce.SetExtension(k, v)
	}

	return ce, nil
}
