// Package codec maps CloudEvents to and from the NATS wire format (a
// *nats.Msg with CE context attributes in headers). The format is identical
// for Core NATS and JetStream, so both transports share this package.
package codec

import (
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	natsgo "github.com/nats-io/nats.go"
)

// Marshaler maps a CloudEvent into a *nats.Msg ready to publish. Swap to
// customize header placement or body encoding (e.g. structured mode).
type Marshaler interface {
	Marshal(subject string, ce cloudevents.Event) (*natsgo.Msg, error)
}

// DefaultMarshaler produces binary-mode messages: CE context attributes go in
// the NATS headers with a "ce-" prefix; the event data goes in Msg.Data.
// String-typed CloudEvent extensions are propagated as "ce-<name>".
//
// It sets no transport-specific headers: JetStream dedup is the publisher's
// concern (via WithMsgID), keeping this codec delivery-agnostic.
type DefaultMarshaler struct{}

// Marshal implements Marshaler.
func (DefaultMarshaler) Marshal(subject string, ce cloudevents.Event) (*natsgo.Msg, error) {
	h := make(natsgo.Header, 6)
	h.Set("ce-id", ce.ID())
	h.Set("ce-source", ce.Source())
	h.Set("ce-type", ce.Type())
	h.Set("ce-specversion", ce.SpecVersion())
	if !ce.Time().IsZero() {
		h.Set("ce-time", ce.Time().Format(time.RFC3339))
	}
	if subj := ce.Subject(); subj != "" {
		h.Set("ce-subject", subj)
	}
	for k, v := range ce.Extensions() {
		if s, ok := v.(string); ok {
			h.Set("ce-"+k, s)
		}
	}
	return &natsgo.Msg{
		Subject: subject,
		Header:  h,
		Data:    ce.Data(),
	}, nil
}
