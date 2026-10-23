// Package codec maps CloudEvents to and from the NATS wire format (a
// *nats.Msg with CE context attributes in headers). The format is identical
// for Core NATS and JetStream, so both transports share this package.
package codec

import (
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	natsgo "github.com/nats-io/nats.go"

	"github.com/norlis/event-driven/v2/pkg/transport/internal/envelope"
)

// contentTypeHeader carries the payload media type; the value
// application/cloudevents+json selects structured mode.
const contentTypeHeader = "Content-Type"

// Marshaler maps a CloudEvent into a *nats.Msg ready to publish. Swap to
// customize header placement or body encoding (e.g. structured mode).
type Marshaler interface {
	Marshal(subject string, ce cloudevents.Event) (*natsgo.Msg, error)
}

// DefaultMarshaler produces binary-mode messages: "ce-*" headers plus
// Content-Type, event data in Msg.Data. It sets no JetStream headers: dedup
// is the publisher's concern (WithMsgID), keeping the codec delivery-agnostic.
type DefaultMarshaler struct{}

// Marshal implements Marshaler.
func (DefaultMarshaler) Marshal(subject string, ce cloudevents.Event) (*natsgo.Msg, error) {
	attrs, err := envelope.Encode(ce)
	if err != nil {
		return nil, fmt.Errorf("nats: marshal: %w", err)
	}
	h := make(natsgo.Header, len(attrs)+1)
	for k, v := range attrs {
		h.Set(k, v)
	}
	h.Set(contentTypeHeader, envelope.ContentType(ce))
	return &natsgo.Msg{Subject: subject, Header: h, Data: ce.Data()}, nil
}
