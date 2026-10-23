package codec

import (
	"fmt"
	"iter"
	"uuid"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	natsgo "github.com/nats-io/nats.go"

	"github.com/norlis/event-driven/v2/pkg/transport/internal/envelope"
)

var defaults = envelope.Defaults{Type: "io.nats.message", Source: "//nats.io"}

// Unmarshal reads binary mode ("ce-*" headers) or structured mode
// (Content-Type application/cloudevents+json). Taking (headers, data) rather
// than a concrete message type lets it serve both *nats.Msg (core) and
// jetstream.Msg. Without ce-id the event takes the JetStream dedup id
// (Nats-Msg-Id) or a generated UUID v7: core NATS has no broker id and the mux
// needs one for logs and results.
func Unmarshal(headers natsgo.Header, data []byte) (cloudevents.Event, error) {
	def := defaults
	def.ID = headers.Get(natsgo.MsgIdHdr)
	if def.ID == "" {
		def.ID = uuid.NewV7().String()
	}
	ce, err := envelope.Decode(headers.Get(contentTypeHeader), firstValues(headers), data, def)
	if err != nil {
		return cloudevents.Event{}, fmt.Errorf("nats: unmarshal: %w", err)
	}
	return ce, nil
}

// Rescue builds the event for a NATS message Unmarshal could not decode: Nats-Msg-Id or a UUID v7 as id, the codec defaults
// and the raw data. Core and JetStream subscribers hand it to the router as an
// undecodable message.
func Rescue(headers natsgo.Header, data []byte) cloudevents.Event {
	def := defaults
	def.ID = headers.Get(natsgo.MsgIdHdr)
	if def.ID == "" {
		def.ID = uuid.NewV7().String()
	}
	return envelope.Rescue(def, headers.Get(contentTypeHeader), data)
}

// firstValues yields each header with its first value, the way Header.Get
// reads it.
func firstValues(h natsgo.Header) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for k, vs := range h {
			if len(vs) == 0 {
				continue
			}
			if !yield(k, vs[0]) {
				return
			}
		}
	}
}
