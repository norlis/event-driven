package event

import (
	"errors"
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

// ExtCorrelationID is the CloudEvents extension carrying the business
// correlation id: chosen by the originating client or minted by the first
// transport that receives the event, and copied to every derived event.
const ExtCorrelationID = "correlationid"

// ExtCausationID is the CloudEvents extension carrying the id of the event
// whose handling produced this one. Set by the mux on every result event;
// handler metadata and incoming extensions cannot override it, so each hop
// points at its immediate parent.
const ExtCausationID = "causationid"

// MaxCorrelationIDLen bounds client-supplied ids: they end up in broker
// attributes, HTTP headers and log fields.
const MaxCorrelationIDLen = 128

// ErrInvalidCorrelationID is returned by ValidateCorrelationID.
var ErrInvalidCorrelationID = errors.New("invalid correlation id")

// ValidateCorrelationID accepts 1..MaxCorrelationIDLen bytes of printable
// ASCII without spaces (0x21..0x7E).
func ValidateCorrelationID(s string) error {
	if s == "" {
		return fmt.Errorf("%w: empty", ErrInvalidCorrelationID)
	}
	if len(s) > MaxCorrelationIDLen {
		return fmt.Errorf("%w: %d bytes exceeds %d", ErrInvalidCorrelationID, len(s), MaxCorrelationIDLen)
	}
	for i := range len(s) {
		if c := s[i]; c < 0x21 || c > 0x7E {
			return fmt.Errorf("%w: byte 0x%02x at offset %d", ErrInvalidCorrelationID, c, i)
		}
	}
	return nil
}

// CorrelationID returns the correlationid extension, or "" when absent or
// not a string. It reads the extension in place: the router calls it on every
// log record, and Event.Extensions() copies the whole map per call.
func CorrelationID(ce cloudevents.Event) string {
	v, _ := extension(ce, ExtCorrelationID).(string)
	return v
}
