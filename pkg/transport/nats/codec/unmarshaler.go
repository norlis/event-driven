package codec

import (
	"encoding/json"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	natsgo "github.com/nats-io/nats.go"
)

// reservedCEHeaders are ce- headers mapped to first-class CloudEvent
// attributes; every other ce-* header becomes an extension.
var reservedCEHeaders = map[string]struct{}{
	"ce-id": {}, "ce-source": {}, "ce-type": {}, "ce-specversion": {},
	"ce-time": {}, "ce-subject": {},
}

// Unmarshaler maps raw NATS headers + data into a CloudEvent. Taking
// (headers, data) rather than a concrete message type lets the same
// implementation serve both *nats.Msg (core) and jetstream.Msg.
type Unmarshaler interface {
	Unmarshal(headers natsgo.Header, data []byte) (cloudevents.Event, error)
}

// DefaultUnmarshaler reads CloudEvents in either:
//   - Structured mode: header Content-Type == "application/cloudevents+json"
//     and the whole event is JSON-encoded in data.
//   - Binary mode: ce-* headers carry context attributes; data is the payload.
//
// Structured-mode parse failures fall back to binary mode.
type DefaultUnmarshaler struct{}

// Unmarshal implements Unmarshaler.
func (DefaultUnmarshaler) Unmarshal(headers natsgo.Header, data []byte) (cloudevents.Event, error) {
	if headers.Get("Content-Type") == "application/cloudevents+json" {
		var ce cloudevents.Event
		if err := json.Unmarshal(data, &ce); err == nil {
			return ce, nil
		}
		// fall through to binary mode
	}

	ce := cloudevents.New()
	ce.SetSpecVersion("1.0")

	if id := headers.Get("ce-id"); id != "" {
		ce.SetID(id)
	}
	if t := headers.Get("ce-type"); t != "" {
		ce.SetType(t)
	} else {
		ce.SetType("io.nats.message")
	}
	if src := headers.Get("ce-source"); src != "" {
		ce.SetSource(src)
	} else {
		ce.SetSource("//nats.io")
	}
	if subj := headers.Get("ce-subject"); subj != "" {
		ce.SetSubject(subj)
	}
	if t := headers.Get("ce-time"); t != "" {
		if parsed, err := time.Parse(time.RFC3339, t); err == nil {
			ce.SetTime(parsed)
		}
	} else {
		ce.SetTime(time.Now())
	}

	_ = ce.SetData(cloudevents.ApplicationJSON, data)

	// Forward remaining ce-* headers as extensions (prefix stripped).
	for k := range headers {
		lk := strings.ToLower(k)
		if !strings.HasPrefix(lk, "ce-") {
			continue
		}
		if _, reserved := reservedCEHeaders[lk]; reserved {
			continue
		}
		ce.SetExtension(strings.TrimPrefix(lk, "ce-"), headers.Get(k))
	}

	return ce, nil
}
