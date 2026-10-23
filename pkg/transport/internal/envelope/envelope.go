// Package envelope maps CloudEvents to and from the binary content mode
// shared by every transport (attributes as "ce-*" key/value pairs, payload
// as the message body) and to and from the structured mode envelope. It is
// the single place where that mapping lives; transports only adapt their SDK
// message type to it.
package envelope

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"iter"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/types"

	"github.com/norlis/event-driven/v2/pkg/event"
)

// Prefix marks CloudEvents context attributes in binary content mode.
const Prefix = "ce-"

const (
	keyID          = Prefix + "id"
	keySpecVersion = Prefix + "specversion"
	keyType        = Prefix + "type"
	keySource      = Prefix + "source"
	keySubject     = Prefix + "subject"
	keyTime        = Prefix + "time"
	keyDataSchema  = Prefix + "dataschema"
	// keyDataContentType may arrive from other bindings; the transport
	// Content-Type is authoritative, so it is ignored rather than rejected.
	keyDataContentType = Prefix + "datacontenttype"
)

// Defaults fill what the producer did not send: the broker message ID and
// the transport's own type and source.
type Defaults struct {
	ID     string
	Type   string
	Source string
}

// Encode returns the context attributes of ce as "ce-*" pairs. Extensions are
// formatted with types.Format so non-string values (bool, int32, URI,
// Timestamp) are not silently dropped.
func Encode(ce cloudevents.Event) (map[string]string, error) {
	attrs := map[string]string{
		keyID:          ce.ID(),
		keySpecVersion: ce.SpecVersion(),
		keyType:        ce.Type(),
		keySource:      ce.Source(),
	}
	if subj := ce.Subject(); subj != "" {
		attrs[keySubject] = subj
	}
	if t := ce.Time(); !t.IsZero() {
		attrs[keyTime] = t.Format(time.RFC3339Nano)
	}
	if schema := ce.DataSchema(); schema != "" {
		attrs[keyDataSchema] = schema
	}
	for name, v := range ce.Extensions() {
		s, err := types.Format(v)
		if err != nil {
			return nil, fmt.Errorf("envelope: extension %q: %w", name, err)
		}
		attrs[Prefix+name] = s
	}
	return attrs, nil
}

// ContentType is the transport Content-Type for ce's payload.
func ContentType(ce cloudevents.Event) string {
	if ct := ce.DataContentType(); ct != "" {
		return ct
	}
	return cloudevents.ApplicationJSON
}

// EncodeStructured returns ce in the CloudEvents JSON format
// (application/cloudevents+json): every context attribute and extension in
// the body. Valid JSON data is written inline as "data", so it stays readable
// in a queue console and does not grow by a third; anything else (binary,
// text, or the invalid JSON a decode-failure dead-letter copy carries) goes
// to "data_base64" so the envelope remains parseable. Transports use it when
// the binary mode does not fit their attribute limits.
func EncodeStructured(ce cloudevents.Event) ([]byte, error) {
	// ce is a copy: switching the flag does not leak to the caller.
	if ce.DataBase64 && isJSONMediaType(ce.DataMediaType()) && jsontext.Value(ce.Data()).IsValid() {
		ce.DataBase64 = false
	}
	body, err := json.Marshal(ce)
	if err != nil {
		return nil, fmt.Errorf("structured envelope: %w", err)
	}
	return body, nil
}

// isJSONMediaType mirrors the media types the CloudEvents SDK writes inline
// as "data": application/json, text/json and empty (the JSON default). The
// SDK writes any other type, "+json" suffixes included, as a quoted string, so
// those keep base64.
func isJSONMediaType(mt string) bool {
	return mt == "" || mt == cloudevents.ApplicationJSON || mt == cloudevents.TextJSON
}

// Decode rebuilds the event from the transport's Content-Type, its attribute
// pairs and its body. A structured envelope that does not parse, an invalid
// ce-time and an event that fails Validate are decode errors: redelivery
// cannot fix them, so callers treat them like any other poison payload.
func Decode(contentType string, attrs iter.Seq2[string, string], data []byte, def Defaults) (cloudevents.Event, error) {
	if contentType == cloudevents.ApplicationCloudEventsJSON {
		var ce cloudevents.Event
		if err := json.Unmarshal(data, &ce); err != nil {
			return cloudevents.Event{}, fmt.Errorf("%w: structured envelope: %w", event.ErrDecode, err)
		}
		// UnmarshalJSON does not check required attributes; both modes share
		// the same contract.
		if err := ce.Validate(); err != nil {
			return cloudevents.Event{}, fmt.Errorf("%w: structured envelope: %w", event.ErrDecode, err)
		}
		return ce, nil
	}

	ce := cloudevents.New()
	ce.SetSpecVersion(cloudevents.CloudEventsVersionV1)
	ce.SetID(def.ID)
	ce.SetType(def.Type)
	ce.SetSource(def.Source)

	// Brokers do not canonicalize keys: "ce-id" and "Ce-Id" together would be
	// resolved by map order, so a repeated name is rejected instead.
	seen := make(map[string]struct{})
	for key, value := range attrs {
		name, ok := strings.CutPrefix(strings.ToLower(key), Prefix)
		if !ok {
			continue
		}
		if _, dup := seen[name]; dup {
			return cloudevents.Event{}, fmt.Errorf("%w: duplicate attribute %s%s", event.ErrDecode, Prefix, name)
		}
		seen[name] = struct{}{}
		if err := setAttribute(&ce, name, value); err != nil {
			return cloudevents.Event{}, err
		}
	}

	if contentType == "" {
		contentType = cloudevents.ApplicationJSON
	}
	if err := ce.SetData(contentType, data); err != nil {
		return cloudevents.Event{}, fmt.Errorf("%w: set data: %w", event.ErrDecode, err)
	}
	if err := ce.Validate(); err != nil {
		return cloudevents.Event{}, fmt.Errorf("%w: %w", event.ErrDecode, err)
	}
	return ce, nil
}

// Rescue builds the event a transport hands to the router when Decode failed:
// the broker id, the transport's default type and source, and the raw body as
// data under the transport Content-Type (application/json when empty). It
// always satisfies Validate as long as def.ID is set, so the router can log
// it and publish a recoverable dead-letter copy; it carries no attributes the
// producer sent, since those are what could not be decoded.
func Rescue(def Defaults, contentType string, data []byte) cloudevents.Event {
	ce := cloudevents.New()
	ce.SetSpecVersion(cloudevents.CloudEventsVersionV1)
	ce.SetID(def.ID)
	ce.SetType(def.Type)
	ce.SetSource(def.Source)
	if contentType == "" {
		contentType = cloudevents.ApplicationJSON
	}
	// SetData only fails for a value it must encode; raw bytes are stored as is.
	_ = ce.SetData(contentType, data)
	return ce
}

// setAttribute applies one "ce-<name>" pair: known context attributes go to
// their setters, anything else is a string extension.
func setAttribute(ce *cloudevents.Event, name, value string) error {
	switch Prefix + name {
	case keyID:
		ce.SetID(value)
	case keySpecVersion:
		ce.SetSpecVersion(value)
	case keyType:
		ce.SetType(value)
	case keySource:
		ce.SetSource(value)
	case keySubject:
		ce.SetSubject(value)
	case keyDataSchema:
		ce.SetDataSchema(value)
	case keyDataContentType:
	case keyTime:
		t, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return fmt.Errorf("%w: ce-time %q: %w", event.ErrDecode, value, err)
		}
		ce.SetTime(t)
	default:
		ce.SetExtension(name, value)
	}
	return nil
}
