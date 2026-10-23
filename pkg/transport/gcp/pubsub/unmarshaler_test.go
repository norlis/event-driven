package pubsub

import (
	"bytes"
	"errors"
	"testing"

	gpubsub "cloud.google.com/go/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/event"
)

func TestUnmarshalStructured(t *testing.T) {
	t.Parallel()

	m := &gpubsub.Message{
		ID:         "pubsub-msg-1",
		Data:       []byte(`{"specversion":"1.0","id":"abc","source":"/s","type":"t","data":{"k":1}}`),
		Attributes: map[string]string{"Content-Type": cloudevents.ApplicationCloudEventsJSON},
	}
	ce, err := unmarshal(m)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if ce.ID() != "abc" {
		t.Errorf("ID = %q, want %q", ce.ID(), "abc")
	}
	if got := string(ce.Data()); got != `{"k":1}` {
		t.Errorf("Data = %q, want %q", got, `{"k":1}`)
	}
}

// A structured envelope that does not parse is a poison message, not a
// binary-mode message in disguise.
func TestUnmarshalStructuredInvalidIsDecodeError(t *testing.T) {
	t.Parallel()

	m := &gpubsub.Message{
		ID:         "pubsub-msg-1",
		Data:       []byte(`{"specversion":"1.0","id":"a","id":"b","source":"/s","type":"t"}`),
		Attributes: map[string]string{"Content-Type": cloudevents.ApplicationCloudEventsJSON},
	}
	_, err := unmarshal(m)
	if !errors.Is(err, event.ErrDecode) {
		t.Fatalf("Unmarshal() error = %v, want errors.Is(err, event.ErrDecode)", err)
	}
}

func TestUnmarshalBinaryDefaultsAndPrefixedExtensions(t *testing.T) {
	t.Parallel()

	m := &gpubsub.Message{
		ID:   "pubsub-msg-1",
		Data: []byte(`{"k":1}`),
		Attributes: map[string]string{
			"ce-correlationid":          "c-1",
			"correlationid":             "legacy, ignored",
			"googclient_schemaencoding": "JSON",
		},
	}
	ce, err := unmarshal(m)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if ce.ID() != "pubsub-msg-1" {
		t.Errorf("ID = %q, want broker id", ce.ID())
	}
	if ce.Type() != "com.google.cloud.pubsub.message" || ce.Source() != "//pubsub.googleapis.com" {
		t.Errorf("defaults = %q %q", ce.Type(), ce.Source())
	}
	if got := ce.Extensions()["correlationid"]; got != "c-1" {
		t.Errorf("correlationid = %v, want c-1 (from ce-correlationid only)", got)
	}
	if len(ce.Extensions()) != 1 {
		t.Errorf("Extensions = %v, want only correlationid", ce.Extensions())
	}
}

func TestUnmarshalIgnoresLowercaseContentType(t *testing.T) {
	t.Parallel()

	m := &gpubsub.Message{
		ID:         "pubsub-msg-1",
		Data:       []byte(`{"specversion":"1.0","id":"abc","source":"/s","type":"t"}`),
		Attributes: map[string]string{"content-type": cloudevents.ApplicationCloudEventsJSON},
	}
	ce, err := unmarshal(m)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	// Binary mode: the envelope is the payload and the broker id wins.
	if ce.ID() != "pubsub-msg-1" {
		t.Errorf("ID = %q, want broker id (binary mode)", ce.ID())
	}
	if !bytes.Equal(ce.Data(), m.Data) {
		t.Errorf("Data = %s, want the raw envelope bytes", ce.Data())
	}
}
