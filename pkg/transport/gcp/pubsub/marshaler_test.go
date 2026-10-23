package pubsub

import (
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

func TestMarshalBinary(t *testing.T) {
	t.Parallel()

	ce := cloudevents.New()
	ce.SetID("id-1")
	ce.SetSource("/s")
	ce.SetType("t")
	ce.SetTime(time.Date(2026, 1, 2, 3, 4, 5, 600000000, time.UTC))
	ce.SetExtension("correlationid", "c-1")
	ce.SetExtension("attempt", int32(2))
	_ = ce.SetData("application/avro", []byte(`x`))

	m, err := marshal(ce)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	want := map[string]string{
		"ce-id": "id-1", "ce-specversion": "1.0", "ce-type": "t", "ce-source": "/s",
		"ce-time": "2026-01-02T03:04:05.6Z", "ce-correlationid": "c-1", "ce-attempt": "2",
		"Content-Type": "application/avro",
	}
	for k, v := range want {
		if m.Attributes[k] != v {
			t.Errorf("Attributes[%s] = %q, want %q", k, m.Attributes[k], v)
		}
	}
	if len(m.Attributes) != len(want) {
		t.Errorf("Attributes = %v, want exactly %d keys", m.Attributes, len(want))
	}
	if string(m.Data) != "x" {
		t.Errorf("Data = %q, want x", m.Data)
	}
}
