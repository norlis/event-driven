package codec

import (
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

func TestDefaultMarshalerBinary(t *testing.T) {
	t.Parallel()

	ce := cloudevents.New()
	ce.SetID("id-1")
	ce.SetSource("/s")
	ce.SetType("t")
	ce.SetTime(time.Date(2026, 1, 2, 3, 4, 5, 600000000, time.UTC))
	ce.SetExtension("replayed", true)
	_ = ce.SetData(cloudevents.ApplicationJSON, []byte(`{"k":1}`))

	msg, err := DefaultMarshaler{}.Marshal("orders.created", ce)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	want := map[string]string{
		"ce-id": "id-1", "ce-specversion": "1.0", "ce-type": "t", "ce-source": "/s",
		"ce-time": "2026-01-02T03:04:05.6Z", "ce-replayed": "true", "Content-Type": "application/json",
	}
	for k, v := range want {
		if got := msg.Header.Get(k); got != v {
			t.Errorf("Header[%s] = %q, want %q", k, got, v)
		}
	}
	if len(msg.Header) != len(want) {
		t.Errorf("Header = %v, want exactly %d keys", msg.Header, len(want))
	}
	if msg.Subject != "orders.created" || string(msg.Data) != `{"k":1}` {
		t.Errorf("Subject=%q Data=%s", msg.Subject, msg.Data)
	}
}
