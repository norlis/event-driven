package codec

import (
	"errors"
	"testing"
	"uuid"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	natsgo "github.com/nats-io/nats.go"

	"github.com/norlis/event-driven/v2/pkg/event"
)

func structuredHeaders() natsgo.Header {
	h := natsgo.Header{}
	h.Set("Content-Type", cloudevents.ApplicationCloudEventsJSON)
	return h
}

func TestUnmarshalStructured(t *testing.T) {
	t.Parallel()

	data := []byte(`{"specversion":"1.0","id":"abc","source":"/s","type":"t","data":{"k":1}}`)
	ce, err := Unmarshal(structuredHeaders(), data)
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

func TestUnmarshalStructuredInvalidIsDecodeError(t *testing.T) {
	t.Parallel()

	body := `{"specversion":"1.0","id":"a","id":"b","source":"/s","type":"t"}`
	_, err := Unmarshal(structuredHeaders(), []byte(body))
	if !errors.Is(err, event.ErrDecode) {
		t.Fatalf("Unmarshal() error = %v, want ErrDecode", err)
	}
}

func TestUnmarshalBinaryIDFallback(t *testing.T) {
	t.Parallel()

	t.Run("Nats-Msg-Id wins", func(t *testing.T) {
		t.Parallel()
		h := natsgo.Header{}
		h.Set(natsgo.MsgIdHdr, "dedup-1")
		h.Set("ce-type", "t")
		ce, err := Unmarshal(h, []byte(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		if ce.ID() != "dedup-1" {
			t.Errorf("ID = %q, want dedup-1", ce.ID())
		}
		if ce.Type() != "t" || ce.Source() != "//nats.io" {
			t.Errorf("type=%q source=%q", ce.Type(), ce.Source())
		}
	})
	t.Run("generated UUID v7 otherwise", func(t *testing.T) {
		t.Parallel()
		ce, err := Unmarshal(natsgo.Header{}, []byte(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		id, err := uuid.Parse(ce.ID())
		if err != nil {
			t.Fatalf("ID %q is not a UUID: %v", ce.ID(), err)
		}
		if v := id[6] >> 4; v != 7 {
			t.Errorf("UUID version = %d, want 7", v)
		}
		if ce.Type() != "io.nats.message" {
			t.Errorf("Type = %q", ce.Type())
		}
	})
}

func TestRescueUsesMsgIDOrGeneratesOne(t *testing.T) {
	t.Parallel()

	h := natsgo.Header{}
	h.Set(natsgo.MsgIdHdr, "dedup-1")
	h.Set("Content-Type", "application/octet-stream")
	ce := Rescue(h, []byte{0xff})
	if err := ce.Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
	if ce.ID() != "dedup-1" || ce.Type() != defaults.Type || ce.DataContentType() != "application/octet-stream" || string(ce.Data()) != "\xff" {
		t.Errorf("rescue = %s/%s %s %q", ce.ID(), ce.Type(), ce.DataContentType(), ce.Data())
	}
	if generated := Rescue(natsgo.Header{}, nil); generated.ID() == "" || generated.Validate() != nil {
		t.Errorf("rescue without Nats-Msg-Id must generate an id: %q", generated.ID())
	}
}
