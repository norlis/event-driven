package event

import (
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

func newTestEvent(id string) cloudevents.Event {
	ce := cloudevents.New()
	ce.SetID(id)
	ce.SetType("test.event")
	ce.SetSource("test://source")
	return ce
}

func TestMessage_Ack(t *testing.T) {
	t.Parallel()

	ackCalled := false
	msg := NewMessage(newTestEvent("ack-1"), func() { ackCalled = true }, func() {})

	msg.Ack()
	msg.Ack() // second call should have no effect

	if !ackCalled {
		t.Error("ackFunc was not called after msg.Ack()")
	}

	select {
	case <-msg.Context().Done():
	default:
		t.Error("message context was not cancelled after Ack()")
	}
}

func TestMessage_Nack(t *testing.T) {
	t.Parallel()

	nackCalled := false
	msg := NewMessage(newTestEvent("nack-1"), func() {}, func() { nackCalled = true })

	msg.Nack()
	msg.Nack()

	if !nackCalled {
		t.Error("nackFunc was not called after msg.Nack()")
	}

	select {
	case <-msg.Context().Done():
	default:
		t.Error("message context was not cancelled after Nack()")
	}
}

func TestMessage_AckOrNackOnlyOnce(t *testing.T) {
	t.Parallel()

	t.Run("Ack_then_Nack", func(t *testing.T) {
		t.Parallel()
		ackCount, nackCount := 0, 0
		msg := NewMessage(newTestEvent("once-1"),
			func() { ackCount++ },
			func() { nackCount++ },
		)
		msg.Ack()
		msg.Nack()

		if ackCount != 1 {
			t.Errorf("ackFunc called %d times, expected 1", ackCount)
		}
		if nackCount != 0 {
			t.Errorf("nackFunc called %d times, expected 0", nackCount)
		}
		if msg.Context().Err() == nil {
			t.Error("message context was not cancelled")
		}
	})

	t.Run("Nack_then_Ack", func(t *testing.T) {
		t.Parallel()
		ackCount, nackCount := 0, 0
		msg := NewMessage(newTestEvent("once-2"),
			func() { ackCount++ },
			func() { nackCount++ },
		)
		msg.Nack()
		msg.Ack()

		if nackCount != 1 {
			t.Errorf("nackFunc called %d times, expected 1", nackCount)
		}
		if ackCount != 0 {
			t.Errorf("ackFunc called %d times, expected 0", ackCount)
		}
		if msg.Context().Err() == nil {
			t.Error("message context was not cancelled")
		}
	})
}

func TestMessage_CloudEventFields(t *testing.T) {
	t.Parallel()

	ce := cloudevents.New()
	ce.SetID("ce-123")
	ce.SetType("com.example.test")
	ce.SetSource("//pubsub/project/sub")
	_ = ce.SetData(cloudevents.ApplicationJSON, []byte(`{"name":"test"}`))

	msg := NewMessageWithoutAck(ce)

	if msg.ID() != "ce-123" {
		t.Errorf("ID() = %q, want %q", msg.ID(), "ce-123")
	}
	if msg.Type() != "com.example.test" {
		t.Errorf("Type() = %q, want %q", msg.Type(), "com.example.test")
	}
	if msg.Source() != "//pubsub/project/sub" {
		t.Errorf("Source() = %q, want %q", msg.Source(), "//pubsub/project/sub")
	}
	if string(msg.Data()) != `{"name":"test"}` {
		t.Errorf("Data() = %q, want %q", string(msg.Data()), `{"name":"test"}`)
	}
}
