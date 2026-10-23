package event

import (
	"context"
	"errors"
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
	msg := NewMessage(t.Context(), newTestEvent("ack-1"), Delivery{Ack: func() error { ackCalled = true; return nil }})

	_ = msg.Ack()
	_ = msg.Ack() // second call should have no effect

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
	msg := NewMessage(t.Context(), newTestEvent("nack-1"), Delivery{Nack: func() error { nackCalled = true; return nil }})

	_ = msg.Nack(nil)
	_ = msg.Nack(nil)

	if !nackCalled {
		t.Error("nackFunc was not called after msg.Nack(nil)")
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
		msg := NewMessage(
			t.Context(),
			newTestEvent("once-1"),
			Delivery{Ack: func() error { ackCount++; return nil }, Nack: func() error { nackCount++; return nil }},
		)
		_ = msg.Ack()
		_ = msg.Nack(nil)

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
		msg := NewMessage(
			t.Context(),
			newTestEvent("once-2"),
			Delivery{Ack: func() error { ackCount++; return nil }, Nack: func() error { nackCount++; return nil }},
		)
		_ = msg.Nack(nil)
		_ = msg.Ack()

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

	msg := NewMessage(t.Context(), ce, Delivery{})

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

func TestMessage_Outcome(t *testing.T) {
	t.Parallel()

	cause := errors.New("boom")
	tests := []struct {
		name        string
		act         func(m *Message)
		wantOutcome Outcome
		wantErr     error
		wantAcks    int
		wantNacks   int
	}{
		{name: "pending", act: func(*Message) {}, wantOutcome: OutcomePending},
		{name: "ack", act: func(m *Message) { _ = m.Ack() }, wantOutcome: OutcomeAcked, wantAcks: 1},
		{name: "nack", act: func(m *Message) { _ = m.Nack(cause) }, wantOutcome: OutcomeNacked, wantErr: cause, wantNacks: 1},
		{name: "discard acks the broker", act: func(m *Message) { _ = m.Discard(cause) }, wantOutcome: OutcomeDiscarded, wantErr: cause, wantAcks: 1},
		{name: "first decision wins", act: func(m *Message) { _ = m.Nack(cause); _ = m.Ack(); _ = m.Discard(errors.New("other")) }, wantOutcome: OutcomeNacked, wantErr: cause, wantNacks: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			acks, nacks := 0, 0
			m := NewMessage(t.Context(), newTestEvent("o-1"), Delivery{Ack: func() error { acks++; return nil }, Nack: func() error { nacks++; return nil }})
			tt.act(m)
			outcome, err := m.Outcome()
			if outcome != tt.wantOutcome {
				t.Errorf("Outcome() = %v, want %v", outcome, tt.wantOutcome)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Outcome() err = %v, want %v", err, tt.wantErr)
			}
			if acks != tt.wantAcks || nacks != tt.wantNacks {
				t.Errorf("acks=%d nacks=%d, want acks=%d nacks=%d", acks, nacks, tt.wantAcks, tt.wantNacks)
			}
		})
	}
}

func TestOutcome_String(t *testing.T) {
	t.Parallel()
	want := map[Outcome]string{OutcomePending: "pending", OutcomeAcked: "acked", OutcomeNacked: "nacked", OutcomeDiscarded: "discarded", Outcome(9): "outcome(9)"}
	for o, s := range want {
		if got := o.String(); got != s {
			t.Errorf("Outcome(%d).String() = %q, want %q", uint8(o), got, s)
		}
	}
}

func TestMessage_DeliveryCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		count     uint64
		wantKnown bool
	}{
		{name: "unknown when zero", count: 0, wantKnown: false},
		{name: "first delivery", count: 1, wantKnown: true},
		{name: "redelivery", count: 7, wantKnown: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := NewMessage(t.Context(), newTestEvent("d-1"), Delivery{Count: tt.count})
			got, known := m.DeliveryCount()
			if got != tt.count || known != tt.wantKnown {
				t.Errorf("DeliveryCount() = %d, %v; want %d, %v", got, known, tt.count, tt.wantKnown)
			}
		})
	}
}

func TestMessage_DiscardPrefersDeliveryDiscard(t *testing.T) {
	t.Parallel()

	t.Run("uses Discard when set", func(t *testing.T) {
		t.Parallel()
		acks, discards := 0, 0
		m := NewMessage(t.Context(), newTestEvent("dd-1"), Delivery{Ack: func() error { acks++; return nil }, Discard: func() error { discards++; return nil }})
		m.Discard(errors.New("boom"))
		if acks != 0 || discards != 1 {
			t.Errorf("acks=%d discards=%d, want 0 and 1", acks, discards)
		}
	})
	t.Run("falls back to Ack", func(t *testing.T) {
		t.Parallel()
		acks := 0
		m := NewMessage(t.Context(), newTestEvent("dd-2"), Delivery{Ack: func() error { acks++; return nil }})
		m.Discard(errors.New("boom"))
		if acks != 1 {
			t.Errorf("acks=%d, want 1", acks)
		}
	})
}

func TestMessage_DecisionReturnsBrokerError(t *testing.T) {
	t.Parallel()

	errBroker := errors.New("broker down")
	tests := []struct {
		name    string
		act     func(m *Message) error
		wantErr error
	}{
		{name: "ack", act: func(m *Message) error { return m.Ack() }, wantErr: errBroker},
		{name: "nack", act: func(m *Message) error { return m.Nack(errors.New("x")) }, wantErr: errBroker},
		{name: "discard", act: func(m *Message) error { return m.Discard(errors.New("x")) }, wantErr: errBroker},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fail := func() error { return errBroker }
			m := NewMessage(t.Context(), newTestEvent("e-1"), Delivery{Ack: fail, Nack: fail, Discard: fail})
			if err := tt.act(m); !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			// The decision is recorded even though the broker refused it.
			if outcome, _ := m.Outcome(); outcome == OutcomePending {
				t.Error("outcome still pending after a failed decision")
			}
		})
	}
}

func TestMessage_NilCallbackAndSecondDecisionReturnNil(t *testing.T) {
	t.Parallel()

	m := NewMessage(t.Context(), newTestEvent("e-2"), Delivery{})
	if err := m.Ack(); err != nil {
		t.Errorf("Ack with nil callback = %v, want nil", err)
	}

	calls := 0
	fail := func() error { calls++; return errors.New("broker down") }
	m2 := NewMessage(t.Context(), newTestEvent("e-3"), Delivery{Ack: fail, Nack: fail})
	_ = m2.Nack(errors.New("x"))
	if err := m2.Ack(); err != nil {
		t.Errorf("second decision = %v, want nil", err)
	}
	if calls != 1 {
		t.Errorf("broker called %d times, want 1", calls)
	}
}

func TestDeliveryCountFromContext(t *testing.T) {
	t.Parallel()

	if _, known := DeliveryCountFromContext(t.Context()); known {
		t.Fatal("plain context reports a delivery count")
	}
	unknown := NewMessage(t.Context(), cloudevents.New(), Delivery{})
	if _, known := DeliveryCountFromContext(unknown.Context()); known {
		t.Fatal("Count 0 must stay unknown")
	}
	msg := NewMessage(t.Context(), cloudevents.New(), Delivery{Count: 3})
	type otherKey struct{}
	child := context.WithValue(msg.Context(), otherKey{}, "x")
	if count, known := DeliveryCountFromContext(child); !known || count != 3 {
		t.Fatalf("DeliveryCountFromContext = %d, %v; want 3, true", count, known)
	}
}

func TestNewUndecodableCarriesTheDecodeError(t *testing.T) {
	t.Parallel()

	if err := NewMessage(t.Context(), cloudevents.New(), Delivery{}).DecodeError(); err != nil {
		t.Fatalf("NewMessage DecodeError() = %v, want nil", err)
	}
	cause := errors.New("bad envelope")
	acked := 0
	msg := NewUndecodable(t.Context(), cloudevents.New(), Delivery{Ack: func() error { acked++; return nil }, Count: 2}, cause)
	if !errors.Is(msg.DecodeError(), cause) {
		t.Fatalf("DecodeError() = %v, want %v", msg.DecodeError(), cause)
	}
	if count, known := msg.DeliveryCount(); !known || count != 2 {
		t.Errorf("DeliveryCount() = %d, %v; want 2, true", count, known)
	}
	if err := msg.Discard(cause); err != nil || acked != 1 {
		t.Errorf("Discard() = %v, acked=%d; want nil and 1 (Discard falls back to Ack)", err, acked)
	}
}
