package event

import (
	"context"
	"fmt"
	"sync"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

// Outcome is the router's final decision about a message.
type Outcome uint8

// Outcome values, in the order the router can reach them.
const (
	OutcomePending   Outcome = iota // the router has not decided yet
	OutcomeAcked                    // processed successfully
	OutcomeNacked                   // failed; the broker redelivers
	OutcomeDiscarded                // failed definitively; acknowledged and dropped
)

func (o Outcome) String() string {
	switch o {
	case OutcomePending:
		return "pending"
	case OutcomeAcked:
		return "acked"
	case OutcomeNacked:
		return "nacked"
	case OutcomeDiscarded:
		return "discarded"
	}
	return fmt.Sprintf("outcome(%d)", uint8(o))
}

// Delivery carries the broker mechanics of one delivery of a message. Each
// callback returns the broker's error so the router can log it once.
type Delivery struct {
	// Ack confirms the message. Nil for transports without delivery semantics.
	Ack func() error
	// Nack asks the broker to redeliver. Nil when the broker cannot.
	Nack func() error
	// Discard confirms the message as definitively failed. Nil means Ack.
	Discard func() error
	// Count is the 1-based delivery number reported by the broker; 0 = unknown.
	Count uint64
}

// Message composes a CloudEvent with message-broker delivery mechanics and
// records the router's Outcome so synchronous transports (HTTP) can answer
// the caller truthfully.
type Message struct {
	cloudevents.Event

	delivery Delivery
	once     sync.Once
	ctx      context.Context
	cancel   context.CancelFunc

	outcome   Outcome
	cause     error
	decodeErr error
}

// NewMessage wraps a CloudEvent with its broker Delivery and a parent context
// so transports can seed per-message values such as the trace. Only the first
// Ack, Nack or Discard takes effect.
func NewMessage(parent context.Context, ce cloudevents.Event, d Delivery) *Message {
	if parent == nil {
		parent = context.Background()
	}
	if d.Discard == nil {
		d.Discard = d.Ack
	}
	if d.Count > 0 {
		// Only transports that report a count pay for the context value; it
		// lets middlewares (retry.SkipOnRedelivery) see the broker's retries.
		parent = context.WithValue(parent, deliveryCountKey{}, d.Count)
	}
	ctx, cancel := context.WithCancel(parent)
	return &Message{Event: ce, delivery: d, ctx: ctx, cancel: cancel}
}

// NewUndecodable wraps a message whose transport envelope could not be decoded
// into a CloudEvent. ce is the transport's rescue event (broker id, default
// type and source, the raw body as data) so the router can log it, dead-letter
// a recoverable copy and settle it; err is what the transport decoder returned. The
// router never routes such a message: DecodeError short-circuits to the
// poison-message policy.
func NewUndecodable(parent context.Context, ce cloudevents.Event, d Delivery, err error) *Message {
	m := NewMessage(parent, ce, d)
	m.decodeErr = err
	return m
}

// DecodeError returns the transport's envelope error for a message built with
// NewUndecodable, nil otherwise.
func (m *Message) DecodeError() error { return m.decodeErr }

type deliveryCountKey struct{}

// DeliveryCountFromContext returns the broker's 1-based delivery number of the
// message whose context (or a child of it) is ctx, and whether the transport
// reported one. Handlers and middlewares use it; the mux reads
// Message.DeliveryCount directly.
func DeliveryCountFromContext(ctx context.Context) (count uint64, known bool) {
	count, known = ctx.Value(deliveryCountKey{}).(uint64)
	return count, known
}

// Context returns the per-message context cancelled when the outcome is decided.
func (m *Message) Context() context.Context { return m.ctx }

// Outcome returns the router's decision and the error that motivated a Nack
// or Discard. It is meant to be read after the router returned control to
// the transport; it is not synchronized against a concurrent decision.
func (m *Message) Outcome() (Outcome, error) { return m.outcome, m.cause }

// CanNack reports whether the transport can redeliver the message: false for
// transports without a Nack primitive (Core NATS), where a retryable failure
// is definitive.
func (m *Message) CanNack() bool { return m.delivery.Nack != nil }

// DeliveryCount returns the broker's 1-based delivery number and whether the
// transport reported it.
func (m *Message) DeliveryCount() (count uint64, known bool) {
	return m.delivery.Count, m.delivery.Count > 0
}

// Ack signals that the message was processed successfully. It returns the
// broker's error, if any; the outcome is recorded either way.
func (m *Message) Ack() error { return m.decide(OutcomeAcked, nil, m.delivery.Ack) }

// Nack signals that processing failed and the broker should redeliver.
func (m *Message) Nack(cause error) error { return m.decide(OutcomeNacked, cause, m.delivery.Nack) }

// Discard signals a definitive failure: the broker is told not to redeliver
// (Delivery.Discard, or Ack when the transport has no distinct primitive).
func (m *Message) Discard(cause error) error {
	return m.decide(OutcomeDiscarded, cause, m.delivery.Discard)
}

// decide records the first decision and applies it. Later decisions are
// no-ops that return nil: the broker was already told once.
func (m *Message) decide(outcome Outcome, cause error, fn func() error) error {
	var err error
	m.once.Do(func() {
		m.outcome, m.cause = outcome, cause
		if fn != nil {
			err = fn()
		}
		m.cancel()
	})
	return err
}
