package event

import (
	"context"
	"sync"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

var noop = func() {
	// use for without ack
}

// PreflightCallback is invoked by the router to notify the result of initial checks (validation, routing).
type PreflightCallback func(err error)

// Message composes a CloudEvent with message-broker delivery mechanics (Ack/Nack).
type Message struct {
	cloudevents.Event

	ack    func()
	nack   func()
	once   sync.Once
	ctx    context.Context
	cancel context.CancelFunc

	preflightCallback PreflightCallback
}

// NewMessageWithoutAck wraps a CloudEvent without Ack/Nack semantics. Use
// from synchronous transports (e.g. HTTP) where ack/nack don't apply.
func NewMessageWithoutAck(ce cloudevents.Event) *Message {
	return NewMessage(ce, noop, noop)
}

// NewMessage wraps a CloudEvent with broker-level ack and nack callbacks.
// The callbacks are de-duplicated: only the first Ack or Nack fires.
func NewMessage(ce cloudevents.Event, ackFn, nackFn func()) *Message {
	ctx, cancel := context.WithCancel(context.Background())
	return &Message{
		Event:  ce,
		ack:    ackFn,
		nack:   nackFn,
		ctx:    ctx,
		cancel: cancel,
	}
}

// SetPreflightCallback registers a callback for immediate preflight result notification.
func (m *Message) SetPreflightCallback(cb PreflightCallback) {
	m.preflightCallback = cb
}

// NotifyPreflightDone invokes the preflight callback if set.
func (m *Message) NotifyPreflightDone(err error) {
	if m.preflightCallback != nil {
		m.preflightCallback(err)
	}
}

// Context returns the per-message context cancelled when Ack or Nack fires.
func (m *Message) Context() context.Context {
	return m.ctx
}

// Ack signals that the message was processed successfully.
func (m *Message) Ack() {
	m.once.Do(func() {
		if m.ack != nil {
			m.ack()
		}
		m.cancel()
	})
}

// Nack signals that message processing failed.
func (m *Message) Nack() {
	m.once.Do(func() {
		if m.nack != nil {
			m.nack()
		}
		m.cancel()
	})
}
