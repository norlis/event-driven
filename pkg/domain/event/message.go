package event

import (
	"context"
	"sync"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

var noop = func() {}

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

func NewMessageWithoutAck(ce cloudevents.Event) *Message {
	return NewMessage(ce, noop, noop)
}

func NewMessage(ce cloudevents.Event, ackFn, nackFn func()) *Message {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // cancel is stored in Message and called in Ack/Nack
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
