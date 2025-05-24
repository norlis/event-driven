package domain

import (
	"context"
	"sync"
)

type Payload []byte

type Message struct {
	UUID     string
	Metadata map[string]string
	Payload  []byte

	ack    func()
	nack   func()
	once   sync.Once
	ctx    context.Context
	cancel context.CancelFunc
}

func NewMessage(uuid string, payload []byte, attrs map[string]string, ackFn, nackFn func()) *Message {
	ctx, cancel := context.WithCancel(context.Background())
	return &Message{
		UUID:     uuid,
		Payload:  payload,
		Metadata: attrs,
		ack:      ackFn,
		nack:     nackFn,
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (m *Message) Context() context.Context {
	return m.ctx
}

// Ack indica que el mensaje fue procesado exitosamente
func (m *Message) Ack() {
	m.once.Do(func() {
		if m.ack != nil {
			m.ack()
		}
		m.cancel()
	})
}

// Nack indica que el procesamiento del mensaje falló
func (m *Message) Nack() {
	m.once.Do(func() {
		if m.nack != nil {
			m.nack()
		}
		m.cancel()
	})
}
