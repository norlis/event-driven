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

	ack    chan struct{}
	noAck  chan struct{}
	once   sync.Once
	ctx    context.Context
	cancel context.CancelFunc
}

func NewMessage(uuid string, payload []byte, attrs map[string]string) *Message {
	ctx, cancel := context.WithCancel(context.Background())
	return &Message{
		UUID:     uuid,
		Payload:  payload,
		Metadata: attrs,
		ack:      make(chan struct{}),
		noAck:    make(chan struct{}),
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
		close(m.ack)
		m.cancel()
	})
}

// Nack indica que el procesamiento del mensaje falló
func (m *Message) Nack() {
	m.once.Do(func() {
		close(m.noAck)
		m.cancel()
	})
}

// Acked retorna un canal que se cierra cuando se llama Ack()
func (m *Message) Acked() <-chan struct{} {
	return m.ack
}

// Nacked retorna un canal que se cierra cuando se llama Nack()
func (m *Message) Nacked() <-chan struct{} {
	return m.noAck
}
