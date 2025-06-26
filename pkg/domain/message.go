package domain

import (
	"context"
	"sync"
)

// dummy ack / nack
var dummy = func() {
	// ack / nack
}

// PreflightCallback es una función que el router puede invocar para notificar
// el resultado de los chequeos iniciales (validación, enrutamiento).
type PreflightCallback func(err error)

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

	// preflightCallback es invocado tan pronto como el router decide el destino del mensaje.
	preflightCallback PreflightCallback
}

func NewNewMessageWithoutAck(uuid string, payload []byte, attrs map[string]string) *Message {
	return NewMessage(uuid, payload, attrs, dummy, dummy)
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

// SetPreflightCallback permite a un suscriptor (como el de HTTP) registrar un
// callback para recibir notificación inmediata del resultado del pre-vuelo.
func (m *Message) SetPreflightCallback(cb PreflightCallback) {
	m.preflightCallback = cb
}

// NotifyPreflightDone es usado por el router para invocar el callback si existe.
func (m *Message) NotifyPreflightDone(err error) {
	if m.preflightCallback != nil {
		m.preflightCallback(err)
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
