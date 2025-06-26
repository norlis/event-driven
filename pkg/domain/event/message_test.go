package event

import (
	"testing"
)

func TestMessage_Ack(t *testing.T) {
	t.Parallel()

	ackCalled := false
	ackFunc := func() {
		ackCalled = true
	}
	nackFunc := func() {}

	msg := NewMessage("test-uuid", []byte("payload"), nil, ackFunc, nackFunc)

	// Llamar Ack múltiples veces
	msg.Ack()
	msg.Ack() // La segunda llamada no debería tener efecto

	if !ackCalled {
		t.Error("ackFunc no fue llamada después de msg.Ack()")
	}

	select {
	case <-msg.Context().Done():
		// El contexto del mensaje debería estar cancelado
	default:
		t.Error("El contexto del mensaje no fue cancelado después de Ack()")
	}

	// Verificar que la función Ack original solo se llamó una vez (implícito por sync.Once)
	// Para probar esto explícitamente, necesitaríamos un contador en ackFunc.
	// Por ahora, confiamos en la semántica de sync.Once.
}

func TestMessage_Nack(t *testing.T) {
	t.Parallel()

	nackCalled := false
	ackFunc := func() {}
	nackFunc := func() {
		nackCalled = true
	}

	msg := NewMessage("test-uuid", []byte("payload"), nil, ackFunc, nackFunc)

	msg.Nack()
	msg.Nack() // Segunda llamada sin efecto

	if !nackCalled {
		t.Error("nackFunc no fue llamada después de msg.Nack()")
	}

	select {
	case <-msg.Context().Done():
		// El contexto del mensaje debería estar cancelado
	default:
		t.Error("El contexto del mensaje no fue cancelado después de Nack()")
	}
}

func TestMessage_AckOrNackOnlyOnce(t *testing.T) {
	t.Parallel()

	ackCount := 0
	nackCount := 0

	ackFunc := func() { ackCount++ }
	nackFunc := func() { nackCount++ }

	t.Run("Ack_then_Nack", func(t *testing.T) {
		ackCount, nackCount = 0, 0 // Reset contadores
		msg := NewMessage("uuid1", nil, nil, ackFunc, nackFunc)
		msg.Ack()
		msg.Nack() // No debería llamar a nackFunc ni afectar el contexto otra vez

		if ackCount != 1 {
			t.Errorf("ackFunc fue llamada %d veces, se esperaba 1", ackCount)
		}
		if nackCount != 0 {
			t.Errorf("nackFunc fue llamada %d veces, se esperaba 0", nackCount)
		}
		if msg.Context().Err() == nil { // El contexto debe estar cancelado por el primer Ack
			t.Error("El contexto del mensaje no se canceló")
		}
	})

	t.Run("Nack_then_Ack", func(t *testing.T) {
		ackCount, nackCount = 0, 0 // Reset contadores
		msg := NewMessage("uuid2", nil, nil, ackFunc, nackFunc)
		msg.Nack()
		msg.Ack() // No debería llamar a ackFunc

		if nackCount != 1 {
			t.Errorf("nackFunc fue llamada %d veces, se esperaba 1", nackCount)
		}
		if ackCount != 0 {
			t.Errorf("ackFunc fue llamada %d veces, se esperaba 0", ackCount)
		}
		if msg.Context().Err() == nil { // El contexto debe estar cancelado por el primer Nack
			t.Error("El contexto del mensaje no se canceló")
		}
	})
}
