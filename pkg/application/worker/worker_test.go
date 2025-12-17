package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/norlis/event-driven/pkg/domain/event"

	"go.uber.org/zap/zaptest"
)

// Mock para domain.Publisher usado en el worker
type mockWorkerPublisher struct {
	PublishFunc func(msg *event.Message) error
	Called      bool
}

func (m *mockWorkerPublisher) Publish(msg *event.Message) error {
	m.Called = true
	if m.PublishFunc != nil {
		return m.PublishFunc(msg)
	}
	return nil
}

func TestWorker_Start_ProcessJobAndAck(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	jobQueue := make(chan Job, 1)
	var wg sync.WaitGroup
	workerEnded := make(chan int, 1)

	workerInstance := NewWorker(1, jobQueue, &wg, logger)
	workerInstance.Start(workerEnded) // Inicia la goroutine del worker

	handlerCalled := false
	mockMsgAckCalled := false
	mockMsgNackCalled := false

	mockMsg := event.NewMessage("uuid-test", []byte("payload"), nil,
		func() { mockMsgAckCalled = true },  // AckFunc
		func() { mockMsgNackCalled = true }, // NackFunc
	)

	job := Job{
		Msg: mockMsg,
		Handler: func(_ context.Context, msg *event.Message) (json.RawMessage, error) {
			handlerCalled = true
			return []byte("result"), nil // Handler exitoso
		},
		Publisher: nil,
	}

	jobQueue <- job // Enviar trabajo al worker

	// Esperar un poco para que el worker procese
	// En un test real, podríamos usar canales para sincronizar mejor
	time.Sleep(100 * time.Millisecond)

	if !handlerCalled {
		t.Error("Se esperaba que el handler del Job fuera llamado")
	}
	if !mockMsgAckCalled {
		t.Error("Se esperaba que Message.Ack() fuera llamado")
	}
	if mockMsgNackCalled {
		t.Error("No se esperaba que Message.Nack() fuera llamado")
	}

	close(jobQueue) // Cerrar el canal para detener el worker
	wg.Wait()       // Esperar que la goroutine del worker termine
}

func TestWorker_Start_ProcessJobAndNackOnError(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	jobQueue := make(chan Job, 1)
	var wg sync.WaitGroup
	workerEnded := make(chan int, 1)

	workerInstance := NewWorker(1, jobQueue, &wg, logger)
	workerInstance.Start(workerEnded)

	handlerCalled := false
	mockMsgAckCalled := false
	mockMsgNackCalled := false
	expectedError := errors.New("handler processing error")

	mockMsg := event.NewMessage("uuid-test-nack", []byte("payload"), nil,
		func() { mockMsgAckCalled = true },
		func() { mockMsgNackCalled = true },
	)

	job := Job{
		Msg: mockMsg,
		Handler: func(_ context.Context, msg *event.Message) (json.RawMessage, error) {
			handlerCalled = true
			return nil, expectedError // Handler falla
		},
		Publisher: nil,
	}

	jobQueue <- job

	time.Sleep(100 * time.Millisecond)

	if !handlerCalled {
		t.Error("Se esperaba que el handler del Job fuera llamado")
	}
	if mockMsgAckCalled {
		t.Error("No se esperaba que Message.Ack() fuera llamado")
	}
	if !mockMsgNackCalled {
		t.Error("Se esperaba que Message.Nack() fuera llamado")
	}

	close(jobQueue) // Cerrar el canal para detener el worker
	wg.Wait()
}

func TestWorker_Start_ProcessJobAndPublish(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	jobQueue := make(chan Job, 1)
	var wg sync.WaitGroup
	workerEnded := make(chan int, 1)

	workerInstance := NewWorker(1, jobQueue, &wg, logger)
	workerInstance.Start(workerEnded)

	mockPub := &mockWorkerPublisher{}
	mockMsgAckCalled := false

	mockMsg := event.NewMessage("uuid-test-publish", []byte("payload"), nil,
		func() { mockMsgAckCalled = true },
		func() {},
	)

	job := Job{
		Msg:       mockMsg,
		Handler:   func(_ context.Context, msg *event.Message) (json.RawMessage, error) { return []byte("result"), nil },
		Publisher: mockPub,
	}

	jobQueue <- job
	time.Sleep(100 * time.Millisecond)

	if !mockMsgAckCalled {
		t.Error("Se esperaba que Message.Ack() fuera llamado")
	}
	if !mockPub.Called {
		t.Error("Se esperaba que Publisher.Publish() fuera llamado")
	}

	close(jobQueue) // Cerrar el canal para detener el worker
	wg.Wait()
}

func TestWorker_Stop(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	jobQueue := make(chan Job) // No buffer, para que el worker bloquee si no hay quit
	var wg sync.WaitGroup
	workerEnded := make(chan int, 1)

	workerInstance := NewWorker(1, jobQueue, &wg, logger)
	workerInstance.Start(workerEnded)

	close(jobQueue) // Cerrar el canal para detener el worker

	// Esperar a que el WaitGroup termine, con un timeout para evitar que el test se cuelgue.
	waitChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitChan)
	}()

	select {
	case <-waitChan:
		// El worker terminó correctamente
	case <-time.After(1 * time.Second): // Timeout razonable
		t.Fatal("Se esperaba que el worker terminara después de cerrar el canal, pero hizo timeout")
	}
}

func TestWorker_JobQueueClosed(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	jobQueue := make(chan Job, 1)
	var wg sync.WaitGroup
	workerEnded := make(chan int, 1)

	workerInstance := NewWorker(1, jobQueue, &wg, logger)
	workerInstance.Start(workerEnded)

	close(jobQueue) // Cerrar la cola de trabajos

	waitChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitChan)
	}()

	select {
	case <-waitChan:
		// El worker debería terminar cuando la JobQueue se cierra
	case <-time.After(1 * time.Second):
		t.Fatal("Se esperaba que el worker terminara cuando JobQueue se cierra, pero hizo timeout")
	}
	// No es necesario llamar a workerInstance.Stop() aquí ya que probamos el cierre de la cola.
}
