package worker

import (
	"github.com/norlis/event-driven/pkg/domain"
	"sync"

	"go.uber.org/zap"
)

type Job struct {
	Msg       *domain.Message
	Handler   func(*domain.Message) (any, error)
	Publisher domain.Publisher
}

type Worker struct {
	ID       int
	JobQueue <-chan Job // Canal de solo lectura
	Quit     chan struct{}
	Wg       *sync.WaitGroup
	logger   *zap.Logger
}

func NewWorker(id int, jobQueue <-chan Job, wg *sync.WaitGroup, logger *zap.Logger) *Worker {
	return &Worker{
		ID:       id,
		JobQueue: jobQueue,
		Quit:     make(chan struct{}),
		Wg:       wg,
		logger:   logger.With(zap.Int("workerID", id)),
	}
}

func (w *Worker) Start() {
	w.Wg.Add(1) // Incrementar contador para el WaitGroup del dispatcher
	go func() {
		defer w.Wg.Done() // Decrementar contador cuando la goroutine termina
		w.logger.Info("Worker iniciado")
		for {
			select {
			case job, ok := <-w.JobQueue:
				if !ok {
					w.logger.Info("JobQueue cerrado, worker deteniéndose")
					return
				}
				w.logger.Debug("Procesando mensaje", zap.String("messageUUID", job.Msg.UUID))

				// Aquí se podría usar el contexto del mensaje: job.Msg.Context()
				// si el handler o el publisher necesitan ser conscientes de la cancelación del mensaje.
				data, err := job.Handler(job.Msg) // El resultado del handler se ignora por ahora
				if err != nil {
					w.logger.Error("Error en el manejador del worker", zap.Error(err), zap.String("messageUUID", job.Msg.UUID))
					job.Msg.Nack() // Nack en caso de error del handler
				} else {
					job.Msg.Ack() // Ack si el handler fue exitoso
				}

				// Publicar el mensaje original si hay un publisher.
				// Considerar si se debe publicar el resultado del handler en lugar del mensaje original.
				if job.Publisher != nil && data != nil {
					// Crear un nuevo domain.Message si se quiere publicar el resultado del handler.
					// Por ahora, se publica el mensaje original como estaba antes.
					if pubErr := job.Publisher.Publish(job.Msg); pubErr != nil {
						w.logger.Error("Error publicando mensaje después del handler", zap.Error(pubErr), zap.String("messageUUID", job.Msg.UUID))
						// Decidir si un fallo en la publicación debe causar Nack del mensaje original
						// si no se ha hecho Ack/Nack aún (actualmente ya se hizo).
					}
				}

			case <-w.Quit:
				w.logger.Info("Worker recibiendo señal de quit, deteniéndose")
				return
			}
		}
	}()
}

func (w *Worker) Stop() {
	w.logger.Debug("Enviando señal de stop al worker")
	close(w.Quit) // Cerrar el canal para señalar la detención
}
