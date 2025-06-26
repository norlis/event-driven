package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/norlis/event-driven/pkg/application/router/metadata"
	"github.com/norlis/event-driven/pkg/domain/event"
	"github.com/norlis/event-driven/pkg/port"

	"go.uber.org/zap"
)

type Job struct {
	Msg       *event.Message
	Handler   func(context.Context, *event.Message) (json.RawMessage, error)
	Publisher port.Publisher
}

type Worker struct {
	ID       int
	JobQueue <-chan Job // Canal de solo lectura
	Quit     chan struct{}
	Wg       *sync.WaitGroup
	logger   *zap.Logger
	stopOnce sync.Once
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

func (w *Worker) Start(workerEnded chan<- int) {
	w.Wg.Add(1)
	go func() {
		defer func() {
			w.logger.Info("WORKER: Goroutine terminando, llamando a Wg.Done()")
			w.Wg.Done()
			workerEnded <- w.ID
		}()

		defer func() { // Este defer es para el recover
			if r := recover(); r != nil {
				w.logger.Error(
					"WORKER ENTRO EN PANICO RECUPERADO",
					zap.Any("reason", r),
				)
			}
		}()

		w.logger.Info("Worker iniciado")
		for {
			w.logger.Debug("Worker esperando en select")
			select {
			case job, ok := <-w.JobQueue:
				if !ok {
					w.logger.Info("JobQueue cerrado, worker deteniéndose")
					return
				}
				w.logger.Info("Procesando mensaje", zap.String("messageUUID", job.Msg.UUID))

				parentCtx, cancelHandler := context.WithCancel(context.Background())

				// 2. Inicia la goroutine vigilante que cancelará el contexto si el worker recibe la señal de Quit.
				handlerDone := make(chan struct{})
				go func() {
					defer close(handlerDone)
					select {
					case <-w.Quit:
						w.logger.Info("Señal Quit recibida por el worker, cancelando handler en curso", zap.String("messageUUID", job.Msg.UUID))
						cancelHandler()
					case <-parentCtx.Done():
						// El contexto terminó por otra razón (el handler finalizó), así que salimos.
					}
				}()

				store := metadata.NewStore()
				handlerCtx := metadata.NewContext(parentCtx, store)

				data, err := job.Handler(handlerCtx, job.Msg)

				if handlerCtx.Err() == nil { // Si el contexto no fue previamente cancelado
					cancelHandler()
				}

				<-handlerDone // Espera a que la goroutine vigilante termine.

				if err != nil {
					w.logger.Error(
						"Error en el handler del worker",
						zap.Error(err), zap.String("messageUUID", job.Msg.UUID),
						zap.Bool("Canceled", errors.Is(err, context.Canceled)),
					)
					job.Msg.Nack()
				} else {
					job.Msg.Ack() // Ack si el handler fue exitoso
				}

				// Publicar el mensaje original si hay un publisher.

				if job.Publisher != nil && data != nil {
					finalMetadata := store.All()

					for k, v := range job.Msg.Metadata {
						if _, exists := finalMetadata[k]; !exists {
							finalMetadata[k] = v
						}
					}

					if pubErr := job.Publisher.Publish(event.NewNewMessageWithoutAck(job.Msg.UUID, data, finalMetadata)); pubErr != nil {
						w.logger.Error("Error publicando mensaje después del handler", zap.Error(pubErr), zap.String("messageUUID", job.Msg.UUID))
						// TODO: Decidir si un fallo en la publicación debe causar Nack del mensaje original
						// si no se ha hecho Ack/Nack aún (actualmente ya se hizo).
					}
				}

			case <-w.Quit:
				w.logger.Info("WORKER: recibiendo señal de quit, deteniéndose")
				return
			}
		}
	}()
}

func (w *Worker) Stop() {
	w.logger.Info("WORKER: Stop() llamado")
	w.stopOnce.Do(func() {
		w.logger.Debug("WORKER: Stop() (stopOnce) - Cerrando Quit chan", zap.Int("workerID", w.ID))
		close(w.Quit)
	})
}
