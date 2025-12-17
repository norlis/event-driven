package worker

import (
	"context"
	"encoding/json"
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
	JobQueue <-chan Job
	wg       *sync.WaitGroup
	logger   *zap.Logger
}

// NewWorker crea una nueva instancia de Worker.
func NewWorker(id int, jobQueue <-chan Job, wg *sync.WaitGroup, logger *zap.Logger) *Worker {
	return &Worker{
		ID:       id,
		JobQueue: jobQueue,
		wg:       wg,
		logger:   logger.With(zap.Int("workerID", id)),
	}
}

func (w *Worker) Start(workerEnded chan<- int) {
	defer func() {
		w.recoverPanics()
		w.wg.Done()
		workerEnded <- w.ID
	}()

	w.logger.Info("Worker started")

	for job := range w.JobQueue {
		w.processJob(job)
	}

	w.logger.Info("Worker stopping because job channel was closed.")
}

func (w *Worker) processJob(job Job) {
	w.logger.Debug("Processing job", zap.String("messageUUID", job.Msg.UUID))

	store := metadata.NewStore()
	handlerCtx := metadata.NewContext(job.Msg.Context(), store)

	data, err := job.Handler(handlerCtx, job.Msg)

	if err != nil {
		w.logger.Error("Handler execution failed",
			zap.Error(err),
			zap.String("messageUUID", job.Msg.UUID),
		)
		job.Msg.Nack()
		return
	}

	job.Msg.Ack()

	if job.Publisher != nil && data != nil {
		w.publishResult(job, data, store)
	}
}

func (w *Worker) publishResult(job Job, data json.RawMessage, store *metadata.Store) {
	finalMetadata := store.All()
	for k, v := range job.Msg.Metadata {
		if _, exists := finalMetadata[k]; !exists {
			finalMetadata[k] = v
		}
	}

	newEvent := event.NewMessageWithoutAck(job.Msg.UUID, data, finalMetadata)

	if pubErr := job.Publisher.Publish(newEvent); pubErr != nil {
		w.logger.Error("Failed to publish result event",
			zap.Error(pubErr),
			zap.String("originalMessageUUID", job.Msg.UUID),
		)
	}
}

func (w *Worker) recoverPanics() {
	if r := recover(); r != nil {
		w.logger.Error("Worker panicked but recovered",
			zap.Any("panic", r),
			zap.Stack("stacktrace"),
		)
	}
}
