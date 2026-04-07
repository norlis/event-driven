package worker

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

type DispatcherConfig struct {
	NumWorkers int
	QueueSize  int
}

type Dispatcher struct {
	JobQueue   chan Job
	maxWorkers int
	wg         sync.WaitGroup
	logger     *zap.Logger
	stopOnce   sync.Once
	config     DispatcherConfig

	// worker supervision
	workerCtx    context.Context
	workerCancel context.CancelFunc
	workerEnded  chan int
}

func NewDispatcher(cfg DispatcherConfig, logger *zap.Logger) *Dispatcher {
	jobQueue := make(chan Job, cfg.QueueSize)
	workerEndedChan := make(chan int, cfg.NumWorkers)

	return &Dispatcher{
		JobQueue:    jobQueue,
		maxWorkers:  cfg.NumWorkers,
		logger:      logger,
		config:      cfg,
		workerEnded: workerEndedChan,
	}
}

func (d *Dispatcher) Run(ctx context.Context) {
	// Creamos un contexto que podemos cancelar para detener la supervisión.
	d.workerCtx, d.workerCancel = context.WithCancel(context.Background()) //nolint:gosec // cancel is stored in d.workerCancel and called in Stop()

	d.logger.Info("Dispatcher starting initial workers...", zap.Int("numWorkers", d.maxWorkers))
	for i := 1; i <= d.maxWorkers; i++ {
		d.startNewWorker(i)
	}
	d.logger.Info("All initial workers started.")

	// Bucle de Supervisión
	go func() {
		for {
			select {
			case workerID := <-d.workerEnded:
				d.logger.Warn("Worker has stopped. It will be restarted.", zap.Int("workerID", workerID))

				if d.workerCtx.Err() != nil {
					d.logger.Info("Dispatcher is stopping, worker will not be restarted.", zap.Int("workerID", workerID))
					continue
				}

				d.startNewWorker(workerID)

			case <-d.workerCtx.Done():
				d.logger.Info("Worker supervision context cancelled. Stopping supervisor loop.")
				return
			}
		}
	}()

	go func() {
		<-ctx.Done()
		d.logger.Info("Dispatcher supervision context cancelled by application shutdown.", zap.Error(ctx.Err()))
		d.Stop()
	}()
}

func (d *Dispatcher) startNewWorker(id int) {
	d.logger.Info("Starting new worker instance.", zap.Int("workerID", id))
	worker := NewWorker(id, d.JobQueue, &d.wg, d.logger)
	d.wg.Add(1)
	go worker.Start(d.workerEnded)
}

func (d *Dispatcher) Stop() {
	d.logger.Info("Dispatcher.Stop() called.")
	d.stopOnce.Do(func() {
		d.logger.Info("Initiating graceful shutdown of workers...")

		// Detenemos el bucle de supervisión para no reiniciar más workers.
		if d.workerCancel != nil {
			d.workerCancel()
		}

		// Cerramos el canal JobQueue para que los workers actuales terminen.
		close(d.JobQueue)
		d.logger.Info("JobQueue closed. Workers will finish after processing remaining jobs.")

		// Esperamos a que todos los workers terminen.
		d.wg.Wait()
		d.logger.Info("All workers have finished.")
	})
}
