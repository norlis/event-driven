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
	JobQueue      chan Job // Este canal será escrito por el Router
	workers       []*Worker
	maxWorkers    int
	wg            sync.WaitGroup // Para esperar a que todos los workers terminen
	logger        *zap.Logger
	cancelFunc    context.CancelFunc // Para detener el dispatcher y workers
	stopOnce      sync.Once
	workerEnded   chan int
	config        DispatcherConfig
	dispatcherCtx context.Context

	activeWorkers      map[int]*Worker
	activeWorkersMutex sync.Mutex
}

func NewDispatcher(cfg DispatcherConfig, logger *zap.Logger) *Dispatcher {
	jobQueue := make(chan Job, cfg.QueueSize)
	return &Dispatcher{
		JobQueue:      jobQueue,
		maxWorkers:    cfg.NumWorkers,
		logger:        logger,
		workerEnded:   make(chan int, cfg.NumWorkers),
		config:        cfg,
		activeWorkers: make(map[int]*Worker),
	}
}

func (d *Dispatcher) Run(ctx context.Context) {
	//d.dispatcherCtx, d.cancelFunc = context.WithCancel(ctx)
	d.dispatcherCtx, d.cancelFunc = context.WithCancel(context.Background())

	d.logger.Info("Dispatcher iniciando workers...", zap.Int("numWorkers", d.maxWorkers))
	d.workers = make([]*Worker, 0, d.maxWorkers)
	d.activeWorkersMutex.Lock()
	for i := 1; i <= d.maxWorkers; i++ {
		d.wg.Add(1)
		workerInstance := d.startNewWorker(i)
		d.activeWorkers[i] = workerInstance
	}
	d.activeWorkersMutex.Unlock()
	d.logger.Info("Todos los workers iniciados por el Dispatcher")

	go func() {
		for {
			select {
			case workerID := <-d.workerEnded:
				d.activeWorkersMutex.Lock()
				delete(d.activeWorkers, workerID) // Eliminar worker terminado del mapa
				d.activeWorkersMutex.Unlock()

				if d.dispatcherCtx.Err() != nil {
					d.logger.Info("Dispatcher deteniéndose, no se reiniciará el worker que terminó.", zap.Int("workerID", workerID))
					continue
				}

				d.logger.Warn("Worker terminó. Se reiniciará.", zap.Int("terminatedWorkerID", workerID))
				d.wg.Add(1)
				newInstance := d.startNewWorker(workerID)
				d.activeWorkersMutex.Lock()
				d.activeWorkers[workerID] = newInstance
				d.activeWorkersMutex.Unlock()

			case <-d.dispatcherCtx.Done():
				d.logger.Info("Contexto del dispatcher cancelado (señal de parada). Deteniendo workers...", zap.Error(d.dispatcherCtx.Err()))
				d.signalWorkersToStop()
				return

			case <-ctx.Done():
				d.logger.Info("DISPATCHER: Run goroutine - Contexto de Fx (pasado a Run) cancelado. Deteniendo dispatcher y workers...", zap.Error(ctx.Err()))
				d.Stop()
			}
		}
	}()

	//go func() {
	//	<-d.dispatcherCtx.Done()
	//	d.logger.Info("Contexto interno del dispatcher cancelado, iniciando lógica de detención (vía goroutine de Run)...", zap.Error(d.dispatcherCtx.Err()))
	//	d.signalWorkersToStop()
	//}()

}

func (d *Dispatcher) startNewWorker(id int) *Worker {
	w := NewWorker(id, d.JobQueue, &d.wg, d.logger) // Pasamos el logger del dispatcher
	w.Start(d.workerEnded)
	d.logger.Info("Nueva instancia de worker iniciada.", zap.Int("workerID", id))
	return w
}

func (d *Dispatcher) signalWorkersToStop() {
	d.activeWorkersMutex.Lock()
	defer d.activeWorkersMutex.Unlock()

	d.logger.Info("Dispatcher: Señalando a los workers para que se detengan...")
	for _, worker := range d.workers {
		d.logger.Debug("Enviando señal Stop al worker activo", zap.Int("workerID", worker.ID))
		worker.Stop()
	}

}

func (d *Dispatcher) executeStopSequence() {
	//d.signalWorkersToStop()
	d.wg.Wait()
	d.logger.Info("Dispatcher: Todos los workers han terminado.")

	close(d.JobQueue)
	d.logger.Info("Dispatcher: JobQueue cerrado.")
}

func (d *Dispatcher) Stop() {
	d.logger.Info("Dispatcher.Stop() llamado.")
	d.stopOnce.Do(func() {
		d.logger.Info("Dispatcher.Stop(): Cancelando contexto interno del dispatcher (stopOnce)...")
		if d.cancelFunc != nil {
			d.cancelFunc() // Esto cancelará d.dispatcherCtx
		}
		d.executeStopSequence()
	})

}
