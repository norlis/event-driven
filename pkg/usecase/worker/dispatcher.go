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
	JobQueue   chan Job // Este canal será escrito por el Router
	workers    []*Worker
	maxWorkers int
	wg         sync.WaitGroup // Para esperar a que todos los workers terminen
	logger     *zap.Logger
	cancelFunc context.CancelFunc // Para detener el dispatcher y workers
}

func NewDispatcher(cfg DispatcherConfig, logger *zap.Logger) *Dispatcher {
	// El JobQueue es creado y gestionado por el Dispatcher.
	// El Router le enviará trabajos.
	jobQueue := make(chan Job, cfg.QueueSize)
	return &Dispatcher{
		JobQueue:   jobQueue,
		maxWorkers: cfg.NumWorkers,
		logger:     logger,
	}
}

// Run inicia los workers y el dispatcher.
// Debería tomar un contexto para manejar el apagado ordenado.
func (d *Dispatcher) Run(ctx context.Context) {
	// Crear un contexto derivado para el dispatcher y sus workers
	// que pueda ser cancelado para señalarlos a todos.
	var dispatcherCtx context.Context
	dispatcherCtx, d.cancelFunc = context.WithCancel(ctx)

	d.logger.Info("Dispatcher iniciando workers...", zap.Int("numWorkers", d.maxWorkers))
	d.workers = make([]*Worker, 0, d.maxWorkers) // Inicializar slice vacío con capacidad
	for i := 1; i <= d.maxWorkers; i++ {
		worker := NewWorker(i, d.JobQueue, &d.wg, d.logger)
		d.workers = append(d.workers, worker)
		worker.Start() // Los workers escucharán en d.JobQueue
	}
	d.logger.Info("Todos los workers iniciados por el Dispatcher")

	// Mantener el dispatcher activo o realizar tareas de supervisión si es necesario.
	// Si el dispatcher no tiene un bucle propio, Run puede simplemente regresar
	// después de iniciar los workers. El apagado se manejará a través de Stop().
	// Opcionalmente, puede escuchar el contexto aquí también.
	go func() {
		<-dispatcherCtx.Done() // Esperar a que el contexto del dispatcher sea cancelado
		d.logger.Info("Contexto del dispatcher cancelado, iniciando detención de workers si no se ha hecho ya.")
		// Asegurar que los workers sean detenidos si Stop() no fue llamado explícitamente.
		// Esto es un seguro, Stop() debería ser el método principal de apagado.
		d.internalStop()
	}()
}

// internalStop es llamado cuando el contexto se cancela o por Stop()
func (d *Dispatcher) internalStop() {
	d.logger.Info("Dispatcher: Señalando a los workers para que se detengan...")
	for _, worker := range d.workers {
		worker.Stop() // Esto ahora cierra el canal Quit del worker
	}
	// Cerrar JobQueue una vez que todos los workers han sido señalados
	// para que no intenten leer de un canal cerrado si aún están finalizando.
	// Sin embargo, si los workers pueden tardar en detenerse, cerrar JobQueue aquí
	// podría ser prematuro si aún hay jobs encolados que se quieren procesar antes del Stop.
	// Es mejor que los workers dejen de leer porque su Quit channel se activó.
	// Si JobQueue se cierra, los workers que leen de él obtendrán el valor zero y 'ok == false'.

	// d.logger.Info("Dispatcher: Esperando que todos los workers terminen...")
	// d.wg.Wait() // Esperar aquí
	// d.logger.Info("Dispatcher: Todos los workers han terminado.")
}

// Stop detiene ordenadamente el dispatcher y todos sus workers.
func (d *Dispatcher) Stop() {
	d.logger.Info("Dispatcher.Stop() llamado. Iniciando apagado ordenado...")
	if d.cancelFunc != nil {
		// Cancelar el contexto del dispatcher, lo que también detendrá su goroutine de supervisión (si existe)
		// y puede ser usado por los workers si se propaga.
		d.cancelFunc()
	}
	// La lógica de detener workers y esperar ya está en internalStop,
	// pero la llamada a wg.Wait() debería estar aquí para asegurar que Stop() bloquee.
	d.internalStop() // Señala a los workers

	d.logger.Info("Dispatcher: Esperando que todos los workers terminen...")
	d.wg.Wait() // Esperar que terminen las goroutines de los workers
	d.logger.Info("Dispatcher: Todos los workers han terminado después de Stop().")

	// Es seguro cerrar JobQueue después de que todos los workers hayan terminado
	// o después de que se les haya señalado para detenerse y ya no lean de él.
	// Si se cierra antes, un worker podría intentar leer y causar un panic si no maneja el canal cerrado.
	// Los workers ahora deberían terminar al recibir la señal de Quit o si JobQueue se cierra y leen 'ok == false'.
	close(d.JobQueue)
	d.logger.Info("Dispatcher: JobQueue cerrado.")
}
