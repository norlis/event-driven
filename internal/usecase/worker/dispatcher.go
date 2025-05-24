package worker

import (
	"log"
)

type Dispatcher struct {
	JobQueue   chan Job
	Workers    []*Worker
	MaxWorkers int
}

func NewDispatcher(numWorkers int, queueSize int) *Dispatcher {
	jobQueue := make(chan Job, queueSize)
	return &Dispatcher{
		JobQueue:   jobQueue,
		MaxWorkers: numWorkers,
	}
}

// ctx context.Context
func (d *Dispatcher) Run() {
	for i := 1; i <= d.MaxWorkers; i++ {
		worker := NewWorker(i, d.JobQueue)
		d.Workers = append(d.Workers, worker)
		worker.Start()
	}
	log.Println("[Dispatcher] All workers started")
}

func (d *Dispatcher) Stop() {
	for _, worker := range d.Workers {
		worker.Quit <- true
	}
}
