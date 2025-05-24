package worker

import (
	"event-router/internal/domain"
	"log"
)

type Job struct {
	Msg       domain.Message
	Handler   func(domain.Message) error
	Publisher domain.Publisher
}

type Worker struct {
	ID       int
	JobQueue chan Job
	Quit     chan bool
}

func NewWorker(id int, jobQueue chan Job) *Worker {
	return &Worker{
		ID:       id,
		JobQueue: jobQueue,
		Quit:     make(chan bool),
	}
}

func (w *Worker) Start() {
	go func() {
		for {
			select {
			case job := <-w.JobQueue:
				log.Printf("[Worker %d] Processing message ID: %s", w.ID, job.Msg.ID)
				if err := job.Handler(job.Msg); err != nil {
					log.Printf("[Worker %d] Handler error: %v", w.ID, err)
					return
				}
				if job.Publisher != nil {
					job.Publisher.Publish(job.Msg)
				}
			case <-w.Quit:
				log.Printf("[Worker %d] Stopping", w.ID)
				return
			}
		}
	}()
}
