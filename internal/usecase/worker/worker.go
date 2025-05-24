package worker

import (
	"event-router/internal/domain"
	"log"
)

type Job struct {
	Msg       *domain.Message
	Handler   func(*domain.Message) (any, error)
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
				log.Printf("[Worker %d] Processing message ID: %s", w.ID, job.Msg.UUID)
				_, err := job.Handler(job.Msg)
				if err != nil {
					job.Msg.Nack()
					log.Printf("[Worker %d] Handler error: %v", w.ID, err)
					return
				} else {
					job.Msg.Ack()
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
