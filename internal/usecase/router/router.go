package router

import (
	"context"
	"encoding/json"
	"event-router/internal/domain"
	"event-router/internal/usecase/worker"
	"log"
)

type Filter interface {
	Match(msg domain.Message) bool
}

type HandlerFunc func(data interface{}) error

type Route struct {
	Pub     domain.Publisher
	Filter  Filter
	Handler HandlerFunc
}

type Router struct {
	sub        domain.Subscription
	routes     []Route
	dispatcher *worker.Dispatcher
}

// New creates a new Router for a given Subscription source (PubSub, HTTP, etc.)
func New(sub domain.Subscription) *Router {
	dispatcher := worker.NewDispatcher(20, 100)
	dispatcher.Run()
	return &Router{
		sub:        sub,
		dispatcher: dispatcher,
	}
}

// Register adds a route for the current Subscription source.
func (r *Router) Register(pub domain.Publisher, filter Filter, handler HandlerFunc) {
	r.routes = append(r.routes, Route{
		Pub:     pub,
		Filter:  filter,
		Handler: handler,
	})
}

// Run starts the Subscription and processes all registered routes.
func (r *Router) Run(ctx context.Context) error {
	return r.sub.Start(ctx, func(msg domain.Message) {
		for _, rt := range r.routes {
			// Aplica el filtro solo si está definido
			if rt.Filter != nil && !rt.Filter.Match(msg) {
				continue
			}

			var payload interface{}
			if err := json.Unmarshal(msg.Data, &payload); err != nil {
				log.Printf("[Router] Error unmarshaling payload: %v", err)
				continue
			}

			job := worker.Job{
				Msg:       msg,
				Publisher: rt.Pub,
				Handler: func(msg domain.Message) error {
					return rt.Handler(payload)
				},
			}

			r.dispatcher.JobQueue <- job
		}
	})
}
