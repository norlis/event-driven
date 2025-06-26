package router

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/norlis/event-driven/pkg/domain/event"
	"github.com/norlis/event-driven/pkg/port"

	"github.com/norlis/event-driven/pkg/application/worker"
	"github.com/norlis/event-driven/pkg/domain"
	"go.uber.org/zap"
)

var noopHandler = func(ctx context.Context, data any) (json.RawMessage, error) { return nil, nil }

type HandlerFunc func(ctx context.Context, data any) (json.RawMessage, error)

type Route struct {
	Pub        port.Publisher
	Filter     port.Filter
	Handler    HandlerFunc
	ObjectType any
}

// Config contiene la configuración para el Router.
type Config struct {
	Subscription     port.Subscription  // Fuente de mensajes
	WorkerDispatcher *worker.Dispatcher // Dispatcher para procesar trabajos
	Logger           *zap.Logger

	// ReportOnNoMatch, si es true, marcará un mensaje con el estado
	// domain.ErrNoRouteMatched si no coincide con ninguna ruta.
	// Esto es útil para suscriptores síncronos como HTTP que necesitan
	// devolver un código de error específico. El valor por defecto es false.
	ReportOnNoMatch bool
}

type Router struct {
	cfg                  Config
	routes               []Route
	middlewares          []Middleware
	preflightMiddlewares []Middleware // Middlewares síncronos para validación
}

// New creates a new Router for a given Subscription source (PubSub, HTTP, etc.)
func New(cfg Config) *Router {
	if cfg.Logger == nil {
		// Fallback a un logger no-op si no se provee uno, aunque es mejor que la app lo configure.
		cfg.Logger = zap.NewNop()
	}
	if cfg.WorkerDispatcher == nil {
		cfg.Logger.Fatal("Router.New: WorkerDispatcher no puede ser nil")
	}
	// El Dispatcher.Run() ahora es llamado por la aplicación que usa la librería.
	return &Router{
		cfg: cfg,
	}
}

// Register añade una nueva ruta al router.
func (r *Router) Register(pub port.Publisher, filter port.Filter, objectType any, handler HandlerFunc) {
	r.routes = append(r.routes, Route{
		Pub:        pub,
		Filter:     filter,
		Handler:    handler,
		ObjectType: objectType,
	})
	r.cfg.Logger.Info("Ruta registrada", zap.Int("totalRoutes", len(r.routes)))
}

// Run starts the Subscription and processes all registered routes.
func (r *Router) Run(ctx context.Context) error {
	r.cfg.Logger.Info("Router iniciando, comenzando suscripción...")

	return r.cfg.Subscription.Start(ctx, func(msg *event.Message) {
		r.cfg.Logger.Debug("Router recibió mensaje de la suscripción", zap.String("messageUUID", msg.UUID))
		matchedAtLeastOneRoute := false
		for _, rt := range r.routes {
			if rt.Filter != nil && !rt.Filter.Match(msg) {
				r.cfg.Logger.Debug("Mensaje no coincide con el filtro para una ruta", zap.String("messageUUID", msg.UUID))
				continue
			}
			matchedAtLeastOneRoute = true
			r.cfg.Logger.Debug("Mensaje coincide con filtro (o no hay filtro), procesando ruta...", zap.String("messageUUID", msg.UUID))

			eventPayload, err := NewInterface(reflect.TypeOf(rt.ObjectType), msg.Payload)
			if err != nil {
				r.cfg.Logger.Error("Error al desempaquetar payload para la ruta",
					zap.Error(err),
					zap.String("messageUUID", msg.UUID),
					zap.String("targetType", reflect.TypeOf(rt.ObjectType).String()),
					zap.Any("event", msg.Payload),
				)
				msg.NotifyPreflightDone(err)
				msg.Nack()
				continue
			}

			preflightChain := chainMiddlewares(noopHandler, r.preflightMiddlewares...)
			if _, preflightErr := preflightChain(context.Background(), eventPayload); preflightErr != nil {
				msg.NotifyPreflightDone(preflightErr)
				msg.Nack()
				return
			}

			msg.NotifyPreflightDone(nil)

			effectiveHandler := chainMiddlewares(rt.Handler, r.middlewares...)

			job := worker.Job{
				Msg:       msg,
				Publisher: rt.Pub,
				Handler: func(ctx context.Context, processedMsg *event.Message) (json.RawMessage, error) {
					return effectiveHandler(ctx, eventPayload)
				},
			}

			// Enviar el trabajo al JobQueue del dispatcher.
			// Esto podría bloquear si la cola está llena. Considerar select con ctx.Done().
			select {
			case r.cfg.WorkerDispatcher.JobQueue <- job:
				r.cfg.Logger.Debug("Trabajo enviado al dispatcher", zap.String("messageUUID", msg.UUID))
			case <-ctx.Done():
				r.cfg.Logger.Warn("Contexto cancelado, no se pudo enviar trabajo al dispatcher", zap.String("messageUUID", msg.UUID))
				msg.Nack()
				return
				// enlocar
				//case <-time.After(5 *time.Second): // Timeout para encolar
				//	r.cfg.Logger.Warn("Timeout esperando para enviar trabajo al JobQueue (cola llena o workers lentos)",
				//		zap.String("messageUUID", msg.UUID),
				//		zap.Duration("timeout", 5 *time.Second))
				//	msg.Nack()
			}

		}

		if !matchedAtLeastOneRoute {
			r.cfg.Logger.Debug("Mensaje no coincidió con ninguna ruta", zap.String("messageUUID", msg.UUID))
			if r.cfg.ReportOnNoMatch {
				msg.NotifyPreflightDone(domain.ErrNoRouteMatched)
			}
			msg.Ack() // Ack si no hay rutas coincidentes, para evitar que quede en la cola.
		}
	})
}

func (r *Router) Use(middlewares ...Middleware) {
	r.middlewares = append(r.middlewares, middlewares...)
}

func (r *Router) UsePreflight(middlewares ...Middleware) {
	r.preflightMiddlewares = append(r.preflightMiddlewares, middlewares...)
}

// chainMiddlewares aplica una cadena de middlewares a un handler.
// Los middlewares se aplican en orden inverso (el último añadido es el más externo).
func chainMiddlewares(handler HandlerFunc, mws ...Middleware) HandlerFunc {
	chainedHandler := handler
	for i := len(mws) - 1; i >= 0; i-- {
		chainedHandler = mws[i](chainedHandler)
	}
	return chainedHandler
}
