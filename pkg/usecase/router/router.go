package router

import (
	"context"
	"encoding/json"
	"github.com/norlis/event-driven/pkg/domain"
	"github.com/norlis/event-driven/pkg/usecase/worker"
	"reflect"

	"go.uber.org/zap"
)

type Filter interface {
	Match(msg *domain.Message) bool
}

type HandlerFunc func(data any) (json.RawMessage, error)

type Route struct {
	Pub        domain.Publisher
	Filter     Filter
	Handler    HandlerFunc
	ObjectType any
}

// Config contiene la configuración para el Router.
type Config struct {
	Subscription     domain.Subscription // Fuente de mensajes
	WorkerDispatcher *worker.Dispatcher  // Dispatcher para procesar trabajos
	Logger           *zap.Logger
}

type Router struct {
	cfg    Config
	routes []Route
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
func (r *Router) Register(pub domain.Publisher, filter Filter, objectType any, handler HandlerFunc) {
	r.routes = append(r.routes, Route{
		Pub:        pub,
		Filter:     filter,
		Handler:    handler,
		ObjectType: objectType,
	})
	r.cfg.Logger.Info("Ruta registrada", zap.Int("totalRoutes", len(r.routes)))
}

// Run starts the Subscription and processes all registered routes.
// Run inicia la suscripción y comienza a procesar mensajes a través de las rutas registradas.
// Es una operación bloqueante hasta que el contexto es cancelado o la suscripción falla.
func (r *Router) Run(ctx context.Context) error {
	r.cfg.Logger.Info("Router iniciando, comenzando suscripción...")
	// El WorkerDispatcher.Run() debe ser llamado por el código que configura el Router.
	// Aquí solo usamos el JobQueue del dispatcher.

	return r.cfg.Subscription.Start(ctx, func(msg *domain.Message) {
		r.cfg.Logger.Debug("Router recibió mensaje de la suscripción", zap.String("messageUUID", msg.UUID))
		matchedAtLeastOneRoute := false
		for _, rt := range r.routes {
			if rt.Filter != nil && !rt.Filter.Match(msg) {
				r.cfg.Logger.Debug("Mensaje no coincide con el filtro para una ruta", zap.String("messageUUID", msg.UUID))
				continue // Saltar esta ruta si el filtro no coincide
			}
			matchedAtLeastOneRoute = true
			r.cfg.Logger.Debug("Mensaje coincide con filtro (o no hay filtro), procesando ruta...", zap.String("messageUUID", msg.UUID))

			// Deserializar el payload al tipo de objeto especificado para la ruta.
			eventPayload, err := NewInterface(reflect.TypeOf(rt.ObjectType), msg.Payload)
			if err != nil {
				r.cfg.Logger.Error("Error al desempaquetar payload para la ruta",
					zap.Error(err),
					zap.String("messageUUID", msg.UUID),
					zap.String("targetType", reflect.TypeOf(rt.ObjectType).String()))
				// Considerar si se debe Nackear el mensaje aquí o si es un error de configuración de ruta.
				// Si es un error de payload, Nack es apropiado.
				msg.Nack()
				continue // Saltar al siguiente mensaje o ruta si esta falla
			}

			// Crear un trabajo para el worker.
			job := worker.Job{
				Msg:       msg,
				Publisher: rt.Pub, // Publisher asociado a esta ruta
				Handler: func(processedMsg *domain.Message) (any, error) {
					// El 'processedMsg' aquí es el mismo 'msg' original,
					// pero el handler de la ruta espera el 'eventPayload' desempaquetado.
					return rt.Handler(eventPayload)
				},
			}

			// Enviar el trabajo al JobQueue del dispatcher.
			// Esto podría bloquear si la cola está llena. Considerar select con ctx.Done().
			select {
			case r.cfg.WorkerDispatcher.JobQueue <- job:
				r.cfg.Logger.Debug("Trabajo enviado al dispatcher", zap.String("messageUUID", msg.UUID))
			case <-ctx.Done():
				r.cfg.Logger.Warn("Contexto cancelado, no se pudo enviar trabajo al dispatcher", zap.String("messageUUID", msg.UUID))
				msg.Nack() // Si no se puede encolar por apagado, Nack.
				return     // Salir del handler de suscripción
			}
			// Una vez que un trabajo se encola para una ruta que coincide, podríamos romper el bucle de rutas
			// si un mensaje solo debe ser manejado por la primera ruta coincidente.
			// O continuar si un mensaje puede disparar múltiples rutas.
			// Por ahora, continúa (un mensaje puede activar múltiples rutas si los filtros coinciden).
		}

		if !matchedAtLeastOneRoute {
			r.cfg.Logger.Debug("Mensaje no coincidió con ninguna ruta", zap.String("messageUUID", msg.UUID))
			// Decidir qué hacer con mensajes que no coinciden con ninguna ruta.
			// Podría ser Ack (ignorar), Nack (reintentar, podría ser problemático), o enviar a una DLQ.
			// Por seguridad y para evitar bucles de Nack, Ack es a menudo una opción si no hay DLQ.
			msg.Ack() // Opcional: Ack si no hay rutas coincidentes, para evitar que quede en la cola.
		}
	})
}
