package example

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/norlis/event-driven/pkg/domain"
	"github.com/norlis/event-driven/pkg/infrastructure/httpdriven"
	messaging "github.com/norlis/event-driven/pkg/infrastructure/pubsub"
	"github.com/norlis/event-driven/pkg/logger"
	"github.com/norlis/event-driven/pkg/usecase/router"
	"github.com/norlis/event-driven/pkg/usecase/router/middlewares"
	"github.com/norlis/event-driven/pkg/usecase/worker"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type SubscriptionParams struct {
	fx.In

	HttpSubscription  domain.Subscription `name:"HttpSubscription"`
	AppSubscription   domain.Subscription `name:"AppSubscription"`
	TraceSubscription domain.Subscription `name:"TraceSubscription"`
}

type RouterParams struct {
	fx.In

	HttpRouter      *router.Router `name:"HttpRouter"`
	PrincipalRouter *router.Router `name:"PrincipalRouter"`
	TraceRouter     *router.Router `name:"TraceRouter"`
}

type EventParams struct {
	fx.In

	Configuration *Configuration
	Log           *zap.Logger
	Dispatcher    *worker.Dispatcher
	Logger        *zap.Logger
	Handler1      UseCaseExample
}

func NewLogger(cfg *Configuration) (*zap.Logger, error) {
	debugMode := strings.ToLower(cfg.LogLevel) == "debug"
	l, err := logger.New(debugMode)
	if err != nil {
		return nil, fmt.Errorf("fallo al crear logger: %w", err)
	}
	return l, nil
}

//func NewOpenTelemetry(lc fx.Lifecycle, envCfg *AppEnvConfig, logger *zap.Logger) (trace.Tracer, error) {
//	if !envCfg.OtelEnabled {
//		logger.Info("OpenTelemetry Tracing está DESHABILITADO por configuración.")
//		return trace.NewNoopTracerProvider().Tracer("noop-tracer"), nil
//	}
//
//	// Usando el otelsetup de tu librería
//	otelShutdown, err := otelsetup.InitTracerProvider(
//		context.Background(), // FX maneja el contexto de apagado a través de lc.Append
//		"otra-app-fx-service",
//		"1.0.0",
//		envCfg.OtelEndpoint,
//		envCfg.OtelEnabled, // true en este bloque
//		logger.Named("otelsetup"),
//	)
//	if err != nil {
//		// InitTracerProvider ahora puede devolver un Noop si falla la conexión,
//		// así que solo logueamos y continuamos con un NoopTracer.
//		logger.Warn("Fallo al inicializar el proveedor de trazas OTel completamente, se usará NoopTracerProvider.", zap.Error(err))
//		return trace.NewNoopTracerProvider().Tracer("otel-init-failed-tracer"), nil
//	}
//
//	lc.Append(fx.Hook{
//		OnStop: func(ctx context.Context) error {
//			logger.Info("Apagando OpenTelemetry Provider...")
//			return otelShutdown(ctx)
//		},
//	})
//
//	return otel.Tracer("otra-app-fx/main"), nil // Un tracer para la app
//}

func NewPubSubClient(lc fx.Lifecycle, configuration *Configuration, logger *zap.Logger) (*pubsub.Client, error) {
	client, err := pubsub.NewClient(context.Background(), configuration.Cloud.GCloudProjectId)
	if err != nil {
		logger.Error("Fallo al crear cliente Pub/Sub", zap.Error(err))
		return nil, fmt.Errorf("fallo al crear cliente Pub/Sub: %w", err)
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Cliente Pub/Sub listo (sin acción de inicio explícita necesaria).")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Cerrando cliente Pub/Sub...")
			_, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			return client.Close()
		},
	})
	return client, nil
}

func NewAppSubscription(psClient *pubsub.Client, configuration *Configuration, logger *zap.Logger) domain.Subscription {
	subscriberCfg := messaging.SubscriberConfig{
		ProjectID:              configuration.Cloud.GCloudProjectId,
		SubscriptionID:         configuration.Messaging.SubscribeDestination,
		MaxOutstandingMessages: 120, // NumWorkers + QueueSize
		NumGoroutines:          10,
		MaxExtension:           60 * time.Second,
	}
	return messaging.NewSubscription(psClient, subscriberCfg, logger.Named("app-subscription"))
}

func NewTraceSubscription(psClient *pubsub.Client, configuration *Configuration, logger *zap.Logger) domain.Subscription {
	subscriberCfg := messaging.SubscriberConfig{
		ProjectID:              configuration.Cloud.GCloudProjectId,
		SubscriptionID:         configuration.Messaging.SubscribeTrace,
		MaxOutstandingMessages: 120, // NumWorkers + QueueSize
		NumGoroutines:          10,
		MaxExtension:           60 * time.Second,
	}
	return messaging.NewSubscription(psClient, subscriberCfg, logger.Named("trace-subscription"))
}

func NewEventPublisher(psClient *pubsub.Client, configuration *Configuration, logger *zap.Logger) (domain.Publisher, error) {
	if configuration.Messaging.PublishTraceTopic == "" {
		logger.Info("No se configuró PublishTraceTopic, no se creará el publicador de resultados.")
		return nil, nil
	}
	publisherCfg := messaging.PublisherConfig{
		ProjectID: configuration.Cloud.GCloudProjectId,
		TopicID:   configuration.Messaging.PublishTraceTopic,
	}
	return messaging.NewPublisher(psClient, publisherCfg, logger.Named("trace-publisher")), nil
}

// NewWorkerDispatcher Provider para el Dispatcher de Workers
func NewWorkerDispatcher(lc fx.Lifecycle, logger *zap.Logger) *worker.Dispatcher {
	dispatcherCfg := worker.DispatcherConfig{
		NumWorkers: 10, // TODO: Estos valores deberían venir de configuration
		QueueSize:  100,
	}
	d := worker.NewDispatcher(dispatcherCfg, logger.Named("worker-dispatcher"))

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go d.Run(context.Background())
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Deteniendo Worker Dispatcher...")
			d.Stop()
			if ctx.Err() != nil { // Verifica si el contexto del hook Fx ya expiró
				logger.Warn(">>> HOOK: NewWorkerDispatcher OnStop - Contexto del hook Fx (stopCtx) expiró.", zap.Error(ctx.Err()))
			}
			return nil
		},
	})
	return d
}

func NewHttpRouter(lc fx.Lifecycle, params EventParams, subs SubscriptionParams, logger *zap.Logger) *router.Router {
	r := router.New(router.Config{
		Subscription:     subs.HttpSubscription,
		WorkerDispatcher: params.Dispatcher,
		Logger:           params.Logger.Named("http-router"),
	})
	r.Use(middlewares.Recoverer)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("stating http Event Router...")
			go func() {
				childCtx, cancel := context.WithCancel(context.Background())
				defer cancel()

				if err := r.Run(childCtx); err != nil && !errors.Is(err, context.Canceled) {
					logger.Error("Error crítico en http Event Router", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Router shutdown manejado por cancelación externa.")
			return nil
		},
	})

	return r

}

// NewPrincipalRouter Provider para el Router
func NewPrincipalRouter(lc fx.Lifecycle, params EventParams, subs SubscriptionParams, logger *zap.Logger) *router.Router {
	routerCfg := router.Config{
		Subscription:     subs.AppSubscription,
		WorkerDispatcher: params.Dispatcher,
		Logger:           params.Logger.Named("pubsub-router-principal"),
	}
	r := router.New(routerCfg)

	//r.Use(
	//	middlewares.NewIgnoreErrors(
	//		logger, ErrInvalidObject, ErrDataNotFound,)
	//	.Middleware,
	//)

	r.Use(
		middlewares.NewIgnoreErrors(
			logger,
			middlewares.IgnoreSpecificError("ErrInvalidObject", ErrInvalidObject),
			middlewares.IgnoreSpecificError("ErrDataNotFound", ErrDataNotFound),
		).Middleware,
		middlewares.Recoverer,
	)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Iniciando Event Router...")
			go func() {
				childCtx, cancel := context.WithCancel(context.Background())
				defer cancel()

				if err := r.Run(childCtx); err != nil && !errors.Is(err, context.Canceled) {
					logger.Error("Error crítico en Event Router", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Router shutdown manejado por cancelación externa.")
			return nil
		},
	})
	return r
}

// NewTraceRouter Provider para el Router
func NewTraceRouter(params EventParams, subs SubscriptionParams) *router.Router {
	routerCfg := router.Config{
		Subscription:     subs.TraceSubscription,
		WorkerDispatcher: params.Dispatcher,
		Logger:           params.Logger.Named("pubsub-router-trace"),
	}
	return router.New(routerCfg)
}

func NewHttpServer(lc fx.Lifecycle, logger *zap.Logger) *http.ServeMux {
	s := http.NewServeMux()
	server := &http.Server{
		Addr:              ":8880",
		Handler:           s,
		ReadHeaderTimeout: 30 * time.Second,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Iniciando servidor HTTP ")
			go func() {
				if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					logger.Error("Error al iniciar servidor HTTP: %v", zap.Error(err))
				}
			}()
			logger.Info("Servidor HTTP escuchando en :8880")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Deteniendo servidor HTTP...")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := server.Shutdown(shutdownCtx); err != nil {
				logger.Error("Error durante el apagado del servidor HTTP: %v", zap.Error(err))
				return err
			}
			logger.Info("Servidor HTTP detenido correctamente.")
			return nil
		},
	})

	return s
}

func NewHttpSubscriber(server *http.ServeMux, logger *zap.Logger) (domain.Subscription, error) {
	return httpdriven.NewSubscriber(
		server,
		httpdriven.SubscriberConfig{
			Pattern: "POST /command",
			Logger:  logger.Named("http-subscriber"),
		})
}
