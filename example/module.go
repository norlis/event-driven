package example

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/norlis/event-driven/pkg/adapter/httpdriven"
	messaging "github.com/norlis/event-driven/pkg/adapter/pubsub"
	"github.com/norlis/event-driven/pkg/application/router"
	"github.com/norlis/event-driven/pkg/application/router/middlewares"
	applogger "github.com/norlis/event-driven/pkg/kit/logger"
	"github.com/norlis/event-driven/pkg/port"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type SubscriptionParams struct {
	fx.In

	HttpSubscription port.Subscription `name:"HttpSubscription"`
	AppSubscription  port.Subscription `name:"AppSubscription"`
}

type RouterParams struct {
	fx.In

	HttpMux      *router.EventMux `name:"HttpMux"`
	PrincipalMux *router.EventMux `name:"PrincipalMux"`
}

type EventParams struct {
	fx.In

	Configuration *Configuration
	Log           *zap.Logger
	Logger        *zap.Logger
	Handler1      UseCaseExample
}

func NewLogger(cfg *Configuration) (*zap.Logger, error) {
	debugMode := strings.EqualFold(cfg.LogLevel, "debug")

	l, err := applogger.New(debugMode)
	if err != nil {
		return nil, fmt.Errorf("fallo al crear logger: %w", err)
	}
	return l, nil
}

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

func NewAppSubscription(psClient *pubsub.Client, configuration *Configuration, logger *zap.Logger) port.Subscription {
	subscriberCfg := messaging.SubscriberConfig{
		ProjectID:              configuration.Cloud.GCloudProjectId,
		SubscriptionID:         configuration.Messaging.SubscribeDestination,
		MaxOutstandingMessages: 50,
		NumGoroutines:          10,
		MaxExtension:           60 * time.Second,
	}
	return messaging.NewSubscription(psClient, subscriberCfg, logger.Named("app-subscription"))
}

func NewEventPublisher(psClient *pubsub.Client, configuration *Configuration, logger *zap.Logger) (port.Publisher, error) {
	if configuration.Messaging.PublishDestinationTopic == "" {
		logger.Info("No se configuró PublishTraceTopic, no se creará el publicador de resultados.")
		return nil, nil //nolint:nilnil // fx expects nil publisher when topic is not configured
	}
	publisherCfg := messaging.PublisherConfig{
		ProjectID: configuration.Cloud.GCloudProjectId,
		TopicID:   configuration.Messaging.PublishDestinationTopic,
	}
	return messaging.NewPublisher(psClient, publisherCfg, logger.Named("publisher")), nil
}

func NewHttpMux(lc fx.Lifecycle, params EventParams, subs SubscriptionParams, logger *zap.Logger) *router.EventMux {
	mux := router.NewEventMux(router.Config{
		Subscription:    subs.HttpSubscription,
		Logger:          logger.Named("http-mux"),
		ReportOnNoMatch: true,
	})
	mux.Use(middlewares.Recoverer)
	mux.UsePreflight(middlewares.ValidationMiddleware(logger))

	var cancel context.CancelFunc

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Starting HTTP EventMux...")

			var childCtx context.Context
			childCtx, cancel = context.WithCancel(context.Background()) //nolint:gosec // cancel is called in OnStop hook

			go func() {
				if err := mux.Run(childCtx); err != nil && !errors.Is(err, context.Canceled) {
					logger.Error("Error crítico en HTTP EventMux", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("EventMux shutdown manejado por cancelación externa.")
			if cancel != nil {
				cancel()
			}
			return nil
		},
	})

	return mux
}

// NewPrincipalMux Provider para el EventMux principal.
func NewPrincipalMux(lc fx.Lifecycle, params EventParams, subs SubscriptionParams, logger *zap.Logger) *router.EventMux {
	mux := router.NewEventMux(router.Config{
		Subscription: subs.AppSubscription,
		Logger:       logger.Named("pubsub-mux-principal"),
	})

	mux.Use(
		middlewares.NewIgnoreErrors(
			logger,
			middlewares.IgnoreSpecificError("ErrInvalidObject", ErrInvalidObject),
			middlewares.IgnoreSpecificError("ErrDataNotFound", ErrDataNotFound),
		).Middleware,
		middlewares.Recoverer,
	)

	var cancel context.CancelFunc

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Starting EventMux...")

			var childCtx context.Context
			childCtx, cancel = context.WithCancel(context.Background()) //nolint:gosec // cancel is called in OnStop hook

			go func() {
				if err := mux.Run(childCtx); err != nil && !errors.Is(err, context.Canceled) {
					logger.Error("Error crítico en EventMux", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("EventMux shutdown manejado por cancelación externa.")
			if cancel != nil {
				cancel()
			}
			return nil
		},
	})
	return mux
}

func NewHttpServerMux(lc fx.Lifecycle, logger *zap.Logger) *http.ServeMux {
	s := http.NewServeMux()
	server := &http.Server{
		Addr:              ":8880",
		Handler:           s,
		ReadHeaderTimeout: 30 * time.Second,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Iniciando servidor HTTP")
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
				return fmt.Errorf("http server shutdown: %w", err)
			}
			logger.Info("Servidor HTTP detenido correctamente.")
			return nil
		},
	})

	return s
}

func NewHttpSubscriber(server *http.ServeMux, logger *zap.Logger) (port.Subscription, error) {
	sub, err := httpdriven.NewSubscriber(
		server,
		httpdriven.SubscriberConfig{
			Pattern: "POST /command",
			Logger:  logger.Named("http-subscriber"),
		})
	if err != nil {
		return nil, fmt.Errorf("http subscriber: %w", err)
	}
	return sub, nil
}
