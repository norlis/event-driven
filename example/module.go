package example

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	sdkpubsub "cloud.google.com/go/pubsub/v2"
	"github.com/norlis/event-driven/pkg/eventmux"
	"github.com/norlis/event-driven/pkg/kit/fxmux"
	"github.com/norlis/event-driven/pkg/middleware/recover"
	"github.com/norlis/event-driven/pkg/middleware/skiperr"
	"github.com/norlis/event-driven/pkg/middleware/validate"
	"github.com/norlis/event-driven/pkg/provider/eventhttp"
	"github.com/norlis/event-driven/pkg/provider/gcp/pubsub"
	"go.uber.org/fx"
)

// Env vars consumed by the example.
const (
	envProjectID    = "GCP_PROJECT_ID"
	envSubscription = "EVT_SUBSCRIPTION"
	envPublishTopic = "EVT_PUBLISH"
	envWebhookURL   = "WEBHOOK_URL"
	envLogLevel     = "LOG_LEVEL" // "debug" or "info" (default)
)

type SubscriptionParams struct {
	fx.In

	HttpSubscription eventmux.Subscription `name:"HttpSubscription"`
	AppSubscription  eventmux.Subscription `name:"AppSubscription"`
}

type RouterParams struct {
	fx.In

	HttpMux      *eventmux.Mux `name:"HttpMux"`
	PrincipalMux *eventmux.Mux `name:"PrincipalMux"`
}

type EventParams struct {
	fx.In

	Logger  *slog.Logger
	Handler UseCase
}

// NewLogger returns a JSON slog logger whose level is controlled by LOG_LEVEL.
func NewLogger() *slog.Logger {
	level := slog.LevelInfo
	if strings.EqualFold(os.Getenv(envLogLevel), "debug") {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}

func NewPubSubClient(lc fx.Lifecycle, logger *slog.Logger) (*sdkpubsub.Client, error) {
	client, err := sdkpubsub.NewClient(context.Background(), os.Getenv(envProjectID))
	if err != nil {
		return nil, fmt.Errorf("pubsub client: %w", err)
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			logger.Info("Closing Pub/Sub client...")
			return client.Close()
		},
	})
	return client, nil
}

func NewAppSubscription(c *sdkpubsub.Client, logger *slog.Logger) eventmux.Subscription {
	return pubsub.NewSubscriber(c, pubsub.SubscriberConfig{
		ProjectID:              os.Getenv(envProjectID),
		SubscriptionID:         os.Getenv(envSubscription),
		MaxOutstandingMessages: 50,
		NumGoroutines:          10,
		MaxExtension:           60 * time.Second,
	}, logger.With(slog.String("logger", "app-subscription")))
}

func NewEventPublisher(c *sdkpubsub.Client, logger *slog.Logger) (eventmux.Publisher, error) {
	topic := os.Getenv(envPublishTopic)
	if topic == "" {
		logger.Info("No publish topic configured; result publisher disabled.")
		return nil, nil //nolint:nilnil // fx expects nil publisher when topic is not configured
	}
	return pubsub.NewPublisher(c, pubsub.PublisherConfig{
		ProjectID: os.Getenv(envProjectID),
		TopicID:   topic,
	}, logger.With(slog.String("logger", "publisher"))), nil
}

func NewHttpMux(lc fx.Lifecycle, subs SubscriptionParams, logger *slog.Logger, sd fx.Shutdowner) *eventmux.Mux {
	mux := eventmux.New(eventmux.Config{
		Name:            "http-mux",
		Subscription:    subs.HttpSubscription,
		Logger:          logger.With(slog.String("logger", "http-mux")),
		ReportOnNoMatch: true,
	})
	mux.Use(recover.Middleware)
	mux.UsePreflight(validate.New(logger))

	fxmux.Bind(lc, mux, logger, sd)
	return mux
}

func NewPrincipalMux(lc fx.Lifecycle, subs SubscriptionParams, logger *slog.Logger, sd fx.Shutdowner) *eventmux.Mux {
	mux := eventmux.New(eventmux.Config{
		Name:         "pubsub-principal",
		Subscription: subs.AppSubscription,
		Logger:       logger.With(slog.String("logger", "pubsub-mux")),
	})

	mux.Use(
		skiperr.New(
			logger,
			skiperr.ByErr("ErrInvalidObject", ErrInvalidObject),
			skiperr.ByErr("ErrDataNotFound", ErrDataNotFound),
		).Middleware,
		recover.Middleware,
	)

	fxmux.Bind(lc, mux, logger, sd)
	return mux
}

func NewHttpServerMux(lc fx.Lifecycle, logger *slog.Logger) *http.ServeMux {
	s := http.NewServeMux()
	server := &http.Server{
		Addr:              ":8880",
		Handler:           s,
		ReadHeaderTimeout: 30 * time.Second,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("HTTP server listening on :8880")
			go func() {
				if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					logger.Error("HTTP server failed", slog.Any("error", err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return server.Shutdown(shutdownCtx)
		},
	})

	return s
}

func NewHttpSubscriber(server *http.ServeMux, logger *slog.Logger) (eventmux.Subscription, error) {
	return eventhttp.NewSubscriber(server, eventhttp.SubscriberConfig{
		Pattern: "POST /command",
		Logger:  logger.With(slog.String("logger", "http-subscriber")),
	})
}

// ProjectID exposes the GCP project ID for components outside the FX graph
// (e.g. the healthchecker wiring in cmd/main.go).
func ProjectID() string { return os.Getenv(envProjectID) }

// PublishTopic returns the topic name configured via EVT_PUBLISH.
func PublishTopic() string { return os.Getenv(envPublishTopic) }

// SubscriptionID returns the subscription ID configured via EVT_SUBSCRIPTION.
func SubscriptionID() string { return os.Getenv(envSubscription) }

// WebhookURL returns the webhook URL configured via WEBHOOK_URL.
func WebhookURL() string { return os.Getenv(envWebhookURL) }
