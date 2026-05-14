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

	gpubsub "cloud.google.com/go/pubsub/v2"
	"go.uber.org/fx"

	"github.com/norlis/event-driven/pkg/eventmux"
	"github.com/norlis/event-driven/pkg/kit/fxmux"
	"github.com/norlis/event-driven/pkg/middleware/recover"
	"github.com/norlis/event-driven/pkg/middleware/skiperr"
	"github.com/norlis/event-driven/pkg/middleware/validate"
	"github.com/norlis/event-driven/pkg/transport/eventhttp"
	"github.com/norlis/event-driven/pkg/transport/gcp/pubsub"
)

// Env vars consumed by the example.
const (
	envProjectID    = "GCP_PROJECT_ID"
	envSubscription = "EVT_SUBSCRIPTION"
	envPublishTopic = "EVT_PUBLISH"
	envWebhookURL   = "WEBHOOK_URL"
	envLogLevel     = "LOG_LEVEL" // "debug" or "info" (default)
)

// SubscriptionParams collects the named Subscription dependencies consumed by
// the two muxes (one HTTP, one Pub/Sub).
type SubscriptionParams struct {
	fx.In

	HTTPSubscription eventmux.Subscription `name:"HTTPSubscription"`
	AppSubscription  eventmux.Subscription `name:"AppSubscription"`
}

// RouterParams collects the two muxes registered in the FX graph.
type RouterParams struct {
	fx.In

	HTTPMux      *eventmux.Mux `name:"HTTPMux"`
	PrincipalMux *eventmux.Mux `name:"PrincipalMux"`
}

// EventParams is injected into RegisterEventHandlers so it has access to the
// logger and the application use case.
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

// NewPubSubClient constructs the GCP Pub/Sub client and registers a Close hook
// against the FX lifecycle.
func NewPubSubClient(lc fx.Lifecycle, logger *slog.Logger) (*gpubsub.Client, error) {
	client, err := gpubsub.NewClient(context.Background(), os.Getenv(envProjectID))
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

// NewAppSubscription builds the Pub/Sub subscriber bound to EVT_SUBSCRIPTION.
func NewAppSubscription(c *gpubsub.Client, logger *slog.Logger) eventmux.Subscription {
	return pubsub.NewSubscriber(c, pubsub.SubscriberConfig{
		ProjectID:              os.Getenv(envProjectID),
		SubscriptionID:         os.Getenv(envSubscription),
		MaxOutstandingMessages: 50,
		NumGoroutines:          10,
		MaxExtension:           60 * time.Second,
	}, logger.With(slog.String("logger", "app-subscription")))
}

// NewEventPublisher builds the Pub/Sub publisher bound to EVT_PUBLISH. Returns
// (nil, nil) when the env var is empty so consumers may use the result publisher
// as optional in the FX graph.
func NewEventPublisher(c *gpubsub.Client, logger *slog.Logger) (eventmux.Publisher, error) {
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

// NewHTTPMux builds the mux consuming the HTTP subscriber.
func NewHTTPMux(lc fx.Lifecycle, subs SubscriptionParams, logger *slog.Logger, sd fx.Shutdowner) *eventmux.Mux {
	mux := eventmux.New(eventmux.Config{
		Name:            "http-mux",
		Subscription:    subs.HTTPSubscription,
		Logger:          logger.With(slog.String("logger", "http-mux")),
		ReportOnNoMatch: true,
	})
	mux.Use(recover.Middleware)
	mux.UsePreflight(validate.New(logger))

	fxmux.Bind(lc, mux, logger, sd)
	return mux
}

// NewPrincipalMux builds the mux consuming the Pub/Sub subscriber.
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

// NewHTTPServerMux constructs the *http.ServeMux that backs the HTTP server,
// and starts/stops the server via fx.Lifecycle hooks.
func NewHTTPServerMux(lc fx.Lifecycle, logger *slog.Logger) *http.ServeMux {
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

// NewHTTPSubscriber registers the eventhttp subscriber under POST /command.
func NewHTTPSubscriber(server *http.ServeMux, logger *slog.Logger) (eventmux.Subscription, error) {
	sub, err := eventhttp.NewSubscriber(server, eventhttp.SubscriberConfig{
		Pattern: "POST /command",
		Logger:  logger.With(slog.String("logger", "http-subscriber")),
	})
	if err != nil {
		return nil, fmt.Errorf("example: new http subscriber: %w", err)
	}
	return sub, nil
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
