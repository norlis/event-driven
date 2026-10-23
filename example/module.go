package example

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"
	"go.uber.org/fx"

	"github.com/norlis/httpgate/logging"
	"github.com/norlis/httpgate/server"

	"github.com/norlis/event-driven/v2/pkg/eventmux"
	"github.com/norlis/event-driven/v2/pkg/kit/fxmux"
	"github.com/norlis/event-driven/v2/pkg/kit/httpserver"
	"github.com/norlis/event-driven/v2/pkg/kit/lifecycle"
	"github.com/norlis/event-driven/v2/pkg/middleware/skiperr"
	"github.com/norlis/event-driven/v2/pkg/middleware/validate"
	"github.com/norlis/event-driven/v2/pkg/transport/eventhttp"
	"github.com/norlis/event-driven/v2/pkg/transport/gcp/pubsub"
)

// Env vars consumed by the example.
const (
	envProjectID    = "GCP_PROJECT_ID"
	envSubscription = "EVT_SUBSCRIPTION"
	envPublishTopic = "EVT_PUBLISH"
	envWebhookURL   = "WEBHOOK_URL"
	envLogLevel     = "LOG_LEVEL"       // "debug" or "info" (default)
	envServiceName  = "SERVICE_NAME"    // service.name (default "event-driven-example")
	envServiceVer   = "SERVICE_VERSION" // service.version (default "dev")
	envEnvironment  = "ENVIRONMENT"     // deployment.environment.name (default "local")
	envHTTPAddr     = "HTTP_ADDR"       // HTTP listen address (default ":8880")
)

// getenvDefault returns the environment variable value, or fallback when unset.
func getenvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

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

	HTTPMux   *eventmux.Mux `name:"HTTPMux"`
	PubSubMux *eventmux.Mux `name:"PubSubMux"`
}

// EventParams is injected into RegisterEventHandlers so it has access to the
// logger and the application use case.
type EventParams struct {
	fx.In

	Logger  *slog.Logger
	Handler UseCase
}

// NewLogger returns the platform-standard logger (NDJSON, OTel fields, ISO
// 8601 UTC timestamps, automatic trace_id/span_id injection). DEBUG stays off
// unless LOG_LEVEL=debug.
func NewLogger() *slog.Logger {
	level := slog.LevelInfo
	if strings.EqualFold(os.Getenv(envLogLevel), "debug") {
		level = slog.LevelDebug
	}
	return logging.New(
		os.Stdout,
		logging.WithService(getenvDefault(envServiceName, "event-driven-example"), getenvDefault(envServiceVer, "dev")),
		logging.WithEnvironment(getenvDefault(envEnvironment, "local")),
		logging.WithLevel(level),
	)
}

// NewPubSubClient constructs the GCP Pub/Sub client. BindLifecycle closes it
// after the muxes drain.
func NewPubSubClient() (*gpubsub.Client, error) {
	projectID := os.Getenv(envProjectID)
	if projectID == "" {
		return nil, fmt.Errorf("%s is not set", envProjectID)
	}
	client, err := gpubsub.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, fmt.Errorf("pubsub client: %w", err)
	}
	return client, nil
}

// NewAppSubscription builds the Pub/Sub subscriber bound to EVT_SUBSCRIPTION.
func NewAppSubscription(c *gpubsub.Client, logger *slog.Logger) (eventmux.Subscription, error) {
	sub, err := pubsub.NewSubscriber(c, pubsub.SubscriberConfig{
		SubscriptionID:         os.Getenv(envSubscription),
		MaxOutstandingMessages: 50,
		NumGoroutines:          10,
		MaxExtension:           60 * time.Second,
	}, logger)
	if err != nil {
		return nil, fmt.Errorf("example: new pubsub subscriber: %w", err)
	}
	return sub, nil
}

// NewEventPublisher builds the Pub/Sub publisher bound to EVT_PUBLISH; nil
// when the env var is empty. BindLifecycle closes it (flushing the batch)
// after the muxes drain.
func NewEventPublisher(c *gpubsub.Client, logger *slog.Logger) (*pubsub.Publisher, error) {
	topic := os.Getenv(envPublishTopic)
	if topic == "" {
		logger.Info("result publisher disabled")
		return nil, nil //nolint:nilnil // fx expects nil publisher when topic is not configured
	}
	pub, err := pubsub.NewPublisher(c, pubsub.PublisherConfig{TopicID: topic}, logger)
	if err != nil {
		return nil, fmt.Errorf("example: new pubsub publisher: %w", err)
	}
	return pub, nil
}

// NewResultPublisher offers the Pub/Sub publisher to the routes. A nil
// pointer becomes an untyped nil: inside the interface it would be non-nil
// and a route would call Publish on it.
func NewResultPublisher(p *pubsub.Publisher) eventmux.Publisher {
	if p == nil {
		return nil
	}
	return p
}

// NewHTTPMux builds the mux consuming the HTTP subscriber.
func NewHTTPMux(subs SubscriptionParams, logger *slog.Logger, obs eventmux.Observer) (*eventmux.Mux, error) {
	mux, err := eventmux.New(eventmux.Config{
		Name:         "http-mux",
		Subscription: subs.HTTPSubscription,
		Logger:       logger,
		Observer:     obs,
	})
	if err != nil {
		return nil, fmt.Errorf("example: %w", err)
	}
	mux.UsePreflight(validate.New())
	return mux, nil
}

// NewPubSubMux builds the mux consuming the Pub/Sub subscriber.
func NewPubSubMux(subs SubscriptionParams, logger *slog.Logger, obs eventmux.Observer) (*eventmux.Mux, error) {
	mux, err := eventmux.New(eventmux.Config{
		Name:         "pubsub-mux",
		Subscription: subs.AppSubscription,
		Logger:       logger,
		Observer:     obs,
	})
	if err != nil {
		return nil, fmt.Errorf("example: %w", err)
	}

	mux.Use(
		skiperr.New(
			logger,
			skiperr.ByErr("ErrInvalidObject", ErrInvalidObject),
			skiperr.ByErr("ErrDataNotFound", ErrDataNotFound),
		).Middleware,
	)
	return mux, nil
}

// NewServeMux is the router shared by the HTTP subscriber and the health
// endpoints.
func NewServeMux() *http.ServeMux { return http.NewServeMux() }

// NewHTTPServer serves mux on HTTP_ADDR (default ":8880").
func NewHTTPServer(mux *http.ServeMux, logger *slog.Logger) *httpserver.Server {
	return httpserver.New(mux, logger, server.WithAddr(getenvDefault(envHTTPAddr, ":8880")))
}

// LifecycleParams collects everything BindLifecycle starts and stops.
type LifecycleParams struct {
	fx.In

	Client    *gpubsub.Client
	Publisher *pubsub.Publisher
	HTTPMux   *eventmux.Mux `name:"HTTPMux"`
	PubSubMux *eventmux.Mux `name:"PubSubMux"`
	Server    *httpserver.Server
}

// BindLifecycle fixes the start/stop order in one place, whatever order fx
// built the parts in: resources first, then muxes, the HTTP server last.
// Shutdown runs in reverse, so the publisher flushes after the muxes drain.
func BindLifecycle(lc fx.Lifecycle, sd fx.Shutdowner, logger *slog.Logger, p LifecycleParams) {
	g := lifecycle.NewGroup(logger, 0)
	g.Add("pubsub-client", lifecycle.Closer(p.Client.Close))
	if p.Publisher != nil {
		g.Add("pubsub-publisher", lifecycle.Closer(p.Publisher.Close))
	}
	g.Add(p.PubSubMux.Name(), p.PubSubMux)
	g.Add(p.HTTPMux.Name(), p.HTTPMux)
	g.Add("http-server", p.Server)
	fxmux.BindGroup(lc, sd, g, logger)
}

// NewHTTPSubscriber registers the eventhttp subscriber under POST /command.
func NewHTTPSubscriber(mux *http.ServeMux, logger *slog.Logger) (eventmux.Subscription, error) {
	sub, err := eventhttp.NewSubscriber(mux, eventhttp.SubscriberConfig{
		Pattern:     "POST /command",
		DefaultType: "http.command",
	}, logger)
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
