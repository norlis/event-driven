package router

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"github.com/google/uuid"
	"github.com/norlis/event-driven/pkg/application/router/metadata"
	"github.com/norlis/event-driven/pkg/domain"
	"github.com/norlis/event-driven/pkg/domain/event"
	"github.com/norlis/event-driven/pkg/port"
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

// Config holds the EventMux configuration.
type Config struct {
	Subscription    port.Subscription
	Logger          *zap.Logger
	ReportOnNoMatch bool
	MaxRetries      int // 0 = infinite retries (current behavior)
}

type EventMux struct {
	cfg                  Config
	routes               []*Route
	middlewares          []Middleware
	preflightMiddlewares []Middleware
}

// NewEventMux creates a new EventMux for a given Subscription source (PubSub, HTTP, etc.).
func NewEventMux(cfg Config) *EventMux {
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	return &EventMux{
		cfg: cfg,
	}
}

// Register adds a new route to the mux.
func (mux *EventMux) Register(pub port.Publisher, filter port.Filter, objectType any, handler HandlerFunc) {
	mux.routes = append(mux.routes, &Route{
		Pub:        pub,
		Filter:     filter,
		Handler:    handler,
		ObjectType: objectType,
	})
	mux.cfg.Logger.Info("Route registered", zap.Int("totalRoutes", len(mux.routes)))
}

// Run starts the Subscription and processes all registered routes.
func (mux *EventMux) Run(ctx context.Context) error {
	mux.cfg.Logger.Info("EventMux starting subscription...")

	if err := mux.cfg.Subscription.Start(ctx, func(msg *event.Message) {
		mux.cfg.Logger.Debug("EventMux received message",
			zap.String("id", msg.ID()),
			zap.String("type", msg.Type()),
			zap.String("source", msg.Source()),
		)

		matchingRoute := mux.findMatchingRoute(msg)
		if matchingRoute == nil {
			mux.handleNoRouteFound(msg)
			return
		}

		mux.cfg.Logger.Debug("Message matched filter, processing route...", zap.String("id", msg.ID()))
		mux.processAndHandle(msg, matchingRoute)
	}); err != nil {
		return fmt.Errorf("subscription start: %w", err)
	}
	return nil
}

func (mux *EventMux) findMatchingRoute(msg *event.Message) *Route {
	for _, rt := range mux.routes {
		if rt.Filter == nil || rt.Filter.Match(msg) {
			return rt
		}
	}
	return nil
}

func (mux *EventMux) handleNoRouteFound(msg *event.Message) {
	mux.cfg.Logger.Debug("Message did not match any route", zap.String("id", msg.ID()))
	if mux.cfg.ReportOnNoMatch {
		msg.NotifyPreflightDone(domain.ErrNoRouteMatched)
	}
	msg.Ack()
}

func (mux *EventMux) processAndHandle(msg *event.Message, rt *Route) {
	// Retry count tracking.
	attempt := 1
	if v, ok := msg.Extensions()["retrycount"].(int32); ok {
		attempt = int(v) + 1
	}
	msg.SetExtension("retrycount", attempt)

	if mux.cfg.MaxRetries > 0 && attempt > mux.cfg.MaxRetries {
		mux.cfg.Logger.Warn("Max retries exceeded, discarding message",
			zap.String("id", msg.ID()),
			zap.Int("attempts", attempt),
		)
		msg.Ack()
		return
	}

	eventPayload, err := NewInterface(reflect.TypeOf(rt.ObjectType), msg.Data())
	if err != nil {
		mux.cfg.Logger.Error("Failed to unmarshal payload",
			zap.Error(err),
			zap.String("id", msg.ID()),
			zap.String("targetType", reflect.TypeOf(rt.ObjectType).String()),
		)
		msg.NotifyPreflightDone(err)
		msg.Nack()
		return
	}

	preflightChain := ChainMiddlewares(noopHandler, mux.preflightMiddlewares...)
	if _, preflightErr := preflightChain(context.Background(), eventPayload); preflightErr != nil {
		msg.NotifyPreflightDone(preflightErr)
		msg.Nack()
		return
	}

	msg.NotifyPreflightDone(nil)

	// Execute handler inline — runs in the broker SDK's goroutine.
	effectiveHandler := ChainMiddlewares(rt.Handler, mux.middlewares...)
	store := metadata.NewStore()
	store.Set("retrycount", strconv.Itoa(attempt))
	handlerCtx := metadata.NewContext(msg.Context(), store)

	data, err := effectiveHandler(handlerCtx, eventPayload)
	if err != nil {
		mux.cfg.Logger.Error("Handler execution failed",
			zap.Error(err),
			zap.String("id", msg.ID()),
		)
		msg.Nack()
		return
	}

	msg.Ack()

	if rt.Pub != nil && data != nil {
		mux.publishResult(msg, data, store, rt.Pub)
	}
}

func (mux *EventMux) publishResult(msg *event.Message, data json.RawMessage, store *metadata.Store, pub port.Publisher) {
	ce := cloudevents.New()
	ce.SetID(uuid.NewString())
	ce.SetSource(msg.Source())
	ce.SetType(msg.Type() + ".result")
	ce.SetTime(time.Now())
	_ = ce.SetData(cloudevents.ApplicationJSON, data)

	// Propagate handler metadata as extensions.
	for k, v := range store.All() {
		ce.SetExtension(k, v)
	}
	// Carry over original extensions not overridden by the handler.
	for k, v := range msg.Extensions() {
		if _, exists := store.All()[k]; !exists {
			ce.SetExtension(k, v)
		}
	}

	if err := pub.Publish(ce); err != nil {
		mux.cfg.Logger.Error("Failed to publish result",
			zap.Error(err),
			zap.String("id", msg.ID()),
		)
	}
}

func (mux *EventMux) Use(middlewares ...Middleware) {
	mux.middlewares = append(mux.middlewares, middlewares...)
}

func (mux *EventMux) UsePreflight(middlewares ...Middleware) {
	mux.preflightMiddlewares = append(mux.preflightMiddlewares, middlewares...)
}
