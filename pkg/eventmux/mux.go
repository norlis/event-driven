package eventmux

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"github.com/google/uuid"

	"github.com/norlis/event-driven/pkg/event"
	"github.com/norlis/event-driven/pkg/eventmux/metadata"
)

var noopHandler = func(ctx context.Context, data any) (json.RawMessage, error) { return nil, nil }

// HandlerFunc is the user-supplied processor for a route. data is the decoded
// payload (always of the type registered via Register). The optional
// json.RawMessage return becomes the body of the result event published when
// the Route has a Publisher attached.
type HandlerFunc func(ctx context.Context, data any) (json.RawMessage, error)

// Route binds a Filter, a typed Handler and an optional result Publisher.
// ObjectType drives the JSON decoding of the incoming event payload.
type Route struct {
	Pub        Publisher
	Filter     Filter
	Handler    HandlerFunc
	ObjectType any
}

// Config holds the Mux configuration.
type Config struct {
	Name            string // Identifies this mux in logs (e.g. "pubsub-orders", "http-commands").
	Subscription    Subscription
	Logger          *slog.Logger
	ReportOnNoMatch bool
}

// OnErrorFunc is called when RunBackground detects a fatal (non-cancellation) error.
type OnErrorFunc func(err error)

// Mux is the event router. It pulls messages from a Subscription, decodes
// them, dispatches to the first Route whose Filter matches, runs the handler
// through the registered middleware chain, and (optionally) republishes the
// result.
type Mux struct {
	cfg                  Config
	routes               []*Route
	middlewares          []Middleware
	preflightMiddlewares []Middleware
}

// New creates a new Mux for a given Subscription source (PubSub, HTTP, etc.).
func New(cfg Config) *Mux {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Mux{
		cfg: cfg,
	}
}

// Name returns the mux name for logging. Defaults to "eventmux" if not configured.
func (mux *Mux) Name() string {
	if mux.cfg.Name != "" {
		return mux.cfg.Name
	}
	return "eventmux"
}

// RunBackground starts the Mux in a background goroutine.
// parentCtx allows inheriting values (e.g. TraceID) or upstream cancellations.
// onError is called if mux.Run exits with a non-cancellation error (can be nil).
// Returns a stop function; pass a timeout to avoid infinite hangs.
func (mux *Mux) RunBackground(parentCtx context.Context, onError OnErrorFunc) (stop func(timeout time.Duration) error) {
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, cancel := context.WithCancelCause(parentCtx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		err := mux.Run(ctx)

		if err != nil && !errors.Is(err, context.Canceled) {
			mux.cfg.Logger.Error("Mux crashed",
				slog.Any("error", err),
				slog.String("name", mux.Name()),
				slog.Any("cause", context.Cause(ctx)),
			)
			if onError != nil {
				onError(err)
			}
			return
		}

		mux.cfg.Logger.Info("Mux stopped",
			slog.String("name", mux.Name()),
			slog.Any("cause", context.Cause(ctx)),
		)
	}()

	return func(timeout time.Duration) error {
		cancel(errors.New("shutdown requested"))
		select {
		case <-done:
			return nil
		case <-time.After(timeout):
			return fmt.Errorf("eventmux %s: stop timeout after %s", mux.Name(), timeout)
		}
	}
}

// Register adds a new route to the mux.
func (mux *Mux) Register(pub Publisher, filter Filter, objectType any, handler HandlerFunc) {
	mux.routes = append(mux.routes, &Route{
		Pub:        pub,
		Filter:     filter,
		Handler:    handler,
		ObjectType: objectType,
	})
	mux.cfg.Logger.Info("Route registered", slog.Int("totalRoutes", len(mux.routes)))
}

// Run starts the Subscription and processes all registered routes.
func (mux *Mux) Run(ctx context.Context) error {
	mux.cfg.Logger.Info("Mux starting subscription...")

	if err := mux.cfg.Subscription.Start(ctx, func(msg *event.Message) {
		mux.cfg.Logger.Debug("Mux received message",
			slog.String("id", msg.ID()),
			slog.String("type", msg.Type()),
			slog.String("source", msg.Source()),
		)

		matchingRoute := mux.findMatchingRoute(msg)
		if matchingRoute == nil {
			mux.handleNoRouteFound(msg)
			return
		}

		mux.cfg.Logger.Debug("Message matched filter, processing route...", slog.String("id", msg.ID()))
		mux.processAndHandle(msg, matchingRoute)
	}); err != nil {
		return fmt.Errorf("subscription start: %w", err)
	}
	return nil
}

func (mux *Mux) findMatchingRoute(msg *event.Message) *Route {
	for _, rt := range mux.routes {
		if rt.Filter == nil || rt.Filter.Match(msg) {
			return rt
		}
	}
	return nil
}

func (mux *Mux) handleNoRouteFound(msg *event.Message) {
	mux.cfg.Logger.Debug("Message did not match any route", slog.String("id", msg.ID()))
	if mux.cfg.ReportOnNoMatch {
		msg.NotifyPreflightDone(event.ErrNoRoute)
	}
	msg.Ack()
}

func (mux *Mux) processAndHandle(msg *event.Message, rt *Route) {
	eventPayload, err := decodeInto(reflect.TypeOf(rt.ObjectType), msg.Data())
	if err != nil {
		mux.cfg.Logger.Error("Failed to unmarshal payload",
			slog.Any("error", err),
			slog.String("id", msg.ID()),
			slog.String("targetType", reflect.TypeOf(rt.ObjectType).String()),
		)
		msg.NotifyPreflightDone(err)
		msg.Nack()
		return
	}

	preflightChain := Chain(noopHandler, mux.preflightMiddlewares...)
	if _, preflightErr := preflightChain(context.Background(), eventPayload); preflightErr != nil {
		msg.NotifyPreflightDone(preflightErr)
		msg.Nack()
		return
	}

	msg.NotifyPreflightDone(nil)

	// Execute handler inline — runs in the broker SDK's goroutine.
	effectiveHandler := Chain(rt.Handler, mux.middlewares...)
	store := metadata.NewStore()
	handlerCtx := metadata.NewContext(msg.Context(), store)

	data, err := effectiveHandler(handlerCtx, eventPayload)
	if err != nil {
		mux.cfg.Logger.Error("Handler execution failed",
			slog.Any("error", err),
			slog.String("id", msg.ID()),
		)

		// NonRetryableError → Ack (discard). Retrying won't fix it (e.g. validation, bad payload).
		// Retryable error → Nack. The broker will redeliver with its own backoff/DLQ policy.
		if _, ok := errors.AsType[*event.NonRetryableError](err); ok {
			mux.cfg.Logger.Warn("Non-retryable error, discarding message",
				slog.Any("error", err),
				slog.String("id", msg.ID()),
			)
			msg.Ack()
		} else {
			msg.Nack()
		}
		return
	}

	msg.Ack()

	if rt.Pub != nil && data != nil {
		mux.publishResult(msg, data, store, rt.Pub)
	}
}

func (mux *Mux) publishResult(msg *event.Message, data json.RawMessage, store *metadata.Store, pub Publisher) {
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
			slog.Any("error", err),
			slog.String("id", msg.ID()),
		)
	}
}

// Use registers middlewares that wrap every route's handler. They are applied
// in reverse order so the last Use() added is the outermost middleware.
func (mux *Mux) Use(middlewares ...Middleware) {
	mux.middlewares = append(mux.middlewares, middlewares...)
}

// UsePreflight registers middlewares run before the handler with a no-op
// terminal handler. Used to short-circuit on validation / authorization
// failures while still allowing the message to be Nacked.
func (mux *Mux) UsePreflight(middlewares ...Middleware) {
	mux.preflightMiddlewares = append(mux.preflightMiddlewares, middlewares...)
}
