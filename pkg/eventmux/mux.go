// Package eventmux is a transport-agnostic router that decodes incoming
// CloudEvents into a typed payload, matches them against registered routes
// (filter + handler), runs middleware chains, and optionally publishes a
// result event back to a Publisher. The transport (Pub/Sub, SQS, HTTP, …) is
// plugged in via the Subscription and Publisher interfaces.
package eventmux

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"
	"uuid"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/httpgate/logging"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/eventmux/metadata"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

// reservedExtensions cannot be set by handlers through metadata.
var reservedExtensions = []string{event.ExtCorrelationID, event.ExtCausationID, event.ExtDeadLetterReason, event.ExtDeadLetterError}

var noopHandler = func(ctx context.Context, data any) (jsontext.Value, error) { return nil, nil }

// route binds a Filter, the typed decode step, the handler and an optional
// result Publisher. chain is handler wrapped by the mux middlewares; Start
// fixes it once so no chain is rebuilt per message.
type route struct {
	pub     Publisher
	filter  Filter
	decode  func([]byte) (any, error)
	handler HandlerFunc
	chain   HandlerFunc
}

// Config holds the Mux configuration.
type Config struct {
	Name         string // Identifies this mux in logs (e.g. "pubsub-orders", "http-commands").
	Subscription Subscription
	Logger       *slog.Logger
	// MaxRetries is the number of redeliveries tolerated for a retryable
	// handler error before the message is dead-lettered: delivery MaxRetries+1
	// is the last one. 0 disables the limit. Only effective when the transport
	// reports Delivery.Count (JetStream, SQS, Pub/Sub with a dead-letter policy).
	// Keep MaxRetries+1 below the broker's own limit (JetStream MaxDeliver, SQS
	// maxReceiveCount, Pub/Sub max-delivery-attempts) or the broker removes the
	// message before the mux can dead-letter it.
	MaxRetries uint64
	// DeadLetter receives a copy of every definitively failed message before
	// it is discarded. Nil keeps the current behaviour: discard and log. When
	// the copy cannot be published with a retryable error, the message is
	// nacked for another attempt only while MaxRetries bounds the redelivery
	// (the transport reports Delivery.Count and it is still within the limit);
	// otherwise the copy is lost and the message discarded, so a
	// deterministic failure never loops forever on a broken dead-letter topic.
	DeadLetter Publisher
	// PublishAfterAck acknowledges the message before publishing the handler's
	// result, so a publish failure is only logged and the result is lost
	// (at-most-once results). Off by default: the result is published first and
	// a failure nacks or dead-letters the message, which requires an idempotent
	// handler because a redelivery runs it again.
	PublishAfterAck bool
	// Observer receives outcomes and publish results for metrics or tracing.
	// Nil disables it. See Observer for the contract.
	Observer Observer
}

// Mux is the event router. It pulls messages from a Subscription, decodes
// them, dispatches to the first Route whose Filter matches, runs the handler
// through the registered middleware chain, and (optionally) republishes the
// result.
type Mux struct {
	cfg                  Config
	routes               []*route
	middlewares          []Middleware
	preflightMiddlewares []Middleware
	warnNoDeliveryCount  sync.Once

	mu        sync.Mutex
	started   bool
	frozen    bool
	preflight HandlerFunc
}

// New creates a new Mux for a given Subscription source (PubSub, HTTP, etc.).
func New(cfg Config) (*Mux, error) {
	if cfg.Subscription == nil {
		return nil, errors.New("eventmux: Config.Subscription is nil")
	}
	mux := &Mux{cfg: cfg}
	mux.cfg.Logger = logfields.Component(cfg.Logger, "eventmux").With(slog.String(logfields.KeyMuxName, mux.Name()))
	if mux.cfg.Observer == nil {
		mux.cfg.Observer = noopObserver{}
	}
	return mux, nil
}

// Name returns the mux name for logging. Defaults to "eventmux" if not configured.
func (mux *Mux) Name() string {
	if mux.cfg.Name != "" {
		return mux.cfg.Name
	}
	return "eventmux"
}

// Handle registers a route: messages that pass filter (nil matches all) are
// decoded into T with encoding/json/v2 and handed to fn; when pub is not nil
// the returned value is published as a "<type>.result" event. Routes are
// tried in registration order and the first match wins, so registering the
// same filter twice panics: the second route would be unreachable.
func (mux *Mux) Handle[T any](pub Publisher, filter Filter, fn func(context.Context, T) (jsontext.Value, error)) {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	mux.mustBeOpen("Handle")
	for _, existing := range mux.routes {
		// The first matching route wins, so a second route with the same
		// filter could never run, whatever its publisher: a wiring bug.
		if identical(existing.filter, filter) {
			panic("eventmux: duplicate route")
		}
	}
	typeName := reflect.TypeFor[T]().String()
	rt := &route{
		pub:    pub,
		filter: filter,
		decode: func(data []byte) (any, error) {
			var v T
			if err := json.Unmarshal(data, &v); err != nil {
				return nil, fmt.Errorf("%w: unmarshal into %s: %w", event.ErrDecode, typeName, err)
			}
			return v, nil
		},
		handler: func(ctx context.Context, data any) (jsontext.Value, error) {
			v, ok := data.(T)
			if !ok {
				// Only a middleware that swapped the payload gets here: a bug,
				// so dead-letter instead of redelivering forever.
				return nil, event.NewNonRetryableError(fmt.Errorf("eventmux: route expects %s, got %T", typeName, data))
			}
			return fn(ctx, v)
		},
	}
	mux.routes = append(mux.routes, rt)
	mux.cfg.Logger.Info("route registered", slog.Int(logfields.KeyMuxRoutesTotal, len(mux.routes)))
}

// identical reports whether a and b are the same filter or publisher by
// interface identity. A func-typed or otherwise uncomparable implementation
// has no identity, so it is never a duplicate instead of panicking at ==.
func identical(a, b any) bool {
	if a == nil || b == nil {
		return a == b
	}
	va := reflect.ValueOf(a)
	return va.Comparable() && va.Equal(reflect.ValueOf(b))
}

// mustBeOpen panics when registration happens after the first Start: the
// route table and the chains are read without locks from then on.
func (mux *Mux) mustBeOpen(method string) {
	if mux.frozen {
		panic("eventmux: " + method + " after Start")
	}
}

// Start establishes the subscription and returns; a setup failure is
// returned synchronously. onError (may be nil) is called when the
// subscription later fails for good; the mux logs the failure either way. It
// must not block; calling Stop from it is safe. A second Start returns
// event.ErrAlreadyStarted.
func (mux *Mux) Start(ctx context.Context, onError func(error)) error {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	if mux.started {
		return event.ErrAlreadyStarted
	}
	// Built once: nothing can register after the first Start, and rebuilding
	// on a restart would race with a handler still running from before Stop.
	if !mux.frozen {
		mux.frozen = true
		mux.preflight = Chain(noopHandler, mux.preflightMiddlewares...)
		for _, rt := range mux.routes {
			rt.chain = Chain(rt.handler, mux.middlewares...)
		}
		mux.observePublishers()
	}
	if err := mux.cfg.Subscription.Start(ctx, mux.handle, mux.subscriptionFailed(onError)); err != nil {
		return fmt.Errorf("eventmux %s: start: %w", mux.Name(), err)
	}
	mux.started = true
	mux.cfg.Logger.Info("mux started")
	return nil
}

// observePublishers wraps every publisher the mux calls so OnPublish sees each
// attempt. A noop Observer leaves them bare: no decorator on the hot path.
func (mux *Mux) observePublishers() {
	if _, noop := mux.cfg.Observer.(noopObserver); noop {
		return
	}
	for _, rt := range mux.routes {
		if rt.pub != nil {
			rt.pub = observedPublisher{pub: rt.pub, mux: mux}
		}
	}
	if mux.cfg.DeadLetter != nil {
		mux.cfg.DeadLetter = observedPublisher{pub: mux.cfg.DeadLetter, mux: mux}
	}
}

// Stop ends delivery and waits for in-flight handlers until ctx expires.
// A mux that was never started, or already stopped, returns nil.
func (mux *Mux) Stop(ctx context.Context) error {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	if !mux.started {
		return nil
	}
	if err := mux.cfg.Subscription.Stop(ctx); err != nil {
		// Still started: the transport may keep delivering, so a later Stop
		// must retry instead of answering nil. INFO on purpose: the error is
		// returned, and the caller (fx, main) owns the ERROR record.
		mux.cfg.Logger.Info("mux stopped", logging.Err(err))
		return fmt.Errorf("eventmux %s: stop: %w", mux.Name(), err)
	}
	mux.started = false
	mux.cfg.Logger.Info("mux stopped")
	return nil
}

func (mux *Mux) subscriptionFailed(onError func(error)) func(error) {
	return func(err error) {
		mux.cfg.Logger.Error("subscription failed", logging.Err(err))
		if onError != nil {
			onError(err)
		}
	}
}

// handle is the per-message entry point handed to the Subscription.
func (mux *Mux) handle(msg *event.Message) {
	start := time.Now()
	// Filters and decoders are user code outside invoke's recover: a panic
	// there must settle the message, not unwind into the broker SDK.
	defer mux.recoverOutsideHandler(msg, start)
	// The first event of a chain identifies the chain: deterministic, so a
	// redelivery carries the same correlation id. An empty id (a transport
	// that skipped Defaults.ID) must not become an empty correlation id.
	if event.CorrelationID(msg.Event) == "" && msg.ID() != "" {
		msg.SetExtension(event.ExtCorrelationID, msg.ID())
	}
	// Guarded: at INFO the three attrs would still be built and boxed per message.
	if ctx := msg.Context(); mux.cfg.Logger.Enabled(ctx, slog.LevelDebug) {
		mux.cfg.Logger.LogAttrs(ctx, slog.LevelDebug, "message received",
			slog.String(logfields.KeyMessagingMessageID, msg.ID()),
			slog.String(logfields.KeyCloudEventsType, msg.Type()),
			slog.String(logfields.KeyCloudEventsSource, msg.Source()),
		)
	}

	if _, known := msg.DeliveryCount(); mux.cfg.MaxRetries > 0 && !known {
		mux.warnNoDeliveryCount.Do(func() {
			mux.cfg.Logger.Warn("MaxRetries set but transport reports no delivery count")
		})
	}

	// The transport could not decode the envelope: same policy as a payload
	// that does not decode into the route type, before any route is consulted.
	if err := msg.DecodeError(); err != nil {
		mux.fail(msg, start, event.ReasonDecode, err, nil)
		return
	}

	matchingRoute := mux.findMatchingRoute(msg)
	if matchingRoute == nil {
		mux.handleNoRouteFound(msg, start)
		return
	}
	mux.processAndHandle(msg, matchingRoute, start)
}

// settle applies the router's decision, logs once when the broker refuses it
// and reports the decision to the Observer: the one place every outcome passes.
func (mux *Mux) settle(msg *event.Message, outcome logfields.Outcome, reason event.DeadLetterReason, start time.Time, decide func() error) {
	if err := decide(); err != nil {
		mux.cfg.Logger.ErrorContext(msg.Context(), "message ack failed",
			logging.Err(err),
			slog.String(logfields.KeyMessagingMessageID, msg.ID()),
			correlationAttr(msg),
			outcome.Attr(),
		)
	}
	ctx := msg.Context()
	mux.observe(ctx, func() { mux.cfg.Observer.OnMessage(ctx, msg, outcome, reason, time.Since(start)) })
}

func (mux *Mux) findMatchingRoute(msg *event.Message) *route {
	for _, rt := range mux.routes {
		if rt.filter == nil || rt.filter.Match(msg) {
			return rt
		}
	}
	return nil
}

// handleNoRouteFound discards the message: redelivery cannot make a route
// appear. INFO because a silent drop in production must be visible.
func (mux *Mux) handleNoRouteFound(msg *event.Message, start time.Time) {
	mux.cfg.Logger.LogAttrs(msg.Context(), slog.LevelInfo, "no route matched",
		slog.String(logfields.KeyMessagingMessageID, msg.ID()),
		slog.String(logfields.KeyCloudEventsType, msg.Type()),
		correlationAttr(msg),
		logfields.OutcomeDiscarded.Attr(),
	)
	mux.settle(msg, logfields.OutcomeDiscarded, "", start, func() error { return msg.Discard(event.ErrNoRoute) })
}

// correlationAttr is an empty Attr (ignored by slog handlers) when the
// message carries no correlationid.
func correlationAttr(msg *event.Message) slog.Attr {
	id := event.CorrelationID(msg.Event)
	if id == "" {
		return slog.Attr{}
	}
	return slog.String(logfields.KeyCorrelationID, id)
}

func (mux *Mux) processAndHandle(msg *event.Message, rt *route, start time.Time) {
	payload, err := rt.decode(msg.Data())
	if err != nil {
		mux.fail(msg, start, event.ReasonDecode, err, nil)
		return
	}

	// Runs inline in the broker SDK's goroutine. The metadata store only
	// feeds the result event's extensions, so a terminal route gets none.
	handlerCtx := msg.Context()
	var store *metadata.Store
	if rt.pub != nil {
		store = metadata.NewStore()
		handlerCtx = metadata.NewContext(handlerCtx, store)
	}
	data, err := mux.invoke(handlerCtx, rt, payload)
	if err != nil {
		stack, cause := unwrapFailure(err)
		mux.fail(msg, start, mux.classify(msg, err), cause, stack)
		return
	}

	if !mux.cfg.PublishAfterAck && rt.pub != nil && data != nil {
		if err := mux.publishResult(handlerCtx, msg, data, store, rt.pub); err != nil {
			mux.fail(msg, start, mux.classifyPublish(msg, err), fmt.Errorf("%w: %w", event.ErrPublicationFailed, err), nil)
			return
		}
	}

	mux.settle(msg, logfields.OutcomeAcked, "", start, msg.Ack)

	// The single per-message completion log (§2.5 rule 2). LogAttrs: the
	// variadic ...any form boxes every attr even when the record is dropped.
	mux.cfg.Logger.LogAttrs(handlerCtx, slog.LevelInfo, "message processed",
		slog.String(logfields.KeyMessagingMessageID, msg.ID()),
		correlationAttr(msg),
		deliveryAttr(msg),
		logfields.OutcomeAcked.Attr(),
		slog.Int64(logging.KeyEventDuration, time.Since(start).Nanoseconds()),
	)

	if mux.cfg.PublishAfterAck && rt.pub != nil && data != nil {
		// The Ack already cancelled the per-message context; WithoutCancel keeps
		// its values (trace, metadata) without the cancellation. In the default
		// order the message is still undecided, so a drain deadline reaching the
		// publisher is wanted: the message is nacked and redelivered.
		if err := mux.publishResult(context.WithoutCancel(handlerCtx), msg, data, store, rt.pub); err != nil {
			mux.cfg.Logger.ErrorContext(handlerCtx, "result publish failed", logging.Err(err),
				slog.String(logfields.KeyMessagingMessageID, msg.ID()), correlationAttr(msg))
		}
	}
}

// publishResult builds and publishes the result event. A data value that
// cannot be encoded is NonRetryable: a redelivery produces the same bytes.
func (mux *Mux) publishResult(ctx context.Context, msg *event.Message, data jsontext.Value, store *metadata.Store, pub Publisher) error {
	ce := cloudevents.New()
	// v7 is time-ordered, so result events sort by creation in logs and stores.
	ce.SetID(uuid.NewV7().String())
	ce.SetSource(msg.Source())
	ce.SetType(msg.Type() + ".result")
	ce.SetTime(time.Now())
	if err := ce.SetData(cloudevents.ApplicationJSON, data); err != nil {
		return event.NewNonRetryableError(fmt.Errorf("encode result data: %w", err))
	}

	// Propagate handler metadata as extensions. Reserved extensions belong to
	// the originating client or to the mux, never to business code.
	meta := store.All()
	for _, k := range reservedExtensions {
		if _, hijack := meta[k]; hijack {
			mux.cfg.Logger.WarnContext(ctx, "reserved extension ignored",
				slog.String(logfields.KeyCloudEventsExtension, k),
				slog.String(logfields.KeyMessagingMessageID, msg.ID()),
				correlationAttr(msg))
			delete(meta, k)
		}
	}
	for k, v := range meta {
		ce.SetExtension(k, v)
	}
	// Carry over original extensions not overridden by the handler. A copy
	// re-injected from the dead-letter topic must not mark its result as
	// failed, and the parent's causationid is the grandparent, not ours.
	for k, v := range event.Extensions(msg.Event) {
		if _, exists := meta[k]; exists || k == event.ExtCausationID || k == event.ExtDeadLetterReason || k == event.ExtDeadLetterError {
			continue
		}
		ce.SetExtension(k, v)
	}
	if msg.ID() != "" {
		ce.SetExtension(event.ExtCausationID, msg.ID())
	}

	//nolint:wrapcheck // the caller classifies the raw error and wraps it when it fails the message
	return pub.Publish(ctx, ce)
}

// Use appends middlewares to every route's handler chain. The first one
// registered is the outermost: Use(A, B) runs A, then B, then the handler.
// Panics after Start.
func (mux *Mux) Use(middlewares ...Middleware) {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	mux.mustBeOpen("Use")
	mux.middlewares = append(mux.middlewares, middlewares...)
}

// UsePreflight registers middlewares run before the handler with a no-op
// terminal handler. Used to short-circuit on validation / authorization
// failures while still allowing the message to be Nacked. Panics after Start.
func (mux *Mux) UsePreflight(middlewares ...Middleware) {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	mux.mustBeOpen("UsePreflight")
	mux.preflightMiddlewares = append(mux.preflightMiddlewares, middlewares...)
}
