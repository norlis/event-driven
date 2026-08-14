package eventhttp

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/google/uuid"

	"github.com/norlis/httpgate/logging"
	"github.com/norlis/httpgate/trace"

	"github.com/norlis/event-driven/pkg/event"
	"github.com/norlis/event-driven/pkg/eventmux"
	"github.com/norlis/event-driven/pkg/kit/logfields"
)

// SubscriberConfig configures the HTTP subscriber. Pattern is the
// http.ServeMux pattern (e.g. "POST /command") the subscriber registers.
type SubscriberConfig struct {
	Pattern string
	Logger  *slog.Logger
}

// Subscriber turns incoming HTTP requests into CloudEvents and forwards them
// to the eventmux handler. It satisfies eventmux.Subscription.
type Subscriber struct {
	server         *http.ServeMux
	config         SubscriberConfig
	logger         *slog.Logger
	errorResponder *ErrorResponder
}

// NewSubscriber registers the HTTP handler under cfg.Pattern.
func NewSubscriber(server *http.ServeMux, cfg SubscriberConfig) (eventmux.Subscription, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	logger = logger.With(
		slog.String(logfields.KeyLogLogger, "eventhttp-subscriber"),
		slog.String(logfields.KeyMessagingSystem, logfields.SystemHTTP),
		slog.String(logfields.KeyMessagingDestination, cfg.Pattern),
	)
	return &Subscriber{
		server:         server,
		config:         cfg,
		logger:         logger,
		errorResponder: NewErrorResponder(),
	}, nil
}

// Handler builds the http.HandlerFunc bound to handler. Exposed so callers
// can wrap it with their own middlewares before registering on the ServeMux.
func (h *Subscriber) Handler(handler func(msg *event.Message)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ce, err := h.extractCloudEvent(r)
		if err != nil {
			// Malformed client input is expected behavior, not a system fault.
			h.logger.WarnContext(r.Context(), "cloudevent extraction failed", logging.Err(err))
			messageID := uuid.NewString()
			NewResponseBuilder().
				WithID(messageID, uuid.New().String(), uuid.New().String()).
				WithInstance(r.Pattern).
				WithError("Failed to read request body", "001").
				WithStatus(http.StatusInternalServerError).
				Build().
				JSON(w, r)
			return
		}

		messageID := ce.ID()
		// This transport is the trace entry point: extract traceparent from
		// the inbound request, or seed a new trace when absent.
		msgCtx := event.ContextWithTrace(r.Context(), r.Header.Get(trace.Header), ce)
		msg := event.NewMessageWithParent(msgCtx, *ce, nil, nil)
		preflightResultChan := make(chan error, 1)
		msg.SetPreflightCallback(func(err error) {
			preflightResultChan <- err
		})

		handler(msg)

		select {
		case err = <-preflightResultChan:
			if err != nil {
				if h.errorResponder.Respond(w, r, err, messageID) {
					return
				}
			}
		case <-time.After(5 * time.Second):
			h.logger.ErrorContext(msgCtx, "preflight result timed out",
				slog.String(logfields.KeyMessagingMessageID, messageID))
			NewResponseBuilder().
				WithID(messageID, uuid.New().String(), uuid.New().String()).
				WithError("Internal processing timeout", "TIMEOUT").
				WithStatus(http.StatusInternalServerError).
				Build().JSON(w, r)
			return
		}

		NewResponseBuilder().
			WithID(messageID, uuid.New().String(), uuid.New().String()).
			Build().JSON(w, r)
	}
}

// extractCloudEvent tries the CloudEvents SDK first (supports binary + structured content modes),
// then falls back to building a CloudEvent manually for plain HTTP clients.
func (h *Subscriber) extractCloudEvent(r *http.Request) (*cloudevents.Event, error) {
	// Try CloudEvents SDK: handles both binary (Ce-* headers) and structured
	// (Content-Type: application/cloudevents+json) modes automatically.
	if ce, err := cehttp.NewEventFromHTTPRequest(r); err == nil {
		return ce, nil
	}

	// Fallback: plain HTTP request without CloudEvents headers.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(r.Body)

	messageID := r.Header.Get("X-Message-UUID")
	if messageID == "" {
		messageID = uuid.NewString()
	}

	ce := cloudevents.New()
	ce.SetID(messageID)
	ce.SetSpecVersion("1.0")
	ce.SetType("com.example.http.command")
	ce.SetSource(fmt.Sprintf("%s//%s%s", r.Proto, r.Host, r.URL.Path))
	ce.SetTime(time.Now())
	_ = ce.SetData(cloudevents.ApplicationJSON, body)

	return &ce, nil
}

// Start registers the HTTP handler under the configured pattern and blocks
// until ctx is cancelled. Blocking matches the contract of other transports
// (Pub/Sub, NATS) and lets the parent Mux treat HTTP routes as long-lived
// instead of reporting them as "stopped" immediately after Start returns.
func (h *Subscriber) Start(ctx context.Context, handler func(msg *event.Message)) error {
	h.logger.Info("subscription started")
	h.server.HandleFunc(h.config.Pattern, h.Handler(handler))
	<-ctx.Done()
	h.logger.Info("subscription stopped")
	return nil
}
