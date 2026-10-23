package eventhttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"sync/atomic"
	"time"
	"unicode/utf8"
	"uuid"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"

	"github.com/norlis/httpgate/logging"
	"github.com/norlis/httpgate/trace"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

const (
	// DefaultMaxBodyBytes bounds request bodies when SubscriberConfig.MaxBodyBytes is 0.
	DefaultMaxBodyBytes int64 = 256 << 10 // the SNS/SQS message limit; results republished there would fail above it
	// HeaderMessageID carries the CloudEvent id of a plain JSON request.
	HeaderMessageID = "X-Message-UUID"
	// HeaderCorrelationID carries the correlationid extension of a plain JSON request.
	HeaderCorrelationID = "X-Correlation-ID"

	mediaTypeJSON = "application/json"
)

// SubscriberConfig configures the HTTP subscriber.
type SubscriberConfig struct {
	// Pattern is the http.ServeMux pattern (e.g. "POST /command"). Required.
	Pattern string
	// DefaultType is the CloudEvent type given to plain JSON requests that
	// carry no CloudEvents headers. Required.
	DefaultType string
	// MaxBodyBytes caps the request body; 0 means DefaultMaxBodyBytes.
	MaxBodyBytes int64
	// ErrorRules map router errors to HTTP responses. They are evaluated
	// before the default rules, so they can override them.
	ErrorRules []ErrorRule
}

// Subscriber turns incoming HTTP requests into CloudEvents, hands them to the
// eventmux handler synchronously and answers with the router's outcome. It
// satisfies eventmux.Subscription.
type Subscriber struct {
	cfg            SubscriberConfig
	logger         *slog.Logger
	errorResponder *ErrorResponder
	handler        atomic.Pointer[func(msg *event.Message)]
}

// NewSubscriber validates cfg and registers the route on server. Requests
// arriving before Start, or after Stop, get 503.
func NewSubscriber(server *http.ServeMux, cfg SubscriberConfig, logger *slog.Logger) (*Subscriber, error) {
	switch {
	case server == nil:
		return nil, errors.New("eventhttp: server is nil")
	case cfg.Pattern == "":
		return nil, errors.New("eventhttp: SubscriberConfig.Pattern is empty")
	case cfg.DefaultType == "":
		return nil, errors.New("eventhttp: SubscriberConfig.DefaultType is empty")
	case cfg.MaxBodyBytes < 0:
		return nil, fmt.Errorf("eventhttp: SubscriberConfig.MaxBodyBytes is negative: %d", cfg.MaxBodyBytes)
	}
	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = DefaultMaxBodyBytes
	}
	s := &Subscriber{
		cfg:            cfg,
		logger:         logfields.Transport(logger, "eventhttp-subscriber", logfields.SystemHTTP, cfg.Pattern),
		errorResponder: NewErrorResponder(cfg.ErrorRules...),
	}
	server.HandleFunc(cfg.Pattern, func(w http.ResponseWriter, r *http.Request) {
		h := s.handler.Load()
		if h == nil {
			http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		s.Handler(*h)(w, r)
	})
	return s, nil
}

// Start wires handler and returns. A second call returns
// event.ErrAlreadyStarted. HTTP has no fatal runtime failure: onError is
// never called.
func (h *Subscriber) Start(_ context.Context, handler func(msg *event.Message), _ func(error)) error {
	if !h.handler.CompareAndSwap(nil, &handler) {
		return event.ErrAlreadyStarted
	}
	h.logger.Info("subscription started")
	return nil
}

// Stop unwires the handler: new requests get 503. In-flight requests are
// the http.Server's to drain (Shutdown), so Stop returns at once.
func (h *Subscriber) Stop(context.Context) error {
	if h.handler.Swap(nil) == nil {
		return nil
	}
	h.logger.Info("subscription stopped")
	return nil
}

// rejection is a request-level refusal decided before the router runs.
type rejection struct {
	status int
	code   ErrorCode
	detail string
	cause  error
}

// Handler builds the http.HandlerFunc bound to handler. Exposed so callers
// can wrap it with their own middlewares before registering on the ServeMux.
func (h *Subscriber) Handler(handler func(msg *event.Message)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, h.cfg.MaxBodyBytes)

		ce, rej := h.extractCloudEvent(r)
		if rej != nil {
			// Client input problems are expected behaviour, not system faults.
			h.logger.WarnContext(r.Context(), "request rejected", logging.Err(rej.cause),
				slog.String(logfields.KeyURLFull, r.URL.Path))
			h.respond(w, r, ce, rej.status, rej.code, rej.detail)
			return
		}

		// This transport is the trace entry point: inherit the inbound
		// traceparent or seed a new trace.
		// Nack is a no-op with meaning: the caller gets a 5xx and retries, so
		// the router treats retryable failures as such instead of definitive.
		msg := event.NewMessage(event.ContextWithTrace(r.Context(), r.Header.Get(trace.Header), &ce), ce, event.Delivery{Nack: func() error { return nil }})
		handler(msg)

		outcome, cause := msg.Outcome()
		switch outcome {
		case event.OutcomeAcked:
			h.respond(w, r, ce, http.StatusAccepted, "", "")
		case event.OutcomePending:
			// The router returned without deciding: a router bug, never a client error.
			h.logger.ErrorContext(msg.Context(), "router returned without outcome",
				slog.String(logfields.KeyMessagingMessageID, ce.ID()))
			h.respond(w, r, ce, http.StatusInternalServerError, CodeHandlerFailed, GenericFailureDetail)
		default:
			if h.errorResponder.Respond(w, r, cause, ce, h.cfg.Pattern) {
				return
			}
			// For HTTP a discarded event is as unprocessed as a nacked one.
			h.respond(w, r, ce, http.StatusInternalServerError, CodeHandlerFailed, GenericFailureDetail)
		}
	}
}

func (h *Subscriber) respond(w http.ResponseWriter, r *http.Request, ce cloudevents.Event, status int, code ErrorCode, detail string) {
	newResponse(ce, h.cfg.Pattern, status, code, detail).JSON(w, r)
}

// extractCloudEvent tries the CloudEvents SDK first (binary and structured
// modes), then falls back to a plain JSON request. Either way the returned
// event carries a validated correlationid. On rejection the event holds
// whatever identifiers were parsed so the response can echo them.
func (h *Subscriber) extractCloudEvent(r *http.Request) (cloudevents.Event, *rejection) {
	ce, err := cehttp.NewEventFromHTTPRequest(r)
	var tooLarge *http.MaxBytesError
	switch {
	case err == nil:
		// SDK path.
	case errors.As(err, &tooLarge):
		return cloudevents.New(), reject(r, http.StatusRequestEntityTooLarge, CodePayloadTooLarge, fmt.Sprintf("Body exceeds %d bytes", h.cfg.MaxBodyBytes), err)
	case isCloudEventsRequest(r):
		// The client meant to send a CloudEvent and it is malformed: do not
		// fall back to treating the envelope as a payload.
		return cloudevents.New(), reject(r, http.StatusBadRequest, CodeInvalidRequest, "Malformed CloudEvent", err)
	default:
		var rej *rejection
		if ce, rej = h.plainEvent(r); rej != nil {
			return *ce, rej
		}
	}

	if err := event.ValidateCorrelationID(ce.ID()); err != nil {
		return echoable(*ce, true), reject(r, http.StatusBadRequest, CodeInvalidRequest, "Invalid event id", err)
	}
	// The SDK accepts any bytes in type; the response (and every log) is JSON.
	if !utf8.ValidString(ce.Type()) {
		return echoable(*ce, false), reject(r, http.StatusBadRequest, CodeInvalidRequest, "Invalid event type", errors.New("type is not valid UTF-8"))
	}
	if cid := event.CorrelationID(*ce); cid == "" {
		// Same rule as the mux: the first event of a chain identifies it.
		ce.SetExtension(event.ExtCorrelationID, ce.ID())
	} else if err := event.ValidateCorrelationID(cid); err != nil {
		ce.SetExtension(event.ExtCorrelationID, "")
		return *ce, reject(r, http.StatusBadRequest, CodeInvalidCorrelationID, "Invalid correlation id", err)
	}
	return *ce, nil
}

// echoable returns a copy of ce safe to echo in a rejection: the SDK keeps
// an id or type it cannot validate (possibly not even UTF-8), so the copy
// drops the id, keeps the type only when asked, and keeps correlationid.
func echoable(ce cloudevents.Event, keepType bool) cloudevents.Event {
	clean := cloudevents.New()
	if keepType {
		clean.SetType(ce.Type())
	}
	if cid := event.CorrelationID(ce); cid != "" {
		clean.SetExtension(event.ExtCorrelationID, cid)
	}
	return clean
}

// isCloudEventsRequest reports whether the request declares CloudEvents
// binary (Ce-Specversion) or structured (application/cloudevents+json) mode.
func isCloudEventsRequest(r *http.Request) bool {
	if r.Header.Get("Ce-Specversion") != "" {
		return true
	}
	mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return mt == cloudevents.ApplicationCloudEventsJSON
}

// plainEvent builds a CloudEvent from a bare JSON body.
func (h *Subscriber) plainEvent(r *http.Request) (*cloudevents.Event, *rejection) {
	ce := cloudevents.New()
	ce.SetSpecVersion(cloudevents.CloudEventsVersionV1)
	ce.SetType(h.cfg.DefaultType)
	ce.SetSource(requestScheme(r) + "://" + r.Host + r.URL.Path)
	ce.SetTime(time.Now())

	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mt != mediaTypeJSON {
		return &ce, reject(r, http.StatusUnsupportedMediaType, CodeUnsupportedMediaType, "Content-Type must be application/json", err)
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		if _, tooLarge := errors.AsType[*http.MaxBytesError](err); tooLarge {
			return &ce, reject(r, http.StatusRequestEntityTooLarge, CodePayloadTooLarge, fmt.Sprintf("Body exceeds %d bytes", h.cfg.MaxBodyBytes), err)
		}
		return &ce, reject(r, http.StatusBadRequest, CodeInvalidRequest, "Unreadable body", err)
	}

	id := r.Header.Get(HeaderMessageID)
	if id == "" {
		id = uuid.NewV7().String()
	}
	ce.SetID(id)
	if cid := r.Header.Get(HeaderCorrelationID); cid != "" {
		ce.SetExtension(event.ExtCorrelationID, cid)
	}
	if err := ce.SetData(cloudevents.ApplicationJSON, body); err != nil {
		return &ce, reject(r, http.StatusBadRequest, CodeInvalidRequest, "Invalid body", err)
	}
	return &ce, nil
}

// requestScheme is the URL scheme the request arrived on: r.URL carries none
// on the server side, and r.Proto is the HTTP version, not the scheme.
func requestScheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func reject(r *http.Request, status int, code ErrorCode, detail string, cause error) *rejection {
	if cause == nil {
		cause = errors.New(detail)
	}
	return &rejection{status: status, code: code, detail: detail, cause: fmt.Errorf("%s %s: %w", r.Method, r.URL.Path, cause)}
}
